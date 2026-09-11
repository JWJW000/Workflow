use driver_drission::{BrowserDriver, DriverError};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use workflow_artifacts::{ArtifactError, ArtifactStore};
use workflow_core::{RunRecord, RunStatus, StepRunRecord, StepStatus};
use workflow_schema::{BrowserDefinition, RetryPolicy, Step, WorkflowDocument, WorkflowIr};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerRequest {
    pub run_id: String,
    pub workflow_source: String,
    pub inputs: Value,
    pub artifact_root: PathBuf,
    #[serde(default)]
    pub control_path: Option<PathBuf>,
    #[serde(default)]
    pub completed_steps: Vec<String>,
    #[serde(default)]
    pub initial_context: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunnerControl {
    Running,
    Paused,
    Cancelled,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RunnerResponse {
    pub run: Option<RunRecord>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RunnerOutput {
    Event { event_type: String, payload: Value },
    Result { response: Box<RunnerResponse> },
}

pub fn execute_runner_request(request: RunnerRequest) -> RunnerResponse {
    execute_runner_request_with_events(request, |_, _| {})
}

pub fn execute_runner_request_with_events<F>(
    request: RunnerRequest,
    mut events: F,
) -> RunnerResponse
where
    F: FnMut(&str, Value),
{
    let registry = workflow_actions::ActionRegistry::built_in();
    let ir = match workflow_schema::compile(&request.workflow_source, &registry) {
        Ok(ir) => ir,
        Err(error) => {
            return RunnerResponse {
                run: None,
                error_code: Some("WORKFLOW_VALIDATION_ERROR".into()),
                error_message: Some(error.to_string()),
            };
        }
    };
    let artifacts = match ArtifactStore::new(request.artifact_root) {
        Ok(artifacts) => artifacts,
        Err(error) => {
            return RunnerResponse {
                run: None,
                error_code: Some("ARTIFACT_ERROR".into()),
                error_message: Some(error.to_string()),
            };
        }
    };
    let mut engine = Engine::new(driver_drission::DrissionDriver::new(), artifacts);
    let control_path = request.control_path;
    RunnerResponse {
        run: Some(engine.execute_resume_with_control_and_events(
            request.run_id,
            &ir,
            request.inputs,
            &request.completed_steps,
            request.initial_context,
            move || check_control(control_path.as_deref()),
            &mut events,
        )),
        error_code: None,
        error_message: None,
    }
}

pub fn execute_single_step(
    action: &str,
    input: Value,
    mock_context: Option<Value>,
    artifact_root: Option<PathBuf>,
) -> Result<Value, EngineError> {
    let root = artifact_root
        .unwrap_or_else(|| std::env::temp_dir().join(format!("dry-run-{}", std::process::id())));
    let artifacts = ArtifactStore::new(&root)?;
    let mut driver = driver_drission::DrissionDriver::new();

    if action.starts_with("page.")
        || action.starts_with("element.")
        || action.starts_with("browser.")
    {
        let _ = driver.launch(&BrowserDefinition::default());
    }

    let mut engine = Engine::new(driver, artifacts);
    let mut context = Context::new(mock_context.unwrap_or_else(|| json!({})));
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut noop_control = || Ok(());

    let mut step_input = BTreeMap::new();
    if let Some(map) = input.as_object() {
        for (k, v) in map {
            step_input.insert(k.clone(), v.clone());
        }
    }

    let step = Step {
        id: "test_step".into(),
        name: None,
        action: Some(action.into()),
        condition: None,
        timeout: None,
        retry: None,
        foreach: None,
        repeat: None,
        on_error: None,
        save_as: None,
        uses: None,
        input: step_input,
    };

    engine.execute_action(&step, &mut context, deadline, &mut noop_control)
}

fn check_control(path: Option<&std::path::Path>) -> Result<(), EngineError> {
    let Some(path) = path else {
        return Ok(());
    };
    loop {
        let control = match std::fs::read(path) {
            Ok(bytes) => serde_json::from_slice::<RunnerControl>(&bytes)
                .map_err(|error| EngineError::Control(format!("invalid control file: {error}")))?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => RunnerControl::Running,
            Err(error) => return Err(EngineError::Control(error.to_string())),
        };
        match control {
            RunnerControl::Running => return Ok(()),
            RunnerControl::Cancelled => return Err(EngineError::Control("cancelled".into())),
            RunnerControl::Paused => thread::sleep(Duration::from_millis(100)),
        }
    }
}

#[derive(Debug, Error)]
pub enum EngineError {
    #[error("invalid inputs: {0}")]
    InvalidInputs(String),
    #[error("expression error: {0}")]
    Expression(String),
    #[error("action failed [{code}]: {message}")]
    Action {
        code: String,
        message: String,
        retryable: bool,
    },
    #[error("runner control failed: {0}")]
    Control(String),
    #[error(transparent)]
    Artifact(#[from] ArtifactError),
}

impl EngineError {
    pub fn code(&self) -> &str {
        match self {
            Self::InvalidInputs(_) => "INPUT_VALIDATION_ERROR",
            Self::Expression(_) => "EXPRESSION_ERROR",
            Self::Action { code, .. } => code,
            Self::Control(message) if message == "cancelled" => "CANCELLED",
            Self::Control(_) => "RUNNER_CONTROL_ERROR",
            Self::Artifact(ArtifactError::PathViolation(_)) => "OUTPUT_PATH_VIOLATION",
            Self::Artifact(_) => "ARTIFACT_ERROR",
        }
    }

    fn retryable(&self) -> bool {
        matches!(
            self,
            Self::Action {
                retryable: true,
                ..
            }
        )
    }
}

pub struct Engine<D> {
    driver: D,
    artifacts: ArtifactStore,
}

impl<D: BrowserDriver> Engine<D> {
    pub fn new(driver: D, artifacts: ArtifactStore) -> Self {
        Self { driver, artifacts }
    }

    pub fn execute(&mut self, run_id: String, ir: &WorkflowIr, inputs: Value) -> RunRecord {
        self.execute_with_control(run_id, ir, inputs, || Ok(()))
    }

    pub fn execute_with_control<F>(
        &mut self,
        run_id: String,
        ir: &WorkflowIr,
        inputs: Value,
        control: F,
    ) -> RunRecord
    where
        F: FnMut() -> Result<(), EngineError>,
    {
        self.execute_with_control_and_events(run_id, ir, inputs, control, &mut |_, _| {})
    }

    pub fn execute_with_control_and_events<F, E>(
        &mut self,
        run_id: String,
        ir: &WorkflowIr,
        inputs: Value,
        control: F,
        events: &mut E,
    ) -> RunRecord
    where
        F: FnMut() -> Result<(), EngineError>,
        E: FnMut(&str, Value),
    {
        self.execute_resume_with_control_and_events(run_id, ir, inputs, &[], None, control, events)
    }

    pub fn execute_resume_with_control_and_events<F, E>(
        &mut self,
        run_id: String,
        ir: &WorkflowIr,
        inputs: Value,
        completed_steps: &[String],
        initial_context: Option<Value>,
        mut control: F,
        events: &mut E,
    ) -> RunRecord
    where
        F: FnMut() -> Result<(), EngineError>,
        E: FnMut(&str, Value),
    {
        let mut merged_inputs = match inputs {
            Value::Object(map) => map,
            _ => serde_json::Map::new(),
        };
        for (name, def) in &ir.document.inputs {
            if !merged_inputs.contains_key(name) {
                if let Some(def_val) = &def.default {
                    merged_inputs.insert(name.clone(), def_val.clone());
                }
            }
        }
        let final_inputs = Value::Object(merged_inputs);

        let mut run = RunRecord {
            id: run_id,
            workflow_name: ir.name.clone(),
            workflow_hash: ir.content_hash.clone(),
            workflow_id: None,
            workflow_version_id: None,
            run_mode: "legacy".into(),
            source_snapshot: None,
            draft_revision: None,
            parent_run_id: None,
            resume_checkpoint_id: None,
            status: RunStatus::Preparing,
            inputs: final_inputs.clone(),
            outputs: None,
            error_code: None,
            error_message: None,
            steps: Vec::new(),
        };
        let mut context = Context::new(final_inputs);
        if let Some(init) = initial_context {
            if let Some(steps_map) = init.get("steps").and_then(Value::as_object) {
                for (k, v) in steps_map {
                    context.steps.insert(k.clone(), v.clone());
                }
            }
            if let Some(vars_map) = init.get("vars").and_then(Value::as_object) {
                for (k, v) in vars_map {
                    context.vars.insert(k.clone(), v.clone());
                }
            }
        }
        let completed_set: std::collections::BTreeSet<String> =
            completed_steps.iter().cloned().collect();
        let result = validate_inputs(&ir.document, &context.inputs)
            .and_then(|_| self.ensure_browser(&ir.document))
            .and_then(|_| {
                self.execute_steps_with_skip(
                    &ir.document.steps,
                    &ir.document,
                    &mut context,
                    &mut run,
                    "",
                    &completed_set,
                    &mut control,
                    events,
                )
            })
            .and_then(|_| evaluate_object(&ir.document.outputs, &context));
        match result {
            Ok(outputs) => {
                run.outputs = Some(outputs);
                run.status = RunStatus::Succeeded;
            }
            Err(error) => {
                run.status = if error.code() == "CANCELLED" {
                    RunStatus::Cancelled
                } else {
                    RunStatus::Failed
                };
                run.error_code = Some(error.code().into());
                run.error_message = Some(error.to_string());
                // 失败和取消时始终清理浏览器；成功时是否关闭由显式 browser.close 节点决定。
                let _ = self.driver.close();
            }
        }
        run
    }

    fn ensure_browser(&mut self, document: &WorkflowDocument) -> Result<(), EngineError> {
        let needs_browser = document.steps.iter().any(step_needs_browser);
        if !needs_browser {
            return Ok(());
        }
        let config = document.browser.clone().unwrap_or(BrowserDefinition {
            mode: "launch".into(),
            endpoint: None,
            headless: false,
            profile: Some("ephemeral".into()),
            download_dir: None,
            chrome_path: None,
            user_agent: None,
            proxy: None,
            no_images: false,
            args: vec![],
        });
        self.driver
            .launch(&config)
            .map(|_| ())
            .map_err(driver_error)
    }

    #[allow(clippy::too_many_arguments)]
    fn execute_steps_with_skip(
        &mut self,
        steps: &[Step],
        document: &WorkflowDocument,
        context: &mut Context,
        run: &mut RunRecord,
        prefix: &str,
        skip_steps: &std::collections::BTreeSet<String>,
        control: &mut impl FnMut() -> Result<(), EngineError>,
        events: &mut impl FnMut(&str, Value),
    ) -> Result<(), EngineError> {
        run.status = RunStatus::Running;
        for step in steps {
            control()?;
            let step_id = if prefix.is_empty() {
                step.id.clone()
            } else {
                format!("{prefix}.{}", step.id)
            };

            // If step was already successfully executed in previous run (checkpoint resume), reuse output
            if skip_steps.contains(&step_id) {
                let cached_output = context.steps.get(&step.id).cloned();
                run.steps.push(StepRunRecord {
                    step_id: step_id.clone(),
                    attempt: 1,
                    status: StepStatus::Succeeded,
                    output: cached_output,
                    error_code: None,
                    error_message: None,
                });
                events("step.resumed", json!({"stepId": step_id, "attempt": 1}));
                continue;
            }

            if let Some(condition) = &step.condition
                && !truthy(&evaluate_string(condition, context)?)
            {
                run.steps.push(StepRunRecord {
                    step_id: step_id.clone(),
                    attempt: 1,
                    status: StepStatus::Skipped,
                    output: None,
                    error_code: None,
                    error_message: None,
                });
                events("step.skipped", json!({"stepId": step_id, "attempt": 1}));
                continue;
            }
            if let Some(foreach) = &step.foreach {
                let items = evaluate_value(&foreach.items, context)?;
                let items = items.as_array().ok_or_else(|| {
                    EngineError::Expression(format!("foreach {} items must be an array", step.id))
                })?;
                for (index, item) in items.iter().take(foreach.max_items as usize).enumerate() {
                    context
                        .locals
                        .insert(foreach.item_name.clone(), item.clone());
                    context
                        .locals
                        .insert("loop".into(), json!({ "index": index }));
                    self.execute_steps_with_skip(
                        &foreach.steps,
                        document,
                        context,
                        run,
                        &format!("{step_id}[{index}]"),
                        skip_steps,
                        control,
                        events,
                    )?;
                }
                context.locals.remove(&foreach.item_name);
                context.locals.remove("loop");
                continue;
            }
            if let Some(repeat) = &step.repeat {
                let mut completed = false;
                for index in 0..repeat.max_iterations {
                    context
                        .locals
                        .insert("loop".into(), json!({ "index": index }));
                    self.execute_steps_with_skip(
                        &repeat.steps,
                        document,
                        context,
                        run,
                        &format!("{step_id}[{index}]"),
                        skip_steps,
                        control,
                        events,
                    )?;
                    if truthy(&evaluate_string(&repeat.until, context)?) {
                        completed = true;
                        break;
                    }
                }
                context.locals.remove("loop");
                if !completed {
                    return Err(EngineError::Action {
                        code: "LOOP_LIMIT_REACHED".into(),
                        message: format!(
                            "repeat {} did not satisfy until within {} iterations",
                            step.id, repeat.max_iterations
                        ),
                        retryable: false,
                    });
                }
                continue;
            }
            let policy = step.retry.as_ref().or(document.defaults.retry.as_ref());
            let timeout = effective_timeout(step, document);
            let attempts = policy.map(|value| value.max_attempts).unwrap_or(1).max(1);
            let mut last_error = None;
            for attempt in 1..=attempts {
                control()?;
                let record_index = run.steps.len();
                run.steps.push(StepRunRecord {
                    step_id: step_id.clone(),
                    attempt,
                    status: StepStatus::Running,
                    output: None,
                    error_code: None,
                    error_message: None,
                });
                events(
                    "step.started",
                    json!({"stepId": step_id, "attempt": attempt}),
                );
                let started = Instant::now();
                let deadline = started.checked_add(timeout).unwrap_or(started);
                match self.execute_action(step, context, deadline, control) {
                    Ok(output) => {
                        if started.elapsed() > timeout {
                            let error = timeout_error(&step_id, timeout);
                            run.steps[record_index].status = StepStatus::Failed;
                            run.steps[record_index].error_code = Some(error.code().into());
                            run.steps[record_index].error_message = Some(error.to_string());
                            events(
                                "step.failed",
                                json!({"stepId": step_id, "attempt": attempt, "errorCode": "STEP_TIMEOUT", "errorMessage": error.to_string()}),
                            );
                            last_error = Some(error);
                            break;
                        }
                        context.steps.insert(step.id.clone(), output.clone());
                        if let Some(name) = &step.save_as {
                            context.vars.insert(name.clone(), output.clone());
                        }
                        run.steps[record_index].status = StepStatus::Succeeded;
                        run.steps[record_index].output = Some(output.clone());
                        events(
                            "step.succeeded",
                            json!({"stepId": step_id, "attempt": attempt, "output": output}),
                        );
                        last_error = None;
                        break;
                    }
                    Err(error) => {
                        run.steps[record_index].status = StepStatus::Failed;
                        run.steps[record_index].error_code = Some(error.code().into());
                        run.steps[record_index].error_message = Some(error.to_string());
                        let may_retry = attempt < attempts && retry_matches(policy, &error);
                        events(
                            "step.failed",
                            json!({"stepId": step_id, "attempt": attempt, "errorCode": error.code(), "errorMessage": error.to_string()}),
                        );
                        last_error = Some(error);
                        if may_retry {
                            let delay = backoff_duration(policy, attempt);
                            events(
                                "step.retry_wait",
                                json!({"stepId": step_id, "attempt": attempt, "delayMs": delay.as_millis()}),
                            );
                            interruptible_sleep(delay, None, control)?;
                        } else {
                            break;
                        }
                    }
                }
            }
            if let Some(error) = last_error {
                if step.on_error.as_deref() == Some("continue") {
                    // Do not leak the previous loop item's extract/download output
                    // into the next journal or article.
                    context.steps.insert(step.id.clone(), json!([]));
                    continue;
                }
                return Err(error);
            }
        }
        Ok(())
    }

    fn execute_action(
        &mut self,
        step: &Step,
        context: &Context,
        deadline: Instant,
        control: &mut impl FnMut() -> Result<(), EngineError>,
    ) -> Result<Value, EngineError> {
        let action = step.action.as_deref().ok_or_else(|| EngineError::Action {
            code: "UNSUPPORTED_STEP".into(),
            message: "sub-workflows are not implemented".into(),
            retryable: false,
        })?;
        let input = if action == "data.filter" {
            let mut input = evaluate_object(&step.input, context).or_else(|error| {
                if step.input.contains_key("condition") {
                    let mut values = step.input.clone();
                    values.remove("condition");
                    evaluate_object(&values, context)
                } else {
                    Err(error)
                }
            })?;
            if let Some(condition) = step.input.get("condition") {
                input
                    .as_object_mut()
                    .expect("evaluated step input is an object")
                    .insert("condition".into(), condition.clone());
            }
            input
        } else {
            evaluate_object(&step.input, context)?
        };
        match action {
            "browser.launch" => self
                .driver
                .launch(&browser_from_input(&input)?)
                .map_err(driver_error),
            "browser.close" => self.driver.close().map_err(driver_error),
            "page.tab.new" => {
                let url = input.get("url").and_then(Value::as_str);
                self.driver.new_tab(url).map_err(driver_error)
            }
            "page.tab.switch" => {
                let index = input
                    .get("index")
                    .and_then(Value::as_u64)
                    .map(|v| v as usize);
                let title = input.get("title").and_then(Value::as_str);
                self.driver.switch_tab(index, title).map_err(driver_error)
            }
            "page.tab.close" => self.driver.close_tab().map_err(driver_error),
            "browser.setCookie" => {
                let name = required_str(&input, "name")?;
                let value = required_str(&input, "value")?;
                let domain = input.get("domain").and_then(Value::as_str);
                let path = input.get("path").and_then(Value::as_str);
                self.driver
                    .set_cookie(name, value, domain, path)
                    .map_err(driver_error)
            }
            "browser.getCookies" => self.driver.get_cookies().map_err(driver_error),
            "browser.injectStealth" => self.driver.inject_stealth().map_err(driver_error),
            "network.waitForResponse" => {
                let pattern = required_str(&input, "urlPattern")?;
                let timeout = parse_duration(
                    input
                        .get("timeout")
                        .and_then(Value::as_str)
                        .unwrap_or("15s"),
                );
                interruptible_sleep(
                    timeout.min(Duration::from_millis(100)),
                    Some(deadline),
                    control,
                )?;
                Ok(json!({ "matched": true, "urlPattern": pattern, "status": 200, "body": {} }))
            }
            "page.goto" => {
                let url = normalize_navigation_url(required_str(&input, "url")?)?;
                self.driver.goto(&url).map_err(driver_error)
            }
            "page.reload" => self.driver.reload().map_err(driver_error),
            "page.back" => self.driver.back().map_err(driver_error),
            "page.forward" => self.driver.forward().map_err(driver_error),
            "page.wait" => {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    return Err(timeout_error(&step.id, Duration::ZERO));
                }
                self.execute_with_locator_candidates(&input, |driver, locator| {
                    driver.wait(
                        locator,
                        parse_duration(
                            input
                                .get("timeout")
                                .and_then(Value::as_str)
                                .unwrap_or("15s"),
                        )
                        .min(remaining),
                    )
                })
            }
            "page.screenshot" => {
                let path = input
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or("screenshot.png");
                let resolved = self.artifacts.resolve(path)?;
                if let Some(parent) = resolved.parent() {
                    let _ = std::fs::create_dir_all(parent);
                }
                self.driver
                    .screenshot(&resolved.to_string_lossy())
                    .map_err(driver_error)?;
                let metadata = self.artifacts.metadata(path, "image/png")?;
                Ok(
                    json!({ "artifact": metadata.relative_path, "byteCount": metadata.byte_count, "sha256": metadata.sha256 }),
                )
            }
            "download.click" => {
                let raw_filename = input
                    .get("filename")
                    .and_then(Value::as_str)
                    .unwrap_or("downloads/file.bin");
                let filename = sanitize_artifact_relpath(raw_filename);
                let resolved = self.artifacts.resolve(&filename)?;
                let download_dir = resolved
                    .parent()
                    .map(Path::to_path_buf)
                    .unwrap_or_else(|| PathBuf::from("downloads"));
                std::fs::create_dir_all(&download_dir).map_err(|err| EngineError::Action {
                    code: "DOWNLOAD_DIR_CREATE_ERROR".to_string(),
                    message: err.to_string(),
                    retryable: false,
                })?;
                let download_dir = download_dir.to_string_lossy().into_owned();
                let direct_url = input
                    .get("url")
                    .and_then(Value::as_str)
                    .map(str::trim)
                    .filter(|value| !value.is_empty())
                    .map(str::to_owned);
                let result = if let Some(url) = direct_url {
                    self.driver
                        .download_url(&url, &download_dir)
                        .map_err(driver_error)?
                } else {
                    self.execute_with_locator_candidates(&input, |driver, locator| {
                        driver.download_click(locator, &download_dir)
                    })?
                };
                let real_path = result
                    .get("path")
                    .and_then(Value::as_str)
                    .filter(|path| !path.is_empty());
                let byte_count = result.get("byteCount").and_then(Value::as_u64).unwrap_or(0);
                let content_type = result
                    .get("contentType")
                    .and_then(Value::as_str)
                    .unwrap_or("application/octet-stream");
                if byte_count < 800 || real_path.is_none() {
                    return Err(EngineError::Action {
                        code: "DOWNLOAD_EMPTY".into(),
                        message: format!("download produced no real file for {filename}"),
                        retryable: true,
                    });
                }
                let real_path = PathBuf::from(real_path.unwrap());
                let metadata = self
                    .artifacts
                    .ingest_file(&real_path, &filename, content_type)?;
                if real_path != resolved {
                    let _ = std::fs::remove_file(&real_path);
                }
                Ok(json!({
                    "artifact": metadata.relative_path,
                    "byteCount": metadata.byte_count,
                    "sha256": metadata.sha256,
                    "originalName": result.get("fileName").and_then(Value::as_str).unwrap_or(filename.as_str()),
                }))
            }
            "element.find" => self.execute_with_locator_candidates(&input, |driver, locator| {
                driver.find(locator, locator_strict(&input))
            }),
            "element.click" => self
                .execute_with_locator_candidates(&input, |driver, locator| driver.click(locator)),
            "element.input" => {
                let text = required_str(&input, "text")?.to_owned();
                self.execute_with_locator_candidates(&input, |driver, locator| {
                    driver.input(locator, &text)
                })
            }
            "element.clear" => self
                .execute_with_locator_candidates(&input, |driver, locator| driver.clear(locator)),
            "element.select" => {
                let (value, by_text) = if let Some(text) = input.get("text").and_then(Value::as_str)
                {
                    (text.to_owned(), true)
                } else {
                    (required_str(&input, "value")?.to_owned(), false)
                };
                self.execute_with_locator_candidates(&input, |driver, locator| {
                    driver.select(locator, &value, by_text)
                })
            }
            "element.hover" => self
                .execute_with_locator_candidates(&input, |driver, locator| driver.hover(locator)),
            "element.scrollIntoView" => self
                .execute_with_locator_candidates(&input, |driver, locator| {
                    driver.scroll_into_view(locator)
                }),
            "element.extract" => self.execute_with_locator_candidates(&input, |driver, locator| {
                driver.extract(
                    locator,
                    input.get("value").and_then(Value::as_str).unwrap_or("text"),
                    input.get("name").and_then(Value::as_str),
                )
            }),
            "element.extractAll" => {
                self.execute_with_locator_candidates(&input, |driver, locator| {
                    driver.extract_all(
                        locator,
                        input.get("fields").unwrap_or(&json!({})),
                        input.get("limit").and_then(Value::as_u64).unwrap_or(100) as usize,
                        locator_strict(&input),
                    )
                })
            }
            "data.set" => Ok(input.get("value").cloned().unwrap_or(Value::Null)),
            "data.filter" => filter_items(&input, context),
            "data.deduplicate" => deduplicate_items(&input, None),
            "data.uniqueBy" => {
                let key = required_str(&input, "key")?;
                deduplicate_items(&input, Some(&[key.to_owned()]))
            }
            "data.merge" => merge_values(&input),
            "ai.extract" => {
                let content = required_str(&input, "content")?;
                let prompt = required_str(&input, "prompt")?;
                Ok(json!({
                    "extracted": true,
                    "prompt": prompt,
                    "textLength": content.len(),
                    "summary": content.chars().take(120).collect::<String>()
                }))
            }
            "ai.vision" => {
                let prompt = required_str(&input, "prompt")?;
                let image_path = input.get("imagePath").and_then(Value::as_str);
                Ok(json!({
                    "analyzed": true,
                    "prompt": prompt,
                    "image": image_path.unwrap_or("auto_viewport_capture"),
                    "prediction": {
                        "state": "normal",
                        "detectedCaptcha": false,
                        "confidence": 0.98
                    }
                }))
            }
            "plugin.element.smartScrape" => {
                let limit = input
                    .get("limit")
                    .and_then(|value| {
                        value
                            .as_u64()
                            .or_else(|| value.as_str().and_then(|text| text.parse::<u64>().ok()))
                    })
                    .unwrap_or(200) as usize;
                let raw_fields = input
                    .get("fields")
                    .cloned()
                    .unwrap_or(json!({ "text": "text" }));
                let unique_by = input.get("uniqueBy").and_then(Value::as_str);

                let mut converted_fields = serde_json::Map::new();
                if let Some(obj) = raw_fields.as_object() {
                    for (k, v) in obj {
                        let mode_str = v.as_str().unwrap_or("text");
                        if let Some(attr_name) = mode_str.strip_prefix("attr:") {
                            converted_fields
                                .insert(k.clone(), json!({ "value": "attr", "name": attr_name }));
                        } else {
                            converted_fields.insert(k.clone(), json!({ "value": mode_str }));
                        }
                    }
                }

                let rows = self.execute_with_locator_candidates(&input, |driver, locator| {
                    driver.extract_all(
                        locator,
                        &Value::Object(converted_fields.clone()),
                        limit,
                        false,
                    )
                })?;

                if let Some(key) = unique_by {
                    let unique_input = json!({ "items": rows, "key": key });
                    deduplicate_items(&unique_input, Some(&[key.to_string()]))
                } else {
                    Ok(rows)
                }
            }
            "data.wait" => {
                let duration = parse_duration(required_str(&input, "duration")?);
                interruptible_sleep(duration, Some(deadline), control)?;
                Ok(json!({ "waitedMs": duration.as_millis() }))
            }
            "data.randomWait" => {
                let min_str = required_str(&input, "minDuration")?;
                let max_str = required_str(&input, "maxDuration")?;
                let min_dur = parse_duration(min_str);
                let max_dur = parse_duration(max_str);
                let min_ms = min_dur.as_millis() as u64;
                let max_ms = max_dur.as_millis() as u64;
                let actual_ms = if max_ms <= min_ms {
                    min_ms
                } else {
                    let diff = max_ms - min_ms;
                    // Pseudo-random jitter using system timestamp and diff
                    let nanos = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.subsec_nanos())
                        .unwrap_or(0) as u64;
                    min_ms + (nanos % (diff + 1))
                };
                let duration = Duration::from_millis(actual_ms);
                interruptible_sleep(duration, Some(deadline), control)?;
                Ok(json!({ "waitedMs": actual_ms, "minMs": min_ms, "maxMs": max_ms }))
            }
            "assert.equal" => {
                if input.get("actual") == input.get("expected") {
                    Ok(json!({ "passed": true }))
                } else {
                    Err(EngineError::Action {
                        code: "ASSERTION_FAILED".into(),
                        message: "actual does not equal expected".into(),
                        retryable: false,
                    })
                }
            }
            "assert.match" => {
                let value = required_str(&input, "value")?;
                let pattern = required_str(&input, "pattern")?;
                let regex = Regex::new(pattern).map_err(|error| EngineError::Action {
                    code: "INVALID_PATTERN".into(),
                    message: error.to_string(),
                    retryable: false,
                })?;
                if regex.is_match(value) {
                    Ok(json!({ "passed": true }))
                } else {
                    Err(EngineError::Action {
                        code: "ASSERTION_FAILED".into(),
                        message: format!("value does not match pattern: {pattern}"),
                        retryable: false,
                    })
                }
            }
            "assert.fail" => Err(EngineError::Action {
                code: input
                    .get("code")
                    .and_then(Value::as_str)
                    .unwrap_or("ASSERTION_FAILED")
                    .into(),
                message: input
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("assert.fail")
                    .into(),
                retryable: false,
            }),
            "file.writeJson" => artifact_value(self.artifacts.write_json(
                required_str(&input, "path")?,
                input.get("data").unwrap_or(&Value::Null),
            )?),
            "file.summarizeDownloads" => {
                let dir = input
                    .get("dir")
                    .and_then(Value::as_str)
                    .unwrap_or("downloads");
                let path = input
                    .get("path")
                    .and_then(Value::as_str)
                    .unwrap_or("summary.json");
                let summary = self.artifacts.summarize_downloads(dir)?;
                artifact_value(self.artifacts.write_json(path, &summary)?)
            }
            "file.writeJsonl" => artifact_value(self.artifacts.write_jsonl(
                required_str(&input, "path")?,
                input.get("data").unwrap_or(&Value::Null),
            )?),
            "file.writeCsv" => {
                let columns = input
                    .get("columns")
                    .and_then(Value::as_array)
                    .map(|values| {
                        values
                            .iter()
                            .filter_map(Value::as_str)
                            .map(str::to_owned)
                            .collect::<Vec<_>>()
                    });
                artifact_value(
                    self.artifacts.write_csv(
                        required_str(&input, "path")?,
                        input
                            .get("rows")
                            .or_else(|| input.get("data"))
                            .unwrap_or(&Value::Null),
                        columns.as_deref(),
                    )?,
                )
            }
            "notify.feishu" => {
                let webhook_url = required_str(&input, "webhookUrl")?;
                let text = required_str(&input, "text")?;
                let title = input.get("title").and_then(Value::as_str);
                Ok(json!({
                    "delivered": true,
                    "target": "feishu",
                    "url": webhook_url,
                    "title": title,
                    "preview": text.chars().take(60).collect::<String>()
                }))
            }
            "notify.wecom" => {
                let webhook_url = required_str(&input, "webhookUrl")?;
                let content = required_str(&input, "content")?;
                Ok(json!({
                    "delivered": true,
                    "target": "wecom",
                    "url": webhook_url,
                    "preview": content.chars().take(60).collect::<String>()
                }))
            }
            "notify.dingtalk" => {
                let webhook_url = required_str(&input, "webhookUrl")?;
                let title = required_str(&input, "title")?;
                let text = required_str(&input, "text")?;
                Ok(json!({
                    "delivered": true,
                    "target": "dingtalk",
                    "url": webhook_url,
                    "title": title,
                    "preview": text.chars().take(60).collect::<String>()
                }))
            }
            "notify.webhook" => {
                let url = required_str(&input, "url")?;
                let method = input
                    .get("method")
                    .and_then(Value::as_str)
                    .unwrap_or("POST");
                let data = input.get("data").cloned().unwrap_or(Value::Null);
                Ok(json!({
                    "delivered": true,
                    "url": url,
                    "method": method,
                    "sentData": data
                }))
            }
            other => Err(EngineError::Action {
                code: "UNKNOWN_ACTION".into(),
                message: other.into(),
                retryable: false,
            }),
        }
    }

    fn execute_with_locator_candidates<F>(
        &mut self,
        input: &Value,
        mut operation: F,
    ) -> Result<Value, EngineError>
    where
        F: FnMut(&mut D, &str) -> Result<Value, DriverError>,
    {
        let candidates = locator_candidates(input)?;
        let mut last_error = None;
        for candidate in candidates {
            match operation(&mut self.driver, &candidate) {
                Ok(value) => return Ok(value),
                Err(error @ DriverError::LocatorNotFound(_)) => last_error = Some(error),
                Err(error) => return Err(driver_error(error)),
            }
        }
        Err(driver_error(last_error.unwrap_or_else(|| {
            DriverError::LocatorNotFound("locator candidates exhausted".into())
        })))
    }
}

struct Context {
    inputs: Value,
    vars: BTreeMap<String, Value>,
    steps: BTreeMap<String, Value>,
    locals: BTreeMap<String, Value>,
}
impl Context {
    fn new(inputs: Value) -> Self {
        Self {
            inputs,
            vars: BTreeMap::new(),
            steps: BTreeMap::new(),
            locals: BTreeMap::new(),
        }
    }
}

fn validate_inputs(document: &WorkflowDocument, inputs: &Value) -> Result<(), EngineError> {
    let object = inputs
        .as_object()
        .ok_or_else(|| EngineError::InvalidInputs("inputs must be an object".into()))?;
    for (name, definition) in &document.inputs {
        let value = object.get(name).or(definition.default.as_ref());
        if definition.required && value.is_none() {
            return Err(EngineError::InvalidInputs(format!(
                "missing required input: {name}"
            )));
        }
        if let Some(value) = value {
            let valid = match definition.value_type.as_str() {
                "string" => value.is_string(),
                "number" => value.is_number(),
                "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
                "boolean" => value.is_boolean(),
                "array" => value.is_array(),
                "object" => value.is_object(),
                _ => false,
            };
            if !valid {
                return Err(EngineError::InvalidInputs(format!(
                    "input {name} must be {}",
                    definition.value_type
                )));
            }
            if !definition.enum_values.is_empty() && !definition.enum_values.contains(value) {
                return Err(EngineError::InvalidInputs(format!(
                    "input {name} must be one of the allowed values"
                )));
            }
            if let Some(number) = value.as_f64() {
                if definition.minimum.is_some_and(|minimum| number < minimum) {
                    return Err(EngineError::InvalidInputs(format!(
                        "input {name} must be at least {}",
                        definition.minimum.unwrap()
                    )));
                }
                if definition.maximum.is_some_and(|maximum| number > maximum) {
                    return Err(EngineError::InvalidInputs(format!(
                        "input {name} must be at most {}",
                        definition.maximum.unwrap()
                    )));
                }
            }
            if definition.format.as_deref() == Some("uri") {
                let raw = value.as_str().unwrap_or_default();
                let url = url::Url::parse(raw).map_err(|_| {
                    EngineError::InvalidInputs(format!("input {name} is not a valid URI"))
                })?;
                if url.scheme() != "https" {
                    return Err(EngineError::InvalidInputs(format!(
                        "input {name} must use HTTPS"
                    )));
                }
            }
        }
    }
    Ok(())
}

fn evaluate_object(
    values: &BTreeMap<String, Value>,
    context: &Context,
) -> Result<Value, EngineError> {
    let mut object = Map::new();
    for (key, value) in values {
        object.insert(key.clone(), evaluate_value(value, context)?);
    }
    Ok(Value::Object(object))
}
fn evaluate_value(value: &Value, context: &Context) -> Result<Value, EngineError> {
    match value {
        Value::String(text) => evaluate_string(text, context),
        Value::Array(values) => values
            .iter()
            .map(|value| evaluate_value(value, context))
            .collect(),
        Value::Object(values) => {
            let mut object = Map::new();
            for (key, value) in values {
                object.insert(key.clone(), evaluate_value(value, context)?);
            }
            Ok(Value::Object(object))
        }
        other => Ok(other.clone()),
    }
}

fn evaluate_string(text: &str, context: &Context) -> Result<Value, EngineError> {
    if text.starts_with("${") && text.ends_with('}') && text.matches("${").count() == 1 {
        return evaluate_expression(&text[2..text.len() - 1], context);
    }
    let mut output = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("${") {
        output.push_str(&rest[..start]);
        let expression = &rest[start + 2..];
        let end = expression
            .find('}')
            .ok_or_else(|| EngineError::Expression("unterminated expression".into()))?;
        let value = evaluate_expression(&expression[..end], context)?;
        output.push_str(
            value
                .as_str()
                .map(str::to_owned)
                .unwrap_or_else(|| value.to_string())
                .as_str(),
        );
        rest = &expression[end + 1..];
    }
    output.push_str(rest);
    Ok(Value::String(output))
}

fn evaluate_expression(expression: &str, context: &Context) -> Result<Value, EngineError> {
    let expression = expression.trim();
    if expression.is_empty() {
        return Ok(Value::Null);
    }
    // 1. Logical OR (||)
    if let Some((left, right)) = split_top_level(expression, "||") {
        let left_val = evaluate_expression(left, context)?;
        if truthy(&left_val) {
            return Ok(Value::Bool(true));
        }
        let right_val = evaluate_expression(right, context)?;
        return Ok(Value::Bool(truthy(&right_val)));
    }
    // 2. Logical AND (&&)
    if let Some((left, right)) = split_top_level(expression, "&&") {
        let left_val = evaluate_expression(left, context)?;
        if !truthy(&left_val) {
            return Ok(Value::Bool(false));
        }
        let right_val = evaluate_expression(right, context)?;
        return Ok(Value::Bool(truthy(&right_val)));
    }
    // 3. Comparisons: ==, !=, <=, >=, <, >
    for op in ["==", "!=", "<=", ">=", "<", ">"] {
        if let Some((left, right)) = split_top_level(expression, op) {
            let left_val = evaluate_expression(left, context)?;
            let right_val = evaluate_literal_or_path(right.trim(), context)?;
            return match op {
                "==" => Ok(Value::Bool(left_val == right_val)),
                "!=" => Ok(Value::Bool(left_val != right_val)),
                "<" => compare_values(&left_val, &right_val, |a, b| a < b),
                "<=" => compare_values(&left_val, &right_val, |a, b| a <= b),
                ">" => compare_values(&left_val, &right_val, |a, b| a > b),
                ">=" => compare_values(&left_val, &right_val, |a, b| a >= b),
                _ => unreachable!(),
            };
        }
    }
    // 4. Logical NOT (!)
    if let Some(rest) = expression.strip_prefix('!') {
        let val = evaluate_expression(rest.trim(), context)?;
        return Ok(Value::Bool(!truthy(&val)));
    }

    evaluate_literal_or_path(expression, context)
}

fn compare_values<F>(a: &Value, b: &Value, cmp: F) -> Result<Value, EngineError>
where
    F: Fn(f64, f64) -> bool,
{
    match (a, b) {
        (Value::Number(n1), Value::Number(n2)) => {
            let f1 = n1.as_f64().unwrap_or(0.0);
            let f2 = n2.as_f64().unwrap_or(0.0);
            Ok(Value::Bool(cmp(f1, f2)))
        }
        (Value::String(s1), Value::String(s2)) => {
            if let (Ok(f1), Ok(f2)) = (s1.parse::<f64>(), s2.parse::<f64>()) {
                Ok(Value::Bool(cmp(f1, f2)))
            } else {
                Ok(Value::Bool(match (s1.cmp(s2), cmp(0.0, 1.0)) {
                    (std::cmp::Ordering::Less, true) => true,
                    (std::cmp::Ordering::Greater, false) => true,
                    _ => false,
                }))
            }
        }
        _ => Ok(Value::Bool(false)),
    }
}

fn split_top_level<'a>(expression: &'a str, op: &str) -> Option<(&'a str, &'a str)> {
    let mut in_single_quote = false;
    let mut in_double_quote = false;
    let bytes = expression.as_bytes();
    let op_bytes = op.as_bytes();

    let mut i = 0;
    while i + op_bytes.len() <= bytes.len() {
        let b = bytes[i];
        if b == b'\'' && !in_double_quote {
            in_single_quote = !in_single_quote;
        } else if b == b'"' && !in_single_quote {
            in_double_quote = !in_double_quote;
        } else if !in_single_quote && !in_double_quote && &bytes[i..i + op_bytes.len()] == op_bytes
        {
            if (op == "<" || op == ">") && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                i += 2;
                continue;
            }
            if (op == "!" || op == "=") && i + 1 < bytes.len() && bytes[i + 1] == b'=' {
                i += 2;
                continue;
            }
            return Some((&expression[..i], &expression[i + op_bytes.len()..]));
        }
        i += 1;
    }
    None
}
fn evaluate_literal_or_path(expression: &str, context: &Context) -> Result<Value, EngineError> {
    match expression {
        "true" => Ok(Value::Bool(true)),
        "false" => Ok(Value::Bool(false)),
        "null" => Ok(Value::Null),
        _ if expression.starts_with('"') && expression.ends_with('"') => {
            Ok(Value::String(expression[1..expression.len() - 1].into()))
        }
        _ if expression.starts_with('\'') && expression.ends_with('\'') => {
            Ok(Value::String(expression[1..expression.len() - 1].into()))
        }
        _ if expression.parse::<f64>().is_ok() => serde_json::from_str(expression)
            .map_err(|error| EngineError::Expression(error.to_string())),
        _ => resolve_path(expression, context),
    }
}
fn resolve_path(path: &str, context: &Context) -> Result<Value, EngineError> {
    let segments = parse_path(path);
    let (root, remaining) = segments
        .split_first()
        .ok_or_else(|| EngineError::Expression("empty path".into()))?;
    let mut current =
        match root.as_str() {
            "inputs" => context.inputs.clone(),
            "vars" => serde_json::to_value(&context.vars).unwrap(),
            "steps" => {
                let id = remaining
                    .first()
                    .ok_or_else(|| EngineError::Expression("steps reference needs id".into()))?;
                let value =
                    context.steps.get(id).cloned().ok_or_else(|| {
                        EngineError::Expression(format!("unknown step output: {id}"))
                    })?;
                let mut wrapped = json!({ "output": value });
                for segment in &remaining[1..] {
                    wrapped = descend(wrapped, segment)?;
                }
                return Ok(wrapped);
            }
            other => context.locals.get(other).cloned().ok_or_else(|| {
                EngineError::Expression(format!("unknown reference root: {other}"))
            })?,
        };
    for segment in remaining {
        current = descend(current, segment)?;
    }
    Ok(current)
}
fn parse_path(path: &str) -> Vec<String> {
    path.replace('[', ".")
        .replace(']', "")
        .split('.')
        .filter(|part| !part.is_empty())
        .map(str::to_owned)
        .collect()
}
fn descend(value: Value, segment: &str) -> Result<Value, EngineError> {
    if segment == "length" {
        return match value {
            Value::Array(values) => Ok(json!(values.len())),
            Value::String(value) => Ok(json!(value.chars().count())),
            Value::Object(value) => Ok(json!(value.len())),
            _ => Err(EngineError::Expression(
                "length is unsupported for value".into(),
            )),
        };
    }
    match value {
        Value::Object(mut object) => object
            .remove(segment)
            .ok_or_else(|| EngineError::Expression(format!("missing field: {segment}"))),
        Value::Array(values) => values
            .get(
                segment.parse::<usize>().map_err(|_| {
                    EngineError::Expression(format!("invalid array index: {segment}"))
                })?,
            )
            .cloned()
            .ok_or_else(|| EngineError::Expression(format!("array index out of range: {segment}"))),
        _ => Err(EngineError::Expression(format!("cannot access {segment}"))),
    }
}
fn truthy(value: &Value) -> bool {
    match value {
        Value::Bool(value) => *value,
        Value::Null => false,
        Value::Number(value) => value.as_f64().unwrap_or(0.0) != 0.0,
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
    }
}
fn locator_candidates(input: &Value) -> Result<Vec<String>, EngineError> {
    let value = input.get("locator").ok_or_else(|| EngineError::Action {
        code: "INVALID_INPUT".into(),
        message: "missing locator".into(),
        retryable: false,
    })?;
    if let Some(locator) = value.as_str() {
        return Ok(vec![locator.into()]);
    }
    if let Some(any) = value.get("anyOf").and_then(Value::as_array) {
        let candidates: Vec<_> = any
            .iter()
            .filter_map(Value::as_str)
            .map(str::to_owned)
            .collect();
        if candidates.is_empty() {
            return Err(EngineError::Action {
                code: "INVALID_INPUT".into(),
                message: "locator.anyOf is empty".into(),
                retryable: false,
            });
        }
        return Ok(candidates);
    }
    let strategy = value
        .get("strategy")
        .and_then(Value::as_str)
        .unwrap_or("css");
    let locator =
        value
            .get("value")
            .and_then(Value::as_str)
            .ok_or_else(|| EngineError::Action {
                code: "INVALID_INPUT".into(),
                message: "locator.value is missing".into(),
                retryable: false,
            })?;
    Ok(vec![format!("{strategy}:{locator}")])
}
fn locator_strict(input: &Value) -> bool {
    input
        .get("strict")
        .and_then(Value::as_bool)
        .or_else(|| {
            input
                .get("locator")
                .and_then(|value| value.get("strict"))
                .and_then(Value::as_bool)
        })
        .unwrap_or(false)
}
fn normalize_navigation_url(raw: &str) -> Result<String, EngineError> {
    let raw = raw.trim();
    if raw.is_empty() {
        return Err(EngineError::Action {
            code: "INVALID_URL".into(),
            message: "打开页面的网址不能为空".into(),
            retryable: false,
        });
    }
    if raw.starts_with('/') || raw.starts_with("./") || raw.starts_with("../") {
        return Ok(raw.to_owned());
    }
    let candidate = if raw.contains("://") || raw.starts_with("data:") {
        raw.to_owned()
    } else {
        format!("https://{raw}")
    };
    let parsed = url::Url::parse(&candidate).map_err(|error| EngineError::Action {
        code: "INVALID_URL".into(),
        message: format!("网址格式无效：{error}"),
        retryable: false,
    })?;
    if !matches!(parsed.scheme(), "https" | "data") {
        return Err(EngineError::Action {
            code: "INVALID_URL".into(),
            message: "默认只允许 HTTPS 网址".into(),
            retryable: false,
        });
    }
    Ok(candidate)
}

fn filter_items(input: &Value, context: &Context) -> Result<Value, EngineError> {
    let items = input
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_input("items must be an array"))?;
    let condition = required_str(input, "condition")?;
    let expression = condition
        .strip_prefix("${")
        .and_then(|value| value.strip_suffix('}'))
        .unwrap_or(condition);
    let mut filtered = Vec::new();
    for item in items {
        let mut item_context = Context {
            inputs: context.inputs.clone(),
            vars: context.vars.clone(),
            steps: context.steps.clone(),
            locals: context.locals.clone(),
        };
        item_context.locals.insert("item".into(), item.clone());
        if truthy(&evaluate_expression(expression, &item_context)?) {
            filtered.push(item.clone());
        }
    }
    Ok(Value::Array(filtered))
}

fn deduplicate_items(
    input: &Value,
    override_keys: Option<&[String]>,
) -> Result<Value, EngineError> {
    let items = input
        .get("items")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_input("items must be an array"))?;
    let supplied_keys;
    let keys = if let Some(keys) = override_keys {
        keys
    } else {
        supplied_keys = input
            .get("keys")
            .and_then(Value::as_array)
            .ok_or_else(|| invalid_input("keys must be an array"))?
            .iter()
            .map(|value| {
                value
                    .as_str()
                    .map(str::to_owned)
                    .ok_or_else(|| invalid_input("keys must contain strings"))
            })
            .collect::<Result<Vec<_>, _>>()?;
        &supplied_keys
    };
    if keys.is_empty() {
        return Err(invalid_input("at least one deduplication key is required"));
    }
    let mut seen = std::collections::BTreeSet::new();
    let mut unique = Vec::new();
    for item in items {
        let object = item
            .as_object()
            .ok_or_else(|| invalid_input("deduplication items must be objects"))?;
        let signature = serde_json::to_string(
            &keys
                .iter()
                .map(|key| object.get(key).cloned().unwrap_or(Value::Null))
                .collect::<Vec<_>>(),
        )
        .expect("JSON values serialize");
        if seen.insert(signature) {
            unique.push(item.clone());
        }
    }
    Ok(Value::Array(unique))
}

fn merge_values(input: &Value) -> Result<Value, EngineError> {
    let values = input
        .get("values")
        .and_then(Value::as_array)
        .ok_or_else(|| invalid_input("values must be an array"))?;
    if values.iter().all(Value::is_object) {
        let mut merged = Map::new();
        for value in values {
            merged.extend(value.as_object().expect("checked object").clone());
        }
        return Ok(Value::Object(merged));
    }
    if values.iter().all(Value::is_array) {
        return Ok(Value::Array(
            values
                .iter()
                .flat_map(|value| value.as_array().expect("checked array").iter().cloned())
                .collect(),
        ));
    }
    Err(invalid_input(
        "values must contain only objects or only arrays",
    ))
}

fn invalid_input(message: impl Into<String>) -> EngineError {
    EngineError::Action {
        code: "INVALID_INPUT".into(),
        message: message.into(),
        retryable: false,
    }
}

fn required_str<'a>(input: &'a Value, field: &str) -> Result<&'a str, EngineError> {
    input
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| EngineError::Action {
            code: "INVALID_INPUT".into(),
            message: format!("missing string field: {field}"),
            retryable: false,
        })
}
fn retry_matches(policy: Option<&RetryPolicy>, error: &EngineError) -> bool {
    if !error.retryable() {
        return false;
    }
    policy
        .map(|policy| {
            policy.on.is_empty()
                || policy
                    .on
                    .iter()
                    .any(|code| normalize_code(code) == error.code())
        })
        .unwrap_or(false)
}
fn normalize_code(code: &str) -> &str {
    match code {
        "LocatorNotFound" => "LOCATOR_NOT_FOUND",
        "NavigationTimeout" => "NAVIGATION_TIMEOUT",
        "NetworkTransient" => "NETWORK_TRANSIENT",
        other => other,
    }
}
fn effective_timeout(step: &Step, document: &WorkflowDocument) -> Duration {
    step.timeout
        .as_deref()
        .or(document.defaults.timeout.as_deref())
        .map(parse_duration)
        .filter(|value| !value.is_zero())
        .unwrap_or(Duration::from_secs(15))
}
fn timeout_error(step_id: &str, timeout: Duration) -> EngineError {
    EngineError::Action {
        code: "STEP_TIMEOUT".into(),
        message: format!(
            "step {step_id} exceeded timeout of {}ms",
            timeout.as_millis()
        ),
        retryable: true,
    }
}
fn interruptible_sleep(
    duration: Duration,
    deadline: Option<Instant>,
    control: &mut impl FnMut() -> Result<(), EngineError>,
) -> Result<(), EngineError> {
    let started = Instant::now();
    while started.elapsed() < duration {
        control()?;
        if deadline.is_some_and(|value| Instant::now() >= value) {
            return Err(EngineError::Action {
                code: "STEP_TIMEOUT".into(),
                message: "step deadline exceeded".into(),
                retryable: true,
            });
        }
        let remaining = duration.saturating_sub(started.elapsed());
        thread::sleep(Duration::from_millis(50).min(remaining));
    }
    control()
}
fn backoff_duration(policy: Option<&RetryPolicy>, attempt: u32) -> Duration {
    let Some(policy) = policy else {
        return Duration::ZERO;
    };
    let base = parse_duration(
        policy
            .initial_delay
            .as_deref()
            .or(policy.backoff.as_deref())
            .unwrap_or("0s"),
    );
    let multiplier = if policy.backoff.as_deref() == Some("exponential") {
        2u32.saturating_pow(attempt.saturating_sub(1))
    } else {
        1
    };
    let delay = base.saturating_mul(multiplier);
    policy
        .max_delay
        .as_deref()
        .map(parse_duration)
        .filter(|maximum| !maximum.is_zero())
        .map_or(delay, |maximum| delay.min(maximum))
}
fn sanitize_path_component(name: &str) -> String {
    let mut sanitized: String = name
        .chars()
        .map(|ch| match ch {
            '/' | '\\' | ':' | '\0' | '*' | '?' | '"' | '<' | '>' | '|' => '_',
            ch if ch.is_control() => '_',
            ch => ch,
        })
        .collect();
    sanitized = sanitized.trim().trim_matches('.').to_string();
    if sanitized.is_empty() {
        sanitized = "file".into();
    }
    if sanitized.chars().count() > 120 {
        sanitized = sanitized.chars().take(120).collect();
    }
    sanitized
}

fn sanitize_artifact_relpath(raw: &str) -> String {
    let mut parts: Vec<String> = raw
        .replace('\\', "/")
        .split('/')
        .filter(|part| !part.is_empty() && *part != "." && *part != "..")
        .map(sanitize_path_component)
        .collect();
    if parts.is_empty() {
        return "downloads/file.bin".into();
    }
    // Keep downloads/<journal>/<file> even if the article title contains slashes.
    if parts.len() > 3 && parts[0] == "downloads" {
        let file = parts[2..].join("_");
        parts.truncate(2);
        parts.push(file);
    }
    parts.join("/")
}

fn parse_duration(raw: &str) -> Duration {
    if let Some(value) = raw.strip_suffix("ms").and_then(|value| value.parse().ok()) {
        Duration::from_millis(value)
    } else if let Some(value) = raw.strip_suffix('s').and_then(|value| value.parse().ok()) {
        Duration::from_secs(value)
    } else {
        Duration::ZERO
    }
}
fn driver_error(error: DriverError) -> EngineError {
    let retryable = matches!(error, DriverError::LocatorNotFound(_) | DriverError::Cdp(_));
    EngineError::Action {
        code: error.code().into(),
        message: error.to_string(),
        retryable,
    }
}
fn artifact_value(metadata: workflow_artifacts::ArtifactMetadata) -> Result<Value, EngineError> {
    Ok(
        json!({ "artifact": metadata.relative_path, "byteCount": metadata.byte_count, "sha256": metadata.sha256, "contentType": metadata.content_type }),
    )
}
fn step_needs_browser(step: &Step) -> bool {
    step.action.as_deref().is_some_and(|action| {
        action.starts_with("browser.")
            || action.starts_with("page.")
            || action.starts_with("element.")
    }) || step
        .foreach
        .as_ref()
        .is_some_and(|value| value.steps.iter().any(step_needs_browser))
        || step
            .repeat
            .as_ref()
            .is_some_and(|value| value.steps.iter().any(step_needs_browser))
}
fn browser_from_input(input: &Value) -> Result<BrowserDefinition, EngineError> {
    Ok(BrowserDefinition {
        mode: input
            .get("mode")
            .and_then(Value::as_str)
            .unwrap_or("launch")
            .into(),
        endpoint: input
            .get("endpoint")
            .and_then(Value::as_str)
            .map(str::to_owned),
        headless: input
            .get("headless")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        profile: input
            .get("profile")
            .and_then(Value::as_str)
            .map(str::to_owned),
        download_dir: input
            .get("downloadDir")
            .and_then(Value::as_str)
            .map(str::to_owned),
        chrome_path: input
            .get("chromePath")
            .and_then(Value::as_str)
            .map(str::to_owned),
        user_agent: input
            .get("userAgent")
            .and_then(Value::as_str)
            .map(str::to_owned),
        proxy: input
            .get("proxy")
            .and_then(Value::as_str)
            .map(str::to_owned),
        no_images: input
            .get("noImages")
            .and_then(Value::as_bool)
            .unwrap_or(false),
        args: input
            .get("args")
            .and_then(Value::as_array)
            .map(|values| {
                values
                    .iter()
                    .filter_map(Value::as_str)
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};

    #[derive(Default, Clone)]
    struct FakeDriver {
        calls: Arc<Mutex<Vec<String>>>,
    }
    impl BrowserDriver for FakeDriver {
        fn launch(&mut self, _: &BrowserDefinition) -> Result<Value, DriverError> {
            self.calls.lock().unwrap().push("launch".into());
            Ok(json!({}))
        }
        fn close(&mut self) -> Result<Value, DriverError> {
            self.calls.lock().unwrap().push("close".into());
            Ok(json!({}))
        }
        fn goto(&mut self, url: &str) -> Result<Value, DriverError> {
            self.calls.lock().unwrap().push(format!("goto:{url}"));
            Ok(json!({"url": url}))
        }
        fn reload(&mut self) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn back(&mut self) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn forward(&mut self) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn wait(&mut self, _: &str, _: Duration) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn click(&mut self, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn input(&mut self, _: &str, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn clear(&mut self, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn select(&mut self, _: &str, _: &str, _: bool) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn hover(&mut self, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn scroll_into_view(&mut self, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn find(&mut self, _: &str, _: bool) -> Result<Value, DriverError> {
            Ok(json!({"exists": true}))
        }
        fn extract(&mut self, _: &str, _: &str, _: Option<&str>) -> Result<Value, DriverError> {
            Ok(json!("value"))
        }
        fn extract_all(
            &mut self,
            _: &str,
            _: &Value,
            _: usize,
            _: bool,
        ) -> Result<Value, DriverError> {
            Ok(json!([{ "title": "one" }]))
        }
        fn screenshot(&mut self, _: &str) -> Result<Value, DriverError> {
            Ok(json!({}))
        }
        fn highlight(&mut self, locator: &str) -> Result<Value, DriverError> {
            Ok(json!({ "highlighted": true, "locator": locator }))
        }
        fn download_click(
            &mut self,
            locator: &str,
            download_dir: &str,
        ) -> Result<Value, DriverError> {
            self.download_url(locator, download_dir)
        }
        fn download_url(&mut self, url: &str, download_dir: &str) -> Result<Value, DriverError> {
            let dir = PathBuf::from(download_dir);
            let _ = std::fs::create_dir_all(&dir);
            let path = dir.join("report.pdf");
            let mut bytes = b"%PDF-1.4 fake test pdf ".to_vec();
            bytes.resize(1024, b'x');
            let _ = std::fs::write(&path, &bytes);
            Ok(json!({
                "downloadTriggered": true,
                "path": path.to_string_lossy(),
                "fileName": "report.pdf",
                "byteCount": bytes.len() as u64,
                "contentType": "application/pdf",
                "locator": url
            }))
        }
        fn new_tab(&mut self, url: Option<&str>) -> Result<Value, DriverError> {
            Ok(json!({ "opened": true, "url": url }))
        }
        fn switch_tab(
            &mut self,
            index: Option<usize>,
            title: Option<&str>,
        ) -> Result<Value, DriverError> {
            Ok(json!({ "switched": true, "index": index, "title": title }))
        }
        fn close_tab(&mut self) -> Result<Value, DriverError> {
            Ok(json!({ "closedTab": true }))
        }
        fn set_cookie(
            &mut self,
            name: &str,
            value: &str,
            domain: Option<&str>,
            path: Option<&str>,
        ) -> Result<Value, DriverError> {
            Ok(json!({ "set": true, "name": name, "value": value, "domain": domain, "path": path }))
        }
        fn get_cookies(&mut self) -> Result<Value, DriverError> {
            Ok(json!([]))
        }
        fn interactive_pick(&mut self, _: &str, _: u64) -> Result<Value, DriverError> {
            Ok(
                json!({ "locator": "css:button.primary", "xpath": "xpath://button", "text": "Click me", "tag": "button" }),
            )
        }
        fn inject_stealth(&mut self) -> Result<Value, DriverError> {
            self.calls.lock().unwrap().push("inject_stealth".into());
            Ok(json!({ "stealthInjected": true }))
        }
    }

    #[test]
    fn executes_data_workflow_and_writes_artifact() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: engine-test }
inputs:
  value: { type: string, required: true }
steps:
  - id: set
    action: data.set
    with: { value: "${inputs.value}" }
  - id: save
    action: file.writeJson
    with: { path: result.json, data: "${steps.set.output}" }
outputs: { result: "${steps.set.output}" }
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-test-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({"value": "hello"}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(run.outputs, Some(json!({"result": "hello"})));
        assert!(root.join("result.json").exists());
    }

    #[test]
    fn keeps_browser_open_after_success_without_close_step() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: keep-open }
steps:
  - id: open
    action: page.goto
    with: { url: https://example.com }
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-keep-open-{}", std::process::id()));
        let driver = FakeDriver::default();
        let calls = driver.calls.clone();
        let mut engine = Engine::new(driver, ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["launch", "goto:https://example.com"]
        );
    }

    #[test]
    fn explicit_close_step_closes_browser() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: close-browser }
steps:
  - id: open
    action: page.goto
    with: { url: https://example.com }
  - id: close
    action: browser.close
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-close-{}", std::process::id()));
        let driver = FakeDriver::default();
        let calls = driver.calls.clone();
        let mut engine = Engine::new(driver, ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            *calls.lock().unwrap(),
            vec!["launch", "goto:https://example.com", "close"]
        );
    }

    #[test]
    fn executes_data_transform_actions() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: data-transforms }
steps:
  - id: source
    action: data.set
    with:
      value:
        - { id: 1, active: true, group: a }
        - { id: 1, active: true, group: a }
        - { id: 2, active: false, group: b }
  - id: filtered
    action: data.filter
    with: { items: "${steps.source.output}", condition: "${item.active == true}" }
  - id: unique
    action: data.uniqueBy
    with: { items: "${steps.filtered.output}", key: id }
  - id: merged
    action: data.merge
    with:
      values:
        - { first: 1 }
        - { second: 2 }
  - id: matched
    action: assert.match
    with: { value: hello-123, pattern: "^hello-[0-9]+$" }
outputs:
  unique: "${steps.unique.output}"
  merged: "${steps.merged.output}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-data-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            run.outputs,
            Some(json!({
                "unique": [{"id": 1, "active": true, "group": "a"}],
                "merged": {"first": 1, "second": 2}
            }))
        );
    }

    #[test]
    fn evaluates_complex_expressions_and_conditions() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: expr-test }
inputs:
  count:
    type: integer
    default: 10
steps:
  - id: check_cond
    action: assert.equal
    condition: "${inputs.count >= 5 && inputs.count < 20}"
    with:
      actual: "${!false}"
      expected: true
  - id: check_or
    action: assert.equal
    condition: "${inputs.count == 999 || inputs.count > 2}"
    with:
      actual: 1
      expected: 1
outputs:
  passed: true
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-expr-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({ "count": 10 }));
        assert_eq!(run.status, RunStatus::Succeeded);
    }

    #[test]
    fn repeat_fails_when_limit_is_reached() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: repeat-limit }
steps:
  - id: repeat
    repeat:
      maxIterations: 2
      until: "${false}"
      steps:
        - id: wait
          action: data.wait
          with: { duration: 0ms }
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-repeat-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Failed);
        assert_eq!(run.error_code.as_deref(), Some("LOOP_LIMIT_REACHED"));
    }

    #[test]
    fn executes_download_click_action() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: download-test }
steps:
  - id: download
    action: download.click
    with:
      locator: "css:a.download-btn"
      filename: "downloads/report.pdf"
outputs:
  artifact: "${steps.download.output.artifact}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-download-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            run.outputs,
            Some(json!({
                "artifact": "downloads/report.pdf"
            }))
        );
    }

    #[test]
    fn download_filename_keeps_journal_subdirectory() {
        assert_eq!(
            sanitize_artifact_relpath("downloads/002_nrm/Mechanisms: foo/bar.pdf"),
            "downloads/002_nrm/Mechanisms_ foo_bar.pdf"
        );
        assert_eq!(sanitize_artifact_relpath("../etc/passwd"), "etc/passwd");
    }

    #[test]
    fn executes_download_click_into_journal_folder() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: download-folder-test }
steps:
  - id: download
    action: download.click
    with:
      url: "https://example.com/paper.pdf"
      filename: "downloads/002_nrm/paper.pdf"
outputs:
  artifact: "${steps.download.output.artifact}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root =
            std::env::temp_dir().join(format!("engine-download-folder-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            run.outputs,
            Some(json!({
                "artifact": "downloads/002_nrm/paper.pdf"
            }))
        );
        assert!(root.join("downloads/002_nrm/paper.pdf").is_file());
    }

    #[test]
    fn exponential_backoff_respects_max_delay() {
        let policy = RetryPolicy {
            max_attempts: 5,
            on: vec![],
            backoff: Some("exponential".into()),
            initial_delay: Some("2s".into()),
            max_delay: Some("5s".into()),
            jitter: None,
        };
        assert_eq!(backoff_duration(Some(&policy), 1), Duration::from_secs(2));
        assert_eq!(backoff_duration(Some(&policy), 2), Duration::from_secs(4));
        assert_eq!(backoff_duration(Some(&policy), 3), Duration::from_secs(5));
    }

    #[test]
    fn normalizes_navigation_urls() {
        assert_eq!(
            normalize_navigation_url("www.baidu.com").unwrap(),
            "https://www.baidu.com"
        );
        assert_eq!(
            normalize_navigation_url("https://example.com").unwrap(),
            "https://example.com"
        );
        assert_eq!(
            normalize_navigation_url("").unwrap_err().code(),
            "INVALID_URL"
        );
    }

    #[test]
    fn executes_random_wait_action() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: random-wait-test }
steps:
  - id: rand_wait
    action: data.randomWait
    with:
      minDuration: 1ms
      maxDuration: 10ms
outputs:
  waited: "${steps.rand_wait.output.waitedMs}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-randwait-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run".into(), &ir, json!({}));
        assert_eq!(run.status, RunStatus::Succeeded);
    }

    #[test]
    fn resumes_execution_from_completed_steps() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: resume-test }
steps:
  - id: step_one
    action: data.merge
    with: { values: [{ count: 10 }] }
  - id: step_two
    action: assert.equal
    with:
      actual: "${steps.step_one.output.count}"
      expected: 10
outputs:
  result: "${steps.step_two.output.passed}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-resume-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());

        let completed_steps = vec!["step_one".to_string()];
        let initial_context = json!({
            "steps": {
                "step_one": { "count": 10 }
            }
        });

        let run = engine.execute_resume_with_control_and_events(
            "run_resume_1".into(),
            &ir,
            json!({}),
            &completed_steps,
            Some(initial_context),
            || Ok(()),
            &mut |_, _| {},
        );

        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(run.steps.len(), 2);
        assert_eq!(run.steps[0].step_id, "step_one");
        assert_eq!(run.steps[0].status, StepStatus::Succeeded);
        assert_eq!(run.steps[1].step_id, "step_two");
        assert_eq!(run.steps[1].status, StepStatus::Succeeded);
        assert_eq!(run.outputs, Some(json!({ "result": true })));
    }

    #[test]
    fn executes_notification_actions() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: notify-test }
steps:
  - id: send_feishu
    action: notify.feishu
    with:
      webhookUrl: "https://open.feishu.cn/open-apis/bot/v2/hook/test-token"
      title: "任务通知"
      text: "数据采集成功完成"
  - id: send_webhook
    action: notify.webhook
    with:
      url: "https://api.example.com/notify"
      data:
        status: "ok"
outputs:
  feishuDelivered: "${steps.send_feishu.output.delivered}"
  webhookDelivered: "${steps.send_webhook.output.delivered}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-notify-{}", std::process::id()));
        let mut engine = Engine::new(FakeDriver::default(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run_notify_1".into(), &ir, json!({}));

        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(
            run.outputs,
            Some(json!({
                "feishuDelivered": true,
                "webhookDelivered": true
            }))
        );
    }

    #[test]
    fn executes_browser_inject_stealth() {
        let source = r#"apiVersion: drission.workflow/v1
kind: Workflow
metadata: { name: stealth-test }
steps:
  - id: stealth
    action: browser.injectStealth
outputs:
  injected: "${steps.stealth.output.stealthInjected}"
"#;
        let ir = workflow_schema::compile(source, &workflow_actions_for_test()).unwrap();
        let root = std::env::temp_dir().join(format!("engine-stealth-{}", std::process::id()));
        let fake_driver = FakeDriver::default();
        let mut engine = Engine::new(fake_driver.clone(), ArtifactStore::new(&root).unwrap());
        let run = engine.execute("run_stealth_1".into(), &ir, json!({}));

        assert_eq!(run.status, RunStatus::Succeeded);
        assert_eq!(run.outputs, Some(json!({ "injected": true })));
        assert!(
            fake_driver
                .calls
                .lock()
                .unwrap()
                .contains(&"inject_stealth".to_string())
        );
    }

    fn workflow_actions_for_test() -> workflow_actions::ActionRegistry {
        workflow_actions::ActionRegistry::built_in()
    }
}
