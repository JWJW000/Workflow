use clap::{Parser, Subcommand};
use serde_json::{Map, Value};
use std::{
    env, fs,
    io::Write,
    path::{Path, PathBuf},
    process::{Command as ProcessCommand, ExitCode, Stdio},
};
use uuid::Uuid;
use workflow_actions::ActionRegistry;
use workflow_core::{RunEvent, RunRecord, RunStatus, WorkflowError};
use workflow_engine::{RunnerControl, RunnerOutput, RunnerRequest, RunnerResponse};
use workflow_store::SqliteStore;

#[derive(Debug, Parser)]
#[command(name = "drission-workflow", version, about = "Drission Workflow CLI")]
struct Cli {
    #[arg(long, global = true, default_value = ".drission-workflow/workflows.db")]
    database: PathBuf,
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    Validate {
        workflow: PathBuf,
        #[arg(long)]
        json: bool,
    },
    Run {
        workflow: PathBuf,
        #[arg(long = "input", value_parser = parse_key_value)]
        input: Vec<(String, String)>,
        #[arg(long)]
        inputs: Option<PathBuf>,
        #[arg(long)]
        artifacts: Option<PathBuf>,
        #[arg(long)]
        json: bool,
    },
    Status {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    Pause {
        run_id: String,
    },
    Resume {
        run_id: String,
    },
    Cancel {
        run_id: String,
    },
    Logs {
        run_id: String,
        #[arg(long)]
        json: bool,
    },
    Locator {
        #[arg(long)]
        url: String,
        #[arg(long)]
        locator: String,
        #[arg(long)]
        headless: bool,
        #[arg(long)]
        json: bool,
    },
    Pick {
        #[arg(long)]
        url: String,
        #[arg(long, default_value = "60")]
        timeout: u64,
        #[arg(long)]
        json: bool,
    },
    Actions {
        #[command(subcommand)]
        command: ActionsCommand,
    },
    Doctor {
        #[arg(long)]
        json: bool,
    },
    /// Internal runner process entry point. Input and output are JSON over stdio.
    #[command(hide = true)]
    Runner,
}

#[derive(Debug, Subcommand)]
enum ActionsCommand {
    List {
        #[arg(long)]
        json: bool,
    },
}

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => ExitCode::from(code),
    }
}

fn run(cli: Cli) -> Result<(), u8> {
    let registry = ActionRegistry::built_in();
    match cli.command {
        Command::Validate { workflow, json } => {
            let source = read(&workflow)?;
            match workflow_schema::compile(&source, &registry) {
                Ok(ir) => {
                    if json {
                        print_json(&ir);
                    } else {
                        print_ir(&ir);
                    }
                    Ok(())
                }
                Err(error) => print_workflow_error(error, json),
            }
        }
        Command::Run {
            workflow,
            input,
            inputs,
            artifacts,
            json,
        } => {
            let source = read(&workflow)?;
            let ir = workflow_schema::compile(&source, &registry).map_err(|error| {
                let _ = print_workflow_error(error, json);
                3
            })?;
            let inputs = load_inputs(inputs.as_ref(), input)?;
            let run_id = Uuid::new_v4().to_string();
            let artifact_root =
                artifacts.unwrap_or_else(|| PathBuf::from("artifacts").join(&run_id));
            let store = SqliteStore::open(&cli.database).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            let queued = RunRecord {
                id: run_id.clone(),
                workflow_name: ir.name.clone(),
                workflow_hash: ir.content_hash.clone(),
                workflow_id: None,
                workflow_version_id: None,
                run_mode: "legacy".into(),
                source_snapshot: None,
                draft_revision: None,
                parent_run_id: None,
                resume_checkpoint_id: None,
                status: RunStatus::Queued,
                inputs: inputs.clone(),
                outputs: None,
                error_code: None,
                error_message: None,
                steps: vec![],
            };
            store.create_run(&queued).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            store
                .append_event(&RunEvent {
                    run_id: run_id.clone(),
                    sequence: 1,
                    event_type: "run.queued".into(),
                    payload: Value::Object(Map::new()),
                })
                .map_err(|error| {
                    eprintln!("{error}");
                    7
                })?;
            let mut running = queued.clone();
            running.status = RunStatus::Running;
            store.update_run(&running).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            store
                .append_event(&RunEvent {
                    run_id: run_id.clone(),
                    sequence: 2,
                    event_type: "run.started".into(),
                    payload: Value::Object(Map::new()),
                })
                .map_err(|error| {
                    eprintln!("{error}");
                    7
                })?;
            let request = RunnerRequest {
                run_id: run_id.clone(),
                workflow_source: source,
                inputs,
                artifact_root: artifact_root.clone(),
                control_path: Some(control_path(&cli.database, &run_id)),
                completed_steps: vec![],
                initial_context: None,
            };
            write_control(&cli.database, &run_id, RunnerControl::Running).map_err(|message| {
                eprintln!("cannot initialize runner control: {message}");
                7
            })?;
            let result = invoke_runner(&request).map_err(|message| {
                let interrupted = RunRecord {
                    id: run_id.clone(),
                    workflow_name: ir.name.clone(),
                    workflow_hash: ir.content_hash.clone(),
                    workflow_id: None,
                    workflow_version_id: None,
                    run_mode: "legacy".into(),
                    source_snapshot: None,
                    draft_revision: None,
                    parent_run_id: None,
                    resume_checkpoint_id: None,
                    status: RunStatus::Interrupted,
                    inputs: request.inputs.clone(),
                    outputs: None,
                    error_code: Some("RUNNER_INTERRUPTED".into()),
                    error_message: Some(message.clone()),
                    steps: vec![],
                };
                let _ = store.update_run(&interrupted);
                let _ = store.append_event(&RunEvent {
                    run_id: run_id.clone(),
                    sequence: 3,
                    event_type: "run.interrupted".into(),
                    payload: serde_json::json!({ "message": message }),
                });
                eprintln!("runner failed: {message}");
                7
            })?;
            let result = result.run.ok_or_else(|| {
                eprintln!(
                    "runner failed: {}",
                    result
                        .error_message
                        .as_deref()
                        .unwrap_or("unknown runner error")
                );
                7
            })?;
            for step in &result.steps {
                store.upsert_step(&run_id, step).map_err(|error| {
                    eprintln!("{error}");
                    7
                })?;
                let event_type = match step.status {
                    workflow_core::StepStatus::Succeeded => "step.succeeded",
                    workflow_core::StepStatus::Skipped => "step.skipped",
                    _ => "step.failed",
                };
                let sequence = store.next_sequence(&run_id).map_err(|error| {
                    eprintln!("{error}");
                    7
                })?;
                store
                    .append_event(&RunEvent {
                        run_id: run_id.clone(),
                        sequence,
                        event_type: event_type.into(),
                        payload: serde_json::json!({
                            "stepId": step.step_id,
                            "attempt": step.attempt,
                            "errorCode": step.error_code,
                        }),
                    })
                    .map_err(|error| {
                        eprintln!("{error}");
                        7
                    })?;
            }
            store.update_run(&result).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            let sequence = store.next_sequence(&run_id).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            store
                .append_event(&RunEvent {
                    run_id: run_id.clone(),
                    sequence,
                    event_type: match result.status {
                        RunStatus::Succeeded => "run.succeeded",
                        RunStatus::Cancelled => "run.cancelled",
                        RunStatus::Interrupted => "run.interrupted",
                        _ => "run.failed",
                    }
                    .into(),
                    payload: serde_json::json!({ "errorCode": result.error_code }),
                })
                .map_err(|error| {
                    eprintln!("{error}");
                    7
                })?;
            let _ = fs::remove_file(control_path(&cli.database, &run_id));
            if json {
                print_json(&result);
            } else {
                println!(
                    "run: {}\nstatus: {:?}\nartifacts: {}",
                    result.id,
                    result.status,
                    artifact_root.display()
                );
                if let Some(error) = &result.error_message {
                    println!("error: {error}");
                }
            }
            if result.status == RunStatus::Succeeded {
                Ok(())
            } else if result.status == RunStatus::Cancelled {
                Err(6)
            } else {
                Err(5)
            }
        }
        Command::Status { run_id, json } => {
            let store = SqliteStore::open(&cli.database).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            let run = store
                .get_run(&run_id)
                .map_err(|error| {
                    eprintln!("{error}");
                    7
                })?
                .ok_or_else(|| {
                    eprintln!("run not found: {run_id}");
                    7
                })?;
            if json {
                print_json(&run);
            } else {
                println!(
                    "run: {}\nworkflow: {}\nstatus: {:?}",
                    run.id, run.workflow_name, run.status
                );
                for step in run.steps {
                    println!("  {} #{} {:?}", step.step_id, step.attempt, step.status);
                }
            }
            Ok(())
        }
        Command::Pause { run_id } => control_run(
            &cli.database,
            &run_id,
            RunnerControl::Paused,
            RunStatus::Pausing,
            "run.pause_requested",
        ),
        Command::Resume { run_id } => control_run(
            &cli.database,
            &run_id,
            RunnerControl::Running,
            RunStatus::Running,
            "run.resumed",
        ),
        Command::Cancel { run_id } => control_run(
            &cli.database,
            &run_id,
            RunnerControl::Cancelled,
            RunStatus::Cancelling,
            "run.cancel_requested",
        ),
        Command::Logs { run_id, json } => {
            let store = SqliteStore::open(&cli.database).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            let events = store.events(&run_id).map_err(|error| {
                eprintln!("{error}");
                7
            })?;
            if json {
                print_json(&events);
            } else {
                for event in events {
                    println!(
                        "{:>5} {:<20} {}",
                        event.sequence, event.event_type, event.payload
                    );
                }
            }
            Ok(())
        }
        Command::Locator {
            url,
            locator,
            headless,
            json,
        } => {
            use driver_drission::BrowserDriver;
            let mut driver = driver_drission::DrissionDriver::new();
            let config = workflow_schema::BrowserDefinition {
                mode: "launch".into(),
                headless,
                ..Default::default()
            };
            if let Err(e) = driver.launch(&config) {
                eprintln!("failed to launch browser: {e}");
                return Err(7);
            }
            if let Err(e) = driver.goto(&url) {
                eprintln!("failed to navigate to {url}: {e}");
                let _ = driver.close();
                return Err(7);
            }
            let find_result = driver.find(&locator, false);
            let _ = driver.highlight(&locator);
            let _ = driver.close();
            match find_result {
                Ok(result) => {
                    if json {
                        print_json(&result);
                    } else {
                        println!("Locator '{}' test on {}:", locator, url);
                        println!(
                            "  Matched: {}",
                            result
                                .get("exists")
                                .and_then(Value::as_bool)
                                .unwrap_or(false)
                        );
                        println!(
                            "  Count:   {}",
                            result.get("count").and_then(Value::as_u64).unwrap_or(0)
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    eprintln!("locator error: {e}");
                    Err(7)
                }
            }
        }
        Command::Pick { url, timeout, json } => {
            use driver_drission::BrowserDriver;
            println!("正在启动可视化浏览器进行实时点选，导航至: {}", url);
            let mut driver = driver_drission::DrissionDriver::new();
            let config = workflow_schema::BrowserDefinition {
                mode: "launch".into(),
                headless: false,
                ..Default::default()
            };
            if let Err(e) = driver.launch(&config) {
                eprintln!("failed to launch visible browser: {e}");
                return Err(7);
            }
            let pick_result = driver.interactive_pick(&url, timeout);
            let _ = driver.close();
            match pick_result {
                Ok(result) => {
                    if json {
                        print_json(&result);
                    } else {
                        println!("\n✓ 成功捕获元素:");
                        println!(
                            "  Locator:  {}",
                            result.get("locator").and_then(Value::as_str).unwrap_or("")
                        );
                        println!(
                            "  XPath:    {}",
                            result.get("xpath").and_then(Value::as_str).unwrap_or("")
                        );
                        println!(
                            "  Tag:      <{}>",
                            result.get("tag").and_then(Value::as_str).unwrap_or("")
                        );
                        println!(
                            "  Text:     \"{}\"",
                            result.get("text").and_then(Value::as_str).unwrap_or("")
                        );
                    }
                    Ok(())
                }
                Err(e) => {
                    eprintln!("pick error: {e}");
                    Err(7)
                }
            }
        }
        Command::Actions {
            command: ActionsCommand::List { json },
        } => {
            let actions: Vec<_> = registry.list().collect();
            if json {
                print_json(&actions);
            } else {
                for action in actions {
                    println!(
                        "{:<18} {:<24} {:<8} {:?}",
                        action.title, action.name, action.version, action.side_effect
                    );
                }
            }
            Ok(())
        }
        Command::Doctor { json } => {
            let database_parent = cli
                .database
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."));
            let runner = runner_command();
            let report = serde_json::json!({ "databaseParent": database_parent, "databaseParentExists": database_parent.exists(), "chromeCandidates": chrome_candidates(), "runner": runner.display().to_string(), "rustDrissionVersion": "0.2.5" });
            if json {
                print_json(&report);
            } else {
                println!(
                    "database directory: {} ({})",
                    database_parent.display(),
                    if database_parent.exists() {
                        "ok"
                    } else {
                        "will be created"
                    }
                );
                println!("runner: {}", runner.display());
                println!("Chrome candidates:");
                for path in chrome_candidates() {
                    println!("  {}", path);
                }
            }
            Ok(())
        }
        Command::Runner => run_runner_stdio(),
    }
}

fn control_path(database: &Path, run_id: &str) -> PathBuf {
    let parent = database.parent().unwrap_or_else(|| Path::new("."));
    parent.join("control").join(format!("{run_id}.json"))
}

fn write_control(database: &Path, run_id: &str, control: RunnerControl) -> Result<(), String> {
    let path = control_path(database, run_id);
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).map_err(|error| error.to_string())?;
    }
    let temporary = path.with_extension("json.tmp");
    fs::write(
        &temporary,
        serde_json::to_vec(&control).map_err(|error| error.to_string())?,
    )
    .map_err(|error| error.to_string())?;
    fs::rename(temporary, path).map_err(|error| error.to_string())
}

fn control_run(
    database: &Path,
    run_id: &str,
    control: RunnerControl,
    status: RunStatus,
    event_type: &str,
) -> Result<(), u8> {
    let store = SqliteStore::open(database).map_err(|error| {
        eprintln!("{error}");
        7
    })?;
    let mut run = store
        .get_run(run_id)
        .map_err(|error| {
            eprintln!("{error}");
            7
        })?
        .ok_or_else(|| {
            eprintln!("run not found: {run_id}");
            7
        })?;
    if matches!(
        run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled | RunStatus::Interrupted
    ) {
        eprintln!("run {} is already final: {:?}", run.id, run.status);
        return Err(6);
    }
    write_control(database, run_id, control).map_err(|error| {
        eprintln!("control update failed: {error}");
        7
    })?;
    run.status = status;
    store.update_run(&run).map_err(|error| {
        eprintln!("{error}");
        7
    })?;
    let sequence = store.next_sequence(run_id).map_err(|error| {
        eprintln!("{error}");
        7
    })?;
    store
        .append_event(&RunEvent {
            run_id: run_id.into(),
            sequence,
            event_type: event_type.into(),
            payload: Value::Object(Map::new()),
        })
        .map_err(|error| {
            eprintln!("{error}");
            7
        })?;
    println!("run: {run_id}\nstatus: {:?}", status);
    Ok(())
}

fn invoke_runner(request: &RunnerRequest) -> Result<RunnerResponse, String> {
    let runner = runner_command();
    let mut command = ProcessCommand::new(&runner);
    if runner == env::current_exe().map_err(|error| error.to_string())? {
        command.arg("runner");
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|error| format!("cannot start {}: {error}", runner.display()))?;
    let request_json = serde_json::to_vec(request).map_err(|error| error.to_string())?;
    child
        .stdin
        .take()
        .ok_or_else(|| "runner stdin is unavailable".to_string())?
        .write_all(&request_json)
        .map_err(|error| error.to_string())?;
    let output = child
        .wait_with_output()
        .map_err(|error| error.to_string())?;
    if output.stdout.is_empty() {
        return Err(format!(
            "runner exited with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    let mut response = None;
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        match serde_json::from_str::<RunnerOutput>(line) {
            Ok(RunnerOutput::Result { response: result }) => response = Some(*result),
            Ok(RunnerOutput::Event { .. }) => {}
            Err(error) => {
                return Err(format!(
                    "invalid runner response: {error}; stderr: {}",
                    String::from_utf8_lossy(&output.stderr).trim()
                ));
            }
        }
    }
    response.ok_or_else(|| {
        format!(
            "runner returned no result; stderr: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )
    })
}

fn runner_command() -> PathBuf {
    if let Some(path) = env::var_os("DRISSION_WORKFLOW_RUNNER") {
        return PathBuf::from(path);
    }
    let current = env::current_exe().unwrap_or_else(|_| PathBuf::from("drission-workflow"));
    let sibling = current.with_file_name(if cfg!(windows) {
        "drission-workflow-runner.exe"
    } else {
        "drission-workflow-runner"
    });
    if sibling.exists() { sibling } else { current }
}

fn run_runner_stdio() -> Result<(), u8> {
    use std::io::Read;
    let mut input = String::new();
    std::io::stdin()
        .read_to_string(&mut input)
        .map_err(|error| {
            eprintln!("failed to read runner request: {error}");
            2
        })?;
    let request: RunnerRequest = serde_json::from_str(&input).map_err(|error| {
        eprintln!("invalid runner request: {error}");
        2
    })?;
    let response =
        workflow_engine::execute_runner_request_with_events(request, |event_type, payload| {
            println!(
                "{}",
                serde_json::to_string(&RunnerOutput::Event {
                    event_type: event_type.into(),
                    payload,
                })
                .expect("runner event serializes")
            );
        });
    println!(
        "{}",
        serde_json::to_string(&RunnerOutput::Result {
            response: Box::new(response),
        })
        .expect("runner response serializes")
    );
    Ok(())
}

fn read(path: &Path) -> Result<String, u8> {
    fs::read_to_string(path).map_err(|error| {
        eprintln!("cannot read {}: {error}", path.display());
        2
    })
}
fn load_inputs(path: Option<&PathBuf>, pairs: Vec<(String, String)>) -> Result<Value, u8> {
    let mut object = if let Some(path) = path {
        serde_json::from_str::<Value>(&read(path)?)
            .map_err(|error| {
                eprintln!("invalid inputs JSON: {error}");
                2
            })?
            .as_object()
            .cloned()
            .ok_or_else(|| {
                eprintln!("inputs JSON must be an object");
                2
            })?
    } else {
        Map::new()
    };
    for (key, raw) in pairs {
        object.insert(
            key,
            serde_json::from_str(&raw).unwrap_or(Value::String(raw)),
        );
    }
    Ok(Value::Object(object))
}
fn parse_key_value(raw: &str) -> Result<(String, String), String> {
    raw.split_once('=')
        .map(|(key, value)| (key.into(), value.into()))
        .ok_or_else(|| "input must use NAME=VALUE".into())
}
fn print_json(value: &impl serde::Serialize) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).expect("serializable output")
    );
}
fn print_ir(ir: &workflow_schema::WorkflowIr) {
    println!("valid: {}", ir.name);
    println!("hash: {}", ir.content_hash);
    println!("steps: {}", count_steps(&ir.steps));
    println!(
        "permissions: {}",
        ir.required_permissions
            .iter()
            .cloned()
            .collect::<Vec<_>>()
            .join(", ")
    );
}
fn print_workflow_error(error: WorkflowError, json: bool) -> Result<(), u8> {
    match error {
        WorkflowError::Validation(diagnostics) => {
            if json {
                eprintln!("{}", serde_json::to_string_pretty(&diagnostics).unwrap());
            } else {
                for diagnostic in diagnostics {
                    eprintln!(
                        "{} [{}]: {}",
                        diagnostic.path, diagnostic.code, diagnostic.message
                    );
                }
            }
        }
        other => eprintln!("{other}"),
    }
    Err(3)
}
fn count_steps(steps: &[workflow_schema::IrStep]) -> usize {
    steps
        .iter()
        .map(|step| 1 + count_steps(&step.children))
        .sum()
}
fn chrome_candidates() -> Vec<String> {
    [
        "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome",
        "/Applications/Chromium.app/Contents/MacOS/Chromium",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
    ]
    .into_iter()
    .filter(|path| std::path::Path::new(path).exists())
    .map(str::to_owned)
    .collect()
}
