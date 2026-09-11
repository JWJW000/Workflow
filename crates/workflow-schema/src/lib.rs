use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use workflow_actions::ActionRegistry;
use workflow_core::{Diagnostic, SideEffect, WorkflowError};
use workflow_expression::references;

pub const API_VERSION: &str = "drission.workflow/v1";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkflowDocument {
    pub api_version: String,
    pub kind: String,
    pub metadata: Metadata,
    #[serde(default)]
    pub inputs: BTreeMap<String, InputDefinition>,
    #[serde(default)]
    pub browser: Option<BrowserDefinition>,
    #[serde(default)]
    pub defaults: Defaults,
    #[serde(default)]
    pub steps: Vec<Step>,
    #[serde(default)]
    pub outputs: BTreeMap<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Metadata {
    pub name: String,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub tags: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InputDefinition {
    #[serde(rename = "type")]
    pub value_type: String,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default, rename = "enum")]
    pub enum_values: Vec<Value>,
    #[serde(default)]
    pub minimum: Option<f64>,
    #[serde(default)]
    pub maximum: Option<f64>,
    #[serde(default)]
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrowserDefinition {
    #[serde(default = "default_browser_mode")]
    pub mode: String,
    #[serde(default)]
    pub endpoint: Option<String>,
    #[serde(default)]
    pub headless: bool,
    #[serde(default)]
    pub profile: Option<String>,
    #[serde(default)]
    pub download_dir: Option<String>,
    #[serde(default)]
    pub chrome_path: Option<String>,
    #[serde(default)]
    pub user_agent: Option<String>,
    #[serde(default)]
    pub proxy: Option<String>,
    #[serde(default)]
    pub no_images: bool,
    #[serde(default)]
    pub args: Vec<String>,
}

fn default_browser_mode() -> String {
    "launch".into()
}

impl Default for BrowserDefinition {
    fn default() -> Self {
        Self {
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
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Defaults {
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RetryPolicy {
    pub max_attempts: u32,
    #[serde(default)]
    pub backoff: Option<String>,
    #[serde(default)]
    pub initial_delay: Option<String>,
    #[serde(default)]
    pub max_delay: Option<String>,
    #[serde(default)]
    pub jitter: Option<f64>,
    #[serde(default)]
    pub on: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Step {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub action: Option<String>,
    #[serde(default, rename = "with")]
    pub input: BTreeMap<String, Value>,
    #[serde(default, rename = "if")]
    pub condition: Option<String>,
    #[serde(default)]
    pub timeout: Option<String>,
    #[serde(default)]
    pub retry: Option<RetryPolicy>,
    #[serde(default)]
    pub on_error: Option<String>,
    #[serde(default)]
    pub save_as: Option<String>,
    #[serde(default)]
    pub foreach: Option<Foreach>,
    #[serde(default)]
    pub repeat: Option<Repeat>,
    #[serde(default)]
    pub uses: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Foreach {
    pub items: Value,
    #[serde(default = "default_item_name")]
    pub item_name: String,
    pub max_items: u32,
    #[serde(default = "one")]
    pub concurrency: u32,
    pub steps: Vec<Step>,
}

fn default_item_name() -> String {
    "item".to_owned()
}
fn one() -> u32 {
    1
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Repeat {
    pub max_iterations: u32,
    pub until: String,
    pub steps: Vec<Step>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowIr {
    pub api_version: String,
    pub name: String,
    pub content_hash: String,
    pub required_permissions: BTreeSet<String>,
    pub document: WorkflowDocument,
    pub steps: Vec<IrStep>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrStep {
    pub id: String,
    pub action: Option<String>,
    pub side_effect: SideEffect,
    pub children: Vec<IrStep>,
}

pub fn parse_yaml(source: &str) -> Result<WorkflowDocument, WorkflowError> {
    serde_yaml::from_str(source).map_err(|error| {
        let diagnostic = Diagnostic::error("YAML_PARSE_ERROR", error.to_string(), "$");
        let diagnostic = error.location().map_or(diagnostic.clone(), |location| {
            diagnostic.at(location.line(), location.column())
        });
        WorkflowError::Validation(vec![diagnostic])
    })
}

pub fn compile(source: &str, registry: &ActionRegistry) -> Result<WorkflowIr, WorkflowError> {
    let document = parse_yaml(source)?;
    let diagnostics = validate(&document, registry);
    if !diagnostics.is_empty() {
        return Err(WorkflowError::Validation(diagnostics));
    }

    let normalized = serde_json::to_vec(&document).expect("serializable workflow document");
    let content_hash = hex::encode(Sha256::digest(normalized));
    let mut required_permissions = BTreeSet::new();
    let steps = compile_steps(&document.steps, registry, &mut required_permissions);

    Ok(WorkflowIr {
        api_version: document.api_version.clone(),
        name: document.metadata.name.clone(),
        content_hash,
        required_permissions,
        steps,
        document,
    })
}

pub fn validate(document: &WorkflowDocument, registry: &ActionRegistry) -> Vec<Diagnostic> {
    let mut diagnostics = Vec::new();
    if document.api_version != API_VERSION {
        diagnostics.push(Diagnostic::error(
            "UNSUPPORTED_API_VERSION",
            format!("expected {API_VERSION}"),
            "apiVersion",
        ));
    }
    if document.kind != "Workflow" {
        diagnostics.push(Diagnostic::error(
            "INVALID_KIND",
            "kind must be Workflow",
            "kind",
        ));
    }
    if let Some(browser) = &document.browser {
        if !matches!(browser.mode.as_str(), "launch" | "connect") {
            diagnostics.push(Diagnostic::error(
                "INVALID_BROWSER_MODE",
                "browser.mode must be launch or connect",
                "browser.mode",
            ));
        }
        if browser.mode == "connect" && browser.endpoint.as_deref().unwrap_or_default().is_empty() {
            diagnostics.push(Diagnostic::error(
                "MISSING_BROWSER_ENDPOINT",
                "browser.endpoint is required in connect mode",
                "browser.endpoint",
            ));
        }
    }
    if document.metadata.name.is_empty()
        || !document
            .metadata
            .name
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
    {
        diagnostics.push(Diagnostic::error(
            "INVALID_WORKFLOW_NAME",
            "metadata.name must contain lowercase letters, digits or hyphens",
            "metadata.name",
        ));
    }

    validate_steps(
        &document.steps,
        registry,
        &document.inputs,
        &mut BTreeSet::new(),
        "steps",
        &mut diagnostics,
    );
    diagnostics
}

fn validate_steps(
    steps: &[Step],
    registry: &ActionRegistry,
    inputs: &BTreeMap<String, InputDefinition>,
    ids: &mut BTreeSet<String>,
    path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    for (index, step) in steps.iter().enumerate() {
        let known_step_ids: BTreeSet<_> = ids.iter().cloned().collect();
        let step_path = format!("{path}[{index}]");
        if step.id.is_empty()
            || !step
                .id
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        {
            diagnostics.push(Diagnostic::error(
                "INVALID_STEP_ID",
                "step id contains invalid characters",
                format!("{step_path}.id"),
            ));
        }
        if !ids.insert(step.id.clone()) {
            diagnostics.push(Diagnostic::error(
                "DUPLICATE_STEP_ID",
                format!("duplicate step id: {}", step.id),
                format!("{step_path}.id"),
            ));
        }

        let control_count = usize::from(step.foreach.is_some())
            + usize::from(step.repeat.is_some())
            + usize::from(step.uses.is_some());
        if step.action.is_some() as usize + control_count != 1 {
            diagnostics.push(Diagnostic::error(
                "INVALID_STEP_SHAPE",
                "step must define exactly one of action, foreach, repeat or uses",
                &step_path,
            ));
        }
        if let Some(action) = &step.action
            && registry.get(action).is_none()
        {
            diagnostics.push(Diagnostic::error(
                "UNKNOWN_ACTION",
                format!("unknown action: {action}"),
                format!("{step_path}.action"),
            ));
        }
        if let Some(retry) = &step.retry
            && retry.max_attempts == 0
        {
            diagnostics.push(Diagnostic::error(
                "INVALID_RETRY",
                "maxAttempts must be at least 1",
                format!("{step_path}.retry.maxAttempts"),
            ));
        }
        if let Some(foreach) = &step.foreach {
            if foreach.max_items == 0 {
                diagnostics.push(Diagnostic::error(
                    "INVALID_LOOP_BOUND",
                    "maxItems must be at least 1",
                    format!("{step_path}.foreach.maxItems"),
                ));
            }
            if foreach.concurrency == 0 {
                diagnostics.push(Diagnostic::error(
                    "INVALID_CONCURRENCY",
                    "concurrency must be at least 1",
                    format!("{step_path}.foreach.concurrency"),
                ));
            }
            if foreach.concurrency != 1 {
                diagnostics.push(Diagnostic::error(
                    "UNSUPPORTED_CONCURRENCY",
                    "v1 foreach concurrency must be 1",
                    format!("{step_path}.foreach.concurrency"),
                ));
            }
            validate_steps(
                &foreach.steps,
                registry,
                inputs,
                ids,
                &format!("{step_path}.foreach.steps"),
                diagnostics,
            );
            // Inner steps already validated their own references. Checking the
            // serialized parent step would treat nested `${steps.extract...}`
            // as unknown because those ids were not in the pre-loop snapshot.
            validate_value_references(
                &foreach.items,
                inputs,
                ids,
                &format!("{step_path}.foreach.items"),
                diagnostics,
            );
        }
        if let Some(repeat) = &step.repeat {
            if repeat.max_iterations == 0 {
                diagnostics.push(Diagnostic::error(
                    "INVALID_LOOP_BOUND",
                    "maxIterations must be at least 1",
                    format!("{step_path}.repeat.maxIterations"),
                ));
            }
            validate_steps(
                &repeat.steps,
                registry,
                inputs,
                ids,
                &format!("{step_path}.repeat.steps"),
                diagnostics,
            );
        }
        if step.foreach.is_none() && step.repeat.is_none() {
            validate_value_references(
                &serde_json::to_value(step).unwrap(),
                inputs,
                &known_step_ids,
                &step_path,
                diagnostics,
            );
        }
    }
}

fn validate_value_references(
    value: &Value,
    inputs: &BTreeMap<String, InputDefinition>,
    known_step_ids: &BTreeSet<String>,
    path: &str,
    diagnostics: &mut Vec<Diagnostic>,
) {
    match value {
        Value::String(text) => match references(text) {
            Ok(references) => {
                for reference in references {
                    let mut segments = reference.path.split('.');
                    match (segments.next(), segments.next()) {
                        (Some("inputs"), Some(name)) if !inputs.contains_key(name) => diagnostics
                            .push(Diagnostic::error(
                                "UNKNOWN_INPUT_REFERENCE",
                                format!("unknown input: {name}"),
                                path,
                            )),
                        (Some("steps"), Some(id)) if !known_step_ids.contains(id) => diagnostics
                            .push(Diagnostic::error(
                                "FORWARD_OR_UNKNOWN_STEP_REFERENCE",
                                format!("step output must reference a preceding step: {id}"),
                                path,
                            )),
                        _ => {}
                    }
                }
            }
            Err(error) => diagnostics.push(Diagnostic::error(
                "INVALID_EXPRESSION",
                error.to_string(),
                path,
            )),
        },
        Value::Array(values) => {
            for value in values {
                validate_value_references(value, inputs, known_step_ids, path, diagnostics);
            }
        }
        Value::Object(values) => {
            for value in values.values() {
                validate_value_references(value, inputs, known_step_ids, path, diagnostics);
            }
        }
        _ => {}
    }
}

fn compile_steps(
    steps: &[Step],
    registry: &ActionRegistry,
    permissions: &mut BTreeSet<String>,
) -> Vec<IrStep> {
    steps
        .iter()
        .map(|step| {
            let descriptor = step.action.as_deref().and_then(|name| registry.get(name));
            if let Some(descriptor) = descriptor {
                permissions.extend(descriptor.permissions.iter().cloned());
            }
            let nested = step
                .foreach
                .as_ref()
                .map(|v| &v.steps)
                .or_else(|| step.repeat.as_ref().map(|v| &v.steps));
            IrStep {
                id: step.id.clone(),
                action: step.action.clone(),
                side_effect: descriptor
                    .map(|value| value.side_effect)
                    .unwrap_or(SideEffect::Pure),
                children: nested
                    .map(|steps| compile_steps(steps, registry, permissions))
                    .unwrap_or_default(),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID: &str = r#"
apiVersion: drission.workflow/v1
kind: Workflow
metadata:
  name: example
inputs:
  target_url:
    type: string
    format: uri
    required: true
steps:
  - id: open
    action: page.goto
    with:
      url: "${inputs.target_url}"
  - id: save
    action: file.writeJson
    with:
      path: result.json
      data: "${steps.open.output}"
outputs: {}
"#;

    #[test]
    fn compiles_valid_workflow_to_stable_ir() {
        let ir = compile(VALID, &ActionRegistry::built_in()).unwrap();
        assert_eq!(ir.steps.len(), 2);
        assert!(ir.required_permissions.contains("browser.interact"));
        assert!(ir.required_permissions.contains("file.artifact.write"));
        assert_eq!(ir.content_hash.len(), 64);
    }

    #[test]
    fn rejects_forward_references_and_unknown_actions() {
        let source = VALID
            .replace("page.goto", "page.missing")
            .replace("steps.open.output", "steps.later.output");
        let error = compile(&source, &ActionRegistry::built_in()).unwrap_err();
        let WorkflowError::Validation(diagnostics) = error else {
            panic!("expected validation error")
        };
        assert!(
            diagnostics
                .iter()
                .any(|value| value.code == "UNKNOWN_ACTION")
        );
        assert!(
            diagnostics
                .iter()
                .any(|value| value.code == "FORWARD_OR_UNKNOWN_STEP_REFERENCE")
        );
    }
}
