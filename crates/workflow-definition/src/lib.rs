use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
use thiserror::Error;
use workflow_actions::ActionRegistry;
use workflow_core::Diagnostic;
use workflow_schema::{BrowserDefinition, InputDefinition, Step, WorkflowDocument, WorkflowIr};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParseOutcome {
    pub valid: bool,
    pub document: Option<WorkflowDocument>,
    pub diagnostics: Vec<Diagnostic>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum EditCommand {
    InsertStep {
        index: usize,
        action: String,
    },
    MoveStep {
        step_id: String,
        index: usize,
    },
    UpdateStep {
        step_id: String,
        patch: StepPatch,
    },
    DeleteStep {
        step_id: String,
    },
    UpdateInputs {
        inputs: BTreeMap<String, InputDefinition>,
    },
    UpdateBrowser {
        browser: Option<BrowserDefinition>,
    },
    UpdateOutputs {
        outputs: BTreeMap<String, Value>,
    },
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct StepPatch {
    pub name: Option<String>,
    pub input: Option<BTreeMap<String, Value>>,
    pub timeout: Option<Option<String>>,
    pub retry: Option<Option<workflow_schema::RetryPolicy>>,
    pub condition: Option<Option<String>>,
    pub on_error: Option<Option<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EditOutcome {
    pub source: String,
    pub document: WorkflowDocument,
}

#[derive(Debug, Error)]
pub enum DefinitionError {
    #[error("workflow source is invalid")]
    InvalidSource(Vec<Diagnostic>),
    #[error("unknown action: {0}")]
    UnknownAction(String),
    #[error("step not found: {0}")]
    StepNotFound(String),
    #[error("step is referenced by another expression: {0}")]
    ReferencedStep(String),
    #[error("serialization failed: {0}")]
    Serialize(#[from] serde_yaml::Error),
}

#[derive(Debug, Clone, Default)]
pub struct WorkflowDefinition;

impl WorkflowDefinition {
    pub fn parse(&self, source: &str) -> ParseOutcome {
        match workflow_schema::parse_yaml(source) {
            Ok(document) => ParseOutcome {
                valid: true,
                document: Some(document),
                diagnostics: vec![],
            },
            Err(workflow_core::WorkflowError::Validation(diagnostics)) => ParseOutcome {
                valid: false,
                document: None,
                diagnostics,
            },
            Err(error) => ParseOutcome {
                valid: false,
                document: None,
                diagnostics: vec![Diagnostic::error(
                    "WORKFLOW_PARSE_ERROR",
                    error.to_string(),
                    "$;",
                )],
            },
        }
    }

    pub fn validate(
        &self,
        document: &WorkflowDocument,
        catalog: &ActionRegistry,
    ) -> Vec<Diagnostic> {
        workflow_schema::validate(document, catalog)
    }

    pub fn compile(
        &self,
        source: &str,
        catalog: &ActionRegistry,
    ) -> Result<WorkflowIr, DefinitionError> {
        workflow_schema::compile(source, catalog).map_err(|error| match error {
            workflow_core::WorkflowError::Validation(value) => {
                DefinitionError::InvalidSource(value)
            }
            workflow_core::WorkflowError::UnknownAction(value) => {
                DefinitionError::UnknownAction(value)
            }
            other => DefinitionError::InvalidSource(vec![Diagnostic::error(
                "WORKFLOW_COMPILE_ERROR",
                other.to_string(),
                "$",
            )]),
        })
    }

    pub fn apply(
        &self,
        source: &str,
        command: EditCommand,
        catalog: &ActionRegistry,
    ) -> Result<EditOutcome, DefinitionError> {
        let mut document = workflow_schema::parse_yaml(source).map_err(|error| match error {
            workflow_core::WorkflowError::Validation(value) => {
                DefinitionError::InvalidSource(value)
            }
            other => DefinitionError::InvalidSource(vec![Diagnostic::error(
                "WORKFLOW_PARSE_ERROR",
                other.to_string(),
                "$",
            )]),
        })?;
        match command {
            EditCommand::InsertStep { index, action } => {
                let descriptor = catalog
                    .get(&action)
                    .ok_or_else(|| DefinitionError::UnknownAction(action.clone()))?;
                let id = allocate_step_id(&document, &action);
                let step = Step {
                    id,
                    name: Some(descriptor.title.clone()),
                    action: Some(action),
                    input: defaults_from_schema(&descriptor.input_schema),
                    condition: None,
                    timeout: None,
                    retry: None,
                    on_error: None,
                    save_as: None,
                    foreach: None,
                    repeat: None,
                    uses: None,
                };
                let index = index.min(document.steps.len());
                document.steps.insert(index, step);
            }
            EditCommand::MoveStep { step_id, index } => {
                let old = document
                    .steps
                    .iter()
                    .position(|step| step.id == step_id)
                    .ok_or_else(|| DefinitionError::StepNotFound(step_id.clone()))?;
                let step = document.steps.remove(old);
                let index = index.min(document.steps.len());
                document.steps.insert(index, step);
            }
            EditCommand::UpdateStep { step_id, patch } => {
                let step = find_step_mut(&mut document.steps, &step_id)
                    .ok_or(DefinitionError::StepNotFound(step_id))?;
                if let Some(value) = patch.name {
                    step.name = Some(value);
                }
                if let Some(value) = patch.input {
                    step.input = value;
                }
                if let Some(value) = patch.timeout {
                    step.timeout = value;
                }
                if let Some(value) = patch.retry {
                    step.retry = value;
                }
                if let Some(value) = patch.condition {
                    step.condition = value;
                }
                if let Some(value) = patch.on_error {
                    step.on_error = value;
                }
            }
            EditCommand::DeleteStep { step_id } => {
                if document_references_step(&document, &step_id) {
                    return Err(DefinitionError::ReferencedStep(step_id));
                }
                if !delete_step(&mut document.steps, &step_id) {
                    return Err(DefinitionError::StepNotFound(step_id));
                }
            }
            EditCommand::UpdateInputs { inputs } => document.inputs = inputs,
            EditCommand::UpdateBrowser { browser } => document.browser = browser,
            EditCommand::UpdateOutputs { outputs } => document.outputs = outputs,
        }
        let source = self.serialize(&document)?;
        Ok(EditOutcome { source, document })
    }

    pub fn serialize(&self, document: &WorkflowDocument) -> Result<String, DefinitionError> {
        Ok(serde_yaml::to_string(document)?)
    }
}

fn allocate_step_id(document: &WorkflowDocument, action: &str) -> String {
    let base = action.replace('.', "_");
    let mut candidate = base.clone();
    let mut suffix = 2;
    while find_step(&document.steps, &candidate) {
        candidate = format!("{base}_{suffix}");
        suffix += 1;
    }
    candidate
}

fn find_step(steps: &[Step], id: &str) -> bool {
    steps.iter().any(|step| {
        step.id == id
            || step
                .foreach
                .as_ref()
                .is_some_and(|value| find_step(&value.steps, id))
            || step
                .repeat
                .as_ref()
                .is_some_and(|value| find_step(&value.steps, id))
    })
}
fn find_step_mut<'a>(steps: &'a mut [Step], id: &str) -> Option<&'a mut Step> {
    for step in steps {
        if step.id == id {
            return Some(step);
        }
        if let Some(value) = &mut step.foreach
            && let Some(found) = find_step_mut(&mut value.steps, id)
        {
            return Some(found);
        }
        if let Some(value) = &mut step.repeat
            && let Some(found) = find_step_mut(&mut value.steps, id)
        {
            return Some(found);
        }
    }
    None
}
fn delete_step(steps: &mut Vec<Step>, id: &str) -> bool {
    if let Some(index) = steps.iter().position(|step| step.id == id) {
        steps.remove(index);
        return true;
    }
    for step in steps {
        if let Some(value) = &mut step.foreach
            && delete_step(&mut value.steps, id)
        {
            return true;
        }
        if let Some(value) = &mut step.repeat
            && delete_step(&mut value.steps, id)
        {
            return true;
        }
    }
    false
}
fn document_references_step(document: &WorkflowDocument, id: &str) -> bool {
    let needle = format!("${{steps.{id}.");
    serde_json::to_string(document).is_ok_and(|value| value.contains(&needle))
}
fn defaults_from_schema(schema: &Value) -> BTreeMap<String, Value> {
    schema
        .get("properties")
        .and_then(Value::as_object)
        .map(|properties| {
            properties
                .iter()
                .filter_map(|(key, value)| {
                    value
                        .get("default")
                        .map(|default| (key.clone(), default.clone()))
                })
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    const SOURCE: &str = "apiVersion: drission.workflow/v1\nkind: Workflow\nmetadata: { name: demo }\nsteps: []\noutputs: {}\n";

    #[test]
    fn allocates_stable_unique_ids() {
        let module = WorkflowDefinition;
        let catalog = ActionRegistry::built_in();
        let first = module
            .apply(
                SOURCE,
                EditCommand::InsertStep {
                    index: 0,
                    action: "page.goto".into(),
                },
                &catalog,
            )
            .unwrap();
        let second = module
            .apply(
                &first.source,
                EditCommand::InsertStep {
                    index: 1,
                    action: "page.goto".into(),
                },
                &catalog,
            )
            .unwrap();
        assert_eq!(second.document.steps[0].id, "page_goto");
        assert_eq!(second.document.steps[1].id, "page_goto_2");
    }

    #[test]
    fn invalid_yaml_cannot_be_edited() {
        let error = WorkflowDefinition
            .apply(
                "steps: [",
                EditCommand::InsertStep {
                    index: 0,
                    action: "page.goto".into(),
                },
                &ActionRegistry::built_in(),
            )
            .unwrap_err();
        assert!(matches!(error, DefinitionError::InvalidSource(_)));
    }
}
