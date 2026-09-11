use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SideEffect {
    Pure,
    Idempotent,
    BrowserState,
    ExternalWrite,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunStatus {
    Queued,
    Preparing,
    Running,
    Pausing,
    Paused,
    Cancelling,
    Cancelled,
    Succeeded,
    Failed,
    Interrupted,
}

impl RunStatus {
    pub fn can_transition_to(self, next: Self) -> bool {
        use RunStatus::*;
        matches!(
            (self, next),
            (Queued, Preparing | Cancelled)
                | (Preparing, Running | Failed | Interrupted)
                | (
                    Running,
                    Pausing | Cancelling | Succeeded | Failed | Interrupted
                )
                | (Pausing, Paused | Cancelling | Failed | Interrupted)
                | (Paused, Running | Cancelling)
                | (Cancelling, Cancelled | Failed)
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RunCommand {
    Prepare,
    Start,
    RequestPause,
    ConfirmPause,
    Resume,
    RequestCancel,
    ConfirmCancel,
    Succeed,
    Fail,
    Interrupt,
}

pub fn transition_run(current: RunStatus, command: RunCommand) -> Result<RunStatus, &'static str> {
    use RunCommand::*;
    use RunStatus::*;
    let next = match (current, command) {
        (Queued, Prepare) => Preparing,
        (Preparing, Start) | (Pausing, Resume) | (Paused, Resume) => Running,
        (Running, RequestPause) => Pausing,
        (Pausing, ConfirmPause) => Paused,
        (Running | Pausing | Paused, RequestCancel) => Cancelling,
        (Queued, ConfirmCancel) | (Cancelling, ConfirmCancel) => Cancelled,
        (Running, Succeed) => Succeeded,
        (Preparing | Running | Pausing | Cancelling, Fail) => Failed,
        (Preparing | Running | Pausing | Cancelling, Interrupt) => Interrupted,
        _ => return Err("invalid run state transition"),
    };
    Ok(next)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StepStatus {
    Pending,
    Ready,
    Running,
    RetryWait,
    Succeeded,
    Skipped,
    Failed,
    Cancelled,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StepRunRecord {
    pub step_id: String,
    pub attempt: u32,
    pub status: StepStatus,
    pub output: Option<serde_json::Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunRecord {
    pub id: String,
    pub workflow_name: String,
    pub workflow_hash: String,
    #[serde(default)]
    pub workflow_id: Option<String>,
    #[serde(default)]
    pub workflow_version_id: Option<String>,
    #[serde(default = "legacy_run_mode")]
    pub run_mode: String,
    #[serde(default)]
    pub source_snapshot: Option<String>,
    #[serde(default)]
    pub draft_revision: Option<u64>,
    #[serde(default)]
    pub parent_run_id: Option<String>,
    #[serde(default)]
    pub resume_checkpoint_id: Option<String>,
    pub status: RunStatus,
    pub inputs: serde_json::Value,
    pub outputs: Option<serde_json::Value>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub steps: Vec<StepRunRecord>,
}

fn legacy_run_mode() -> String {
    "legacy".into()
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RunEvent {
    pub run_id: String,
    pub sequence: u64,
    pub event_type: String,
    pub payload: serde_json::Value,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkspaceRecord {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub status: String,
    pub settings: serde_json::Value,
    #[serde(default)]
    pub owner_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRecord {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub title: String,
    pub draft_source: String,
    pub draft_revision: u64,
    pub status: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkflowVersionRecord {
    pub id: String,
    pub workflow_id: String,
    pub version: u64,
    pub source: String,
    pub content_hash: String,
    pub required_permissions: serde_json::Value,
    pub change_note: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CheckpointRecord {
    pub id: String,
    pub run_id: String,
    pub step_id: String,
    pub workflow_hash: String,
    pub completed_steps: Vec<String>,
    pub context_snapshot: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct AuditRecord {
    pub id: String,
    pub workspace_id: Option<String>,
    pub action: String,
    pub resource_type: String,
    pub resource_id: String,
    pub actor: String,
    pub details: serde_json::Value,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScheduleRecord {
    pub id: String,
    pub workspace_id: String,
    pub workflow_id: String,
    pub name: String,
    pub cron_expression: String,
    pub inputs: serde_json::Value,
    pub enabled: bool,
    #[serde(default)]
    pub last_run_at: Option<String>,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WebhookRecord {
    pub id: String,
    pub workspace_id: String,
    pub workflow_id: String,
    pub name: String,
    pub token: String,
    pub enabled: bool,
    pub created_at: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CredentialRecord {
    pub id: String,
    pub workspace_id: String,
    pub name: String,
    pub kind: String,
    pub value: String,
    pub created_at: String,
}

/// SecretValue safely wraps sensitive credentials without exposing them in Debug or serialization
#[derive(Clone, PartialEq, Eq)]
pub struct SecretValue(String);

impl SecretValue {
    pub fn new(secret: impl Into<String>) -> Self {
        Self(secret.into())
    }
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Debug for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "SecretValue(***REDACTED***)")
    }
}

impl std::fmt::Display for SecretValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "***REDACTED***")
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ActionDescriptor {
    pub name: String,
    pub title: String,
    pub description: String,
    pub category: String,
    pub version: String,
    #[serde(default)]
    pub input_schema: serde_json::Value,
    #[serde(default)]
    pub output_schema: serde_json::Value,
    #[serde(default)]
    pub ui_schema: serde_json::Value,
    pub permissions: BTreeSet<String>,
    pub side_effect: SideEffect,
    #[serde(default = "default_timeout_ms")]
    pub default_timeout_ms: u64,
    #[serde(default)]
    pub sensitive_paths: Vec<String>,
}

fn default_timeout_ms() -> u64 {
    15_000
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Diagnostic {
    pub code: String,
    pub message: String,
    pub path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub line: Option<usize>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub column: Option<usize>,
}

impl Diagnostic {
    pub fn error(
        code: impl Into<String>,
        message: impl Into<String>,
        path: impl Into<String>,
    ) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            path: path.into(),
            line: None,
            column: None,
        }
    }

    pub fn at(mut self, line: usize, column: usize) -> Self {
        self.line = Some(line);
        self.column = Some(column);
        self
    }
}

#[derive(Debug, Error)]
pub enum WorkflowError {
    #[error("workflow validation failed")]
    Validation(Vec<Diagnostic>),
    #[error("unsupported DSL version: {0}")]
    UnsupportedVersion(String),
    #[error("unknown action: {0}")]
    UnknownAction(String),
    #[error("run not found: {0}")]
    RunNotFound(String),
}

#[cfg(test)]
mod tests {
    use super::RunStatus::*;

    #[test]
    fn run_state_machine_rejects_invalid_transitions() {
        assert!(Queued.can_transition_to(Preparing));
        assert!(Running.can_transition_to(Pausing));
        assert!(!Succeeded.can_transition_to(Running));
        assert!(!Queued.can_transition_to(Succeeded));
        assert_eq!(
            super::transition_run(Running, super::RunCommand::RequestCancel),
            Ok(Cancelling)
        );
        assert_eq!(
            super::transition_run(Pausing, super::RunCommand::Resume),
            Ok(Running)
        );
        assert!(super::transition_run(Cancelling, super::RunCommand::Succeed).is_err());
    }
}
