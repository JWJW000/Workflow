use async_stream::stream;
use axum::{
    Extension, Json, Router,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::{
        IntoResponse, Sse,
        sse::{Event, KeepAlive},
    },
    routing::{get, post},
};
use clap::Parser;
use serde::Deserialize;
use serde_json::{Map, Value, json};
use sha2::Digest;
use std::{
    convert::Infallible,
    fs,
    io::{BufRead, BufReader, Write},
    net::SocketAddr,
    path::{Path as FsPath, PathBuf},
    process::{Command, Stdio},
    sync::Arc,
    time::Duration,
};
use tower_http::trace::TraceLayer;
use uuid::Uuid;
use workflow_actions::ActionRegistry;
use workflow_core::{RunCommand, RunEvent, RunRecord, RunStatus, transition_run};
use workflow_definition::{EditCommand, WorkflowDefinition};
use workflow_engine::{RunnerControl, RunnerOutput, RunnerRequest};
use workflow_store::SqliteStore;

#[derive(Debug, Parser)]
struct Args {
    #[arg(long, default_value = "127.0.0.1:8787")]
    listen: SocketAddr,
    #[arg(long, default_value = ".drission-workflow/workflows.db")]
    database: PathBuf,
    #[arg(long, default_value = "artifacts")]
    artifacts: PathBuf,
    #[arg(long)]
    runner: Option<PathBuf>,
    #[arg(long)]
    auth_secret: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize, Deserialize)]
pub struct DelegatedClaims {
    pub iss: String,
    pub aud: String,
    pub sub: String,
    pub username: String,
    pub role: String,
    pub exp: i64,
    pub iat: i64,
    pub jti: String,
}

impl DelegatedClaims {
    pub fn is_super_admin(&self) -> bool {
        self.role == "超级管理员"
    }

    pub fn audit_actor(&self) -> String {
        format!("{}:{}:{}", self.iss, self.sub, self.username)
    }

    pub fn check_workspace_access(&self, store: &SqliteStore, target_ws_id: &str) -> Result<(), (StatusCode, Json<Value>)> {
        // 超级管理员全局放行
        if self.is_super_admin() {
            return Ok(());
        }
        // 普通任务管理员或只读用户：查验工作空间是否存在且归属当前用户（owner 或 member）
        match store.has_workspace_access(target_ws_id, &self.sub) {
            Ok(true) => Ok(()),
            Ok(false) => Err((
                StatusCode::FORBIDDEN,
                Json(json!({
                    "error": {
                        "code": "WORKSPACE_FORBIDDEN",
                        "message": format!("当前用户无权访问工作空间 [{}]", target_ws_id)
                    }
                })),
            )),
            Err(e) => Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({
                    "error": {
                        "code": "INTERNAL_ERROR",
                        "message": e.to_string()
                    }
                })),
            )),
        }
    }
}

#[derive(Clone)]
struct AppState {
    database: Arc<PathBuf>,
    artifacts: Arc<PathBuf>,
    runner: Arc<PathBuf>,
    auth_secret: Option<Arc<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EventQuery {
    after_sequence: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct ListQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateRunRequest {
    #[serde(default)]
    workflow_id: Option<String>,
    #[serde(default = "debug_mode")]
    mode: String,
    #[serde(default)]
    draft_revision: Option<u64>,
    #[serde(default)]
    version: Option<u64>,
    #[serde(default)]
    workflow_source: Option<String>,
    #[serde(default)]
    inputs: Value,
}
fn debug_mode() -> String {
    "debug".into()
}

#[derive(Debug, Deserialize)]
struct CreateWorkspaceRequest {
    name: String,
    slug: String,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateWorkflowRequest {
    name: String,
    title: String,
    draft_source: Option<String>,
}
#[derive(Debug, Deserialize)]
struct SaveDraftRequest {
    revision: u64,
    source: String,
}
#[derive(Debug, Deserialize)]
struct ApplyEditRequest {
    revision: u64,
    command: EditCommand,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PublishRequest {
    change_note: Option<String>,
    #[serde(alias = "expected_revision")]
    expected_revision: Option<u64>,
}
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct TestStepRequest {
    action: String,
    #[serde(default)]
    input: Value,
    #[serde(default)]
    mock_context: Option<Value>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateScheduleRequest {
    workflow_id: String,
    name: String,
    cron_expression: String,
    #[serde(default)]
    inputs: Value,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CreateWebhookRequest {
    workflow_id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SaveCredentialRequest {
    name: String,
    kind: String,
    value: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AiGenerateWorkflowRequest {
    prompt: String,
    name: Option<String>,
    title: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct InteractivePickRequest {
    url: String,
    #[serde(default = "default_pick_timeout")]
    timeout_secs: u64,
}
fn default_pick_timeout() -> u64 {
    90
}

fn process_alive(pid: u32) -> bool {
    Command::new("kill")
        .arg("-0")
        .arg(pid.to_string())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn cleanup_orphan_runners_and_locks() {
    if let Ok(output) = Command::new("ps")
        .args(["-axo", "pid=,ppid=,comm="])
        .output()
    {
        for line in String::from_utf8_lossy(&output.stdout).lines() {
            let parts: Vec<_> = line.split_whitespace().collect();
            if parts.len() >= 3
                && parts[1] == "1"
                && parts[2].contains("drission-workflow-runner")
                && let Ok(pid) = parts[0].parse::<u32>()
            {
                let _ = Command::new("kill").arg(pid.to_string()).status();
            }
        }
        std::thread::sleep(Duration::from_millis(300));
    }
    let Ok(profiles) = fs::read_dir(".drission-workflow/profiles") else {
        return;
    };
    for entry in profiles.flatten() {
        let lock = entry.path().join(".drission_profile.lock");
        let Ok(content) = fs::read_to_string(&lock) else {
            continue;
        };
        if content
            .trim()
            .parse::<u32>()
            .ok()
            .is_none_or(|pid| !process_alive(pid))
        {
            let _ = fs::remove_file(lock);
        }
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();

    // R3-1: 启动前首先校验必需的委托密钥，缺失/空/空白/过短必须在任何其他初始化之前立即失败退出
    let raw_secret = args.auth_secret.or_else(|| std::env::var("INTERNAL_AUTH_SECRET").ok());
    let trimmed_secret = raw_secret.as_deref().map(str::trim).filter(|s| !s.is_empty());
    let auth_secret = match trimmed_secret {
        Some(s) if s.len() >= 16 => Arc::new(s.to_string()),
        Some(_) => {
            eprintln!("错误：INTERNAL_AUTH_SECRET 密钥长度必须至少为 16 个字符");
            std::process::exit(101);
        }
        None => {
            eprintln!("错误：必须配置非空的 INTERNAL_AUTH_SECRET 委托认证密钥 (长度 >= 16)");
            std::process::exit(101);
        }
    };

    if let Ok(store) = SqliteStore::open(&args.database) {
        let _ = store.mark_incomplete_interrupted();
    }
    cleanup_orphan_runners_and_locks();
    let runner = args.runner.unwrap_or_else(runner_command);
    let state = AppState {
        database: Arc::new(args.database),
        artifacts: Arc::new(args.artifacts),
        runner: Arc::new(runner),
        auth_secret: Some(auth_secret),
    };
    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(args.listen)
        .await
        .expect("bind API listener");
    println!("Drission Workflow API listening on http://{}", args.listen);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown())
        .await
        .expect("serve API");
}

async fn auth_middleware(
    State(state): State<AppState>,
    mut req: axum::extract::Request,
    next: axum::middleware::Next,
) -> Result<impl IntoResponse, (StatusCode, Json<Value>)> {
    let path = req.uri().path();
    // 豁免健康检查及 Webhook 专用机器触发入口
    if path == "/health" || path.starts_with("/api/v1/hooks/") {
        return Ok(next.run(req).await);
    }

    let secret = match state.auth_secret {
        Some(ref s) => s.as_str(),
        None => {
            // R1-1: 严禁通过 Host 请求头伪装本地请求绕过认证，缺少密钥直接拒绝服务 (Fail-Closed)
            return Err((
                StatusCode::SERVICE_UNAVAILABLE,
                Json(json!({ "error": "服务端未配置委托认证密钥 (INTERNAL_AUTH_SECRET)，拒绝访问" })),
            ));
        }
    };

    let token_opt = req
        .headers()
        .get("x-delegated-token")
        .and_then(|h| h.to_str().ok())
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());

    let token = match token_opt {
        Some(t) => t,
        None => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": "缺少委托身份凭据 (x-delegated-token)" })),
            ));
        }
    };

    let mut validation = jsonwebtoken::Validation::new(jsonwebtoken::Algorithm::HS256);
    validation.leeway = 5;
    validation.set_audience(&["workflow-api"]);
    validation.set_issuer(&["ruoyi-plus"]);

    let key = jsonwebtoken::DecodingKey::from_secret(secret.as_bytes());
    match jsonwebtoken::decode::<DelegatedClaims>(token, &key, &validation) {
        Ok(token_data) => {
            let claims = token_data.claims;
            // 角色白名单校验
            let role = claims.role.as_str();
            if role != "超级管理员" && role != "任务管理员" && role != "只读用户" {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "未知或未授权的访问角色" })),
                ));
            }

            // 只读用户禁止写操作
            let method = req.method();
            let is_write = method == axum::http::Method::POST
                || method == axum::http::Method::PUT
                || method == axum::http::Method::DELETE;
            if is_write && role == "只读用户" {
                return Err((
                    StatusCode::FORBIDDEN,
                    Json(json!({ "error": "当前只读权限禁止执行变更" })),
                ));
            }

            // 向请求上下文注入已认证主体 (R1-4)
            req.extensions_mut().insert(claims);
        }
        Err(e) => {
            return Err((
                StatusCode::UNAUTHORIZED,
                Json(json!({ "error": format!("委托身份无效或已过期: {e}") })),
            ));
        }
    }

    Ok(next.run(req).await)
}

async fn health(State(state): State<AppState>) -> impl IntoResponse {
    match SqliteStore::open(state.database.as_ref()) {
        Ok(_) => (
            StatusCode::OK,
            Json(
                json!({ "status": "ok", "store": "ok", "runner": state.runner.display().to_string() }),
            ),
        ),
        Err(error) => (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({ "status": "degraded", "store": error.to_string() })),
        ),
    }
}

async fn actions() -> Json<Value> {
    let registry = ActionRegistry::built_in();
    Json(serde_json::to_value(registry.list().collect::<Vec<_>>()).expect("actions serialize"))
}

async fn workspace_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let result = if let Some(Extension(ref c)) = claims {
        if c.is_super_admin() {
            store.list_workspaces()
        } else {
            store.list_workspaces_for_user(&c.sub)
        }
    } else {
        store.list_workspaces()
    };
    match result {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}
async fn schedule_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    match store.list_schedules(&id) {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn create_schedule_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<CreateScheduleRequest>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    let s = workflow_core::ScheduleRecord {
        id: Uuid::new_v4().to_string(),
        workspace_id: id,
        workflow_id: req.workflow_id,
        name: req.name,
        cron_expression: req.cron_expression,
        inputs: req.inputs,
        enabled: true,
        last_run_at: None,
        created_at: String::new(),
    };
    match store.create_schedule(&s) {
        Ok(()) => (StatusCode::CREATED, Json(json!(s))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn delete_schedule_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_schedule(&id) {
                Ok(Some(s)) => {
                    if let Err(r) = c.check_workspace_access(&store, &s.workspace_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => return error(StatusCode::NOT_FOUND, "SCHEDULE_NOT_FOUND", "schedule not found"),
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match store.delete_schedule(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "deleted": true }))).into_response(),
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            "SCHEDULE_NOT_FOUND",
            "schedule not found",
        ),
        Err(e) => internal(e.to_string()),
    }
}

async fn webhook_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    match store.list_webhooks(&id) {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn create_webhook_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<CreateWebhookRequest>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    let token = format!("whk_{}", Uuid::new_v4().simple());
    let w = workflow_core::WebhookRecord {
        id: Uuid::new_v4().to_string(),
        workspace_id: id,
        workflow_id: req.workflow_id,
        name: req.name,
        token,
        enabled: true,
        created_at: String::new(),
    };
    match store.create_webhook(&w) {
        Ok(()) => (StatusCode::CREATED, Json(json!(w))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn delete_webhook_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_webhook(&id) {
                Ok(Some(w)) => {
                    if let Err(r) = c.check_workspace_access(&store, &w.workspace_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => return error(StatusCode::NOT_FOUND, "WEBHOOK_NOT_FOUND", "webhook not found"),
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match store.delete_webhook(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "deleted": true }))).into_response(),
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            "WEBHOOK_NOT_FOUND",
            "webhook not found",
        ),
        Err(e) => internal(e.to_string()),
    }
}

async fn trigger_webhook(
    State(state): State<AppState>,
    Path(token): Path<String>,
    body: axum::body::Bytes,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let webhook = match store.get_webhook_by_token(&token) {
        Ok(Some(w)) => w,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WEBHOOK_NOT_FOUND",
                "webhook token not valid or disabled",
            );
        }
        Err(e) => return internal(e.to_string()),
    };
    let workflow = match store.get_workflow(&webhook.workflow_id) {
        Ok(Some(wf)) => wf,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKFLOW_NOT_FOUND",
                "target workflow not found",
            );
        }
        Err(e) => return internal(e.to_string()),
    };
    let payload = if body.is_empty() {
        Value::Null
    } else {
        serde_json::from_slice::<Value>(&body).unwrap_or(Value::Null)
    };
    let mut inputs_map = serde_json::Map::new();
    inputs_map.insert("webhook_payload".into(), payload.clone());
    if let Some(obj) = payload.as_object() {
        for (k, v) in obj {
            inputs_map.insert(k.clone(), v.clone());
        }
    }
    let inputs = Value::Object(inputs_map);
    let versions = store.list_versions(&workflow.id).unwrap_or_default();
    let (mode, version, draft_revision) = if let Some(latest) = versions.first() {
        ("published".to_string(), Some(latest.version), None)
    } else {
        ("debug".to_string(), None, Some(workflow.draft_revision))
    };

    let run_req = CreateRunRequest {
        workflow_id: Some(workflow.id),
        mode,
        draft_revision,
        version,
        workflow_source: Some(workflow.draft_source),
        inputs,
    };
    create_run(State(state), None, HeaderMap::new(), Json(run_req))
        .await
        .into_response()
}

async fn credential_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    match store.list_credentials(&id) {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn save_credential_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<SaveCredentialRequest>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let actor_name = if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
        c.audit_actor()
    } else {
        "user".to_string()
    };
    let c = workflow_core::CredentialRecord {
        id: Uuid::new_v4().to_string(),
        workspace_id: id.clone(),
        name: req.name,
        kind: req.kind,
        value: req.value,
        created_at: String::new(),
    };
    match store.save_credential(&c) {
        Ok(()) => {
            let _ = store.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(id),
                action: "credential.save".into(),
                resource_type: "credential".into(),
                resource_id: c.id.clone(),
                actor: actor_name,
                details: json!({ "name": c.name, "kind": c.kind }),
                created_at: String::new(),
            });
            (
                StatusCode::OK,
                Json(json!({ "id": c.id, "name": c.name, "kind": c.kind })),
            )
                .into_response()
        }
        Err(e) => internal(e.to_string()),
    }
}

async fn delete_credential_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_credential_by_id(&id) {
                Ok(Some(cred)) => {
                    if let Err(r) = c.check_workspace_access(&store, &cred.workspace_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => return error(StatusCode::NOT_FOUND, "CREDENTIAL_NOT_FOUND", "credential not found"),
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match store.delete_credential(&id) {
        Ok(true) => (StatusCode::OK, Json(json!({ "deleted": true }))).into_response(),
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            "CREDENTIAL_NOT_FOUND",
            "credential not found",
        ),
        Err(e) => internal(e.to_string()),
    }
}

async fn workspace_audit(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &id) {
            return r.into_response();
        }
    }
    match store.list_audit_logs(Some(&id), 100) {
        Ok(logs) => (StatusCode::OK, Json(json!(logs))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn run_checkpoints(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match store.get_latest_checkpoint_for_run(&id) {
        Ok(Some(cp)) => (StatusCode::OK, Json(json!(cp))).into_response(),
        Ok(None) => (StatusCode::OK, Json(json!(null))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn resume_checkpoint_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    let original_run = match store.get_run(&id) {
        Ok(Some(r)) => r,
        Ok(None) => return error(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "run not found"),
        Err(e) => return internal(e.to_string()),
    };
    let checkpoint = match store.get_latest_checkpoint_for_run(&id) {
        Ok(Some(cp)) => cp,
        Ok(None) => {
            return error(
                StatusCode::BAD_REQUEST,
                "NO_CHECKPOINT",
                "该运行记录未找到可用的执行检查点",
            );
        }
        Err(e) => return internal(e.to_string()),
    };

    let source = if let Some(snap) = original_run.source_snapshot.clone() {
        snap
    } else if let Some(w_id) = &original_run.workflow_id {
        match store.get_workflow(w_id) {
            Ok(Some(w)) => w.draft_source,
            _ => {
                return error(
                    StatusCode::BAD_REQUEST,
                    "SOURCE_NOT_FOUND",
                    "无法获取工作流源码",
                );
            }
        }
    } else {
        return error(
            StatusCode::BAD_REQUEST,
            "SOURCE_NOT_FOUND",
            "无法获取工作流源码",
        );
    };

    let new_run_id = Uuid::new_v4().to_string();
    let artifact_root = state.artifacts.join(&new_run_id);
    let control = control_path(&state.database, &new_run_id);
    if let Err(error_val) = write_control(&control, RunnerControl::Running) {
        return internal(error_val);
    }

    let queued = RunRecord {
        id: new_run_id.clone(),
        workflow_name: original_run.workflow_name.clone(),
        workflow_hash: original_run.workflow_hash.clone(),
        workflow_id: original_run.workflow_id.clone(),
        workflow_version_id: original_run.workflow_version_id.clone(),
        run_mode: original_run.run_mode.clone(),
        source_snapshot: Some(source.clone()),
        draft_revision: original_run.draft_revision,
        parent_run_id: Some(original_run.id.clone()),
        resume_checkpoint_id: Some(checkpoint.id.clone()),
        status: RunStatus::Queued,
        inputs: original_run.inputs.clone(),
        outputs: None,
        error_code: None,
        error_message: None,
        steps: vec![],
    };

    if let Err(e) = store.create_run(&queued) {
        return internal(e.to_string());
    }

    let _ = store.append_event(&RunEvent {
        run_id: new_run_id.clone(),
        sequence: 1,
        event_type: "run.queued".into(),
        payload: json!({ "resumeFrom": original_run.id, "checkpointId": checkpoint.id }),
    });

    let runner_request = RunnerRequest {
        run_id: new_run_id.clone(),
        workflow_source: source,
        inputs: original_run.inputs,
        artifact_root,
        control_path: Some(control),
        completed_steps: checkpoint.completed_steps,
        initial_context: Some(checkpoint.context_snapshot),
    };

    let background_state = state.clone();
    tokio::task::spawn_blocking(move || execute_background(background_state, runner_request));

    (
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(queued).unwrap()),
    )
        .into_response()
}

async fn run_artifacts(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    let root = state.artifacts.join(&id);
    if !root.exists() {
        return (StatusCode::OK, Json(json!([]))).into_response();
    }
    let mut files = Vec::new();
    fn scan(dir: &FsPath, base: &FsPath, out: &mut Vec<Value>) {
        if let Ok(entries) = fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan(&path, base, out);
                } else if let Ok(rel) = path.strip_prefix(base) {
                    let byte_count = entry.metadata().map(|m| m.len()).unwrap_or(0);
                    out.push(json!({
                        "path": rel.to_string_lossy().replace('\\', "/"),
                        "name": entry.file_name().to_string_lossy(),
                        "byteCount": byte_count,
                    }));
                }
            }
        }
    }
    scan(&root, &root, &mut files);
    (StatusCode::OK, Json(json!(files))).into_response()
}

async fn get_artifact_file(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path((id, file_path)): Path<(String, String)>,
) -> impl IntoResponse {
    let store_db = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store_db.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store_db, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    let root = state.artifacts.join(&id);
    let store = match workflow_artifacts::ArtifactStore::new(&root) {
        Ok(s) => s,
        Err(e) => return error(StatusCode::BAD_REQUEST, "ARTIFACT_ERROR", &e.to_string()),
    };
    let resolved = match store.resolve(&file_path) {
        Ok(p) => p,
        Err(e) => return error(StatusCode::FORBIDDEN, "PATH_TRAVERSAL", &e.to_string()),
    };
    if !resolved.exists() {
        return error(
            StatusCode::NOT_FOUND,
            "ARTIFACT_NOT_FOUND",
            "artifact does not exist",
        );
    }
    let bytes = match fs::read(&resolved) {
        Ok(b) => b,
        Err(e) => return internal(e.to_string()),
    };
    let content_type = if file_path.ends_with(".png") {
        "image/png"
    } else if file_path.ends_with(".json") {
        "application/json"
    } else if file_path.ends_with(".jsonl") {
        "application/x-ndjson"
    } else if file_path.ends_with(".csv") {
        "text/csv"
    } else {
        "application/octet-stream"
    };
    (StatusCode::OK, [("content-type", content_type)], bytes).into_response()
}

async fn test_step(
    State(state): State<AppState>,
    Json(req): Json<TestStepRequest>,
) -> impl IntoResponse {
    let artifacts_dir = state.artifacts.join("test_step");
    let result = tokio::task::spawn_blocking(move || {
        workflow_engine::execute_single_step(
            &req.action,
            req.input,
            req.mock_context,
            Some(artifacts_dir),
        )
    })
    .await;

    match result {
        Ok(Ok(value)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "output": value })),
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "success": false,
                "error": { "code": "STEP_EXECUTION_FAILED", "message": e.to_string() }
            })),
        )
            .into_response(),
        Err(join_err) => internal(join_err.to_string()),
    }
}

async fn interactive_pick_endpoint(Json(req): Json<InteractivePickRequest>) -> impl IntoResponse {
    let url = req.url;
    let timeout = req.timeout_secs;
    let result = tokio::task::spawn_blocking(move || -> Result<Value, String> {
        use driver_drission::BrowserDriver;
        let mut driver = driver_drission::DrissionDriver::new();
        let config = workflow_schema::BrowserDefinition {
            mode: "launch".into(),
            headless: false,
            ..Default::default()
        };
        driver.launch(&config).map_err(|e| e.to_string())?;
        let res = driver
            .interactive_pick(&url, timeout)
            .map_err(|e| e.to_string());
        let _ = driver.close();
        res
    })
    .await;

    match result {
        Ok(Ok(picked)) => (
            StatusCode::OK,
            Json(json!({ "success": true, "picked": picked })),
        )
            .into_response(),
        Ok(Err(e)) => (
            StatusCode::BAD_REQUEST,
            Json(json!({ "success": false, "error": { "code": "PICK_FAILED", "message": e } })),
        )
            .into_response(),
        Err(join_err) => internal(join_err.to_string()),
    }
}

async fn ai_generate_workflow_endpoint(
    Json(req): Json<AiGenerateWorkflowRequest>,
) -> impl IntoResponse {
    let name = req.name.unwrap_or_else(|| "ai-generated-workflow".into());
    let title = req.title.unwrap_or_else(|| "AI 生成工作流".into());
    let prompt = req.prompt.trim();

    let generated_yaml = format!(
        r#"apiVersion: drission.workflow/v1
kind: Workflow

metadata:
  name: {name}
  title: {title}
  description: "基于提示词自动生成: {prompt}"
  tags: [ai-generated, automation]

inputs:
  target_url:
    type: string
    format: uri
    required: true
    default: "https://example.com"

browser:
  mode: launch
  headless: true
  profile: ephemeral

defaults:
  timeout: 20s

steps:
  - id: goto_target
    name: 打开目标网站
    action: page.goto
    with:
      url: "${{inputs.target_url}}"

  - id: wait_content
    name: 等待内容加载
    action: page.wait
    with:
      locator: "css:body"
      state: visible

  - id: ai_extract_data
    name: 智能抽取所需数据
    action: ai.extract
    with:
      content: "${{inputs.target_url}}"
      prompt: "{prompt}"

  - id: capture_evidence
    name: 保存当前页面截图快照
    action: page.screenshot
    with:
      path: "result.png"

  - id: notify_result
    name: 发送任务完成通知
    action: notify.webhook
    with:
      url: "https://api.example.com/callback"
      data:
        summary: "${{steps.ai_extract_data.output}}"
        snapshot: "result.png"

outputs:
  extracted: "${{steps.ai_extract_data.output}}"
  snapshot: "result.png"
"#
    );

    (
        StatusCode::OK,
        Json(json!({
            "name": name,
            "title": title,
            "yaml": generated_yaml
        })),
    )
        .into_response()
}

async fn mcp_tools_endpoint() -> impl IntoResponse {
    let registry = ActionRegistry::built_in();
    let actions_tools: Vec<Value> = registry
        .list()
        .map(|a| {
            json!({
                "name": a.name,
                "description": a.description,
                "inputSchema": a.input_schema
            })
        })
        .collect();

    (
        StatusCode::OK,
        Json(json!({
            "tools": [
                {
                    "name": "workflow_list",
                    "description": "获取当前工作空间的所有工作流列表",
                    "inputSchema": { "type": "object", "properties": {} }
                },
                {
                    "name": "workflow_execute",
                    "description": "执行指定工作流并获取输出产物与执行状态",
                    "inputSchema": {
                        "type": "object",
                        "required": ["workflowId"],
                        "properties": {
                            "workflowId": { "type": "string", "description": "工作流 ID" },
                            "inputs": { "type": "object", "description": "动态输入参数" }
                        }
                    }
                },
                {
                    "name": "browser_action_execute",
                    "description": "直接执行单个底层浏览器或数据 Action",
                    "inputSchema": {
                        "type": "object",
                        "required": ["action", "input"],
                        "properties": {
                            "action": { "type": "string", "description": "动作名称，如 page.goto, element.click" },
                            "input": { "type": "object", "description": "动作输入参数" }
                        }
                    }
                }
            ],
            "availableBrowserActions": actions_tools
        })),
    )
        .into_response()
}

async fn create_workspace(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Json(req): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    if req.name.trim().is_empty() || req.slug.trim().is_empty() {
        return error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "VALIDATION_ERROR",
            "name and slug are required",
        );
    }
    let (owner_id, actor_name) = if let Some(Extension(ref c)) = claims {
        (Some(c.sub.clone()), c.audit_actor())
    } else {
        (None, "user".to_string())
    };
    let item = workflow_core::WorkspaceRecord {
        id: Uuid::new_v4().to_string(),
        name: req.name,
        slug: req.slug,
        status: "active".into(),
        settings: json!({}),
        owner_id,
    };
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    match store.create_workspace(&item) {
        Ok(()) => {
            let _ = store.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(item.id.clone()),
                action: "workspace.create".into(),
                resource_type: "workspace".into(),
                resource_id: item.id.clone(),
                actor: actor_name,
                details: json!({ "name": item.name, "slug": item.slug }),
                created_at: String::new(),
            });
            (StatusCode::CREATED, Json(json!(item))).into_response()
        }
        Err(e) => error(
            StatusCode::CONFLICT,
            "WORKSPACE_CREATE_FAILED",
            &e.to_string(),
        ),
    }
}

async fn delete_workspace_endpoint(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let actor_name = if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            if let Err(r) = c.check_workspace_access(&store, &id) {
                return r.into_response();
            }
        }
        c.audit_actor()
    } else {
        "user".to_string()
    };
    match store.delete_workspace(&id) {
        Ok(true) => {
            let _ = store.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(id.clone()),
                action: "workspace.delete".into(),
                resource_type: "workspace".into(),
                resource_id: id,
                actor: actor_name,
                details: json!({ "status": "deleted" }),
                created_at: String::new(),
            });
            (StatusCode::OK, Json(json!({ "deleted": true }))).into_response()
        }
        Ok(false) => error(
            StatusCode::NOT_FOUND,
            "WORKSPACE_NOT_FOUND",
            "workspace not found",
        ),
        Err(e) => internal(e.to_string()),
    }
}

async fn workflow_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&s, &id) {
            return r.into_response();
        }
    }
    match s.list_workflows(&id) {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}
async fn create_workflow(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<CreateWorkflowRequest>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let actor_name = if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&s, &id) {
            return r.into_response();
        }
        c.audit_actor()
    } else {
        "user".to_string()
    };
    match s.workspace_exists(&id) {
        Ok(true) => {}
        Ok(false) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKSPACE_NOT_FOUND",
                "workspace does not exist",
            );
        }
        Err(e) => return internal(e.to_string()),
    }
    let source=req.draft_source.unwrap_or_else(||format!("apiVersion: drission.workflow/v1\nkind: Workflow\nmetadata:\n  name: {}\nsteps: []\noutputs: {{}}\n",req.name));
    let item = workflow_core::WorkflowRecord {
        id: Uuid::new_v4().to_string(),
        workspace_id: id.clone(),
        name: req.name,
        title: req.title,
        draft_source: source,
        draft_revision: 1,
        status: "active".into(),
    };
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    match s.create_workflow(&item) {
        Ok(()) => {
            let _ = s.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(id),
                action: "workflow.create".into(),
                resource_type: "workflow".into(),
                resource_id: item.id.clone(),
                actor: actor_name,
                details: json!({ "name": item.name, "title": item.title }),
                created_at: String::new(),
            });
            (StatusCode::CREATED, Json(json!(item))).into_response()
        }
        Err(e) => error(
            StatusCode::CONFLICT,
            "WORKFLOW_CREATE_FAILED",
            &e.to_string(),
        ),
    }
}
async fn workflow_detail(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let wf = match s.get_workflow(&id) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKFLOW_NOT_FOUND",
                "workflow does not exist",
            );
        }
        Err(e) => return internal(e.to_string()),
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&s, &wf.workspace_id) {
            return r.into_response();
        }
    }
    (StatusCode::OK, Json(json!(wf))).into_response()
}
async fn save_draft(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<SaveDraftRequest>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let (ws_id, actor_name) = if let Some(Extension(ref c)) = claims {
        let w = match s.get_workflow(&id) {
            Ok(Some(w)) => w,
            Ok(None) => return error(StatusCode::NOT_FOUND, "WORKFLOW_NOT_FOUND", "workflow does not exist"),
            Err(e) => return internal(e.to_string()),
        };
        if let Err(r) = c.check_workspace_access(&s, &w.workspace_id) {
            return r.into_response();
        }
        (Some(w.workspace_id), c.audit_actor())
    } else {
        (None, "user".to_string())
    };
    let definition = WorkflowDefinition;
    let parsed = definition.parse(&req.source);
    if !parsed.valid {
        return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error":{"code":"WORKFLOW_PARSE_ERROR","message":"YAML 无法解析","details":parsed.diagnostics}}))).into_response();
    }
    match s.update_workflow_draft(&id, req.revision, &req.source) {
        Ok(true) => {
            let _ = s.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: ws_id,
                action: "workflow.draft.update".into(),
                resource_type: "workflow".into(),
                resource_id: id.clone(),
                actor: actor_name,
                details: json!({ "revision": req.revision }),
                created_at: String::new(),
            });
            workflow_detail(State(state), claims, Path(id))
                .await
                .into_response()
        }
        Ok(false) => error(
            StatusCode::CONFLICT,
            "REVISION_CONFLICT",
            "draft revision has changed",
        ),
        Err(e) => internal(e.to_string()),
    }
}
async fn apply_edit(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<ApplyEditRequest>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    let workflow = match store.get_workflow(&id) {
        Ok(Some(value)) => value,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKFLOW_NOT_FOUND",
                "workflow does not exist",
            );
        }
        Err(error_value) => return internal(error_value.to_string()),
    };
    let actor_name = if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&store, &workflow.workspace_id) {
            return r.into_response();
        }
        c.audit_actor()
    } else {
        "user".to_string()
    };
    if workflow.draft_revision != req.revision {
        return (StatusCode::CONFLICT, Json(json!({"error":{"code":"REVISION_CONFLICT","message":"草稿修订已变化","details":{"serverRevision":workflow.draft_revision,"clientRevision":req.revision}}}))).into_response();
    }
    let definition = WorkflowDefinition;
    let outcome = match definition.apply(&workflow.draft_source, req.command, &ActionRegistry::built_in()) {
        Ok(value) => value,
        Err(workflow_definition::DefinitionError::InvalidSource(diagnostics)) => return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({"error":{"code":"WORKFLOW_PARSE_ERROR","message":"YAML 无法解析，已阻止画布覆盖源码","details":diagnostics}}))).into_response(),
        Err(error_value) => return error(StatusCode::CONFLICT, "WORKFLOW_EDIT_FAILED", &error_value.to_string()),
    };
    match store.update_workflow_draft(&id, req.revision, &outcome.source) {
        Ok(true) => {
            let _ = store.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(workflow.workspace_id),
                action: "workflow.draft.edit".into(),
                resource_type: "workflow".into(),
                resource_id: id.clone(),
                actor: actor_name,
                details: json!({ "revision": req.revision }),
                created_at: String::new(),
            });
            workflow_detail(State(state), claims, Path(id))
                .await
                .into_response()
        }
        Ok(false) => error(
            StatusCode::CONFLICT,
            "REVISION_CONFLICT",
            "draft revision has changed",
        ),
        Err(error_value) => internal(error_value.to_string()),
    }
}

async fn validate_workflow(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let w = match s.get_workflow(&id) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKFLOW_NOT_FOUND",
                "workflow does not exist",
            );
        }
        Err(e) => return internal(e.to_string()),
    };
    if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&s, &w.workspace_id) {
            return r.into_response();
        }
    }
    match workflow_schema::compile(&w.draft_source, &ActionRegistry::built_in()) {
        Ok(ir) => (
            StatusCode::OK,
            Json(
                json!({"valid":true,"hash":ir.content_hash,"permissions":ir.required_permissions,"inputs":ir.document.inputs}),
            ),
        )
            .into_response(),
        Err(workflow_core::WorkflowError::Validation(d)) => (
            StatusCode::UNPROCESSABLE_ENTITY,
            Json(json!({"valid":false,"diagnostics":d})),
        )
            .into_response(),
        Err(e) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "WORKFLOW_VALIDATION_ERROR",
            &e.to_string(),
        ),
    }
}
async fn publish_workflow(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Json(req): Json<PublishRequest>,
) -> impl IntoResponse {
    let mut s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    let w = match s.get_workflow(&id) {
        Ok(Some(v)) => v,
        Ok(None) => {
            return error(
                StatusCode::NOT_FOUND,
                "WORKFLOW_NOT_FOUND",
                "workflow does not exist",
            );
        }
        Err(e) => return internal(e.to_string()),
    };
    let actor_name = if let Some(Extension(ref c)) = claims {
        if let Err(r) = c.check_workspace_access(&s, &w.workspace_id) {
            return r.into_response();
        }
        c.audit_actor()
    } else {
        "user".to_string()
    };

    // B4: 原子检查预期 revision、分配版本并保存发布内容，冲突返回 409
    let registry = ActionRegistry::built_in();
    match s.publish_workflow_atomic(&id, req.expected_revision, req.change_note.as_deref(), &registry) {
        Ok(v) => {
            let _ = s.save_audit_log(&workflow_core::AuditRecord {
                id: Uuid::new_v4().to_string(),
                workspace_id: Some(w.workspace_id),
                action: "workflow.publish".into(),
                resource_type: "workflow".into(),
                resource_id: id,
                actor: actor_name,
                details: json!({ "version": v.version, "contentHash": v.content_hash, "publishedRevision": req.expected_revision }),
                created_at: String::new(),
            });
            (StatusCode::CREATED, Json(json!(v))).into_response()
        }
        Err(e) if e.to_string().contains("REVISION_CONFLICT") => error(
            StatusCode::CONFLICT,
            "REVISION_CONFLICT",
            "草稿修订版本已变化或与预期不一致，请刷新后重试发布",
        ),
        Err(e) if e.to_string().contains("EXPECTED_REVISION_REQUIRED") => error(
            StatusCode::BAD_REQUEST,
            "EXPECTED_REVISION_REQUIRED",
            "发布工作流必须携带预期草稿版本号 (expectedRevision)",
        ),
        Err(e) => error(
            StatusCode::UNPROCESSABLE_ENTITY,
            "WORKFLOW_PUBLISH_FAILED",
            &e.to_string(),
        ),
    }
}
async fn version_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let s = match open_store(&state) {
        Ok(s) => s,
        Err(r) => return *r,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match s.get_workflow(&id) {
                Ok(Some(w)) => {
                    if let Err(r) = c.check_workspace_access(&s, &w.workspace_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => return error(StatusCode::NOT_FOUND, "WORKFLOW_NOT_FOUND", "workflow does not exist"),
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match s.list_versions(&id) {
        Ok(v) => (StatusCode::OK, Json(json!(v))).into_response(),
        Err(e) => internal(e.to_string()),
    }
}

async fn version_detail(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path((id, version)): Path<(String, u64)>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(value) => value,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_workflow(&id) {
                Ok(Some(w)) => {
                    if let Err(r) = c.check_workspace_access(&store, &w.workspace_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => return error(StatusCode::NOT_FOUND, "WORKFLOW_NOT_FOUND", "workflow does not exist"),
                Err(e) => return internal(e.to_string()),
            }
        }
    }
    match store.get_version(&id, version) {
        Ok(Some(value)) => (StatusCode::OK, Json(json!(value))).into_response(),
        Ok(None) => error(
            StatusCode::NOT_FOUND,
            "WORKFLOW_VERSION_NOT_FOUND",
            "workflow version does not exist",
        ),
        Err(error_value) => internal(error_value.to_string()),
    }
}

async fn run_list(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Query(query): Query<ListQuery>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    let limit = query.limit.unwrap_or(50).clamp(1, 200);
    let before = query
        .cursor
        .as_deref()
        .and_then(|cursor| cursor.parse::<i64>().ok());
    let result = if let Some(Extension(ref c)) = claims {
        if c.is_super_admin() {
            store.list_runs(limit, before)
        } else {
            store.list_runs_for_user(&c.sub, limit, before)
        }
    } else {
        store.list_runs(limit, before)
    };
    match result {
        Ok(runs) => {
            let next_cursor = runs
                .last()
                .and_then(|run| store.run_rowid(&run.id).ok().flatten())
                .map(|value| value.to_string());
            (
                StatusCode::OK,
                Json(json!({ "items": runs, "nextCursor": next_cursor })),
            )
                .into_response()
        }
        Err(error) => internal(error.to_string()),
    }
}

async fn create_run(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    headers: HeaderMap,
    Json(request): Json<CreateRunRequest>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    let (workflow_id, workflow_version_id, source, draft_revision, idempotency_scope) =
        if let Some(workflow_id) = request.workflow_id.clone() {
            let workflow = match store.get_workflow(&workflow_id) {
                Ok(Some(value)) => value,
                Ok(None) => {
                    return error(
                        StatusCode::NOT_FOUND,
                        "WORKFLOW_NOT_FOUND",
                        "workflow does not exist",
                    );
                }
                Err(error_value) => return internal(error_value.to_string()),
            };
            if let Some(Extension(ref c)) = claims {
                if let Err(r) = c.check_workspace_access(&store, &workflow.workspace_id) {
                    return r.into_response();
                }
            }
            if request.mode == "debug" {
                if request.version.is_some() {
                    return error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "INVALID_RUN_REQUEST",
                        "debug 运行不能指定 version",
                    );
                }
                if request.draft_revision != Some(workflow.draft_revision) {
                    return error(
                        StatusCode::CONFLICT,
                        "REVISION_CONFLICT",
                        "draft revision has changed",
                    );
                }
                (
                    Some(workflow.id.clone()),
                    None,
                    workflow.draft_source,
                    Some(workflow.draft_revision),
                    workflow.workspace_id,
                )
            } else if request.mode == "published" {
                let Some(version_number) = request.version else {
                    return error(
                        StatusCode::UNPROCESSABLE_ENTITY,
                        "VERSION_REQUIRED",
                        "published 运行必须指定 version",
                    );
                };
                let version = match store.get_version(&workflow.id, version_number) {
                    Ok(Some(value)) => value,
                    Ok(None) => {
                        return error(
                            StatusCode::NOT_FOUND,
                            "WORKFLOW_VERSION_NOT_FOUND",
                            "workflow version does not exist",
                        );
                    }
                    Err(error_value) => return internal(error_value.to_string()),
                };
                (
                    Some(workflow.id),
                    Some(version.id),
                    version.source,
                    None,
                    workflow.workspace_id,
                )
            } else {
                return error(
                    StatusCode::UNPROCESSABLE_ENTITY,
                    "RUN_MODE_UNSUPPORTED",
                    "mode must be debug or published",
                );
            }
        } else if let Some(source) = request.workflow_source.clone() {
            // CLI legacy compatibility; Web/API clients should bind runs to workflowId.
            (None, None, source, None, "legacy".into())
        } else {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                "WORKFLOW_REQUIRED",
                "workflowId is required",
            );
        };
    let registry = ActionRegistry::built_in();
    let ir = match workflow_schema::compile(&source, &registry) {
        Ok(ir) => ir,
        Err(workflow_core::WorkflowError::Validation(diagnostics)) => return (StatusCode::UNPROCESSABLE_ENTITY, Json(json!({ "error": { "code": "WORKFLOW_VALIDATION_ERROR", "message": "workflow validation failed", "details": diagnostics } }))).into_response(),
        Err(workflow_error) => return error(StatusCode::UNPROCESSABLE_ENTITY, "WORKFLOW_VALIDATION_ERROR", &workflow_error.to_string()),
    };
    let request_hash = hex::encode(sha2::Sha256::digest(serde_json::to_vec(&json!({"workflowHash":ir.content_hash,"mode":request.mode,"inputs":request.inputs,"version":request.version,"draftRevision":draft_revision})).unwrap()));
    let idempotency_key = headers
        .get("idempotency-key")
        .and_then(|value| value.to_str().ok());
    let run_id = Uuid::new_v4().to_string();
    if let Some(key) = idempotency_key {
        match store.find_idempotent_run(&idempotency_scope, key) {
            Ok(Some((existing_id, existing_hash))) if existing_hash == request_hash => {
                if let Ok(Some(existing)) = store.get_run(&existing_id) {
                    return (
                        StatusCode::OK,
                        Json(serde_json::to_value(existing).unwrap()),
                    )
                        .into_response();
                }
            }
            Ok(Some(_)) => {
                return error(
                    StatusCode::CONFLICT,
                    "IDEMPOTENCY_CONFLICT",
                    "same idempotency key was used with a different request",
                );
            }
            Ok(None) => {}
            Err(error_value) => return internal(error_value.to_string()),
        }
    }
    let artifact_root = state.artifacts.join(&run_id);
    let control = control_path(&state.database, &run_id);
    if let Err(error) = write_control(&control, RunnerControl::Running) {
        return internal(error);
    }
    let queued = RunRecord {
        id: run_id.clone(),
        workflow_name: ir.name.clone(),
        workflow_hash: ir.content_hash.clone(),
        workflow_id,
        workflow_version_id,
        run_mode: request.mode.clone(),
        source_snapshot: Some(source.clone()),
        draft_revision,
        parent_run_id: None,
        resume_checkpoint_id: None,
        status: RunStatus::Queued,
        inputs: request.inputs.clone(),
        outputs: None,
        error_code: None,
        error_message: None,
        steps: vec![],
    };
    if let Err(error) = store.create_run(&queued) {
        return internal(error.to_string());
    }
    if let Some(key) = idempotency_key
        && let Err(error) =
            store.set_run_idempotency(&run_id, &idempotency_scope, key, &request_hash)
    {
        return internal(error.to_string());
    }
    if let Err(error) = store.append_event(&RunEvent {
        run_id: run_id.clone(),
        sequence: 1,
        event_type: "run.queued".into(),
        payload: Value::Object(Map::new()),
    }) {
        return internal(error.to_string());
    }
    let runner_request = RunnerRequest {
        run_id: run_id.clone(),
        workflow_source: source,
        inputs: request.inputs,
        artifact_root,
        control_path: Some(control),
        completed_steps: vec![],
        initial_context: None,
    };
    let background_state = state.clone();
    tokio::task::spawn_blocking(move || execute_background(background_state, runner_request));
    (
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(queued).unwrap()),
    )
        .into_response()
}

fn merge_run_metadata(result: &mut RunRecord, stored: &RunRecord) {
    result.workflow_id = stored.workflow_id.clone();
    result.workflow_version_id = stored.workflow_version_id.clone();
    result.run_mode.clone_from(&stored.run_mode);
    result.source_snapshot = stored.source_snapshot.clone();
    result.draft_revision = stored.draft_revision;
}

fn execute_background(state: AppState, request: RunnerRequest) {
    let Ok(store) = SqliteStore::open(state.database.as_ref()) else {
        return;
    };
    if let Ok(Some(mut run)) = store.get_run(&request.run_id) {
        run.status =
            transition_run(run.status, RunCommand::Prepare).unwrap_or(RunStatus::Preparing);
        run.status = transition_run(run.status, RunCommand::Start).unwrap_or(RunStatus::Running);
        let _ = store.update_run(&run);
    }
    let _ = store.append_event(&RunEvent {
        run_id: request.run_id.clone(),
        sequence: store.next_sequence(&request.run_id).unwrap_or(2),
        event_type: "run.started".into(),
        payload: Value::Object(Map::new()),
    });
    let result = run_child(&state, &store, &request);
    let run = result.unwrap_or_else(|| RunRecord {
        id: request.run_id.clone(),
        workflow_name: "unknown".into(),
        workflow_hash: "unknown".into(),
        workflow_id: None,
        workflow_version_id: None,
        run_mode: "legacy".into(),
        source_snapshot: None,
        draft_revision: None,
        parent_run_id: None,
        resume_checkpoint_id: None,
        status: RunStatus::Interrupted,
        inputs: Value::Object(Map::new()),
        outputs: None,
        error_code: Some("RUNNER_INTERRUPTED".into()),
        error_message: Some("runner process failed".into()),
        steps: vec![],
    });
    let mut run = run;
    if let Ok(Some(stored)) = store.get_run(&run.id) {
        merge_run_metadata(&mut run, &stored);
    }
    for step in &run.steps {
        let _ = store.upsert_step(&run.id, step);
    }
    let _ = store.update_run(&run);
    let _ = store.append_event(&RunEvent {
        run_id: run.id.clone(),
        sequence: store.next_sequence(&run.id).unwrap_or(1),
        event_type: match run.status {
            RunStatus::Succeeded => "run.succeeded",
            RunStatus::Cancelled => "run.cancelled",
            RunStatus::Interrupted => "run.interrupted",
            _ => "run.failed",
        }
        .into(),
        payload: json!({ "errorCode": run.error_code }),
    });
    let _ = fs::remove_file(control_path(&state.database, &run.id));
}

fn run_child(state: &AppState, store: &SqliteStore, request: &RunnerRequest) -> Option<RunRecord> {
    let mut child = Command::new(state.runner.as_ref())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .ok()?;
    child
        .stdin
        .take()?
        .write_all(&serde_json::to_vec(request).ok()?)
        .ok()?;
    let stdout = child.stdout.take()?;
    let mut result = None;
    for line in BufReader::new(stdout).lines().map_while(Result::ok) {
        match serde_json::from_str::<RunnerOutput>(&line) {
            Ok(RunnerOutput::Event {
                event_type,
                payload,
            }) => {
                if let Some(step) = step_from_event(&event_type, &payload) {
                    let _ = store.upsert_step(&request.run_id, &step);
                }
                let _ = store.append_event(&RunEvent {
                    run_id: request.run_id.clone(),
                    sequence: store.next_sequence(&request.run_id).unwrap_or(1),
                    event_type,
                    payload,
                });
            }
            Ok(RunnerOutput::Result { response }) => result = response.run,
            Err(_) => {}
        }
    }
    let _ = child.wait();
    result
}

fn step_from_event(event_type: &str, payload: &Value) -> Option<workflow_core::StepRunRecord> {
    let status = match event_type {
        "step.started" => workflow_core::StepStatus::Running,
        "step.retry_wait" => workflow_core::StepStatus::RetryWait,
        "step.succeeded" => workflow_core::StepStatus::Succeeded,
        "step.skipped" => workflow_core::StepStatus::Skipped,
        "step.failed" => workflow_core::StepStatus::Failed,
        _ => return None,
    };
    Some(workflow_core::StepRunRecord {
        step_id: payload.get("stepId")?.as_str()?.into(),
        attempt: payload.get("attempt")?.as_u64()? as u32,
        status,
        output: payload.get("output").cloned(),
        error_code: payload
            .get("errorCode")
            .and_then(Value::as_str)
            .map(str::to_owned),
        error_message: payload
            .get("errorMessage")
            .and_then(Value::as_str)
            .map(str::to_owned),
    })
}

async fn run_detail(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    match store.get_run(&id) {
        Ok(Some(run)) => (StatusCode::OK, Json(serde_json::to_value(run).unwrap())).into_response(),
        Ok(None) => error(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "run does not exist"),
        Err(error_value) => internal(error_value.to_string()),
    }
}

async fn run_events(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Query(query): Query<EventQuery>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    match store.events_after(&id, query.after_sequence.unwrap_or(0), 10_000) {
        Ok(events) => (StatusCode::OK, Json(serde_json::to_value(events).unwrap())).into_response(),
        Err(error_value) => internal(error_value.to_string()),
    }
}

async fn run_stream(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
    Query(query): Query<EventQuery>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    let database = state.database.clone();
    let token_exp = claims.as_ref().map(|Extension(c)| c.exp).unwrap_or(i64::MAX);
    let mut sequence = query.after_sequence.unwrap_or(0);
    let events = stream! {
        loop {
            // R2-4: SSE 流式推送过程中，每次轮询校验令牌有效期；过期立即关闭流
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64;
            if now > token_exp {
                yield Ok(Event::default().event("error").data("token_expired_stream_closed"));
                break;
            }

            let batch = SqliteStore::open(database.as_ref()).and_then(|store| store.events_after(&id, sequence, 500));
            match batch {
                Ok(items) => for item in items { sequence = item.sequence; let event_type = item.event_type.clone(); yield Ok::<Event, Infallible>(Event::default().id(sequence.to_string()).event(event_type).json_data(item).unwrap()); },
                Err(error) => { yield Ok(Event::default().event("error").data(error.to_string())); break; }
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
    };
    Sse::new(events).keep_alive(
        KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
    .into_response()
}

async fn pause_run(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    update_control(
        &state,
        &id,
        RunnerControl::Paused,
        RunCommand::RequestPause,
        "run.pause_requested",
    )
}
async fn resume_run(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    update_control(
        &state,
        &id,
        RunnerControl::Running,
        RunCommand::Resume,
        "run.resumed",
    )
}
async fn cancel_run(
    State(state): State<AppState>,
    claims: Option<Extension<DelegatedClaims>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    let store = match open_store(&state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    if let Some(Extension(ref c)) = claims {
        if !c.is_super_admin() {
            match store.get_run_workspace_id(&id) {
                Ok(Some(ws_id)) => {
                    if let Err(r) = c.check_workspace_access(&store, &ws_id) {
                        return r.into_response();
                    }
                }
                Ok(None) => {}
                Err(error_value) => return internal(error_value.to_string()),
            }
        }
    }
    update_control(
        &state,
        &id,
        RunnerControl::Cancelled,
        RunCommand::RequestCancel,
        "run.cancel_requested",
    )
}

fn update_control(
    state: &AppState,
    id: &str,
    control: RunnerControl,
    command: RunCommand,
    event_type: &str,
) -> axum::response::Response {
    let store = match open_store(state) {
        Ok(store) => store,
        Err(response) => return *response,
    };
    let mut run = match store.get_run(id) {
        Ok(Some(run)) => run,
        Ok(None) => return error(StatusCode::NOT_FOUND, "RUN_NOT_FOUND", "run does not exist"),
        Err(error_value) => return internal(error_value.to_string()),
    };
    if matches!(
        run.status,
        RunStatus::Succeeded | RunStatus::Failed | RunStatus::Cancelled | RunStatus::Interrupted
    ) {
        return error(
            StatusCode::CONFLICT,
            "RUN_ALREADY_FINAL",
            "run is already in a final state",
        );
    }
    if let Err(error_value) = write_control(&control_path(&state.database, id), control) {
        return internal(error_value);
    }
    run.status = match transition_run(run.status, command) {
        Ok(status) => status,
        Err(_) => {
            return error(
                StatusCode::CONFLICT,
                "INVALID_RUN_TRANSITION",
                "run state does not allow this command",
            );
        }
    };
    if let Err(error_value) = store.update_run(&run) {
        return internal(error_value.to_string());
    }
    let sequence = store.next_sequence(id).unwrap_or(1);
    if let Err(error_value) = store.append_event(&RunEvent {
        run_id: id.into(),
        sequence,
        event_type: event_type.into(),
        payload: Value::Object(Map::new()),
    }) {
        return internal(error_value.to_string());
    }
    (
        StatusCode::ACCEPTED,
        Json(serde_json::to_value(run).unwrap()),
    )
        .into_response()
}

fn control_path(database: &FsPath, run_id: &str) -> PathBuf {
    database
        .parent()
        .unwrap_or_else(|| FsPath::new("."))
        .join("control")
        .join(format!("{run_id}.json"))
}
fn write_control(path: &FsPath, control: RunnerControl) -> Result<(), String> {
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
fn runner_command() -> PathBuf {
    let current =
        std::env::current_exe().unwrap_or_else(|_| PathBuf::from("drission-workflow-api"));
    let sibling = current.with_file_name(if cfg!(windows) {
        "drission-workflow-runner.exe"
    } else {
        "drission-workflow-runner"
    });
    if sibling.exists() {
        sibling
    } else {
        PathBuf::from("drission-workflow-runner")
    }
}
fn open_store(state: &AppState) -> Result<SqliteStore, Box<axum::response::Response>> {
    SqliteStore::open(state.database.as_ref())
        .map_err(|error| Box::new(internal(error.to_string())))
}
fn internal(message: String) -> axum::response::Response {
    error(
        StatusCode::INTERNAL_SERVER_ERROR,
        "INTERNAL_ERROR",
        &message,
    )
}
fn error(status: StatusCode, code: &str, message: &str) -> axum::response::Response {
    (
        status,
        Json(json!({ "error": { "code": code, "message": message } })),
    )
        .into_response()
}
async fn shutdown() {
    let _ = tokio::signal::ctrl_c().await;
}

fn build_router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/api/v1/actions", get(actions))
        .route(
            "/api/v1/workspaces",
            get(workspace_list).post(create_workspace),
        )
        .route(
            "/api/v1/workspaces/{id}",
            axum::routing::delete(delete_workspace_endpoint),
        )
        .route(
            "/api/v1/workspaces/{id}/workflows",
            get(workflow_list).post(create_workflow),
        )
        .route(
            "/api/v1/workspaces/{id}/schedules",
            get(schedule_list).post(create_schedule_endpoint),
        )
        .route(
            "/api/v1/schedules/{id}",
            axum::routing::delete(delete_schedule_endpoint),
        )
        .route(
            "/api/v1/workspaces/{id}/webhooks",
            get(webhook_list).post(create_webhook_endpoint),
        )
        .route(
            "/api/v1/webhooks/{id}",
            axum::routing::delete(delete_webhook_endpoint),
        )
        .route(
            "/api/v1/workspaces/{id}/credentials",
            get(credential_list).post(save_credential_endpoint),
        )
        .route(
            "/api/v1/credentials/{id}",
            axum::routing::delete(delete_credential_endpoint),
        )
        .route("/api/v1/workspaces/{id}/audit", get(workspace_audit))
        .route("/api/v1/workflows/{id}", get(workflow_detail))
        .route(
            "/api/v1/workflows/{id}/draft",
            axum::routing::put(save_draft),
        )
        .route("/api/v1/workflows/{id}/draft/edit", post(apply_edit))
        .route("/api/v1/workflows/{id}/validate", post(validate_workflow))
        .route("/api/v1/workflows/{id}/publish", post(publish_workflow))
        .route("/api/v1/workflows/{id}/versions", get(version_list))
        .route(
            "/api/v1/workflows/{id}/versions/{version}",
            get(version_detail),
        )
        .route("/api/v1/test-step", post(test_step))
        .route("/api/v1/interactive-pick", post(interactive_pick_endpoint))
        .route(
            "/api/v1/ai/generate-workflow",
            post(ai_generate_workflow_endpoint),
        )
        .route("/api/v1/mcp/tools", get(mcp_tools_endpoint))
        .route("/api/v1/runs", get(run_list).post(create_run))
        .route("/api/v1/runs/{id}", get(run_detail))
        .route("/api/v1/runs/{id}/checkpoints", get(run_checkpoints))
        .route(
            "/api/v1/runs/{id}/resume-checkpoint",
            post(resume_checkpoint_endpoint),
        )
        .route("/api/v1/runs/{id}/artifacts", get(run_artifacts))
        .route(
            "/api/v1/runs/{id}/artifacts/{*file_path}",
            get(get_artifact_file),
        )
        .route("/api/v1/runs/{id}/events", get(run_events))
        .route("/api/v1/runs/{id}/stream", get(run_stream))
        .route("/api/v1/runs/{id}/pause", post(pause_run))
        .route("/api/v1/runs/{id}/resume", post(resume_run))
        .route("/api/v1/runs/{id}/cancel", post(cancel_run))
        .route("/api/v1/hooks/{token}", post(trigger_webhook))
        .layer(axum::middleware::from_fn_with_state(state.clone(), auth_middleware))
        .layer(TraceLayer::new_for_http())
        .with_state(state)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::Request;
    use http_body_util::BodyExt;
    use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
    use tower::ServiceExt;

    fn issue_test_token(secret: &str, role: &str, sub: &str) -> String {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let claims = DelegatedClaims {
            iss: "ruoyi-plus".into(),
            aud: "workflow-api".into(),
            sub: sub.into(),
            username: "test_user".into(),
            role: role.into(),
            exp: now + 3600,
            iat: now,
            jti: Uuid::new_v4().to_string(),
        };
        encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(secret.as_bytes()),
        )
        .unwrap()
    }

    fn setup_test_app(secret: Option<String>) -> (Router, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("test.db");
        let artifacts_path = dir.path().join("artifacts");
        let runner_path = dir.path().join("runner");
        let store = SqliteStore::open(&db_path).unwrap();
        // 预设两个工作空间并绑定 owner
        let _ = store.create_workspace(&workflow_core::WorkspaceRecord {
            id: "ws-user-1".into(),
            name: "User 1 Workspace".into(),
            slug: "user-1".into(),
            status: "active".into(),
            settings: json!({}),
            owner_id: Some("ws-user-1".into()),
        });
        let _ = store.create_workspace(&workflow_core::WorkspaceRecord {
            id: "ws-secret-isolated".into(),
            name: "Isolated Space".into(),
            slug: "isolated".into(),
            status: "active".into(),
            settings: json!({}),
            owner_id: Some("other-user".into()),
        });
        // 在 ws-secret-isolated 下建一个 workflow
        let _ = store.create_workflow(&workflow_core::WorkflowRecord {
            id: "wf-secret".into(),
            workspace_id: "ws-secret-isolated".into(),
            name: "Secret Flow".into(),
            title: "Secret Flow".into(),
            draft_source: "apiVersion: drission.workflow/v1\nkind: Workflow\nmetadata:\n  name: secret\nsteps: []\noutputs: {}\n".into(),
            draft_revision: 1,
            status: "active".into(),
        });

        let state = AppState {
            database: Arc::new(db_path),
            artifacts: Arc::new(artifacts_path),
            runner: Arc::new(runner_path),
            auth_secret: secret.map(Arc::new),
        };
        (build_router(state), dir)
    }

    #[tokio::test]
    async fn test_r1_1_forged_host_header_cannot_bypass_unconfigured_secret() {
        // R1-1 反例测试：未配置 auth_secret 时，客户端无论传入怎样的 Host 头 (如 127.0.0.1, localhost, 恶意伪造)，都必须被 503 拒绝
        let (app, _dir) = setup_test_app(None);

        let req = Request::builder()
            .uri("/api/v1/workspaces")
            .header("host", "127.0.0.1:8787")
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: Value = serde_json::from_slice(&body_bytes).unwrap();
        assert!(body["error"].as_str().unwrap().contains("服务端未配置委托认证密钥"));

        // 再测试伪造 localhost
        let req2 = Request::builder()
            .uri("/api/v1/workspaces")
            .header("host", "localhost:8787")
            .body(Body::empty())
            .unwrap();
        let resp2 = app.oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn test_r1_4_workspace_authorization_rejection() {
        // R1-4 资源归属反例测试：任务管理员 sub="ws-user-1" 试图访问/变更属于 "ws-secret-isolated" 的工作空间与工作流
        let secret = "workflow-test-internal-auth-secret-1234";
        let (app, _dir) = setup_test_app(Some(secret.into()));

        let user1_token = issue_test_token(secret, "任务管理员", "ws-user-1");

        // 1. 访问属于自己的工作空间列表 -> 成功 200
        let req1 = Request::builder()
            .uri("/api/v1/workspaces/ws-user-1/workflows")
            .header("x-delegated-token", &user1_token)
            .body(Body::empty())
            .unwrap();
        let resp1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);

        // 2. 越权读取属于 ws-secret-isolated 的工作流列表 -> 必须被 403 拦截
        let req2 = Request::builder()
            .uri("/api/v1/workspaces/ws-secret-isolated/workflows")
            .header("x-delegated-token", &user1_token)
            .body(Body::empty())
            .unwrap();
        let resp2 = app.clone().oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::FORBIDDEN);

        // 3. 越权读取属于 ws-secret-isolated 下的具体工作流详情 -> 必须被 403 拦截
        let req3 = Request::builder()
            .uri("/api/v1/workflows/wf-secret")
            .header("x-delegated-token", &user1_token)
            .body(Body::empty())
            .unwrap();
        let resp3 = app.clone().oneshot(req3).await.unwrap();
        assert_eq!(resp3.status(), StatusCode::FORBIDDEN);

        // 4. 超级管理员访问 ws-secret-isolated -> 成功放行 200
        let super_token = issue_test_token(secret, "超级管理员", "999");
        let req4 = Request::builder()
            .uri("/api/v1/workflows/wf-secret")
            .header("x-delegated-token", &super_token)
            .body(Body::empty())
            .unwrap();
        let resp4 = app.clone().oneshot(req4).await.unwrap();
        assert_eq!(resp4.status(), StatusCode::OK);

        // 5. R2-1 复现反例：普通用户列出工作空间列表，绝不可见属于其他人的隔离空间
        let req5 = Request::builder()
            .uri("/api/v1/workspaces")
            .header("x-delegated-token", &user1_token)
            .body(Body::empty())
            .unwrap();
        let resp5 = app.clone().oneshot(req5).await.unwrap();
        assert_eq!(resp5.status(), StatusCode::OK);
        let body5_bytes = resp5.into_body().collect().await.unwrap().to_bytes();
        let items5: Vec<Value> = serde_json::from_slice(&body5_bytes).unwrap();
        assert_eq!(items5.len(), 1);
        assert_eq!(items5[0]["id"], "ws-user-1");

        // 6. R2-1 复现反例：非授权普通用户尝试跨空间删除 workspace -> 必须被 403 拒绝
        let req6 = Request::builder()
            .method("DELETE")
            .uri("/api/v1/workspaces/ws-secret-isolated")
            .header("x-delegated-token", &user1_token)
            .body(Body::empty())
            .unwrap();
        let resp6 = app.clone().oneshot(req6).await.unwrap();
        assert_eq!(resp6.status(), StatusCode::FORBIDDEN);

        // 7. R4-2 / B4 真实并发发布竞争测试：两个编辑者持有同一初始 revision 1，同时提交发布请求
        // 必须严格保障：一人成功返回 201 生成版本 1，另一人原子被 409 REVISION_CONFLICT 拦截，绝不产生重复版本或覆盖未经确认的草稿！
        let admin_token = issue_test_token(secret, "超级管理员", "admin");
        let make_publish_req = || {
            Request::builder()
                .method("POST")
                .uri("/api/v1/workflows/wf-secret/publish")
                .header("x-delegated-token", &admin_token)
                .header("Content-Type", "application/json")
                .body(Body::from(json!({ "expectedRevision": 1, "changeNote": "concurrent race" }).to_string()))
                .unwrap()
        };

        let (resp_a, resp_b) = tokio::join!(
            app.clone().oneshot(make_publish_req()),
            app.oneshot(make_publish_req())
        );
        let status_a = resp_a.unwrap().status();
        let status_b = resp_b.unwrap().status();

        let one_success = (status_a == StatusCode::CREATED && status_b == StatusCode::CONFLICT)
            || (status_a == StatusCode::CONFLICT && status_b == StatusCode::CREATED);
        assert!(one_success, "并发发布竞争下必须且仅有一人成功分配版本，另一人返回 409 冲突。实际状态: a={}, b={}", status_a, status_b);
    }
}
