use rusqlite::{Connection, OptionalExtension, params};
use std::path::Path;
use thiserror::Error;
use workflow_core::{
    AuditRecord, CheckpointRecord, CredentialRecord, RunEvent, RunRecord, RunStatus,
    ScheduleRecord, StepRunRecord, WebhookRecord, WorkflowRecord, WorkflowVersionRecord,
    WorkspaceRecord,
};

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("database error: {0}")]
    Database(#[from] rusqlite::Error),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

pub struct SqliteStore {
    connection: Connection,
}

impl SqliteStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        if let Some(parent) = path.as_ref().parent() {
            std::fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        let connection = Connection::open(path)?;
        connection.pragma_update(None, "journal_mode", "WAL")?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    pub fn in_memory() -> Result<Self, StoreError> {
        let connection = Connection::open_in_memory()?;
        connection.pragma_update(None, "foreign_keys", "ON")?;
        let store = Self { connection };
        store.migrate()?;
        Ok(store)
    }

    fn migrate(&self) -> Result<(), StoreError> {
        self.connection.execute_batch(
            "CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP);",
        )?;
        let migrations = [
            (1_i64, include_str!("../migrations/0001_initial.sql")),
            (2_i64, include_str!("../migrations/0002_versioned_runs.sql")),
            (3_i64, include_str!("../migrations/0003_step_artifacts.sql")),
            (
                4_i64,
                include_str!("../migrations/0004_checkpoints_and_audit.sql"),
            ),
            (
                5_i64,
                include_str!("../migrations/0005_triggers_and_credentials.sql"),
            ),
            (
                6_i64,
                include_str!("../migrations/0006_workspace_ownership.sql"),
            ),
        ];
        for (version, sql) in migrations {
            let applied = self.connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=?1)",
                [version],
                |row| row.get::<_, bool>(0),
            )?;
            if applied {
                continue;
            }
            for statement in sql.split(';') {
                let stmt = statement.trim();
                if stmt.is_empty() {
                    continue;
                }
                if let Err(error) = self.connection.execute(stmt, []) {
                    if !error.to_string().contains("duplicate column name") {
                        eprintln!("migration {} stmt '{}' failed: {}", version, stmt, error);
                        return Err(error.into());
                    }
                }
            }
            self.connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version) VALUES (?1)",
                [version],
            )?;
        }
        Ok(())
    }

    pub fn workspace_exists(&self, id: &str) -> Result<bool, StoreError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM workspaces WHERE id=?1 AND status!='deleted')",
            [id],
            |row| row.get(0),
        )?)
    }

    pub fn create_workspace(&self, workspace: &WorkspaceRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO workspaces (id,name,slug,status,settings_json,owner_id) VALUES (?1,?2,?3,?4,?5,?6)",
            params![
                workspace.id,
                workspace.name,
                workspace.slug,
                workspace.status,
                serde_json::to_string(&workspace.settings)?,
                workspace.owner_id
            ],
        )?;
        if let Some(ref owner) = workspace.owner_id {
            let _ = self.connection.execute(
                "INSERT OR IGNORE INTO workspace_members (workspace_id, user_id, role) VALUES (?1, ?2, 'owner')",
                params![workspace.id, owner],
            );
        }
        Ok(())
    }

    pub fn list_workspaces(&self) -> Result<Vec<WorkspaceRecord>, StoreError> {
        let mut statement = self.connection.prepare("SELECT id,name,slug,status,settings_json,owner_id FROM workspaces WHERE status!='deleted' ORDER BY rowid")?;
        let rows = statement.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (id, name, slug, status, settings, owner_id) = row?;
            result.push(WorkspaceRecord {
                id,
                name,
                slug,
                status,
                settings: serde_json::from_str(&settings)?,
                owner_id,
            });
        }
        Ok(result)
    }

    pub fn list_workspaces_for_user(&self, user_id: &str) -> Result<Vec<WorkspaceRecord>, StoreError> {
        let mut statement = self.connection.prepare(
            "SELECT w.id, w.name, w.slug, w.status, w.settings_json, w.owner_id
             FROM workspaces w
             LEFT JOIN workspace_members m ON w.id = m.workspace_id AND m.user_id = ?1
             WHERE w.status != 'deleted' AND (w.owner_id = ?1 OR m.user_id IS NOT NULL)
             ORDER BY w.rowid"
        )?;
        let rows = statement.query_map([user_id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        let mut result = Vec::new();
        for row in rows {
            let (id, name, slug, status, settings, owner_id) = row?;
            result.push(WorkspaceRecord {
                id,
                name,
                slug,
                status,
                settings: serde_json::from_str(&settings)?,
                owner_id,
            });
        }
        Ok(result)
    }

    pub fn get_workspace(&self, id: &str) -> Result<Option<WorkspaceRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, name, slug, status, settings_json, owner_id FROM workspaces WHERE id = ?1 AND status != 'deleted'",
            [id],
            |row| {
                Ok(WorkspaceRecord {
                    id: row.get(0)?,
                    name: row.get(1)?,
                    slug: row.get(2)?,
                    status: row.get(3)?,
                    settings: serde_json::from_str(&row.get::<_, String>(4)?).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    owner_id: row.get(5)?,
                })
            }
        ).optional()?)
    }

    pub fn has_workspace_access(&self, workspace_id: &str, user_id: &str) -> Result<bool, StoreError> {
        Ok(self.connection.query_row(
            "SELECT EXISTS(
                SELECT 1 FROM workspaces w
                LEFT JOIN workspace_members m ON w.id = m.workspace_id AND m.user_id = ?2
                WHERE w.id = ?1 AND w.status != 'deleted' AND (w.owner_id = ?2 OR m.user_id IS NOT NULL)
            )",
            params![workspace_id, user_id],
            |row| row.get(0),
        )?)
    }

    pub fn delete_workspace(&self, id: &str) -> Result<bool, StoreError> {
        let count = self
            .connection
            .execute("UPDATE workspaces SET status='deleted' WHERE id=?1", [id])?;
        Ok(count > 0)
    }

    pub fn create_workflow(&self, workflow: &WorkflowRecord) -> Result<(), StoreError> {
        self.connection.execute("INSERT INTO workflows (id,workspace_id,name,title,draft_source,draft_revision,status) VALUES (?1,?2,?3,?4,?5,?6,?7)", params![workflow.id,workflow.workspace_id,workflow.name,workflow.title,workflow.draft_source,workflow.draft_revision,workflow.status])?;
        Ok(())
    }

    pub fn list_workflows(&self, workspace_id: &str) -> Result<Vec<WorkflowRecord>, StoreError> {
        let mut statement=self.connection.prepare("SELECT id,workspace_id,name,title,draft_source,draft_revision,status FROM workflows WHERE workspace_id=?1 AND status!='deleted' ORDER BY rowid DESC")?;
        let rows = statement.query_map([workspace_id], |r| {
            Ok(WorkflowRecord {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                name: r.get(2)?,
                title: r.get(3)?,
                draft_source: r.get(4)?,
                draft_revision: r.get(5)?,
                status: r.get(6)?,
            })
        })?;
        Ok(rows.collect::<Result<Vec<_>, _>>()?)
    }

    pub fn get_workflow(&self, id: &str) -> Result<Option<WorkflowRecord>, StoreError> {
        Ok(self.connection.query_row("SELECT id,workspace_id,name,title,draft_source,draft_revision,status FROM workflows WHERE id=?1",[id],|r|Ok(WorkflowRecord{id:r.get(0)?,workspace_id:r.get(1)?,name:r.get(2)?,title:r.get(3)?,draft_source:r.get(4)?,draft_revision:r.get(5)?,status:r.get(6)?})).optional()?)
    }

    pub fn update_workflow_draft(
        &self,
        id: &str,
        revision: u64,
        source: &str,
    ) -> Result<bool, StoreError> {
        Ok(self.connection.execute("UPDATE workflows SET draft_source=?3,draft_revision=draft_revision+1,updated_at=CURRENT_TIMESTAMP WHERE id=?1 AND draft_revision=?2",params![id,revision,source])?==1)
    }

    pub fn publish_workflow_atomic(
        &mut self,
        workflow_id: &str,
        expected_revision: Option<u64>,
        change_note: Option<&str>,
        built_in_actions: &workflow_actions::ActionRegistry,
    ) -> Result<WorkflowVersionRecord, StoreError> {
        let tx = self.connection.transaction()?;
        let (draft_source, draft_revision, _status) = tx.query_row(
            "SELECT draft_source, draft_revision, status FROM workflows WHERE id = ?1 AND status != 'deleted'",
            [workflow_id],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, u64>(1)?, r.get::<_, String>(2)?)),
        )?;
        let expected = expected_revision.ok_or_else(|| {
            StoreError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "EXPECTED_REVISION_REQUIRED: 发布必须携带已确认的 expected_revision / expectedRevision",
            )))
        })?;
        if draft_revision != expected {
            return Err(StoreError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "REVISION_CONFLICT",
            ))));
        }
        let ir = workflow_schema::compile(&draft_source, built_in_actions)
            .map_err(|e: workflow_core::WorkflowError| StoreError::Serialization(serde_json::Error::io(std::io::Error::new(std::io::ErrorKind::InvalidData, e.to_string()))))?;

        let next_ver: u64 = tx.query_row(
            "SELECT COALESCE(MAX(version), 0) + 1 FROM workflow_versions WHERE workflow_id = ?1",
            [workflow_id],
            |r| r.get(0),
        )?;

        let version_record = WorkflowVersionRecord {
            id: uuid::Uuid::new_v4().to_string(),
            workflow_id: workflow_id.to_string(),
            version: next_ver,
            source: draft_source,
            content_hash: ir.content_hash,
            required_permissions: serde_json::json!(ir.required_permissions),
            change_note: change_note.unwrap_or_default().to_string(),
        };

        // 原子将草稿 revision 递增，确保并发的另一个发布/保存事务在检查 draft_revision == expected 时必定失败产生冲突
        let rows_affected = tx.execute(
            "UPDATE workflows SET draft_revision = draft_revision + 1, updated_at = CURRENT_TIMESTAMP
             WHERE id = ?1 AND draft_revision = ?2",
            params![workflow_id, expected],
        )?;
        if rows_affected != 1 {
            return Err(StoreError::Serialization(serde_json::Error::io(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                "REVISION_CONFLICT",
            ))));
        }

        tx.execute(
            "INSERT INTO workflow_versions (id, workflow_id, version, source, content_hash, required_permissions_json, change_note)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                version_record.id,
                version_record.workflow_id,
                version_record.version,
                version_record.source,
                version_record.content_hash,
                serde_json::to_string(&version_record.required_permissions)?,
                version_record.change_note
            ],
        )?;

        tx.commit()?;
        Ok(version_record)
    }

    pub fn create_version(&self, version: &WorkflowVersionRecord) -> Result<(), StoreError> {
        self.connection.execute("INSERT INTO workflow_versions (id,workflow_id,version,source,content_hash,required_permissions_json,change_note) VALUES (?1,?2,?3,?4,?5,?6,?7)",params![version.id,version.workflow_id,version.version,version.source,version.content_hash,serde_json::to_string(&version.required_permissions)?,version.change_note])?;
        Ok(())
    }
    pub fn next_workflow_version(&self, workflow_id: &str) -> Result<u64, StoreError> {
        Ok(self.connection.query_row(
            "SELECT COALESCE(MAX(version),0)+1 FROM workflow_versions WHERE workflow_id=?1",
            [workflow_id],
            |r| r.get(0),
        )?)
    }
    pub fn get_version(
        &self,
        workflow_id: &str,
        version: u64,
    ) -> Result<Option<WorkflowVersionRecord>, StoreError> {
        let row = self.connection.query_row(
            "SELECT id,workflow_id,version,source,content_hash,required_permissions_json,change_note FROM workflow_versions WHERE workflow_id=?1 AND version=?2",
            params![workflow_id, version],
            |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?, r.get::<_, u64>(2)?, r.get::<_, String>(3)?, r.get::<_, String>(4)?, r.get::<_, String>(5)?, r.get::<_, String>(6)?)),
        ).optional()?;
        let Some((id, workflow_id, version, source, content_hash, permissions, change_note)) = row
        else {
            return Ok(None);
        };
        Ok(Some(WorkflowVersionRecord {
            id,
            workflow_id,
            version,
            source,
            content_hash,
            required_permissions: serde_json::from_str(&permissions)?,
            change_note,
        }))
    }

    pub fn list_versions(
        &self,
        workflow_id: &str,
    ) -> Result<Vec<WorkflowVersionRecord>, StoreError> {
        let mut s=self.connection.prepare("SELECT id,workflow_id,version,source,content_hash,required_permissions_json,change_note FROM workflow_versions WHERE workflow_id=?1 ORDER BY version DESC")?;
        let rows = s.query_map([workflow_id], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, u64>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, String>(4)?,
                r.get::<_, String>(5)?,
                r.get::<_, String>(6)?,
            ))
        })?;
        let mut v = Vec::new();
        for row in rows {
            let (id, workflow_id, version, source, content_hash, p, change_note) = row?;
            v.push(WorkflowVersionRecord {
                id,
                workflow_id,
                version,
                source,
                content_hash,
                required_permissions: serde_json::from_str(&p)?,
                change_note,
            });
        }
        Ok(v)
    }

    pub fn create_run(&self, run: &RunRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO runs (id, workflow_name, workflow_hash, workflow_id, workflow_version_id, run_mode, source_snapshot, draft_revision, parent_run_id, resume_checkpoint_id, status, inputs_json, outputs_json, error_code, error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)",
            params![
                run.id,
                run.workflow_name,
                run.workflow_hash,
                run.workflow_id,
                run.workflow_version_id,
                run.run_mode,
                run.source_snapshot,
                run.draft_revision,
                run.parent_run_id,
                run.resume_checkpoint_id,
                encode_status(run.status),
                serde_json::to_string(&run.inputs)?,
                run.outputs.as_ref().map(serde_json::to_string).transpose()?,
                run.error_code,
                run.error_message
            ],
        )?;
        Ok(())
    }

    pub fn find_idempotent_run(
        &self,
        scope: &str,
        key: &str,
    ) -> Result<Option<(String, String)>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id,request_hash FROM runs WHERE idempotency_scope=?1 AND idempotency_key=?2",
            params![scope, key],
            |row| Ok((row.get(0)?, row.get(1)?)),
        ).optional()?)
    }

    pub fn set_run_idempotency(
        &self,
        run_id: &str,
        scope: &str,
        key: &str,
        request_hash: &str,
    ) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE runs SET idempotency_scope=?2,idempotency_key=?3,request_hash=?4 WHERE id=?1",
            params![run_id, scope, key, request_hash],
        )?;
        Ok(())
    }

    pub fn update_run(&self, run: &RunRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE runs SET status=?2, outputs_json=?3, error_code=?4, error_message=?5, updated_at=CURRENT_TIMESTAMP WHERE id=?1",
            params![run.id, encode_status(run.status), run.outputs.as_ref().map(serde_json::to_string).transpose()?, run.error_code, run.error_message],
        )?;
        Ok(())
    }

    pub fn upsert_step(&self, run_id: &str, step: &StepRunRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO step_runs (run_id, step_id, attempt, status, output_json, error_code, error_message)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(run_id, step_id, attempt) DO UPDATE SET status=excluded.status, output_json=COALESCE(excluded.output_json, step_runs.output_json), error_code=COALESCE(excluded.error_code, step_runs.error_code), error_message=COALESCE(excluded.error_message, step_runs.error_message)",
            params![run_id, step.step_id, step.attempt, serde_json::to_string(&step.status)?, step.output.as_ref().map(serde_json::to_string).transpose()?, step.error_code, step.error_message],
        )?;
        Ok(())
    }

    pub fn append_event(&self, event: &RunEvent) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO run_events (run_id, sequence, event_type, payload_json) VALUES (?1, ?2, ?3, ?4)",
            params![event.run_id, event.sequence, event.event_type, serde_json::to_string(&event.payload)?],
        )?;
        Ok(())
    }

    pub fn get_run(&self, id: &str) -> Result<Option<RunRecord>, StoreError> {
        let mut run = self.connection.query_row(
            "SELECT id, workflow_name, workflow_hash, workflow_id, workflow_version_id, run_mode, source_snapshot, draft_revision, parent_run_id, resume_checkpoint_id, status, inputs_json, outputs_json, error_code, error_message FROM runs WHERE id=?1",
            [id],
            |row| Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, Option<String>>(6)?,
                row.get::<_, Option<u64>>(7)?,
                row.get::<_, Option<String>>(8)?,
                row.get::<_, Option<String>>(9)?,
                row.get::<_, String>(10)?,
                row.get::<_, String>(11)?,
                row.get::<_, Option<String>>(12)?,
                row.get::<_, Option<String>>(13)?,
                row.get::<_, Option<String>>(14)?,
            )),
        ).optional()?;
        let Some((
            id,
            workflow_name,
            workflow_hash,
            workflow_id,
            workflow_version_id,
            run_mode,
            source_snapshot,
            draft_revision,
            parent_run_id,
            resume_checkpoint_id,
            status,
            inputs,
            outputs,
            error_code,
            error_message,
        )) = run.take()
        else {
            return Ok(None);
        };
        let mut statement = self.connection.prepare("SELECT step_id, attempt, status, output_json, error_code, error_message FROM step_runs WHERE run_id=?1 ORDER BY rowid")?;
        let rows = statement.query_map([&id], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, u32>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, Option<String>>(3)?,
                row.get::<_, Option<String>>(4)?,
                row.get::<_, Option<String>>(5)?,
            ))
        })?;
        let mut steps = Vec::new();
        for row in rows {
            let (step_id, attempt, status, output, error_code, error_message) = row?;
            steps.push(StepRunRecord {
                step_id,
                attempt,
                status: serde_json::from_str(&status)?,
                output: output
                    .map(|value| serde_json::from_str(&value))
                    .transpose()?,
                error_code,
                error_message,
            });
        }
        Ok(Some(RunRecord {
            id,
            workflow_name,
            workflow_hash,
            workflow_id,
            workflow_version_id,
            run_mode,
            source_snapshot,
            draft_revision,
            parent_run_id,
            resume_checkpoint_id,
            status: decode_status(&status),
            inputs: serde_json::from_str(&inputs)?,
            outputs: outputs
                .map(|value| serde_json::from_str(&value))
                .transpose()?,
            error_code,
            error_message,
            steps,
        }))
    }

    pub fn get_run_workspace_id(&self, run_id: &str) -> Result<Option<String>, StoreError> {
        let sql = "SELECT w.workspace_id FROM runs r LEFT JOIN workflows w ON r.workflow_id = w.id WHERE r.id = ?1";
        Ok(self.connection.query_row(sql, [run_id], |row| row.get(0)).optional()?)
    }

    pub fn next_sequence(&self, run_id: &str) -> Result<u64, StoreError> {
        let sequence = self.connection.query_row(
            "SELECT COALESCE(MAX(sequence), 0) + 1 FROM run_events WHERE run_id=?1",
            [run_id],
            |row| row.get(0),
        )?;
        Ok(sequence)
    }

    pub fn mark_incomplete_interrupted(&self) -> Result<usize, StoreError> {
        let changed = self.connection.execute(
            "UPDATE runs SET status='interrupted', error_code='RUNNER_INTERRUPTED', error_message='run was left incomplete by a terminated process', updated_at=CURRENT_TIMESTAMP WHERE status IN ('preparing', 'running', 'pausing', 'cancelling')",
            [],
        )?;
        Ok(changed)
    }

    pub fn list_runs(
        &self,
        limit: usize,
        before_rowid: Option<i64>,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let sql = if before_rowid.is_some() {
            "SELECT id FROM runs WHERE rowid < ?1 ORDER BY rowid DESC LIMIT ?2"
        } else {
            "SELECT id FROM runs ORDER BY rowid DESC LIMIT ?1"
        };
        let mut statement = self.connection.prepare(sql)?;
        let ids = if let Some(before) = before_rowid {
            statement
                .query_map(params![before, limit as i64], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map([limit as i64], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        ids.into_iter()
            .map(|id| self.get_run(&id).map(|run| run.expect("listed run exists")))
            .collect()
    }

    pub fn list_runs_for_user(
        &self,
        user_id: &str,
        limit: usize,
        before_rowid: Option<i64>,
    ) -> Result<Vec<RunRecord>, StoreError> {
        let sql = if before_rowid.is_some() {
            "SELECT r.id FROM runs r
             JOIN workflows w ON r.workflow_id = w.id
             JOIN workspaces ws ON w.workspace_id = ws.id
             LEFT JOIN workspace_members m ON ws.id = m.workspace_id AND m.user_id = ?1
             WHERE (ws.owner_id = ?1 OR m.user_id IS NOT NULL) AND r.rowid < ?2
             ORDER BY r.rowid DESC LIMIT ?3"
        } else {
            "SELECT r.id FROM runs r
             JOIN workflows w ON r.workflow_id = w.id
             JOIN workspaces ws ON w.workspace_id = ws.id
             LEFT JOIN workspace_members m ON ws.id = m.workspace_id AND m.user_id = ?1
             WHERE (ws.owner_id = ?1 OR m.user_id IS NOT NULL)
             ORDER BY r.rowid DESC LIMIT ?2"
        };
        let mut statement = self.connection.prepare(sql)?;
        let ids = if let Some(before) = before_rowid {
            statement
                .query_map(params![user_id, before, limit as i64], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        } else {
            statement
                .query_map(params![user_id, limit as i64], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        ids.into_iter()
            .map(|id| self.get_run(&id).map(|run| run.expect("listed run exists")))
            .collect()
    }

    pub fn run_rowid(&self, id: &str) -> Result<Option<i64>, StoreError> {
        Ok(self
            .connection
            .query_row("SELECT rowid FROM runs WHERE id=?1", [id], |row| row.get(0))
            .optional()?)
    }

    pub fn events_after(
        &self,
        run_id: &str,
        after_sequence: u64,
        limit: usize,
    ) -> Result<Vec<RunEvent>, StoreError> {
        let mut statement = self.connection.prepare("SELECT sequence, event_type, payload_json FROM run_events WHERE run_id=?1 AND sequence>?2 ORDER BY sequence LIMIT ?3")?;
        let rows = statement.query_map(params![run_id, after_sequence, limit as i64], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut events = Vec::new();
        for row in rows {
            let (sequence, event_type, payload) = row?;
            events.push(RunEvent {
                run_id: run_id.into(),
                sequence,
                event_type,
                payload: serde_json::from_str(&payload)?,
            });
        }
        Ok(events)
    }

    pub fn events(&self, run_id: &str) -> Result<Vec<RunEvent>, StoreError> {
        let mut statement = self.connection.prepare("SELECT sequence, event_type, payload_json FROM run_events WHERE run_id=?1 ORDER BY sequence")?;
        let rows = statement.query_map([run_id], |row| {
            Ok((
                row.get::<_, u64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut events = Vec::new();
        for row in rows {
            let (sequence, event_type, payload) = row?;
            events.push(RunEvent {
                run_id: run_id.into(),
                sequence,
                event_type,
                payload: serde_json::from_str(&payload)?,
            });
        }
        Ok(events)
    }

    pub fn save_checkpoint(&self, checkpoint: &CheckpointRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO checkpoints (id, run_id, step_id, workflow_hash, completed_steps_json, context_snapshot_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                checkpoint.id,
                checkpoint.run_id,
                checkpoint.step_id,
                checkpoint.workflow_hash,
                serde_json::to_string(&checkpoint.completed_steps)?,
                serde_json::to_string(&checkpoint.context_snapshot)?
            ],
        )?;
        Ok(())
    }

    pub fn get_checkpoint(&self, id: &str) -> Result<Option<CheckpointRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, run_id, step_id, workflow_hash, completed_steps_json, context_snapshot_json, created_at FROM checkpoints WHERE id=?1",
            [id],
            |row| {
                Ok(CheckpointRecord {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    step_id: row.get(2)?,
                    workflow_hash: row.get(3)?,
                    completed_steps: serde_json::from_str(&row.get::<_, String>(4)?).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    context_snapshot: serde_json::from_str(&row.get::<_, String>(5)?).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    created_at: row.get(6)?,
                })
            }
        ).optional()?)
    }

    pub fn get_latest_checkpoint_for_run(
        &self,
        run_id: &str,
    ) -> Result<Option<CheckpointRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, run_id, step_id, workflow_hash, completed_steps_json, context_snapshot_json, created_at FROM checkpoints WHERE run_id=?1 ORDER BY rowid DESC LIMIT 1",
            [run_id],
            |row| {
                Ok(CheckpointRecord {
                    id: row.get(0)?,
                    run_id: row.get(1)?,
                    step_id: row.get(2)?,
                    workflow_hash: row.get(3)?,
                    completed_steps: serde_json::from_str(&row.get::<_, String>(4)?).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    context_snapshot: serde_json::from_str(&row.get::<_, String>(5)?).map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    created_at: row.get(6)?,
                })
            }
        ).optional()?)
    }

    pub fn save_audit_log(&self, log: &AuditRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO audit_logs (id, workspace_id, action, resource_type, resource_id, actor, details_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                log.id,
                log.workspace_id,
                log.action,
                log.resource_type,
                log.resource_id,
                log.actor,
                serde_json::to_string(&log.details)?
            ],
        )?;
        Ok(())
    }

    pub fn list_audit_logs(
        &self,
        workspace_id: Option<&str>,
        limit: usize,
    ) -> Result<Vec<AuditRecord>, StoreError> {
        if let Some(ws) = workspace_id {
            let mut stmt = self.connection.prepare(
                "SELECT id, workspace_id, action, resource_type, resource_id, actor, details_json, created_at FROM audit_logs WHERE workspace_id=?1 ORDER BY rowid DESC LIMIT ?2"
            )?;
            let rows = stmt.query_map(params![ws, limit as i64], |row| {
                Ok(AuditRecord {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    action: row.get(2)?,
                    resource_type: row.get(3)?,
                    resource_id: row.get(4)?,
                    actor: row.get(5)?,
                    details: serde_json::from_str(&row.get::<_, String>(6)?)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    created_at: row.get(7)?,
                })
            })?;
            let mut logs = Vec::new();
            for r in rows {
                logs.push(r?);
            }
            Ok(logs)
        } else {
            let mut stmt = self.connection.prepare(
                "SELECT id, workspace_id, action, resource_type, resource_id, actor, details_json, created_at FROM audit_logs ORDER BY rowid DESC LIMIT ?1"
            )?;
            let rows = stmt.query_map(params![limit as i64], |row| {
                Ok(AuditRecord {
                    id: row.get(0)?,
                    workspace_id: row.get(1)?,
                    action: row.get(2)?,
                    resource_type: row.get(3)?,
                    resource_id: row.get(4)?,
                    actor: row.get(5)?,
                    details: serde_json::from_str(&row.get::<_, String>(6)?)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    created_at: row.get(7)?,
                })
            })?;
            let mut logs = Vec::new();
            for r in rows {
                logs.push(r?);
            }
            Ok(logs)
        }
    }

    pub fn create_schedule(&self, s: &ScheduleRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO schedules (id, workspace_id, workflow_id, name, cron_expression, inputs_json, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            params![
                s.id,
                s.workspace_id,
                s.workflow_id,
                s.name,
                s.cron_expression,
                serde_json::to_string(&s.inputs)?,
                if s.enabled { 1 } else { 0 }
            ],
        )?;
        Ok(())
    }

    pub fn list_schedules(&self, workspace_id: &str) -> Result<Vec<ScheduleRecord>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, workspace_id, workflow_id, name, cron_expression, inputs_json, enabled, last_run_at, created_at FROM schedules WHERE workspace_id=?1 ORDER BY rowid DESC"
        )?;
        let rows = stmt.query_map([workspace_id], |r| {
            Ok(ScheduleRecord {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                workflow_id: r.get(2)?,
                name: r.get(3)?,
                cron_expression: r.get(4)?,
                inputs: serde_json::from_str(&r.get::<_, String>(5)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                enabled: r.get::<_, i64>(6)? != 0,
                last_run_at: r.get(7)?,
                created_at: r.get(8)?,
            })
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        Ok(v)
    }

    pub fn list_all_enabled_schedules(&self) -> Result<Vec<ScheduleRecord>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, workspace_id, workflow_id, name, cron_expression, inputs_json, enabled, last_run_at, created_at FROM schedules WHERE enabled=1"
        )?;
        let rows = stmt.query_map([], |r| {
            Ok(ScheduleRecord {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                workflow_id: r.get(2)?,
                name: r.get(3)?,
                cron_expression: r.get(4)?,
                inputs: serde_json::from_str(&r.get::<_, String>(5)?)
                    .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                enabled: true,
                last_run_at: r.get(7)?,
                created_at: r.get(8)?,
            })
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        Ok(v)
    }

    pub fn delete_schedule(&self, id: &str) -> Result<bool, StoreError> {
        let count = self
            .connection
            .execute("DELETE FROM schedules WHERE id=?1", [id])?;
        Ok(count > 0)
    }

    pub fn get_schedule(&self, id: &str) -> Result<Option<ScheduleRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, workspace_id, workflow_id, name, cron_expression, inputs_json, enabled, last_run_at, created_at FROM schedules WHERE id=?1",
            [id],
            |r| {
                Ok(ScheduleRecord {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    workflow_id: r.get(2)?,
                    name: r.get(3)?,
                    cron_expression: r.get(4)?,
                    inputs: serde_json::from_str(&r.get::<_, String>(5)?)
                        .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?,
                    enabled: r.get::<_, i64>(6)? != 0,
                    last_run_at: r.get(7)?,
                    created_at: r.get(8)?,
                })
            }
        ).optional()?)
    }

    pub fn update_schedule_last_run(&self, id: &str) -> Result<(), StoreError> {
        self.connection.execute(
            "UPDATE schedules SET last_run_at=CURRENT_TIMESTAMP WHERE id=?1",
            [id],
        )?;
        Ok(())
    }

    pub fn create_webhook(&self, w: &WebhookRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO webhooks (id, workspace_id, workflow_id, name, token, enabled)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            params![
                w.id,
                w.workspace_id,
                w.workflow_id,
                w.name,
                w.token,
                if w.enabled { 1 } else { 0 }
            ],
        )?;
        Ok(())
    }

    pub fn get_webhook_by_token(&self, token: &str) -> Result<Option<WebhookRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, workspace_id, workflow_id, name, token, enabled, created_at FROM webhooks WHERE token=?1 AND enabled=1",
            [token],
            |r| {
                Ok(WebhookRecord {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    workflow_id: r.get(2)?,
                    name: r.get(3)?,
                    token: r.get(4)?,
                    enabled: r.get::<_, i64>(5)? != 0,
                    created_at: r.get(6)?,
                })
            }
        ).optional()?)
    }

    pub fn list_webhooks(&self, workspace_id: &str) -> Result<Vec<WebhookRecord>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, workspace_id, workflow_id, name, token, enabled, created_at FROM webhooks WHERE workspace_id=?1 ORDER BY rowid DESC"
        )?;
        let rows = stmt.query_map([workspace_id], |r| {
            Ok(WebhookRecord {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                workflow_id: r.get(2)?,
                name: r.get(3)?,
                token: r.get(4)?,
                enabled: r.get::<_, i64>(5)? != 0,
                created_at: r.get(6)?,
            })
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        Ok(v)
    }

    pub fn delete_webhook(&self, id: &str) -> Result<bool, StoreError> {
        let count = self
            .connection
            .execute("DELETE FROM webhooks WHERE id=?1", [id])?;
        Ok(count > 0)
    }

    pub fn get_webhook(&self, id: &str) -> Result<Option<WebhookRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, workspace_id, workflow_id, name, token, enabled, created_at FROM webhooks WHERE id=?1",
            [id],
            |r| {
                Ok(WebhookRecord {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    workflow_id: r.get(2)?,
                    name: r.get(3)?,
                    token: r.get(4)?,
                    enabled: r.get::<_, i64>(5)? != 0,
                    created_at: r.get(6)?,
                })
            }
        ).optional()?)
    }

    pub fn save_credential(&self, c: &CredentialRecord) -> Result<(), StoreError> {
        self.connection.execute(
            "INSERT INTO credentials (id, workspace_id, name, kind, value)
             VALUES (?1, ?2, ?3, ?4, ?5)
             ON CONFLICT(workspace_id, name) DO UPDATE SET kind=excluded.kind, value=excluded.value",
            params![c.id, c.workspace_id, c.name, c.kind, c.value],
        )?;
        Ok(())
    }

    pub fn list_credentials(
        &self,
        workspace_id: &str,
    ) -> Result<Vec<CredentialRecord>, StoreError> {
        let mut stmt = self.connection.prepare(
            "SELECT id, workspace_id, name, kind, value, created_at FROM credentials WHERE workspace_id=?1 ORDER BY rowid DESC"
        )?;
        let rows = stmt.query_map([workspace_id], |r| {
            Ok(CredentialRecord {
                id: r.get(0)?,
                workspace_id: r.get(1)?,
                name: r.get(2)?,
                kind: r.get(3)?,
                value: "***REDACTED***".into(),
                created_at: r.get(5)?,
            })
        })?;
        let mut v = Vec::new();
        for row in rows {
            v.push(row?);
        }
        Ok(v)
    }

    pub fn get_credential(
        &self,
        workspace_id: &str,
        name: &str,
    ) -> Result<Option<CredentialRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, workspace_id, name, kind, value, created_at FROM credentials WHERE workspace_id=?1 AND name=?2",
            [workspace_id, name],
            |r| {
                Ok(CredentialRecord {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    name: r.get(2)?,
                    kind: r.get(3)?,
                    value: r.get(4)?,
                    created_at: r.get(5)?,
                })
            }
        ).optional()?)
    }

    pub fn delete_credential(&self, id: &str) -> Result<bool, StoreError> {
        let count = self
            .connection
            .execute("DELETE FROM credentials WHERE id=?1", [id])?;
        Ok(count > 0)
    }

    pub fn get_credential_by_id(&self, id: &str) -> Result<Option<CredentialRecord>, StoreError> {
        Ok(self.connection.query_row(
            "SELECT id, workspace_id, name, kind, value, created_at FROM credentials WHERE id=?1",
            [id],
            |r| {
                Ok(CredentialRecord {
                    id: r.get(0)?,
                    workspace_id: r.get(1)?,
                    name: r.get(2)?,
                    kind: r.get(3)?,
                    value: r.get(4)?,
                    created_at: r.get(5)?,
                })
            }
        ).optional()?)
    }
}

fn encode_status(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Queued => "queued",
        RunStatus::Preparing => "preparing",
        RunStatus::Running => "running",
        RunStatus::Pausing => "pausing",
        RunStatus::Paused => "paused",
        RunStatus::Cancelling => "cancelling",
        RunStatus::Cancelled => "cancelled",
        RunStatus::Succeeded => "succeeded",
        RunStatus::Failed => "failed",
        RunStatus::Interrupted => "interrupted",
    }
}

fn decode_status(status: &str) -> RunStatus {
    match status {
        "queued" => RunStatus::Queued,
        "preparing" => RunStatus::Preparing,
        "running" => RunStatus::Running,
        "pausing" => RunStatus::Pausing,
        "paused" => RunStatus::Paused,
        "cancelling" => RunStatus::Cancelling,
        "cancelled" => RunStatus::Cancelled,
        "succeeded" => RunStatus::Succeeded,
        "interrupted" => RunStatus::Interrupted,
        _ => RunStatus::Failed,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn stores_versioned_run_metadata() {
        let store = SqliteStore::in_memory().unwrap();
        let run = RunRecord {
            id: "versioned".into(),
            workflow_name: "demo".into(),
            workflow_hash: "hash".into(),
            workflow_id: None,
            workflow_version_id: None,
            run_mode: "published".into(),
            source_snapshot: Some("source".into()),
            draft_revision: None,
            parent_run_id: None,
            resume_checkpoint_id: None,
            status: RunStatus::Queued,
            inputs: json!({}),
            outputs: None,
            error_code: None,
            error_message: None,
            steps: vec![],
        };
        store.create_run(&run).unwrap();
        let stored = store.get_run("versioned").unwrap().unwrap();
        assert_eq!(stored.workflow_id, None);
        assert_eq!(stored.workflow_version_id, None);
        assert_eq!(stored.run_mode, "published");
        assert_eq!(stored.source_snapshot.as_deref(), Some("source"));
    }

    #[test]
    fn idempotency_is_scoped_and_hash_aware() {
        let store = SqliteStore::in_memory().unwrap();
        let run = RunRecord {
            id: "run-idem".into(),
            workflow_name: "demo".into(),
            workflow_hash: "hash".into(),
            workflow_id: None,
            workflow_version_id: None,
            run_mode: "debug".into(),
            source_snapshot: None,
            draft_revision: Some(1),
            parent_run_id: None,
            resume_checkpoint_id: None,
            status: RunStatus::Queued,
            inputs: json!({}),
            outputs: None,
            error_code: None,
            error_message: None,
            steps: vec![],
        };
        store.create_run(&run).unwrap();
        store
            .set_run_idempotency("run-idem", "workspace-a", "same-key", "hash-a")
            .unwrap();
        assert_eq!(
            store
                .find_idempotent_run("workspace-a", "same-key")
                .unwrap(),
            Some(("run-idem".into(), "hash-a".into()))
        );
        assert_eq!(
            store
                .find_idempotent_run("workspace-b", "same-key")
                .unwrap(),
            None
        );
    }

    #[test]
    fn round_trips_run_and_events() {
        let store = SqliteStore::in_memory().unwrap();
        assert!(!store.workspace_exists("missing").unwrap());
        let run = RunRecord {
            id: "run-1".into(),
            workflow_name: "demo".into(),
            workflow_hash: "hash".into(),
            workflow_id: None,
            workflow_version_id: None,
            run_mode: "legacy".into(),
            source_snapshot: None,
            draft_revision: None,
            parent_run_id: None,
            resume_checkpoint_id: None,
            status: RunStatus::Queued,
            inputs: json!({}),
            outputs: None,
            error_code: None,
            error_message: None,
            steps: vec![],
        };
        store.create_run(&run).unwrap();
        store
            .append_event(&RunEvent {
                run_id: "run-1".into(),
                sequence: 1,
                event_type: "run.queued".into(),
                payload: json!({}),
            })
            .unwrap();
        assert_eq!(
            store.get_run("run-1").unwrap().unwrap().status,
            RunStatus::Queued
        );
        assert_eq!(store.events("run-1").unwrap().len(), 1);
    }

    #[test]
    fn saves_and_retrieves_checkpoints_and_audit_logs() {
        let store = SqliteStore::in_memory().unwrap();
        let run = RunRecord {
            id: "run-1".into(),
            workflow_name: "demo".into(),
            workflow_hash: "hash123".into(),
            workflow_id: None,
            workflow_version_id: None,
            run_mode: "debug".into(),
            source_snapshot: None,
            draft_revision: None,
            parent_run_id: None,
            resume_checkpoint_id: None,
            status: RunStatus::Queued,
            inputs: json!({}),
            outputs: None,
            error_code: None,
            error_message: None,
            steps: vec![],
        };
        store.create_run(&run).unwrap();

        let cp = CheckpointRecord {
            id: "cp-1".into(),
            run_id: "run-1".into(),
            step_id: "step-1".into(),
            workflow_hash: "hash123".into(),
            completed_steps: vec!["step-0".into(), "step-1".into()],
            context_snapshot: json!({ "vars": { "token": "abc" } }),
            created_at: "2026-08-25T10:00:00Z".into(),
        };
        store.save_checkpoint(&cp).unwrap();
        let loaded = store.get_checkpoint("cp-1").unwrap().unwrap();
        assert_eq!(loaded.step_id, "step-1");
        assert_eq!(loaded.completed_steps.len(), 2);

        let latest = store
            .get_latest_checkpoint_for_run("run-1")
            .unwrap()
            .unwrap();
        assert_eq!(latest.id, "cp-1");

        let audit = AuditRecord {
            id: "aud-1".into(),
            workspace_id: Some("ws-1".into()),
            action: "workflow.create".into(),
            resource_type: "workflow".into(),
            resource_id: "wf-1".into(),
            actor: "test-user".into(),
            details: json!({ "name": "my-wf" }),
            created_at: "".into(),
        };
        store.save_audit_log(&audit).unwrap();
        let logs = store.list_audit_logs(Some("ws-1"), 10).unwrap();
        assert_eq!(logs.len(), 1);
        assert_eq!(logs[0].action, "workflow.create");
    }

    #[test]
    fn manages_schedules_webhooks_and_credentials() {
        let store = SqliteStore::in_memory().unwrap();
        let ws = WorkspaceRecord {
            id: "ws-test".into(),
            name: "test".into(),
            slug: "test".into(),
            status: "active".into(),
            settings: json!({}),
        };
        store.create_workspace(&ws).unwrap();

        let wf = WorkflowRecord {
            id: "wf-test".into(),
            workspace_id: "ws-test".into(),
            name: "wf".into(),
            title: "wf".into(),
            draft_source: "".into(),
            draft_revision: 1,
            status: "active".into(),
        };
        store.create_workflow(&wf).unwrap();

        // 1. Schedule
        let s = ScheduleRecord {
            id: "s-1".into(),
            workspace_id: "ws-test".into(),
            workflow_id: "wf-test".into(),
            name: "Every 5 min".into(),
            cron_expression: "*/5 * * * *".into(),
            inputs: json!({}),
            enabled: true,
            last_run_at: None,
            created_at: "".into(),
        };
        store.create_schedule(&s).unwrap();
        let list_s = store.list_schedules("ws-test").unwrap();
        assert_eq!(list_s.len(), 1);
        assert_eq!(list_s[0].name, "Every 5 min");

        // Workspace deletion
        assert!(store.delete_workspace("ws-test").unwrap());
        assert!(!store.workspace_exists("ws-test").unwrap());
        assert!(store.list_workspaces().unwrap().is_empty());

        // 2. Webhook
        let wh = WebhookRecord {
            id: "wh-1".into(),
            workspace_id: "ws-test".into(),
            workflow_id: "wf-test".into(),
            name: "GitHub Trigger".into(),
            token: "secret_token_123".into(),
            enabled: true,
            created_at: "".into(),
        };
        store.create_webhook(&wh).unwrap();
        let found_wh = store
            .get_webhook_by_token("secret_token_123")
            .unwrap()
            .unwrap();
        assert_eq!(found_wh.workflow_id, "wf-test");

        // 3. Credential
        let cred = CredentialRecord {
            id: "c-1".into(),
            workspace_id: "ws-test".into(),
            name: "API_TOKEN".into(),
            kind: "bearer_token".into(),
            value: "super_secret_token".into(),
            created_at: "".into(),
        };
        store.save_credential(&cred).unwrap();
        let creds = store.list_credentials("ws-test").unwrap();
        assert_eq!(creds.len(), 1);
        assert_eq!(creds[0].value, "***REDACTED***");

        let fetched = store
            .get_credential("ws-test", "API_TOKEN")
            .unwrap()
            .unwrap();
        assert_eq!(fetched.value, "super_secret_token");
    }
}
