ALTER TABLE runs ADD COLUMN workflow_id TEXT REFERENCES workflows(id) ON DELETE SET NULL;
ALTER TABLE runs ADD COLUMN workflow_version_id TEXT REFERENCES workflow_versions(id) ON DELETE SET NULL;
ALTER TABLE runs ADD COLUMN run_mode TEXT NOT NULL DEFAULT 'legacy';
ALTER TABLE runs ADD COLUMN source_snapshot TEXT;
ALTER TABLE runs ADD COLUMN draft_revision INTEGER;
ALTER TABLE runs ADD COLUMN idempotency_scope TEXT;
ALTER TABLE runs ADD COLUMN idempotency_key TEXT;
ALTER TABLE runs ADD COLUMN request_hash TEXT;
ALTER TABLE runs ADD COLUMN started_at TEXT;
ALTER TABLE runs ADD COLUMN finished_at TEXT;
ALTER TABLE runs ADD COLUMN cancel_requested_at TEXT;
ALTER TABLE runs ADD COLUMN parent_run_id TEXT REFERENCES runs(id) ON DELETE SET NULL;

CREATE UNIQUE INDEX IF NOT EXISTS runs_idempotency_scope_key
ON runs(idempotency_scope, idempotency_key)
WHERE idempotency_key IS NOT NULL;
