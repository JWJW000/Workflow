ALTER TABLE step_runs ADD COLUMN started_at TEXT;
ALTER TABLE step_runs ADD COLUMN finished_at TEXT;
ALTER TABLE step_runs ADD COLUMN duration_ms INTEGER;
ALTER TABLE step_runs ADD COLUMN input_json_redacted TEXT;
ALTER TABLE step_runs ADD COLUMN error_details_json TEXT;
ALTER TABLE step_runs ADD COLUMN retry_delay_ms INTEGER;
ALTER TABLE step_runs ADD COLUMN checkpoint_safe INTEGER NOT NULL DEFAULT 0;

CREATE TABLE IF NOT EXISTS artifacts (
  id TEXT PRIMARY KEY,
  run_id TEXT NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  step_id TEXT,
  kind TEXT NOT NULL,
  relative_path TEXT NOT NULL,
  original_name TEXT,
  content_type TEXT NOT NULL,
  byte_count INTEGER NOT NULL,
  sha256 TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
  retention_until TEXT,
  UNIQUE(run_id, relative_path)
);
