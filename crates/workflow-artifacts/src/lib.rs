use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    fs,
    path::{Component, Path, PathBuf},
};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ArtifactMetadata {
    pub relative_path: String,
    pub byte_count: u64,
    pub sha256: String,
    pub content_type: String,
}

#[derive(Debug, Error)]
pub enum ArtifactError {
    #[error("artifact path must be relative and cannot contain parent components: {0}")]
    PathViolation(String),
    #[error("artifact I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("artifact serialization failed: {0}")]
    Serialization(String),
}

#[derive(Debug, Clone)]
pub struct ArtifactStore {
    root: PathBuf,
}

impl ArtifactStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, ArtifactError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    pub fn resolve(&self, relative: &str) -> Result<PathBuf, ArtifactError> {
        let path = Path::new(relative);
        if path.is_absolute()
            || path.components().any(|part| {
                matches!(
                    part,
                    Component::ParentDir | Component::RootDir | Component::Prefix(_)
                )
            })
        {
            return Err(ArtifactError::PathViolation(relative.into()));
        }
        Ok(self.root.join(path))
    }

    pub fn write_json(
        &self,
        relative: &str,
        value: &Value,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let bytes = serde_json::to_vec_pretty(value)
            .map_err(|error| ArtifactError::Serialization(error.to_string()))?;
        self.write(relative, &bytes, "application/json")
    }

    pub fn write_jsonl(
        &self,
        relative: &str,
        value: &Value,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let values = value
            .as_array()
            .map(Vec::as_slice)
            .unwrap_or(std::slice::from_ref(value));
        let mut bytes = Vec::new();
        for value in values {
            serde_json::to_writer(&mut bytes, value)
                .map_err(|error| ArtifactError::Serialization(error.to_string()))?;
            bytes.push(b'\n');
        }
        self.write(relative, &bytes, "application/x-ndjson")
    }

    pub fn write_csv(
        &self,
        relative: &str,
        rows: &Value,
        columns: Option<&[String]>,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let rows = rows
            .as_array()
            .ok_or_else(|| ArtifactError::Serialization("CSV rows must be an array".into()))?;
        let inferred: Vec<String> = rows
            .first()
            .and_then(Value::as_object)
            .map(|row| row.keys().cloned().collect())
            .unwrap_or_default();
        let columns = columns.unwrap_or(&inferred);
        let mut writer = csv::Writer::from_writer(Vec::new());
        writer
            .write_record(columns)
            .map_err(|error| ArtifactError::Serialization(error.to_string()))?;
        for row in rows {
            let row = row.as_object().ok_or_else(|| {
                ArtifactError::Serialization("each CSV row must be an object".into())
            })?;
            let record = columns
                .iter()
                .map(|column| scalar_string(row.get(column).unwrap_or(&Value::Null)));
            writer
                .write_record(record)
                .map_err(|error| ArtifactError::Serialization(error.to_string()))?;
        }
        let bytes = writer
            .into_inner()
            .map_err(|error| ArtifactError::Serialization(error.to_string()))?;
        self.write(relative, &bytes, "text/csv; charset=utf-8")
    }

    pub fn write_bytes(
        &self,
        relative: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        self.write(relative, bytes, content_type)
    }

    pub fn ingest_file(
        &self,
        source: &Path,
        relative: &str,
        content_type: &str,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let bytes = fs::read(source)?;
        self.write(relative, &bytes, content_type)
    }

    /// Count listed articles vs real PDFs under `dir` (one subdirectory per journal).
    pub fn summarize_downloads(&self, dir: &str) -> Result<Value, ArtifactError> {
        use std::collections::BTreeSet;

        let root = self.resolve(dir)?;
        let mut journals = Vec::new();
        let mut total_listed: u64 = 0;
        let mut total_downloaded: u64 = 0;
        let mut total_bytes: u64 = 0;

        let mut entries: Vec<PathBuf> = Vec::new();
        if root.is_dir() {
            for entry in fs::read_dir(&root)? {
                let path = entry?.path();
                if path.is_dir() {
                    entries.push(path);
                }
            }
        }
        entries.sort();

        for folder in entries {
            let name = folder
                .file_name()
                .map(|n| n.to_string_lossy().into_owned())
                .unwrap_or_default();
            let listed_path = folder.join("listed.json");
            let listed = if listed_path.is_file() {
                fs::read_to_string(&listed_path)
                    .ok()
                    .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                    .and_then(|value| value.as_array().map(|rows| rows.len() as u64))
                    .unwrap_or(0)
            } else {
                0
            };
            let journal_name = fs::read_to_string(folder.join("journal.json"))
                .ok()
                .and_then(|text| serde_json::from_str::<Value>(&text).ok())
                .and_then(|value| value.get("name").and_then(Value::as_str).map(str::to_owned));
            let mut hashes = BTreeSet::new();
            let mut bytes = 0u64;
            if let Ok(files) = fs::read_dir(&folder) {
                for file in files.flatten() {
                    let path = file.path();
                    if !path.is_file() {
                        continue;
                    }
                    let ext = path
                        .extension()
                        .and_then(|ext| ext.to_str())
                        .unwrap_or("")
                        .to_ascii_lowercase();
                    if ext != "pdf" {
                        continue;
                    }
                    let Ok(content) = fs::read(&path) else {
                        continue;
                    };
                    if content.len() < 800 || !content.starts_with(b"%PDF") {
                        continue;
                    }
                    let digest = format!("{:x}", Sha256::digest(&content));
                    if hashes.insert(digest) {
                        bytes += content.len() as u64;
                    }
                }
            }
            let downloaded = hashes.len() as u64;
            total_listed += listed;
            total_downloaded += downloaded;
            total_bytes += bytes;
            journals.push(json!({
                "folder": name,
                "name": journal_name,
                "listed": listed,
                "downloaded": downloaded,
                "byteCount": bytes,
            }));
        }

        Ok(json!({
            "dir": dir,
            "journalCount": journals.len(),
            "totalArticlesListed": total_listed,
            "totalDownloaded": total_downloaded,
            "totalBytes": total_bytes,
            "journals": journals,
        }))
    }

    pub fn metadata(
        &self,
        relative: &str,
        content_type: &str,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let path = self.resolve(relative)?;
        let bytes = fs::read(path)?;
        Ok(metadata(relative, &bytes, content_type))
    }

    fn write(
        &self,
        relative: &str,
        bytes: &[u8],
        content_type: &str,
    ) -> Result<ArtifactMetadata, ArtifactError> {
        let path = self.resolve(relative)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let temporary = path.with_extension(format!(
            "{}.tmp",
            path.extension()
                .and_then(|value| value.to_str())
                .unwrap_or("artifact")
        ));
        fs::write(&temporary, bytes)?;
        fs::rename(temporary, path)?;
        Ok(metadata(relative, bytes, content_type))
    }
}

fn metadata(relative: &str, bytes: &[u8], content_type: &str) -> ArtifactMetadata {
    ArtifactMetadata {
        relative_path: relative.into(),
        byte_count: bytes.len() as u64,
        sha256: format!("{:x}", Sha256::digest(bytes)),
        content_type: content_type.into(),
    }
}

fn scalar_string(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::String(value) => value.clone(),
        other => other.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_path_traversal() {
        let store =
            ArtifactStore::new(std::env::temp_dir().join("drission-artifact-test")).unwrap();
        assert!(matches!(
            store.resolve("../secret"),
            Err(ArtifactError::PathViolation(_))
        ));
        assert!(matches!(
            store.resolve("/tmp/secret"),
            Err(ArtifactError::PathViolation(_))
        ));
    }

    #[test]
    fn summarizes_listed_vs_unique_pdfs() {
        let root = std::env::temp_dir().join(format!("drission-sum-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let store = ArtifactStore::new(&root).unwrap();
        let journal = root.join("downloads/002_nrm");
        std::fs::create_dir_all(&journal).unwrap();
        std::fs::write(
            journal.join("journal.json"),
            br#"{"name":"Nature Reviews Molecular Cell Biology"}"#,
        )
        .unwrap();
        std::fs::write(
            journal.join("listed.json"),
            br#"[{"title":"a"},{"title":"b"}]"#,
        )
        .unwrap();
        let mut pdf = b"%PDF-1.4\n".to_vec();
        pdf.resize(1000, b'x');
        std::fs::write(journal.join("a.pdf"), &pdf).unwrap();
        std::fs::write(journal.join("a-copy.pdf"), &pdf).unwrap();
        let summary = store.summarize_downloads("downloads").unwrap();
        assert_eq!(summary["totalArticlesListed"], 2);
        assert_eq!(summary["totalDownloaded"], 1);
        assert_eq!(summary["journals"][0]["folder"], "002_nrm");
        let _ = std::fs::remove_dir_all(&root);
    }
}
