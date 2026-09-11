use regex::Regex;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeSet, sync::OnceLock};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Reference {
    pub root: String,
    pub path: String,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ExpressionError {
    #[error("unterminated expression")]
    Unterminated,
    #[error("unsafe or unsupported expression: {0}")]
    Unsupported(String),
}

fn reference_regex() -> &'static Regex {
    static REGEX: OnceLock<Regex> = OnceLock::new();
    REGEX.get_or_init(|| {
        Regex::new(r"(?x)\b(inputs|vars|steps|secrets|run|loop|item|[A-Za-z_][A-Za-z0-9_]*)\.(?:[A-Za-z_][A-Za-z0-9_]*)(?:\.(?:[A-Za-z_][A-Za-z0-9_]*)|\[[0-9]+\])*").unwrap()
    })
}

/// Extracts data references without evaluating arbitrary code.
///
/// The accepted operator surface is deliberately small: references, literals,
/// comparisons, boolean operators, parentheses, indexing and `.length`.
pub fn references(source: &str) -> Result<BTreeSet<Reference>, ExpressionError> {
    let mut result = BTreeSet::new();
    let mut offset = 0;

    while let Some(start) = source[offset..].find("${") {
        let start = offset + start + 2;
        let Some(relative_end) = source[start..].find('}') else {
            return Err(ExpressionError::Unterminated);
        };
        let end = start + relative_end;
        let expression = source[start..end].trim();
        validate_characters(expression)?;

        for matched in reference_regex().find_iter(expression) {
            let path = matched.as_str();
            let root = path.split('.').next().unwrap_or_default();
            result.insert(Reference {
                root: root.to_owned(),
                path: path.to_owned(),
            });
        }
        offset = end + 1;
    }

    Ok(result)
}

fn validate_characters(expression: &str) -> Result<(), ExpressionError> {
    let forbidden = [";", "=>", "function", "eval", "new ", "`", "\\", "{"];
    if forbidden.iter().any(|token| expression.contains(token)) {
        return Err(ExpressionError::Unsupported(expression.to_owned()));
    }

    if expression.chars().any(|character| {
        !(character.is_ascii_alphanumeric()
            || character.is_ascii_whitespace()
            || "_.'\"[]()=!<>&|+-".contains(character))
    }) {
        return Err(ExpressionError::Unsupported(expression.to_owned()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn extracts_interpolated_references() {
        let refs =
            references("${inputs.url}/q=${vars.keyword}&n=${steps.fetch.output[0].id}").unwrap();
        let paths: Vec<_> = refs.into_iter().map(|reference| reference.path).collect();
        assert_eq!(
            paths,
            ["inputs.url", "steps.fetch.output[0].id", "vars.keyword"]
        );
    }

    #[test]
    fn rejects_code_execution_tokens() {
        assert!(matches!(
            references("${eval('boom')}"),
            Err(ExpressionError::Unsupported(_))
        ));
    }
}
