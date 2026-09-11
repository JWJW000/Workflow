//! BUAA login: only the allowlisted portal receives credentials; one submission.
use crate::DriverError;
use rust_drission::ChromiumPage;
use serde_json::{Value, json};
use std::time::{Duration, Instant};

const SCRIPT: &str = include_str!("buaa_login.js");

pub(crate) fn trusted_url(raw: &str) -> bool {
    url::Url::parse(raw).is_ok_and(|u| {
        u.scheme() == "https"
            && u.host_str() == Some("d.buaa.edu.cn")
            && u.port_or_known_default() == Some(443)
            && u.username().is_empty()
            && u.password().is_none()
    })
}

fn error(message: &str) -> DriverError {
    DriverError::Cdp(format!("BUAA_LOGIN_REQUIRED: {message}"))
}

fn credential_pair(account: &str, password: &str) -> Option<Value> {
    if account.trim().is_empty() || password.is_empty() {
        None
    } else {
        Some(json!({"account": account.trim(), "password": password}))
    }
}

fn load_credentials() -> Result<Value, DriverError> {
    let account = std::env::var("BUAA_ACCOUNT").unwrap_or_default();
    let password = std::env::var("BUAA_PASSWORD").unwrap_or_default();
    if std::env::var_os("BUAA_ACCOUNT").is_some() || std::env::var_os("BUAA_PASSWORD").is_some() {
        return credential_pair(&account, &password)
            .ok_or_else(|| error("Set both BUAA_ACCOUNT and BUAA_PASSWORD"));
    }
    let path = std::env::var_os("DRISSION_ENV_FILE")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../../.env"))
        });
    let contents = std::fs::read_to_string(path)
        .map_err(|_| error("Configure BUAA_ACCOUNT and BUAA_PASSWORD in project .env"))?;
    parse_credentials(&contents)
}

fn parse_credentials(contents: &str) -> Result<Value, DriverError> {
    let mut account = String::new();
    let mut password = String::new();
    for line in contents.lines() {
        let line = line
            .trim()
            .strip_prefix("export ")
            .unwrap_or(line.trim())
            .trim();
        let Some((key, raw)) = line.split_once('=') else {
            continue;
        };
        if !matches!(key.trim(), "BUAA_ACCOUNT" | "BUAA_PASSWORD") {
            continue;
        }
        let raw = raw.trim();
        let value = if raw.starts_with('"') {
            serde_json::from_str::<String>(raw)
                .map_err(|_| error("Invalid quoted credential in .env"))?
        } else if raw.starts_with('\'') {
            raw.strip_prefix('\'')
                .and_then(|s| s.strip_suffix('\''))
                .ok_or_else(|| error("Invalid quoted credential in .env"))?
                .to_owned()
        } else {
            raw.split_once(" #")
                .map_or(raw, |(value, _)| value)
                .trim_end()
                .to_owned()
        };
        if key.trim() == "BUAA_ACCOUNT" {
            account = value;
        } else {
            password = value;
        }
    }
    credential_pair(&account, &password)
        .ok_or_else(|| error("Set both BUAA_ACCOUNT and BUAA_PASSWORD in project .env"))
}

fn probe(page: &ChromiumPage, credentials: Value) -> Result<String, DriverError> {
    // Bind to a specific realm, so navigation invalidates it rather than sending
    // credentials to the next document. The function rechecks origin/form action.
    let global = page
        .tab()
        .run_cdp(
            "Runtime.evaluate",
            Some(json!({
                "expression": "location.origin === 'https://d.buaa.edu.cn' ? globalThis : null",
                "returnByValue": false
            })),
        )
        .map_err(|_| error("Cannot inspect login page"))?;
    let object = global["result"]["objectId"]
        .as_str()
        .ok_or_else(|| error("Login left the trusted portal"))?;
    let result = page.tab().run_cdp(
        "Runtime.callFunctionOn",
        Some(json!({
            "objectId": object,
            "functionDeclaration": SCRIPT,
            "arguments": [{"value": credentials}],
            "returnByValue": true
        })),
    );
    let _ = page
        .tab()
        .run_cdp("Runtime.releaseObject", Some(json!({"objectId": object})));
    let result =
        result.map_err(|_| error("Login page changed; retry after checking the browser"))?;
    Ok(result["result"]["value"]["state"]
        .as_str()
        .unwrap_or("loading")
        .to_owned())
}

pub(crate) fn ensure(page: &ChromiumPage) -> Result<(), DriverError> {
    let deadline = Instant::now() + Duration::from_secs(60);
    let mut submitted = false;
    let mut stable = 0;
    while Instant::now() < deadline {
        if !trusted_url(&page.url().unwrap_or_default()) {
            return Err(error("Login left the trusted portal"));
        }
        match probe(page, Value::Null)?.as_str() {
            "ready" => {
                stable += 1;
                if stable >= 3 {
                    return Ok(());
                }
            }
            "credentials_required" if !submitted => {
                // Count attempts before transmission, including navigation errors.
                submitted = true;
                stable = 0;
                let status = probe(page, load_credentials()?)?;
                if status != "submitted" {
                    return Err(error("Login form changed or requires manual verification"));
                }
            }
            "rejected" => return Err(error("Credentials rejected; automatic retry stopped")),
            "manual_required" => {
                return Err(error(
                    "Complete CAPTCHA or second-factor verification in the browser",
                ));
            }
            "untrusted" => return Err(error("Untrusted login form destination")),
            _ => stable = 0,
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Err(error(
        "Login not confirmed within 60 seconds; no repeat submission",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn login_origin_is_exact_and_https_only() {
        assert!(trusted_url("https://d.buaa.edu.cn/login?service=x"));
        for raw in [
            "http://d.buaa.edu.cn/login",
            "https://d.buaa.edu.cn.evil.test/login",
            "https://evil.test/?next=d.buaa.edu.cn",
            "https://d.buaa.edu.cn:8443/login",
            "https://user@d.buaa.edu.cn/login",
        ] {
            assert!(!trusted_url(raw));
        }
    }
    #[test]
    fn dotenv_values_are_literals_and_validate_pairs() {
        let value =
            parse_credentials("export BUAA_ACCOUNT='fixture'\nBUAA_PASSWORD=\"a@_$(never)\"\n")
                .unwrap();
        assert_eq!(value["password"], "a@_$(never)");
        assert!(parse_credentials("BUAA_ACCOUNT=x").is_err());
        assert!(parse_credentials("BUAA_ACCOUNT=x\nBUAA_PASSWORD=\"unterminated").is_err());
    }
    #[test]
    fn incomplete_credentials_are_rejected() {
        assert!(credential_pair("", "password").is_none());
        assert!(credential_pair("user", "").is_none());
        assert!(credential_pair("user", "password").is_some());
    }
}
