use rust_drission::{BrowserConfig, ChromiumPage};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use thiserror::Error;
use workflow_schema::BrowserDefinition;

mod buaa_login;

pub trait BrowserDriver {
    fn launch(&mut self, config: &BrowserDefinition) -> Result<Value, DriverError>;
    fn close(&mut self) -> Result<Value, DriverError>;
    fn goto(&mut self, url: &str) -> Result<Value, DriverError>;
    fn reload(&mut self) -> Result<Value, DriverError>;
    fn back(&mut self) -> Result<Value, DriverError>;
    fn forward(&mut self) -> Result<Value, DriverError>;
    fn wait(&mut self, locator: &str, timeout: Duration) -> Result<Value, DriverError>;
    fn click(&mut self, locator: &str) -> Result<Value, DriverError>;
    fn input(&mut self, locator: &str, text: &str) -> Result<Value, DriverError>;
    fn clear(&mut self, locator: &str) -> Result<Value, DriverError>;
    fn select(&mut self, locator: &str, value: &str, by_text: bool) -> Result<Value, DriverError>;
    fn hover(&mut self, locator: &str) -> Result<Value, DriverError>;
    fn scroll_into_view(&mut self, locator: &str) -> Result<Value, DriverError>;
    fn find(&mut self, locator: &str, strict: bool) -> Result<Value, DriverError>;
    fn extract(
        &mut self,
        locator: &str,
        value: &str,
        name: Option<&str>,
    ) -> Result<Value, DriverError>;
    fn extract_all(
        &mut self,
        locator: &str,
        fields: &Value,
        limit: usize,
        strict: bool,
    ) -> Result<Value, DriverError>;
    fn screenshot(&mut self, path: &str) -> Result<Value, DriverError>;
    fn highlight(&mut self, locator: &str) -> Result<Value, DriverError>;
    fn download_click(&mut self, locator: &str, download_dir: &str) -> Result<Value, DriverError>;
    fn download_url(&mut self, url: &str, download_dir: &str) -> Result<Value, DriverError>;
    fn new_tab(&mut self, url: Option<&str>) -> Result<Value, DriverError>;
    fn switch_tab(
        &mut self,
        index: Option<usize>,
        title: Option<&str>,
    ) -> Result<Value, DriverError>;
    fn close_tab(&mut self) -> Result<Value, DriverError>;
    fn set_cookie(
        &mut self,
        name: &str,
        value: &str,
        domain: Option<&str>,
        path: Option<&str>,
    ) -> Result<Value, DriverError>;
    fn get_cookies(&mut self) -> Result<Value, DriverError>;
    fn interactive_pick(&mut self, url: &str, timeout_secs: u64) -> Result<Value, DriverError>;
    fn inject_stealth(&mut self) -> Result<Value, DriverError>;
}

#[derive(Debug, Error)]
pub enum DriverError {
    #[error("browser session is not started")]
    SessionNotStarted,
    #[error("locator not found: {0}")]
    LocatorNotFound(String),
    #[error("locator matched multiple elements in strict mode: {0}")]
    LocatorAmbiguous(String),
    #[error("browser profile is locked: {0}")]
    ProfileLocked(String),
    #[error("browser driver error: {0}")]
    Cdp(String),
    #[error("invalid browser configuration: {0}")]
    InvalidConfiguration(String),
}

impl DriverError {
    pub fn code(&self) -> &'static str {
        match self {
            Self::SessionNotStarted => "SESSION_NOT_STARTED",
            Self::LocatorNotFound(_) => "LOCATOR_NOT_FOUND",
            Self::LocatorAmbiguous(_) => "LOCATOR_AMBIGUOUS",
            Self::ProfileLocked(_) => "PROFILE_LOCKED",
            Self::Cdp(_) => "DRIVER_ERROR",
            Self::InvalidConfiguration(_) => "CONFIGURATION_ERROR",
        }
    }
}

#[derive(Default)]
pub struct DrissionDriver {
    page: Option<ChromiumPage>,
    lock_path: Option<std::path::PathBuf>,
    interactive: bool,
    /// The browser belongs to the dashboard's browser-group broker.  A workflow
    /// may disconnect from it, but must never terminate the shared process.
    shared_session: bool,
    /// Chrome profile download folder (`Default/Downloads`). The PDF viewer
    /// often never writes here; we still watch it when Chrome does save a file.
    chrome_download_dir: Option<PathBuf>,
}

impl DrissionDriver {
    pub fn new() -> Self {
        Self::default()
    }

    fn page(&self) -> Result<&ChromiumPage, DriverError> {
        self.page.as_ref().ok_or(DriverError::SessionNotStarted)
    }
}

impl Drop for DrissionDriver {
    fn drop(&mut self) {
        // Release an owned browser before its profile lock, including failed runs.
        let _ = self.close();
    }
}

impl BrowserDriver for DrissionDriver {
    fn launch(&mut self, config: &BrowserDefinition) -> Result<Value, DriverError> {
        if self.page.is_some() {
            return Ok(json!({ "launched": true, "reused": true }));
        }
        if let Ok(endpoint) = std::env::var("DRISSION_SHARED_BROWSER_ENDPOINT")
            && !endpoint.trim().is_empty()
        {
            self.interactive = !config.headless;
            let page = ChromiumPage::connect(endpoint.trim()).map_err(cdp)?;
            install_new_document_stealth(&page);
            self.page = Some(page);
            self.shared_session = true;
            return Ok(json!({
                "launched": true,
                "mode": "connect",
                "reused": true,
                "shared": true,
                "endpoint": endpoint,
            }));
        }
        if let Some(profile) = &config.profile
            && let Some(path) = profile.strip_prefix("persistent:")
        {
            let lock_path = acquire_profile_lock(path)?;
            self.lock_path = Some(lock_path);
        }
        self.interactive = !config.headless;
        let page = if config.mode == "connect" {
            ChromiumPage::connect(config.endpoint.as_deref().ok_or_else(|| {
                DriverError::InvalidConfiguration("missing browser endpoint".into())
            })?)
            .map_err(cdp)?
        } else {
            let mut browser = BrowserConfig::new().headless(config.headless);
            // Avoid colliding with a user Chrome that already owns 9222.
            browser = browser.set_local_port(pick_debug_port());

            // Use Google Chrome by default; explicit workflow configuration still wins.
            let chrome_app_path = "/Applications/Google Chrome.app/Contents/MacOS/Google Chrome";
            if let Some(path) = &config.chrome_path {
                browser = browser.chrome_path(path);
            } else if Path::new(chrome_app_path).is_file() {
                browser = browser.chrome_path(chrome_app_path);
            }

            let easyconnect = easyconnect_tun_active();
            let proxy_mode = if easyconnect { "direct" } else { "system" };

            if let Some(profile) = &config.profile
                && let Some(path) = profile.strip_prefix("persistent:")
            {
                // 每个平台保持独立专属 profile 目录，确保多平台独立窗口并存不互斥
                let path = std::fs::canonicalize(path).map_err(|err| {
                    DriverError::InvalidConfiguration(format!("invalid profile directory: {err}"))
                })?;
                self.chrome_download_dir = Some(prepare_profile_for_vpn(&path.to_string_lossy(), proxy_mode));
                browser = browser.user_data_dir(path.to_string_lossy().as_ref());
            } else {
                // For ephemeral mode, create an isolated clean temporary profile directory to avoid profile conflict popups
                let temp_dir = std::env::temp_dir().join(format!(
                    "drission-profile-{}-{}",
                    std::process::id(),
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_millis()
                ));
                let _ = std::fs::create_dir_all(&temp_dir);
                self.chrome_download_dir = Some(prepare_profile_for_vpn(
                    &temp_dir.to_string_lossy(),
                    proxy_mode,
                ));
                browser = browser.user_data_dir(temp_dir.to_string_lossy().as_ref());
            }
            if let Some(user_agent) = &config.user_agent {
                browser = browser.set_user_agent(user_agent);
            }
            // EasyConnect split-tunnel needs system DNS for campus routes. On a
            // normal network, keep Chrome secure DNS available; forcing it off
            // can make an otherwise healthy Chrome appear completely offline.
            if easyconnect {
                browser = browser.set_argument("--dns-over-https-mode", Some("off"));
                browser = browser.set_argument(
                    "--disable-features",
                    Some("ProfileErrorDialog,DnsOverHttps,Translate,OptimizationHints,MediaRouter"),
                );
            } else {
                browser = browser.set_argument(
                    "--disable-features",
                    Some("ProfileErrorDialog,Translate,OptimizationHints,MediaRouter"),
                );
            }
            // Campus login may now submit credentials; keep TLS verification on.
            browser = browser.ignore_certificate_errors(false);
            let network = if let Some(proxy) = &config.proxy {
                match proxy.trim().to_ascii_lowercase().as_str() {
                    "direct" | "none" | "off" => {
                        browser = browser.set_argument("--no-proxy-server", None::<String>);
                        "explicit-direct"
                    }
                    "system" | "auto" => "explicit-system",
                    _ => {
                        browser = browser.set_proxy(proxy.clone());
                        "configured-proxy"
                    }
                }
            } else if easyconnect {
                // Clash as --proxy-server bypasses the EasyConnect TUN (utun4),
                // so subscribed Nature hosts resolve off-campus. Direct + system
                // routing keeps www.nature.com on 10.230.x.
                browser = browser.set_argument("--no-proxy-server", None::<String>);
                "easyconnect-direct"
            } else if let Some(proxy) = detect_system_http_proxy() {
                browser = browser.set_proxy(proxy);
                "system-proxy"
            } else {
                "direct"
            };
            browser = browser.no_imgs(config.no_images);
            browser = browser.set_argument("--no-first-run", None::<String>);
            browser = browser.set_argument("--no-default-browser-check", None::<String>);
            browser = browser.set_argument("--disable-popup-blocking", None::<String>);
            browser = browser.set_argument("--hide-crash-restore-bubble", None::<String>);
            browser = browser.set_argument(
                "--disable-features",
                Some("ProfileErrorDialog,Translate,OptimizationHints,MediaRouter"),
            );
            for argument in &config.args {
                browser = browser.set_argument(argument, None::<String>);
            }
            let page = ChromiumPage::new(browser).map_err(cdp)?;
            install_new_document_stealth(&page);
            self.page = Some(page);
            return Ok(json!({
                "launched": true,
                "mode": config.mode,
                "network": network,
                "easyconnect": easyconnect,
            }));
        };
        install_new_document_stealth(&page);
        self.page = Some(page);
        Ok(json!({ "launched": true, "mode": config.mode }))
    }

    fn close(&mut self) -> Result<Value, DriverError> {
        if let Some(mut page) = self.page.take() {
            if !self.shared_session {
                page.close_browser();
            }
        }
        if let Some(lock_path) = self.lock_path.take() {
            let _ = std::fs::remove_file(lock_path);
        }
        let shared = self.shared_session;
        self.shared_session = false;
        Ok(json!({ "closed": true, "sharedBrowserKeptAlive": shared }))
    }

    fn goto(&mut self, url: &str) -> Result<Value, DriverError> {
        let page = self.page()?;
        let current = page.url().unwrap_or_default();
        let resolved = resolve_against(&current, url);
        page.get(&resolved).map_err(cdp)?;

        let landed = page.url().unwrap_or_default();
        if is_tju_eds_login_redirect(&resolved, &landed) {
            return Err(DriverError::Cdp(format!(
                "Tianjin EDS login required (redirected to {landed})"
            )));
        }

        // 自动检测 404 / 找不到网页，立即跳过避免无限卡死等待
        if page_has_not_found(page) {
            return Err(DriverError::Cdp(format!(
                "Page returned 404/Not Found ({resolved}), skipping immediately"
            )));
        }

        wait_for_manual_challenge(page, self.interactive)?;
        // Only confirmed crash pages may be refreshed after challenge waiting.
        if page_has_renderer_crash(page) {
            eprintln!("检测到 Chrome 标签页渲染异常（错误代码：5），正在自动执行刷新重载……");
            std::thread::sleep(Duration::from_millis(800));
            let _ = page.refresh();
            std::thread::sleep(Duration::from_millis(1500));
            if page_has_renderer_crash(page) {
                let _ = page.get(&resolved);
                std::thread::sleep(Duration::from_millis(1500));
                if page_has_renderer_crash(page) {
                    return Err(DriverError::Cdp(format!(
                        "Chrome renderer remained crashed after reload ({resolved}); skipping immediately"
                    )));
                }
            }
        }

        wait_for_manual_challenge(page, self.interactive)?;
        let _ = page.run_js(STEALTH_INJECTION_JS);
        Ok(json!({ "url": page.url().map_err(cdp)?, "title": page.title().map_err(cdp)? }))
    }

    fn reload(&mut self) -> Result<Value, DriverError> {
        let page = self.page()?;
        page.refresh().map_err(cdp)?;
        let _ = page.run_js(STEALTH_INJECTION_JS);
        Ok(json!({ "url": page.url().map_err(cdp)?, "title": page.title().map_err(cdp)? }))
    }

    fn back(&mut self) -> Result<Value, DriverError> {
        let page = self.page()?;
        page.back().map_err(cdp)?;
        Ok(json!({ "url": page.url().map_err(cdp)?, "title": page.title().map_err(cdp)? }))
    }

    fn forward(&mut self) -> Result<Value, DriverError> {
        let page = self.page()?;
        page.forward().map_err(cdp)?;
        Ok(json!({ "url": page.url().map_err(cdp)?, "title": page.title().map_err(cdp)? }))
    }

    fn wait(&mut self, locator: &str, timeout: Duration) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        self.page()?.wait(&norm, timeout).map_err(cdp)?;
        Ok(json!({ "matched": true }))
    }

    fn click(&mut self, locator: &str) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        self.page()?.click(&norm).map_err(cdp)?;
        Ok(json!({ "success": true }))
    }

    fn input(&mut self, locator: &str, text: &str) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        self.page()?.input(&norm, text).map_err(cdp)?;
        Ok(json!({ "success": true }))
    }

    fn clear(&mut self, locator: &str) -> Result<Value, DriverError> {
        required_element(self.page()?, locator)?
            .clear()
            .map_err(cdp)?;
        Ok(json!({ "success": true }))
    }

    fn select(&mut self, locator: &str, value: &str, by_text: bool) -> Result<Value, DriverError> {
        required_element(self.page()?, locator)?
            .select(value, by_text)
            .map_err(cdp)?;
        Ok(json!({ "selected": value }))
    }

    fn hover(&mut self, locator: &str) -> Result<Value, DriverError> {
        required_element(self.page()?, locator)?
            .hover()
            .map_err(cdp)?;
        Ok(json!({ "success": true }))
    }

    fn scroll_into_view(&mut self, locator: &str) -> Result<Value, DriverError> {
        required_element(self.page()?, locator)?
            .scroll_into_view()
            .map_err(cdp)?;
        Ok(json!({ "success": true }))
    }

    fn find(&mut self, locator: &str, strict: bool) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        let elements = self.page()?.eles(&norm).map_err(cdp)?;
        if strict && elements.len() > 1 {
            return Err(DriverError::LocatorAmbiguous(locator.into()));
        }
        Ok(json!({ "exists": !elements.is_empty(), "count": elements.len() }))
    }

    fn extract(
        &mut self,
        locator: &str,
        value: &str,
        name: Option<&str>,
    ) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        let element = self
            .page()?
            .ele(&norm)
            .map_err(cdp)?
            .ok_or_else(|| DriverError::LocatorNotFound(locator.into()))?;
        extract_element(&element, value, name)
    }

    fn extract_all(
        &mut self,
        locator: &str,
        fields: &Value,
        limit: usize,
        strict: bool,
    ) -> Result<Value, DriverError> {
        let norm = normalize_locator(locator);
        let elements = self.page()?.eles(&norm).map_err(cdp)?;
        if elements.is_empty() {
            return Err(DriverError::LocatorNotFound(locator.into()));
        }
        if strict && elements.len() > 1 {
            return Err(DriverError::LocatorAmbiguous(locator.into()));
        }
        let fields = fields
            .as_object()
            .ok_or_else(|| DriverError::InvalidConfiguration("fields must be an object".into()))?;
        let mut rows = Vec::with_capacity(elements.len().min(limit));
        for element in elements.into_iter().take(limit) {
            let mut row = serde_json::Map::new();
            for (field, definition) in fields {
                let value = definition
                    .get("value")
                    .and_then(Value::as_str)
                    .unwrap_or("text");
                let name = definition.get("name").and_then(Value::as_str);
                row.insert(field.clone(), extract_element(&element, value, name)?);
            }
            rows.push(Value::Object(row));
        }
        Ok(Value::Array(rows))
    }

    fn screenshot(&mut self, path: &str) -> Result<Value, DriverError> {
        let p = std::path::Path::new(path);
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        self.page()?.screenshot(path).map_err(cdp)?;
        Ok(json!({ "path": path }))
    }

    fn highlight(&mut self, locator: &str) -> Result<Value, DriverError> {
        let _ = required_element(self.page()?, locator)?;
        Ok(json!({ "highlighted": true, "locator": locator }))
    }

    fn download_click(&mut self, locator: &str, download_dir: &str) -> Result<Value, DriverError> {
        let abs_dir = prepare_download_dir(download_dir)?;
        let page = self.page()?;
        let norm = normalize_locator(locator);
        let elem_opt = page.ele(&norm).map_err(cdp)?;
        let element = elem_opt.ok_or_else(|| DriverError::LocatorNotFound(locator.into()))?;
        let href = js_string(
            &element
                .run_js("(() => this.href || this.getAttribute('href') || this.getAttribute('data-href') || '')()")
                .unwrap_or(Value::Null),
        );
        let current = page.url().unwrap_or_default();
        let target = if href.is_empty() {
            current.clone()
        } else {
            resolve_against(&current, &href)
        };
        let extra = extra_watch_dirs(self.chrome_download_dir.as_deref());
        // WebVPN pages expose publisher links with their public origin. A real
        // click is intercepted and rewritten by the WebVPN client, while an
        // eager HTTP/blob fetch bypasses that session and hits Cloudflare.
        if !is_webvpn_external_link(&current, &target) {
            if let Ok(result) =
                download_resolved(page, &target, &abs_dir, &extra, self.interactive, false)
            {
                return Ok(result);
            }
        }
        check_nature_access(page, &target, &abs_dir)?;
        set_download_behavior(page, &abs_dir);
        let (watch_dirs, befores) = watch_download_dirs(&abs_dir, &extra);
        // A PDF link may replace the current document and destroy the old JS
        // execution context even though Chrome has already started a download.
        // Poll the download directories before propagating that click error.
        // The PDF preflight can outlive a DOM rerender. Reacquire the locator
        // instead of clicking the stale remote object captured before it.
        let click_error = required_element(page, locator)?.click().err();
        if let Some(result) = captured_download_anywhere(page, &abs_dir, &watch_dirs[1..], &befores)? {
            return Ok(result);
        }
        if let Some(error) = click_error {
            return Err(cdp(error));
        }
        Err(DriverError::Cdp(format!(
            "clicking {locator} did not produce a PDF we could keep"
        )))
    }

    fn download_url(&mut self, url: &str, download_dir: &str) -> Result<Value, DriverError> {
        let abs_dir = prepare_download_dir(download_dir)?;
        let page = self.page()?;
        let current = page.url().unwrap_or_default();
        let target = resolve_against(&current, url);
        let extra = extra_watch_dirs(self.chrome_download_dir.as_deref());
        download_resolved(page, &target, &abs_dir, &extra, self.interactive, true)
    }

    fn new_tab(&mut self, url: Option<&str>) -> Result<Value, DriverError> {
        let page = self.page()?;
        if let Some(target_url) = url {
            let _ = page.get(target_url);
        }
        Ok(json!({ "opened": true, "url": url }))
    }

    fn switch_tab(
        &mut self,
        index: Option<usize>,
        title: Option<&str>,
    ) -> Result<Value, DriverError> {
        let _ = self.page()?;
        Ok(json!({ "switched": true, "index": index, "title": title }))
    }

    fn close_tab(&mut self) -> Result<Value, DriverError> {
        let _ = self.page()?;
        Ok(json!({ "closedTab": true }))
    }

    fn set_cookie(
        &mut self,
        name: &str,
        value: &str,
        domain: Option<&str>,
        path: Option<&str>,
    ) -> Result<Value, DriverError> {
        let _ = self.page()?;
        Ok(json!({ "set": true, "name": name, "value": value, "domain": domain, "path": path }))
    }

    fn get_cookies(&mut self) -> Result<Value, DriverError> {
        let _ = self.page()?;
        Ok(json!([]))
    }

    fn interactive_pick(&mut self, url: &str, timeout_secs: u64) -> Result<Value, DriverError> {
        let page = self.page()?;
        let _ = page.get(url);
        let picker_js = r#"
        (function() {
          if (window.__drission_active_picker) return;
          window.__drission_active_picker = true;
          window.__DRISSION_PICKED__ = null;
          try { sessionStorage.removeItem('__DRISSION_PICKED__'); } catch(e) {}

          const banner = document.createElement('div');
          banner.id = '__drission_pick_banner__';
          banner.innerHTML = '🎯 <b>Drission 实时点选</b>：请在网页中直接点击目标元素';
          banner.style.cssText = 'position:fixed;top:14px;left:50%;transform:translateX(-50%);background:#10151a;color:#4bd3c5;padding:10px 22px;border-radius:6px;border:2px solid #4bd3c5;z-index:2147483647;font-family:-apple-system,sans-serif;font-size:13px;box-shadow:0 8px 30px rgba(0,0,0,0.6);cursor:default;pointer-events:none;';
          document.body.appendChild(banner);

          const overlay = document.createElement('div');
          overlay.style.cssText = 'position:fixed;pointer-events:none;z-index:2147483646;border:2px solid #4bd3c5;background:rgba(75,211,197,0.25);display:none;border-radius:3px;box-shadow:0 0 10px rgba(75,211,197,0.5);';
          document.body.appendChild(overlay);

          function getCss(el) {
            if (el.id && document.querySelectorAll('#' + CSS.escape(el.id)).length === 1) return 'css:#' + CSS.escape(el.id);
            for (const a of ['data-testid','data-id','name','placeholder','aria-label']) {
              const v = el.getAttribute(a);
              if (v && document.querySelectorAll(el.tagName.toLowerCase() + '[' + a + '="' + CSS.escape(v) + '"]').length === 1) {
                return 'css:' + el.tagName.toLowerCase() + '[' + a + '="' + CSS.escape(v) + '"]';
              }
            }
            if (el.classList.length) {
              const c = el.tagName.toLowerCase() + '.' + Array.from(el.classList).slice(0, 2).map(CSS.escape).join('.');
              if (document.querySelectorAll(c).length === 1) return 'css:' + c;
            }
            return 'css:' + el.tagName.toLowerCase();
          }

          function getXpath(el) {
            if (el.id) return "xpath://*[@id='" + el.id + "']";
            const text = (el.textContent || '').trim();
            if (text && text.length < 25 && !text.includes('\n')) {
              return "xpath://*[contains(text(), '" + text.replace(/'/g, "\\'") + "')]";
            }
            return "xpath://" + el.tagName.toLowerCase();
          }

          document.addEventListener('mousemove', function(e) {
            if (window.__DRISSION_PICKED__) return;
            const t = e.target;
            if (t && t !== overlay && t !== banner) {
              const r = t.getBoundingClientRect();
              overlay.style.top = r.top + 'px';
              overlay.style.left = r.left + 'px';
              overlay.style.width = r.width + 'px';
              overlay.style.height = r.height + 'px';
              overlay.style.display = 'block';
            }
          }, true);

          function handleClick(e) {
            const t = e.target;
            if (t === banner || t === overlay) return;
            e.preventDefault();
            e.stopPropagation();
            e.stopImmediatePropagation();

            const css = getCss(t);
            const xpath = getXpath(t);
            const data = {
              locator: css,
              xpath: xpath,
              tag: t.tagName.toLowerCase(),
              text: (t.textContent || '').trim().slice(0, 80)
            };

            window.__DRISSION_PICKED__ = data;
            try { sessionStorage.setItem('__DRISSION_PICKED__', JSON.stringify(data)); } catch(err) {}

            banner.innerHTML = '✓ <b>捕获成功</b>: ' + css + ' (正在回填...)';
            banner.style.background = '#143324';
            banner.style.color = '#53d990';
            banner.style.borderColor = '#235443';
            overlay.style.borderColor = '#53d990';
          }

          document.addEventListener('click', handleClick, true);
          document.addEventListener('mousedown', function(e) {
            if (e.target !== banner && e.target !== overlay) {
              e.preventDefault();
            }
          }, true);
        })();
        "#;
        let _ = page.run_js(picker_js);

        let start = std::time::Instant::now();
        let timeout = std::time::Duration::from_secs(timeout_secs);
        while start.elapsed() < timeout {
            std::thread::sleep(std::time::Duration::from_millis(200));
            if let Ok(res) = page.run_js("(() => { const v = window.__DRISSION_PICKED__; if (v) return JSON.stringify(v); try { const s = sessionStorage.getItem('__DRISSION_PICKED__'); if (s) return s; } catch(e){} return ''; })()") {
                if let Some(parsed) = extract_picked_payload(&res) {
                    return Ok(parsed);
                }
            }
        }
        Err(DriverError::Cdp(
            "interactive picking timed out or was cancelled".into(),
        ))
    }

    fn inject_stealth(&mut self) -> Result<Value, DriverError> {
        let page = self.page()?;
        page.run_js(STEALTH_INJECTION_JS).map_err(cdp)?;
        Ok(
            json!({ "stealthInjected": true, "timestamp": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_millis() }),
        )
    }
}

pub const STEALTH_INJECTION_JS: &str = r#"
(function() {
  if (window.__drission_stealth_applied__) return;
  window.__drission_stealth_applied__ = true;

  // 1. Pass Webdriver Test. Keep the getter non-enumerable; Cloudflare
  // treats an enumerable navigator.webdriver as a broken browser.
  try {
    Object.defineProperty(navigator, 'webdriver', {
      get: () => undefined,
      configurable: true,
      enumerable: false
    });
  } catch (e) {}

  // Do not overwrite window.chrome / navigator.languages on a real Chrome.
  // Cloudflare uses those natives; fakes show up as “unsupported browser”.

  // 2. Clean cdc automation variables
  try {
    const keys = Object.keys(window);
    for (let i = 0; i < keys.length; i++) {
      if (keys[i].startsWith('cdc_') || keys[i].includes('webdriver')) {
        try { delete window[keys[i]]; } catch(err) {}
      }
    }
  } catch (e) {}

  // Do not patch Canvas. Cloudflare Turnstile/IUAM uses toDataURL; noise
  // here surfaces as “您的浏览器不支持 … 所需的安全验证”.
})();
"#;

fn install_new_document_stealth(page: &ChromiumPage) {
    let _ = page.tab().run_cdp("Page.enable", None);
    let _ = page.tab().run_cdp(
        "Page.addScriptToEvaluateOnNewDocument",
        Some(json!({ "source": STEALTH_INJECTION_JS })),
    );
}

fn acquire_profile_lock(path: &str) -> Result<std::path::PathBuf, DriverError> {
    let profile_dir = std::path::Path::new(path);
    if let Err(err) = std::fs::create_dir_all(profile_dir) {
        return Err(DriverError::InvalidConfiguration(format!(
            "failed to create profile directory: {err}"
        )));
    }
    // rust_drission deletes Chrome's Singleton files on launch. Never let it
    // remove a live browser's lock, even if its workflow runner already exited.
    if let Ok(target) = std::fs::read_link(profile_dir.join("SingletonLock")) {
        if let Some(pid) = target.to_string_lossy().rsplit('-').next().and_then(|s| s.parse::<u32>().ok()) {
            if is_process_alive(pid) {
                return Err(DriverError::ProfileLocked(format!("Chrome profile is in use by PID {pid}")));
            }
        }
    }
    let lock_file = profile_dir.join(".drission_profile.lock");
    if lock_file.exists() {
        if let Ok(content) = std::fs::read_to_string(&lock_file) {
            if let Ok(pid) = content.trim().parse::<u32>() {
                if pid != std::process::id() && is_process_alive(pid) {
                    return Err(DriverError::ProfileLocked(format!(
                        "profile is in use by PID {pid}"
                    )));
                }
            }
        }
    }
    let _ = std::fs::write(&lock_file, std::process::id().to_string());
    Ok(lock_file)
}

fn extract_picked_payload(res: &Value) -> Option<Value> {
    if res.get("locator").is_some() {
        return Some(res.clone());
    }
    if let Some(inner) = res.get("value") {
        if inner.get("locator").is_some() {
            return Some(inner.clone());
        }
        if let Some(s) = inner.as_str() {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                if parsed.get("locator").is_some() {
                    return Some(parsed);
                }
            }
        }
    }
    if let Some(s) = res.as_str() {
        if !s.is_empty() {
            if let Ok(parsed) = serde_json::from_str::<Value>(s) {
                if parsed.get("locator").is_some() {
                    return Some(parsed);
                }
            }
        }
    }
    None
}

fn pick_debug_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .ok()
        .and_then(|listener| listener.local_addr().ok())
        .map(|addr| addr.port())
        .filter(|port| *port != 0)
        .unwrap_or(9333)
}

fn process_name_alive(name: &str) -> bool {
    std::process::Command::new("pgrep")
        .args(["-x", name])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Sangfor EasyConnect puts campus routes on a `utun*` address in 10.230.0.0/8.
fn ifconfig_has_easyconnect_tun(text: &str) -> bool {
    let mut in_utun = false;
    for line in text.lines() {
        if !line.starts_with('\t') && !line.starts_with(' ') {
            in_utun = line.starts_with("utun");
            continue;
        }
        if in_utun
            && (line.contains("inet 10.230.")
                || line.contains("inet 10.231.")
                || line.contains("inet 10.232."))
        {
            return true;
        }
    }
    false
}

fn easyconnect_tun_active() -> bool {
    if process_name_alive("EasyConnect") || process_name_alive("EasyMonitor") {
        return true;
    }
    #[cfg(target_os = "macos")]
    {
        if let Ok(output) = std::process::Command::new("ifconfig").output() {
            return ifconfig_has_easyconnect_tun(&String::from_utf8_lossy(&output.stdout));
        }
    }
    false
}

fn detect_system_http_proxy() -> Option<String> {
    for key in ["HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy"] {
        if let Ok(value) = std::env::var(key) {
            let value = value.trim();
            if !value.is_empty() {
                return Some(if value.contains("://") {
                    value.to_string()
                } else {
                    format!("http://{value}")
                });
            }
        }
    }
    #[cfg(target_os = "macos")]
    {
        let output = std::process::Command::new("scutil")
            .arg("--proxy")
            .output()
            .ok()?;
        let text = String::from_utf8_lossy(&output.stdout);
        let mut enabled = false;
        let mut host: Option<String> = None;
        let mut port: Option<u16> = None;
        for line in text.lines() {
            let line = line.trim();
            if let Some(rest) = line.strip_prefix("HTTPEnable") {
                enabled = rest.contains('1');
            }
            if let Some(rest) = line.strip_prefix("HTTPSEnable") {
                enabled = enabled || rest.contains('1');
            }
            if let Some((_, value)) = line.split_once(':') {
                let value = value.trim();
                if line.starts_with("HTTPProxy") || line.starts_with("HTTPSProxy") {
                    if !value.is_empty() && value != "0" {
                        host = Some(value.to_string());
                    }
                }
                if line.starts_with("HTTPPort") || line.starts_with("HTTPSPort") {
                    port = value.parse().ok();
                }
            }
        }
        if enabled {
            return match (host, port) {
                (Some(host), Some(port)) => Some(format!("http://{host}:{port}")),
                _ => None,
            };
        }
    }
    None
}

fn prepare_profile_for_vpn(profile_dir: &str, proxy_mode: &str) -> PathBuf {
    let default_dir = Path::new(profile_dir).join("Default");
    let download_dir = default_dir.join("Downloads");
    let _ = std::fs::create_dir_all(&download_dir);
    let prefs_path = default_dir.join("Preferences");
    let mut prefs = if let Ok(text) = std::fs::read_to_string(&prefs_path) {
        serde_json::from_str::<Value>(&text).unwrap_or_else(|_| json!({}))
    } else {
        json!({})
    };
    if !prefs.is_object() {
        prefs = json!({});
    }
    if let Some(object) = prefs.as_object_mut() {
        if proxy_mode == "direct" {
            object.insert("dns_over_https".into(), json!({ "mode": "off" }));
        } else {
            object.remove("dns_over_https");
        }
        object.insert("proxy".into(), json!({ "mode": proxy_mode }));
        let mut plugins = object.get("plugins").cloned().unwrap_or_else(|| json!({}));
        if let Some(plugins) = plugins.as_object_mut() {
            plugins.insert("always_open_pdf_externally".into(), json!(true));
        }
        object.insert("plugins".into(), plugins);
        let mut download = object.get("download").cloned().unwrap_or_else(|| json!({}));
        if let Some(download) = download.as_object_mut() {
            download.insert("prompt_for_download".into(), json!(false));
            download.insert("directory_upgrade".into(), json!(true));
            download.insert(
                "default_directory".into(),
                json!(download_dir.to_string_lossy()),
            );
        }
        object.insert("download".into(), download);
    }
    let _ = std::fs::write(
        prefs_path,
        serde_json::to_vec_pretty(&prefs).unwrap_or_else(|_| b"{}".to_vec()),
    );
    download_dir
}

fn is_process_alive(pid: u32) -> bool {
    #[cfg(unix)]
    {
        let status = std::process::Command::new("kill")
            .arg("-0")
            .arg(pid.to_string())
            .status();
        matches!(status, Ok(s) if s.success())
    }
    #[cfg(not(unix))]
    {
        false
    }
}

fn required_element(
    page: &ChromiumPage,
    locator: &str,
) -> Result<rust_drission::Element, DriverError> {
    let norm = normalize_locator(locator);
    page.ele(&norm)
        .map_err(cdp)?
        .ok_or_else(|| DriverError::LocatorNotFound(locator.into()))
}

pub fn normalize_locator(locator: &str) -> String {
    let trimmed = locator.trim();
    if trimmed.starts_with("xpath:") || trimmed.starts_with("x:") {
        trimmed.to_string()
    } else if trimmed.starts_with("//") || trimmed.starts_with('/') || trimmed.starts_with("(//") {
        format!("xpath:{trimmed}")
    } else if trimmed.starts_with("css:")
        || trimmed.starts_with("c:")
        || trimmed.starts_with("text:")
        || trimmed.starts_with("t:")
        || trimmed.starts_with("tag:")
    {
        trimmed.to_string()
    } else {
        trimmed.to_string()
    }
}

fn extract_element(
    element: &rust_drission::Element,
    value: &str,
    name: Option<&str>,
) -> Result<Value, DriverError> {
    match value {
        "text" => Ok(Value::String(element.text().map_err(cdp)?)),
        "html" => Ok(Value::String(element.html().map_err(cdp)?)),
        "attr" => {
            let attr_name = name.ok_or_else(|| {
                DriverError::InvalidConfiguration("attr extraction requires name".into())
            })?;
            Ok(Value::String(element.attr(attr_name).unwrap_or_default()))
        }
        "value" => Ok(Value::String(element.value().map_err(cdp)?)),
        other => Err(DriverError::InvalidConfiguration(format!(
            "unsupported extraction value: {other}"
        ))),
    }
}

fn cdp(error: rust_drission::CdpError) -> DriverError {
    let message = error.to_string();
    let lower = message.to_ascii_lowercase();
    if lower.contains("not found")
        || lower.contains("could not find")
        || lower.contains("no such element")
    {
        DriverError::LocatorNotFound(message)
    } else {
        DriverError::Cdp(message)
    }
}

const DOWNLOAD_POLL_TIMEOUT: Duration = Duration::from_secs(20);

/// Map of every regular file in `dir` to its current byte length.
fn dir_snapshot(dir: &Path) -> HashMap<PathBuf, u64> {
    let mut snapshot = HashMap::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                if metadata.is_file() {
                    snapshot.insert(entry.path(), metadata.len());
                }
            }
        }
    }
    snapshot
}

/// Chrome writes in-progress downloads as `name.crdownload` (or Safari `.download`).
/// `.DS_Store` and other hidden files must not be treated as in-flight downloads —
/// that used to make `~/Downloads` unusable on macOS.
fn is_partial_download(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let lower = name.to_ascii_lowercase();
            lower.ends_with(".crdownload") || lower.ends_with(".download")
        })
        .unwrap_or(false)
}

fn has_partial_download(dirs: &[PathBuf]) -> bool {
    dirs.iter().any(|dir| {
        std::fs::read_dir(dir)
            .ok()
            .into_iter()
            .flatten()
            .flatten()
            .any(|entry| is_partial_download(&entry.path()))
    })
}

fn is_ignored_download(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let lower = name.to_ascii_lowercase();
            name.starts_with('.')
                || lower == "desktop.ini"
                || lower == "thumbs.db"
                || lower.ends_with(".ds_store")
        })
        .unwrap_or(false)
}

/// A file is considered finished when its modification time has been quiet for a beat.
fn is_file_quiet(path: &Path) -> bool {
    path.metadata()
        .and_then(|metadata| metadata.modified())
        .map(|modified| modified.elapsed().unwrap_or_default() >= Duration::from_millis(500))
        .unwrap_or(false)
}

fn prepare_download_dir(download_dir: &str) -> Result<PathBuf, DriverError> {
    let dir = Path::new(download_dir);
    std::fs::create_dir_all(dir).map_err(|error| DriverError::Cdp(error.to_string()))?;
    Ok(std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf()))
}

fn js_string(value: &Value) -> String {
    if let Some(s) = value.as_str() {
        return s.to_string();
    }
    if let Some(inner) = value.get("value").and_then(Value::as_str) {
        return inner.to_string();
    }
    String::new()
}

fn resolve_against(current: &str, raw: &str) -> String {
    let raw = raw.trim();
    if raw.is_empty() {
        return current.to_string();
    }
    if raw.contains("media.springernature.com/articles/") {
        if let Some(id) = raw.split("/articles/").nth(1) {
            return format!(
                "https://www.nature.com/articles/{}",
                id.split(['?', '#']).next().unwrap_or(id)
            );
        }
    }
    if raw.starts_with("/articles/") {
        return format!(
            "https://www.nature.com{}",
            raw.split(['?', '#']).next().unwrap_or(raw)
        );
    }
    if raw.starts_with("http://") || raw.starts_with("https://") || raw.starts_with("data:") {
        if let Ok(parsed) = ::url::Url::parse(raw)
            && parsed
                .host_str()
                .is_some_and(|host| host.contains("media.springernature.com"))
            && parsed.path().starts_with("/articles/")
        {
            return format!("https://www.nature.com{}", parsed.path());
        }
        return raw.to_string();
    }
    if let Ok(base) = ::url::Url::parse(current) {
        let host = base.host_str().unwrap_or_default();
        if host.contains("media.springernature.com") || host.contains("idp.nature.com") {
            if raw.starts_with('/') {
                return format!("https://www.nature.com{raw}");
            }
            return format!("https://www.nature.com/{raw}");
        }
        if let Ok(joined) = base.join(raw) {
            return joined.to_string();
        }
    }
    raw.to_string()
}

fn wiley_doi(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    for prefix in [
        "/doi/pdfdirect/",
        "/doi/pdf/",
        "/doi/epdf/",
        "/doi/full/",
        "/doi/abs/",
        "/doi/",
    ] {
        if let Some(idx) = path.find(prefix) {
            let rest = &path[idx + prefix.len()..];
            if rest.starts_with("10.") {
                return Some(rest.trim_end_matches('/').to_string());
            }
        }
    }
    None
}

fn is_wiley_host(url: &str) -> bool {
    let host = ::url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_ascii_lowercase))
        .unwrap_or_default();
    host.contains("wiley.com")
}

fn rewrite_to_pdf_url(url: &str) -> String {
    let lower = url.to_ascii_lowercase();
    // pdfdirect is Wiley's download endpoint. NEJM/Catalyst use the same
    // path shape but serve a streaming viewer that never finishes and
    // blocks Chrome's CDP — do not rewrite those hosts.
    if is_wiley_host(url)
        && let Some(doi) = wiley_doi(url)
        && !lower.contains("/pdf")
        && let Ok(parsed) = ::url::Url::parse(url)
        && let Some(host) = parsed.host_str()
    {
        return format!(
            "{}://{}/doi/pdfdirect/{}?download=true",
            parsed.scheme(),
            host,
            doi
        );
    }
    if !lower.contains(".pdf")
        && (lower.contains("nature.com/articles/")
            || lower.contains("/articles/s")
            || lower.contains("/articles/n"))
    {
        let clean = url.split(['?', '#']).next().unwrap_or(url);
        return format!("{clean}.pdf");
    }
    if lower.contains("/abs/") && !lower.contains("/pdf") {
        let swapped = url.replace("/abs/", "/pdf/");
        if swapped.to_ascii_lowercase().ends_with(".pdf") {
            return swapped;
        }
        return format!("{swapped}.pdf");
    }
    url.to_string()
}

fn is_real_file(bytes: &[u8]) -> bool {
    if bytes.len() < 800 {
        return false;
    }
    let prefix = &bytes[..bytes.len().min(16)];
    if prefix.starts_with(b"%PDF") || prefix.starts_with(b"PK") || prefix.starts_with(b"\x1f\x8b") {
        return true;
    }
    let head = String::from_utf8_lossy(&bytes[..bytes.len().min(200)]).to_ascii_lowercase();
    !head.contains("<html") && !head.contains("<!doctype") && !head.contains("just a moment")
}

fn download_result(path: PathBuf, size: u64) -> Value {
    let file_name = path
        .file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default();
    json!({
        "downloadTriggered": true,
        "path": path.to_string_lossy(),
        "fileName": file_name,
        "byteCount": size,
        "contentType": mime_from_path(&path),
    })
}

fn copy_into_dir(source: &Path, dir: &Path) -> Option<PathBuf> {
    let name = source.file_name()?.to_os_string();
    let dest = deduplicate_path(dir.join(name));
    std::fs::copy(source, &dest).ok()?;
    Some(dest)
}

fn newest_finished_file(dir: &Path, before: &HashMap<PathBuf, u64>) -> Option<(PathBuf, u64)> {
    let snapshot = dir_snapshot(dir);
    let mut candidates: Vec<(PathBuf, u64)> = snapshot
        .iter()
        .filter(|(path, size)| {
            !is_partial_download(path)
                && !is_ignored_download(path)
                && **size >= 800
                && before
                    .get(path.as_path())
                    .map(|old| old != *size)
                    .unwrap_or(true)
        })
        .map(|(path, size)| (path.clone(), *size))
        .collect();
    candidates.sort_by_key(|(_, size)| std::cmp::Reverse(*size));
    candidates.into_iter().find(|(path, _)| {
        let mut partial = path.as_os_str().to_os_string();
        partial.push(".crdownload");
        if PathBuf::from(&partial).exists() {
            return false;
        }
        if !is_file_quiet(path) {
            return false;
        }
        let html_name = path
            .extension()
            .and_then(|ext| ext.to_str())
            .map(|ext| ext.eq_ignore_ascii_case("html") || ext.eq_ignore_ascii_case("htm"))
            .unwrap_or(false);
        let real = !html_name
            && std::fs::read(path)
                .map(|head| head.starts_with(b"%PDF") && is_real_file(&head))
                .unwrap_or(false);
        real
    })
}

fn find_new_download(
    primary: &Path,
    extra: &[PathBuf],
    befores: &[HashMap<PathBuf, u64>],
) -> Option<Value> {
    let dirs: Vec<&Path> = std::iter::once(primary)
        .chain(extra.iter().map(PathBuf::as_path))
        .collect();
    for (index, dir) in dirs.iter().enumerate() {
        let Some(before) = befores.get(index) else {
            continue;
        };
        if let Some((path, size)) = newest_finished_file(dir, before) {
            let final_path = if *dir == primary {
                path
            } else {
                copy_into_dir(&path, primary)?
            };
            let final_size = std::fs::metadata(&final_path)
                .map(|meta| meta.len())
                .unwrap_or(size);
            return Some(download_result(final_path, final_size));
        }
    }
    None
}

fn captured_download_anywhere(
    page: &ChromiumPage,
    primary: &Path,
    extra: &[PathBuf],
    befores: &[HashMap<PathBuf, u64>],
) -> Result<Option<Value>, DriverError> {
    let hard_deadline = Instant::now() + Duration::from_secs(75);
    let mut deadline = Instant::now() + DOWNLOAD_POLL_TIMEOUT;
    while Instant::now() < deadline {
        if let Some(result) = find_new_download(primary, extra, befores) {
            return Ok(Some(result));
        }
        let before_refresh = Instant::now();
        refresh_frequency_limited_pdf(page)?;
        if before_refresh.elapsed() >= Duration::from_secs(10) {
            deadline = (Instant::now() + DOWNLOAD_POLL_TIMEOUT).min(hard_deadline);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    Ok(find_new_download(primary, extra, befores))
}

fn is_frequency_limit_message(text: &str) -> bool {
    text.contains("系统检测到您已达基础频率限制")
}

fn refresh_frequency_limited_pdf(page: &ChromiumPage) -> Result<(), DriverError> {
    if page_has_rate_limit(page) {
        return Err(DriverError::Cdp("RATE_LIMITED: publisher requests a cooldown".into()));
    }
    for delay in [10, 20, 0] {
        let body = page.run_js("document.body ? document.body.innerText.slice(0, 8000) : ''")
            .map(|value| js_string(&value)).unwrap_or_default();
        if !is_frequency_limit_message(&body) { return Ok(()); }
        if delay == 0 {
            return Err(DriverError::Cdp("temporary PDF frequency limit persists after two delayed refreshes; defer retry".into()));
        }
        eprintln!("PDF 触发基础频率限制，等待 {delay} 秒后刷新");
        std::thread::sleep(Duration::from_secs(delay));
        page.refresh().map_err(cdp)?;
        std::thread::sleep(Duration::from_secs(1));
    }
    Ok(())
}

fn set_download_behavior(page: &ChromiumPage, dir: &Path) {
    let behavior = json!({
        "behavior": "allow",
        "downloadPath": dir.to_string_lossy(),
        "eventsEnabled": true,
    });
    let _ = page
        .tab()
        .run_cdp("Page.setDownloadBehavior", Some(behavior.clone()));
    let _ = page
        .tab()
        .run_cdp("Browser.setDownloadBehavior", Some(behavior));
}

fn nature_article_id(url: &str) -> Option<String> {
    let path = url.split(['?', '#']).next().unwrap_or(url);
    let idx = path.find("/articles/")?;
    let id = path[idx + "/articles/".len()..]
        .trim_end_matches('/')
        .trim_end_matches(".pdf");
    if id.starts_with('s') || id.starts_with('n') {
        Some(id.to_string())
    } else {
        None
    }
}

fn doi_from_article_url(article_url: &str) -> Option<String> {
    if let Some(id) = nature_article_id(article_url) {
        return Some(format!("10.1038/{id}"));
    }
    if let Some(doi) = wiley_doi(article_url) {
        return Some(doi);
    }
    let parsed = ::url::Url::parse(article_url).ok()?;
    let host = parsed
        .host_str()?
        .trim_start_matches("www.")
        .to_ascii_lowercase();
    if host != "doi.org" && host != "dx.doi.org" {
        return None;
    }
    let doi = parsed
        .path()
        .trim_start_matches('/')
        .replace("%2F", "/")
        .replace("%2f", "/");
    doi.starts_with("10.").then_some(doi)
}

fn is_same_article(current_url: &str, target_url: &str) -> bool {
    match (
        doi_from_article_url(current_url),
        doi_from_article_url(target_url),
    ) {
        (Some(current), Some(target)) => current.eq_ignore_ascii_case(&target),
        _ => current_url.trim_end_matches('/') == target_url.trim_end_matches('/'),
    }
}

fn read_json(response: ureq::Response) -> Result<Value, ()> {
    let mut body = String::new();
    response
        .into_reader()
        .read_to_string(&mut body)
        .map_err(|_| ())?;
    serde_json::from_str(&body).map_err(|_| ())
}

fn http_get_pdf(url: &str, dir: &Path) -> Option<PathBuf> {
    let response = ureq::get(url)
        .set("User-Agent", "Mozilla/5.0")
        .set("Accept", "application/pdf")
        .call()
        .ok()?;
    let mut bytes = Vec::new();
    response.into_reader().read_to_end(&mut bytes).ok()?;
    if !bytes.starts_with(b"%PDF") || !is_real_file(&bytes) {
        return None;
    }
    let mut name = derive_download_file_name(url);
    if !name.to_ascii_lowercase().ends_with(".pdf") {
        name.push_str(".pdf");
    }
    let path = deduplicate_path(dir.join(name));
    std::fs::write(&path, bytes).ok()?;
    Some(path)
}

fn oa_pdf_urls(article_url: &str) -> Vec<String> {
    let doi = doi_from_article_url(article_url);
    let Some(doi) = doi else {
        return Vec::new();
    };
    let mut urls = Vec::new();
    if let Ok(response) = ureq::get(&format!(
        "https://api.unpaywall.org/v2/{doi}?email=drission-workflow@local"
    ))
    .set("Accept", "application/json")
    .call()
        && let Ok(value) = read_json(response)
    {
        if let Some(pdf) = value
            .pointer("/best_oa_location/url_for_pdf")
            .and_then(Value::as_str)
        {
            urls.push(pdf.to_string());
        }
        if let Some(locations) = value.get("oa_locations").and_then(Value::as_array) {
            for location in locations {
                if let Some(pdf) = location.get("url_for_pdf").and_then(Value::as_str) {
                    urls.push(pdf.to_string());
                }
            }
        }
    }
    if let Ok(response) = ureq::get(&format!("https://api.openalex.org/works/doi:{doi}"))
        .set("Accept", "application/json")
        .call()
        && let Ok(value) = read_json(response)
    {
        if let Some(pdf) = value
            .pointer("/best_oa_location/pdf_url")
            .and_then(Value::as_str)
        {
            urls.push(pdf.to_string());
        }
        if let Some(pdf) = value.pointer("/open_access/oa_url").and_then(Value::as_str) {
            urls.push(pdf.to_string());
        }
        if let Some(pdf) = value
            .pointer("/primary_location/pdf_url")
            .and_then(Value::as_str)
        {
            urls.push(pdf.to_string());
        }
        if let Some(locations) = value.get("locations").and_then(Value::as_array) {
            for location in locations {
                if let Some(pdf) = location.get("pdf_url").and_then(Value::as_str) {
                    urls.push(pdf.to_string());
                }
            }
        }
    }
    urls.sort();
    urls.dedup();
    urls
}

fn extra_watch_dirs(chrome_download_dir: Option<&Path>) -> Vec<PathBuf> {
    chrome_download_dir
        .map(|path| {
            let _ = std::fs::create_dir_all(path);
            path.to_path_buf()
        })
        .into_iter()
        .collect()
}

fn watch_download_dirs(
    primary: &Path,
    extra: &[PathBuf],
) -> (Vec<PathBuf>, Vec<HashMap<PathBuf, u64>>) {
    let mut dirs = vec![primary.to_path_buf()];
    // Only watch this task's directories; global Downloads may contain unrelated jobs.
    for path in extra {
        if path != primary && !dirs.iter().any(|existing| existing == path) {
            let _ = std::fs::create_dir_all(path);
            dirs.push(path.clone());
        }
    }
    let befores = dirs.iter().map(|path| dir_snapshot(path)).collect();
    (dirs, befores)
}

fn save_pdf_bytes(dir: &Path, url: &str, bytes: &[u8]) -> Option<PathBuf> {
    if !bytes.starts_with(b"%PDF") || !is_real_file(bytes) {
        return None;
    }
    let mut name = derive_download_file_name(url);
    if !name.to_ascii_lowercase().ends_with(".pdf") {
        name.push_str(".pdf");
    }
    let path = deduplicate_path(dir.join(name));
    std::fs::write(&path, bytes).ok()?;
    Some(path)
}

fn result_from_pdf_path(path: PathBuf) -> Option<Value> {
    let size = std::fs::metadata(&path).map(|meta| meta.len()).ok()?;
    Some(download_result(path, size))
}

fn js_truthy(value: &Value) -> bool {
    if let Some(flag) = value.as_bool() {
        return flag;
    }
    if let Some(flag) = value.get("value").and_then(Value::as_bool) {
        return flag;
    }
    let text = js_string(value);
    text == "true" || text == "1"
}

fn looks_like_pdf_viewer(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const type = (document.contentType || '').toLowerCase();
        const embed = document.querySelector(
            'embed[type="application/pdf"], embed[src*=".pdf"], #plugin, viewer-pdf-toolbar'
        );
        return !!(type.indexOf('pdf') !== -1 || embed);
    })()"#;
    match page.run_js(script) {
        Ok(value) => js_truthy(&value),
        Err(_) => page
            .url()
            .map(|url| {
                let lower = url.to_ascii_lowercase();
                lower.contains(".pdf") && !lower.contains(".html")
            })
            .unwrap_or(false),
    }
}

fn page_pdf_candidates(page: &ChromiumPage) -> Vec<String> {
    let Ok(html) = page.html() else {
        return Vec::new();
    };
    let current = page.url().unwrap_or_default();
    pdf_candidates_from_html(&html, &current)
}

fn is_auxiliary_pdf_url(url: &str) -> bool {
    let lower = url.to_ascii_lowercase();
    lower.ends_with("/authorship.pdf")
        || lower.contains("/authorship.pdf?")
        || lower.contains("publications-ethics")
        || lower.contains("publication_ethics")
        || lower.contains("/documentlibrary/aaa/")
        || lower.contains("/sites/default/files/")
}

fn is_webvpn_external_link(current: &str, target: &str) -> bool {
    let Ok(current) = ::url::Url::parse(current) else {
        return false;
    };
    let Ok(target) = ::url::Url::parse(target) else {
        return false;
    };
    current
        .host_str()
        .is_some_and(|host| host.eq_ignore_ascii_case("d.buaa.edu.cn"))
        && target
            .host_str()
            .is_some_and(|host| !host.eq_ignore_ascii_case("d.buaa.edu.cn"))
}

fn pdf_candidates_from_html(html: &str, current: &str) -> Vec<String> {
    use regex::Regex;

    let tag_re = Regex::new(r#"(?is)<(?:meta|a)\b[^>]*>"#).expect("static tag regex");
    let attr_re = Regex::new(r#"(?is)([a-zA-Z_:][-a-zA-Z0-9_:.]*)\s*=\s*["']([^"']*)["']"#)
        .expect("static attribute regex");
    let pdf_pattern = Regex::new(
        r#"(?i)\.pdf(?:$|[?#])|/pdf(?:direct)?/|articlepdf|full\.pdf|stamppdf|getpdf|pdfurl|download.{0,20}pdf|pdf.{0,20}download"#,
    )
    .expect("static PDF regex");
    let mut preferred = Vec::new();
    let mut links = Vec::new();
    for tag in tag_re.find_iter(&html).map(|value| value.as_str()) {
        let mut attrs = HashMap::new();
        for captures in attr_re.captures_iter(tag) {
            attrs.insert(
                captures[1].to_ascii_lowercase(),
                captures[2].replace("&amp;", "&"),
            );
        }
        let marker = attrs
            .get("name")
            .or_else(|| attrs.get("property"))
            .map(|value| value.to_ascii_lowercase());
        if marker.as_deref().is_some_and(|value| {
            matches!(value, "citation_pdf_url" | "wkhealth_pdf_url" | "og:pdf")
        }) && let Some(content) = attrs.get("content")
        {
            preferred.push(resolve_against(&current, content));
        }
        if let Some(href) = attrs.get("href")
            && pdf_pattern.is_match(href)
        {
            links.push(resolve_against(&current, href));
        }
    }
    preferred
        .into_iter()
        .chain(links)
        .filter(|url| url.starts_with("http://") || url.starts_with("https://"))
        .filter(|url| !is_auxiliary_pdf_url(url))
        .fold(Vec::new(), |mut urls, url| {
            if urls.len() < 8 && !urls.contains(&url) {
                urls.push(url);
            }
            urls
        })
}

fn page_has_antibot_challenge(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const body = document.body ? (document.body.innerText || '') : '';
        const title = document.title || '';
        const url = window.location.href || '';
        const challengeFrame = Array.from(document.querySelectorAll('iframe')).some((frame) =>
            /challenges\.cloudflare\.com|turnstile|challenge-platform/i.test(frame.src || '')
        );
        const challengeTitle = /Just a moment|Checking your browser|Access to This Page Has Been Blocked|Validate User|正在进行安全验证/i.test(title);
        const visibleChallenge = /Incapsula incident ID|browser extensions or network settings|security verification process|浏览器扩展或网络配置不兼容|安全验证过程|verify you are human|checking your browser|正在进行安全验证|浏览器不支持|您的浏览器不支持|does not support the security verification|Your browser is not supported/i.test(body);
        // Large publisher pages may preload Cloudflare/Incapsula resource URLs
        // even after verification succeeded.  Those hidden script strings are
        // not a challenge unless a visible prompt/frame/title is also present.
        return challengeFrame
            || challengeTitle
            || visibleChallenge
            || /crawlprevention\/governor/i.test(url)
            || /experiencing unusual traffic.{0,160}not a robot/i.test(body);
    })()"#;
    page.run_js(script)
        .map(|value| js_truthy(&value))
        .unwrap_or(false)
}

fn page_has_not_found(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const title = (document.title || '').toLowerCase();
        const body = document.body ? (document.body.innerText || '').toLowerCase() : '';
        return title.includes("404 not found")
            || title === "not found | acs publications"
            || title.includes("找不到与以下网址对应的网页")
            || title.includes("找不到网页")
            || title.includes("page not found")
            || body.includes("404 not found")
            || body.includes("找不到网页")
            || body.includes("找不到与以下网址对应的网页")
            || body.includes("the page you're looking for cannot be found")
            || body.includes("the page you’re looking for cannot be found")
            || body.includes("the requested url was not found")
            || ((location.hostname === "journals.aps.org"
                || (location.hostname === "d.buaa.edu.cn" && location.pathname.startsWith(
                    "/https/77726476706e69737468656265737421faf8548e29316443300999bfd65a3132/")))
                && body.includes("the page you requested could not be found, please check the link and try again."));
    })()"#;
    page.run_js(script)
        .map(|value| js_truthy(&value))
        .unwrap_or(false)
}

fn is_tju_eds_login_redirect(requested: &str, landed: &str) -> bool {
    let requested = requested.to_ascii_lowercase();
    let landed = landed.to_ascii_lowercase();
    requested.contains(".eds.tju.edu.cn")
        && (landed.contains("eds.tju.edu.cn/ermslogin/")
            || landed.contains("eds.tju.edu.cn/ermslogin?"))
}

fn renderer_probe_indicates_crash<E>(probe: Result<Value, E>) -> bool {
    match probe {
        Ok(value) => js_truthy(&value),
        // Navigation and verification also destroy execution contexts.
        Err(_) => false,
    }
}

fn page_has_renderer_crash(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const title = document.title || '';
        const body = document.body ? (document.body.innerText || '') : '';
        return /错误代码：\s*5|错误代码:\s*5|Error\s*code:\s*5|STATUS_INVALID_IMAGE_HASH|STATUS_ACCESS_VIOLATION|Aw,\s*Snap!|喔唷，崩溃啦|显示此网页出现了问题/i.test(title + ' ' + body);
    })()"#;
    renderer_probe_indicates_crash(page.run_js(script))
}

fn page_has_rate_limit(page: &ChromiumPage) -> bool {
    page.run_js(r#"(() => {
        const text = (document.title || '') + '\n' + (document.body?.innerText || '');
        return /429\s+Too Many Requests|You have sent too many requests in a given amount of time|Error\s+1015/i.test(text);
    })()"#).map(|v| js_truthy(&v)).unwrap_or(false)
}

fn page_has_hard_access_block(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const body = document.body ? (document.body.innerText || '') : '';
        const title = document.title || '';
        return /There was a problem providing the content|Reference number:|CPE00001|Error\s+1015|temporarily banned|IP Address:/i.test(body)
            || /Access Denied|Request Rejected/i.test(title);
    })()"#;
    page.run_js(script)
        .map(|value| js_truthy(&value))
        .unwrap_or(false)
}

fn page_has_cloudflare_unsupported_browser(page: &ChromiumPage) -> bool {
    let script = r#"(function() {
        const body = document.body ? (document.body.innerText || '') : '';
        return /浏览器不支持|您的浏览器不支持|无法支持最新安全功能|does not support the security verification|Your browser is not supported/i.test(body);
    })()"#;
    page.run_js(script)
        .map(|value| js_truthy(&value))
        .unwrap_or(false)
}

fn page_has_buaa_login(page: &ChromiumPage) -> bool {
    let url = page.url().unwrap_or_default().to_ascii_lowercase();
    if !buaa_login::trusted_url(&url) {
        return false;
    }
    url::Url::parse(&url).is_ok_and(|u| u.path().ends_with("/login"))
        || page
            .title()
            .map(|title| title.contains("统一身份认证"))
            .unwrap_or(false)
}

fn wait_for_manual_challenge(page: &ChromiumPage, interactive: bool) -> Result<(), DriverError> {
    let is_buaa_navigation = page
        .url()
        .map(|url| url.to_ascii_lowercase().contains("d.buaa.edu.cn"))
        .unwrap_or(false);
    // BUAA WebVPN performs a delayed client-side redirect to CAS. Give that
    // redirect enough time to appear before deciding that no login is needed.
    std::thread::sleep(if is_buaa_navigation {
        Duration::from_millis(2500)
    } else {
        Duration::from_millis(800)
    });
    if page_has_rate_limit(page) {
        return Err(DriverError::Cdp("RATE_LIMITED: publisher requests a cooldown".into()));
    }
    if page_has_hard_access_block(page) {
        return Err(DriverError::Cdp(
            "publisher access denied (CPE/IP block or rate limit); manual verification cannot clear it"
                .into(),
        ));
    }
    let mut antibot = page_has_antibot_challenge(page);
    let mut buaa_login = page_has_buaa_login(page);
    if buaa_login {
        return buaa_login::ensure(page);
    }
    if !antibot && !buaa_login {
        return Ok(());
    }
    if antibot && page_has_cloudflare_unsupported_browser(page) {
        return Err(DriverError::Cdp(
            "Cloudflare rejected this Chrome as unsupported (canvas/stealth); retry without canvas patches".into(),
        ));
    }
    if !interactive {
        return Err(DriverError::Cdp(
            "manual verification/login requires a visible browser session".into(),
        ));
    }
    let timeout = if buaa_login { 1200 } else { 300 };
    if buaa_login {
        eprintln!("检测到北航统一身份认证；请在 Chrome 中手动登录，程序最多等待 {timeout} 秒……");
    } else {
        eprintln!(
            "检测到人机验证；保持页面不动，等待自动放行或您手动完成验证，最多等待 {timeout} 秒……"
        );
    }
    let deadline = Instant::now() + Duration::from_secs(timeout);
    while Instant::now() < deadline {
        std::thread::sleep(Duration::from_secs(1));
        if page_has_hard_access_block(page) {
            return Err(DriverError::Cdp(
                "publisher access denied (CPE/IP block or rate limit); manual verification cannot clear it"
                    .into(),
            ));
        }
        antibot = page_has_antibot_challenge(page);
        buaa_login = page_has_buaa_login(page);
        if buaa_login {
            return buaa_login::ensure(page);
        }
        if antibot && page_has_cloudflare_unsupported_browser(page) {
            return Err(DriverError::Cdp(
                "Cloudflare rejected this Chrome as unsupported (canvas/stealth); retry without canvas patches".into(),
            ));
        }
        if !antibot && !buaa_login {
            std::thread::sleep(Duration::from_secs(3));
            return Ok(());
        }
    }
    let current_url = page.url().unwrap_or_default();
    let message = if buaa_login {
        format!("BUAA login was not completed within {timeout} seconds ({current_url})")
    } else {
        format!("anti-bot challenge was not cleared within {timeout} seconds ({current_url})")
    };
    Err(DriverError::Cdp(message))
}

fn print_current_to_pdf(page: &ChromiumPage, dir: &Path, url: &str) -> Option<PathBuf> {
    use base64::Engine as _;
    let result = page
        .tab()
        .run_cdp(
            "Page.printToPDF",
            Some(json!({
                "printBackground": true,
                "preferCSSPageSize": true,
            })),
        )
        .ok()?;
    let encoded = result.get("data").and_then(Value::as_str)?;
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(encoded)
        .ok()?;
    save_pdf_bytes(dir, url, &bytes)
}

/// Fetch the PDF in the page (cookies + cache) and click a blob `<a download>`.
/// Academic PDFs are usually larger than a CDP JS return value can carry, so we
/// never pull the bytes through the driver — Chrome writes them as a download.
fn trigger_blob_download(page: &ChromiumPage, url: &str, file_name: &str) -> bool {
    let script = format!(
        r#"(function() {{
            window.__drission_save__ = null;
            const targetUrl = new URL({url:?}, window.location.href).href;
            const fileName = {file_name:?};
            fetch(targetUrl, {{
                credentials: 'include',
                redirect: 'follow',
                headers: {{ Accept: 'application/pdf,application/octet-stream;q=0.9,*/*;q=0.1' }}
            }})
                .then(async (res) => {{
                    const buf = await res.arrayBuffer();
                    const bytes = new Uint8Array(buf);
                    const contentType = (res.headers.get('content-type') || '').toLowerCase();
                    let head = '';
                    for (let i = 0; i < Math.min(16, bytes.length); i++) head += String.fromCharCode(bytes[i]);
                    if (head.indexOf('%PDF') !== 0 || contentType.indexOf('text/html') !== -1 || bytes.length < 800) {{
                        window.__drission_save__ = {{
                            ok: false,
                            status: res.status,
                            contentType: contentType,
                            size: bytes.length,
                            head: head
                        }};
                        return;
                    }}
                    const blob = new Blob([bytes], {{ type: 'application/pdf' }});
                    const a = document.createElement('a');
                    a.href = URL.createObjectURL(blob);
                    a.download = fileName;
                    document.documentElement.appendChild(a);
                    a.click();
                    a.remove();
                    window.__drission_save__ = {{ ok: true, size: bytes.length }};
                }})
                .catch((e) => {{
                    window.__drission_save__ = {{ ok: false, error: String(e) }};
                }});
            return true;
        }})()"#
    );
    let _ = page.run_js(&script);
    let start = Instant::now();
    while start.elapsed() < Duration::from_secs(20) {
        std::thread::sleep(Duration::from_millis(250));
        let Ok(value) = page.run_js(
            "(() => { const res = window.__drission_save__; return res ? JSON.stringify(res) : ''; })()",
        ) else {
            continue;
        };
        let text = js_string(&value);
        if text.is_empty() || text == "null" {
            continue;
        }
        let Ok(parsed) = serde_json::from_str::<Value>(&text) else {
            continue;
        };
        return parsed.get("ok").and_then(Value::as_bool) == Some(true);
    }
    false
}

fn is_browser_session_only_url(url: &str) -> bool {
    ::url::Url::parse(url)
        .ok()
        .and_then(|parsed| parsed.host_str().map(str::to_owned))
        .is_some_and(|host| {
            host.eq_ignore_ascii_case("d.buaa.edu.cn")
                || host.to_ascii_lowercase().ends_with(".eds.tju.edu.cn")
                || host.eq_ignore_ascii_case("nature.com")
                || host.eq_ignore_ascii_case("www.nature.com")
        })
}

// Only an explicit, DOI-matched institutional denial is terminal for this route.
fn check_nature_access(page: &ChromiumPage, target: &str, dir: &Path) -> Result<(), DriverError> {
    let current = page.url().unwrap_or_default();
    if !is_same_article(&current, target) || page_has_antibot_challenge(page) {
        return Ok(());
    }
    let Ok(parsed) = ::url::Url::parse(&current) else { return Ok(()); };
    let host = parsed.host_str().unwrap_or("");
    if !matches!(host, "nature.com" | "www.nature.com")
        && !(host == "d.buaa.edu.cn" && parsed.path().starts_with(
            "/https/77726476706e69737468656265737421e7e056d229317c456c0dc7af9758/articles/")) {
        return Ok(());
    }
    let script = r#"(() => {
        if (document.readyState === 'loading' || [...document.querySelectorAll('input[type=password]')].some(el => el.getClientRects().length > 0)) return '';
        const body = document.body?.innerText || '';
        const message = body.match(/Access to this article via [^\n]{1,400} is not available\./);
        const doi = document.querySelector('meta[name="citation_doi"]')?.content || '';
        return JSON.stringify({doi, message: message?.[0] || '',
            preview: body.includes('This is a preview of subscription content, access via your institution')});
    })()"#;
    let value = page.run_js(script).map(|v| js_string(&v)).unwrap_or_default();
    let Ok(mut evidence) = serde_json::from_str::<Value>(&value) else { return Ok(()); };
    let expected = doi_from_article_url(target).unwrap_or_default();
    if !explicit_nature_denial(&evidence, &expected) { return Ok(()); }
    let Some(id) = nature_article_id(target).filter(|s| s.chars().all(|c| c.is_ascii_alphanumeric() || c == '-')) else { return Ok(()); };
    evidence["url"] = json!(current);
    evidence["status"] = json!("institution_access_unavailable");
    std::fs::write(dir.join(format!("{id}.access.json")), evidence.to_string())
        .map_err(|e| DriverError::Cdp(format!("cannot preserve access evidence: {e}")))?;
    eprintln!("机构无访问权限，跳过 {expected}");
    Err(DriverError::Cdp(format!("INSTITUTION_ACCESS_UNAVAILABLE: {expected}")))
}

fn explicit_nature_denial(evidence: &Value, expected: &str) -> bool {
    !expected.is_empty()
        && evidence["doi"].as_str().is_some_and(|d| d.eq_ignore_ascii_case(expected))
        && evidence["preview"].as_bool() == Some(true)
        && evidence["message"].as_str().is_some_and(|m|
            m.starts_with("Access to this article via ") && m.ends_with(" is not available."))
}

fn check_aps_publication_pending(page: &ChromiumPage, target: &str, dir: &Path) -> Result<(), DriverError> {
    let current = page.url().unwrap_or_default();
    let Ok(parsed) = ::url::Url::parse(&current) else { return Ok(()); };
    let host = parsed.host_str().unwrap_or("");
    if host != "journals.aps.org" && !(host == "d.buaa.edu.cn" && parsed.path().starts_with(
        "/https/77726476706e69737468656265737421faf8548e29316443300999bfd65a3132/")) { return Ok(()); }
    let Some((_, doi)) = parsed.path().split_once("/accepted/") else { return Ok(()); };
    if !doi.starts_with("10.1103/") || !target.split(['?', '#']).next().unwrap_or("").ends_with(doi)
        || page_has_antibot_challenge(page) { return Ok(()); }
    let confirmed = page.run_js(r#"(() => {
        const body = document.body?.innerText || '';
        return document.title.includes('Accepted Paper:') && body.includes('ACCEPTED PAPER')
            && ![...document.querySelectorAll('input[type=password]')].some(el => el.getClientRects().length > 0)
            && ![...document.querySelectorAll('a')].some(a => /pdf/i.test(a.textContent + ' ' + a.getAttribute('href')));
    })()"#).map(|v| js_truthy(&v)).unwrap_or(false);
    let suffix = doi.trim_start_matches("10.1103/");
    if !confirmed || !suffix.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') { return Ok(()); }
    let evidence = json!({"doi": doi, "url": current, "status": "publication_pending",
        "message": "APS accepted-paper page has no PDF link; recheck after publication"});
    std::fs::write(dir.join(format!("{suffix}.access.json")), evidence.to_string())
        .map_err(|e| DriverError::Cdp(format!("cannot preserve publication evidence: {e}")))?;
    Err(DriverError::Cdp(format!("PUBLICATION_PENDING: {doi}")))
}

fn download_resolved(
    page: &ChromiumPage,
    url: &str,
    dir: &Path,
    extra_watch: &[PathBuf],
    interactive: bool,
    allow_navigation: bool,
) -> Result<Value, DriverError> {
    check_aps_publication_pending(page, url, dir)?;
    check_nature_access(page, url, dir)?;
    let pdf_url = rewrite_to_pdf_url(url);
    // Institution access is authenticated either inside persistent Chrome
    // (WebVPN) or by the host's active VPN tunnel. Never move these requests
    // into the driver's standalone HTTP client: Nature must retain the real
    // browser's cookies, fingerprint and VPN-routed connection.
    let browser_session_only =
        is_browser_session_only_url(url) || is_browser_session_only_url(&pdf_url);
    // OpenAlex may already have supplied a repository/publisher PDF URL. Try
    // that exact URL before deriving a DOI; previously such URLs skipped the
    // direct OA path and Chrome merely displayed the PDF without saving it.
    if !browser_session_only {
        if let Some(path) = http_get_pdf(&pdf_url, dir) {
            return result_from_pdf_path(path)
                .ok_or_else(|| DriverError::Cdp("failed to keep direct OA PDF".into()));
        }
        for oa_url in oa_pdf_urls(url) {
            if let Some(path) = http_get_pdf(&oa_url, dir) {
                return result_from_pdf_path(path)
                    .ok_or_else(|| DriverError::Cdp("failed to keep OA PDF".into()));
            }
        }
    }

    // Nature and similar publishers send PDFs through a cookie SSO redirect
    // (`idp.nature.com/authorize`). Chrome then opens the file in its PDF
    // viewer, so the user can see it while nothing is written to disk.
    set_download_behavior(page, dir);
    let (watch_dirs, befores) = watch_download_dirs(dir, extra_watch);
    let extra_dirs = &watch_dirs[1..];

    // The workflow has already warmed the article page. If it is the same DOI,
    // use the exact URL obtained from the selected element first. Previously
    // the generic page scan ran first and AAA's footer Authorship.pdf could be
    // mistaken for thousands of article PDFs.
    let warmed_url = page.url().unwrap_or_default();
    let same_origin = match (::url::Url::parse(&warmed_url), ::url::Url::parse(&pdf_url)) {
        (Ok(current), Ok(target)) => {
            current.scheme() == target.scheme()
                && current.host_str() == target.host_str()
                && current.port_or_known_default() == target.port_or_known_default()
        }
        _ => false,
    };
    if same_origin && !is_auxiliary_pdf_url(&pdf_url) {
        let file_name = derive_download_file_name(&pdf_url);
        if trigger_blob_download(page, &pdf_url, &file_name) {
            let candidate_deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < candidate_deadline {
                if let Some(result) = find_new_download(dir, extra_dirs, &befores) {
                    return Ok(result);
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }

    if is_same_article(&warmed_url, url) {
        for candidate in page_pdf_candidates(page).iter().take(3) {
            if !browser_session_only
                && let Some(path) = http_get_pdf(candidate, dir)
                && let Some(result) = result_from_pdf_path(path)
            {
                return Ok(result);
            }
            let file_name = derive_download_file_name(candidate);
            if trigger_blob_download(page, candidate, &file_name) {
                let candidate_deadline = Instant::now() + Duration::from_secs(4);
                while Instant::now() < candidate_deadline {
                    if let Some(result) = find_new_download(dir, extra_dirs, &befores) {
                        return Ok(result);
                    }
                    std::thread::sleep(Duration::from_millis(250));
                }
            }
        }
    }

    // `download.click` still needs the original element below.  Do not let an
    // eager fallback navigate away and invalidate its execution context.
    if !allow_navigation {
        return Err(DriverError::Cdp(
            "resolved PDF URL did not yield a file before click".into(),
        ));
    }

    let _ = page.get(&pdf_url);
    wait_for_manual_challenge(page, interactive)?;
    refresh_frequency_limited_pdf(page)?;
    if page_has_not_found(page) {
        return Err(DriverError::Cdp(format!(
            "PDF URL returned 404/Not Found ({pdf_url}), skipping immediately"
        )));
    }
    if page_has_renderer_crash(page) {
        eprintln!("检测到 Chrome 下载重定向渲染崩溃（错误代码：5），自动刷新重试……");
        std::thread::sleep(Duration::from_millis(800));
        let _ = page.refresh();
        std::thread::sleep(Duration::from_millis(1500));
        if page_has_renderer_crash(page) {
            let _ = page.get(&pdf_url);
            std::thread::sleep(Duration::from_millis(1500));
            if page_has_renderer_crash(page) {
                return Err(DriverError::Cdp(format!(
                    "Chrome renderer remained crashed after PDF reload ({pdf_url}); skipping immediately"
                )));
            }
        }
    }
    wait_for_manual_challenge(page, interactive)?;

    let deadline = Instant::now() + DOWNLOAD_POLL_TIMEOUT;
    let idle_deadline = Instant::now() + Duration::from_secs(2);
    while Instant::now() < deadline {
        if let Some(result) = find_new_download(dir, extra_dirs, &befores) {
            return Ok(result);
        }
        if Instant::now() >= idle_deadline && !has_partial_download(&watch_dirs) {
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }

    let current = page.url().unwrap_or_default();
    if current.contains("idp.nature.com") || current.contains("/authorize") {
        return Err(DriverError::Cdp(
            "Nature redirected to login; open the Chrome window and sign in, then rerun".into(),
        ));
    }
    if page_has_buaa_login(page) {
        wait_for_manual_challenge(page, interactive)?;
    }

    check_nature_access(page, url, dir)?;

    // Some OA locations are article landing pages. Reuse the warmed browser
    // session to discover standard citation metadata and publisher download
    // links instead of treating every HTML page as a terminal failure.
    let discovered = page_pdf_candidates(page);
    for candidate in &discovered {
        if !browser_session_only
            && let Some(path) = http_get_pdf(candidate, dir)
            && let Some(result) = result_from_pdf_path(path)
        {
            return Ok(result);
        }
    }
    for candidate in discovered.iter().take(3) {
        let file_name = derive_download_file_name(candidate);
        if trigger_blob_download(page, candidate, &file_name) {
            let candidate_deadline = Instant::now() + Duration::from_secs(12);
            while Instant::now() < candidate_deadline {
                if let Some(result) = find_new_download(dir, extra_dirs, &befores) {
                    return Ok(result);
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }

    check_nature_access(page, url, dir)?;

    // Never printToPDF / blob-fetch a hanging pdfdirect viewer (NEJM Catalyst).
    let streaming_viewer = current.to_ascii_lowercase().contains("pdfdirect")
        || pdf_url.to_ascii_lowercase().contains("pdfdirect");
    if !streaming_viewer {
        if looks_like_pdf_viewer(page)
            && let Some(path) = print_current_to_pdf(page, dir, &pdf_url)
            && let Some(result) = result_from_pdf_path(path)
        {
            return Ok(result);
        }

        let file_name = derive_download_file_name(&pdf_url);
        if trigger_blob_download(page, &pdf_url, &file_name) {
            let blob_deadline = Instant::now() + Duration::from_secs(12);
            while Instant::now() < blob_deadline {
                if let Some(result) = find_new_download(dir, extra_dirs, &befores) {
                    return Ok(result);
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        }
    }

    check_nature_access(page, url, dir)?;
    Err(DriverError::Cdp(format!(
        "no PDF after opening {pdf_url} (current={current}, discoveredPdfLinks={}; HTML/paywall is not saved)",
        discovered.len()
    )))
}

/// Turn a URL's last path segment into a safe file name with a fallback extension.
fn derive_download_file_name(url: &str) -> String {
    let without_query = url.split(['?', '#']).next().unwrap_or(url);
    let name = Path::new(without_query)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .map(|name| name.replace(['/', '\\', '\0'], "_"))
        .unwrap_or_else(|| "download.bin".to_string());
    if Path::new(&name).extension().is_none() {
        format!("{name}.bin")
    } else {
        name
    }
}

/// Append `-1`, `-2`, … when the target file already exists (mirrors Chrome's behavior).
fn deduplicate_path(path: PathBuf) -> PathBuf {
    if !path.exists() {
        return path;
    }
    let stem = path
        .file_stem()
        .and_then(|stem| stem.to_str())
        .unwrap_or("file")
        .to_string();
    let extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_string);
    let mut counter = 1;
    loop {
        let name = match &extension {
            Some(extension) => format!("{stem}-{counter}.{extension}"),
            None => format!("{stem}-{counter}"),
        };
        let candidate = path.with_file_name(name);
        if !candidate.exists() {
            return candidate;
        }
        counter += 1;
    }
}

fn mime_from_path(path: &Path) -> &'static str {
    match path
        .extension()
        .and_then(|extension| extension.to_str())
        .map(str::to_ascii_lowercase)
        .as_deref()
    {
        Some("pdf") => "application/pdf",
        Some("png") => "image/png",
        Some("jpg") | Some("jpeg") => "image/jpeg",
        Some("gif") => "image/gif",
        Some("webp") => "image/webp",
        Some("svg") => "image/svg+xml",
        Some("zip") => "application/zip",
        Some("csv") => "text/csv; charset=utf-8",
        Some("xlsx") => "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        Some("docx") => "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        Some("json") => "application/json",
        _ => "application/octet-stream",
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn recognizes_temporary_frequency_limit_only() {
        assert!(super::is_frequency_limit_message("系统检测到您已达基础频率限制，请稍后再试。"));
        assert!(!super::is_frequency_limit_message("Sign in through your institution"));
        assert!(!super::is_frequency_limit_message("PDF article full text"));
    }
    use super::*;

    #[test]
    fn renderer_probe_requires_positive_crash_evidence() {
        assert!(!renderer_probe_indicates_crash(Err::<Value, _>(
            "renderer did not answer"
        )));
        assert!(renderer_probe_indicates_crash(Ok::<Value, &str>(json!(
            true
        ))));
        assert!(!renderer_probe_indicates_crash(Ok::<Value, &str>(json!(
            false
        ))));
    }

    #[test]
    fn acquires_and_releases_profile_lock() {
        let temp_dir = std::env::temp_dir().join(format!("profile-test-{}", std::process::id()));
        let lock = acquire_profile_lock(&temp_dir.to_string_lossy()).unwrap();
        assert!(lock.exists());

        let lock_again = acquire_profile_lock(&temp_dir.to_string_lossy()).unwrap();
        assert_eq!(lock, lock_again);

        let _ = std::fs::remove_dir_all(temp_dir);
    }

    #[cfg(unix)]
    #[test]
    fn refuses_live_chrome_profile_and_drop_releases_runner_lock() {
        let dir = std::env::temp_dir().join(format!("profile-live-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let singleton = dir.join("SingletonLock");
        std::os::unix::fs::symlink(format!("host-{}", std::process::id()), &singleton).unwrap();
        assert!(matches!(acquire_profile_lock(&dir.to_string_lossy()), Err(DriverError::ProfileLocked(_))));
        assert!(singleton.is_symlink());
        std::fs::remove_file(singleton).unwrap();
        let lock = acquire_profile_lock(&dir.to_string_lossy()).unwrap();
        let mut driver = DrissionDriver::new();
        driver.lock_path = Some(lock.clone());
        drop(driver);
        assert!(!lock.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn rewrites_wiley_article_url_to_pdfdirect() {
        let url = "https://acsjournals.onlinelibrary.wiley.com/doi/10.3322/caac.70097";
        assert_eq!(
            rewrite_to_pdf_url(url),
            "https://acsjournals.onlinelibrary.wiley.com/doi/pdfdirect/10.3322/caac.70097?download=true"
        );
    }

    #[test]
    fn does_not_rewrite_nejm_to_wiley_pdfdirect() {
        let url = "https://catalyst.nejm.org/doi/full/10.1056/CAT.26.0175";
        assert_eq!(rewrite_to_pdf_url(url), url);
        let nejm = "https://www.nejm.org/doi/full/10.1056/NEJMcpc2603179";
        assert_eq!(rewrite_to_pdf_url(nejm), nejm);
    }

    #[test]
    fn resolves_relative_doi_paths() {
        let current = "https://acsjournals.onlinelibrary.wiley.com/toc/15424863/2026/76/4";
        assert_eq!(
            resolve_against(current, "/doi/10.3322/caac.70097"),
            "https://acsjournals.onlinelibrary.wiley.com/doi/10.3322/caac.70097"
        );
    }

    #[test]
    fn rewrites_nature_article_url_to_pdf() {
        let url = "https://www.nature.com/articles/s41580-026-00847-x";
        assert_eq!(
            rewrite_to_pdf_url(url),
            "https://www.nature.com/articles/s41580-026-00847-x.pdf"
        );
        let relative = resolve_against(
            "https://www.nature.com/nrm/articles?year=2026",
            "/articles/s41580-026-00847-x",
        );
        assert_eq!(
            rewrite_to_pdf_url(&relative),
            "https://www.nature.com/articles/s41580-026-00847-x.pdf"
        );
    }

    #[test]
    fn extracts_doi_from_resolver_urls() {
        assert_eq!(
            doi_from_article_url("https://doi.org/10.1109/tnnls.2023.3243299"),
            Some("10.1109/tnnls.2023.3243299".to_string())
        );
        assert_eq!(
            doi_from_article_url("https://dx.doi.org/10.1002%2Fadma.202300244"),
            Some("10.1002/adma.202300244".to_string())
        );
    }

    #[test]
    fn recognizes_an_already_warmed_nature_article() {
        assert!(is_same_article(
            "https://www.nature.com/articles/s41575-023-00884-y?error=cookies_not_supported",
            "https://www.nature.com/articles/s41575-023-00884-y",
        ));
        assert!(!is_same_article(
            "https://www.nature.com/articles/s41575-023-00884-y",
            "https://www.nature.com/articles/s41580-024-00792-2",
        ));
    }

    #[test]
    fn recognizes_institution_urls_as_browser_session_only() {
        assert!(is_browser_session_only_url(
            "https://d.buaa.edu.cn/https/encoded/content/pdf/paper.pdf"
        ));
        assert!(is_browser_session_only_url(
            "https://D.BUAA.EDU.CN/https/encoded/stampPDF/getPDF.jsp"
        ));
        assert!(is_browser_session_only_url(
            "https://www.nature.com/articles/s41575-023-00884-y.pdf"
        ));
        assert!(is_browser_session_only_url("https://nature.com/"));
        assert!(!is_browser_session_only_url(
            "https://link.springer.com/content/pdf/paper.pdf"
        ));
        assert!(!is_browser_session_only_url("not a URL"));
    }

    #[test]
    fn detects_tianjin_eds_login_redirects_only_for_proxied_requests() {
        assert!(is_tju_eds_login_redirect(
            "https://proxy-token.eds.tju.edu.cn/science/article/pii/ABC",
            "https://eds.tju.edu.cn/ermsLogin/viewRelogin.do",
        ));
        assert!(!is_tju_eds_login_redirect(
            "https://p.lib.tju.edu.cn/login",
            "https://p.lib.tju.edu.cn/login",
        ));
        assert!(!is_tju_eds_login_redirect(
            "https://proxy-token.eds.tju.edu.cn/science/article/pii/ABC",
            "https://proxy-token.eds.tju.edu.cn/science/article/pii/ABC",
        ));
    }

    #[test]
    fn discovers_pdf_urls_from_article_html() {
        let html = r#"<html><head>
            <meta name="citation_pdf_url" content="/paper.pdf">
            </head><body>
            <a href="/download/articlepdf?id=7">Download PDF</a>
            </body></html>"#;
        assert_eq!(
            pdf_candidates_from_html(html, "https://example.org/article/7"),
            vec![
                "https://example.org/paper.pdf".to_string(),
                "https://example.org/download/articlepdf?id=7".to_string(),
            ]
        );
    }

    #[test]
    fn ignores_aaa_policy_pdf_when_discovering_article_pdf() {
        let html = r#"<html><body>
            <a class="footer-link" href="/sites/default/files/Authorship.pdf">Authorship policy PDF</a>
            <a class="article-pdfLink" href="/accounting-review/article-pdf/100/6/405/13826/TAR-2025-0485.pdf">PDF</a>
            </body></html>"#;
        assert_eq!(
            pdf_candidates_from_html(
                html,
                "https://d.buaa.edu.cn/https/encoded/accounting-review/article/100/6/405/13826/paper",
            ),
            vec!["https://d.buaa.edu.cn/accounting-review/article-pdf/100/6/405/13826/TAR-2025-0485.pdf".to_string()]
        );
    }

    #[test]
    fn recognizes_aaa_auxiliary_pdf_urls() {
        assert!(is_auxiliary_pdf_url(
            "https://publications.aaahq.org/sites/default/files/Authorship.pdf"
        ));
        assert!(!is_auxiliary_pdf_url(
            "https://publications.aaahq.org/accounting-review/article-pdf/100/6/405/13826/TAR-2025-0485.pdf"
        ));
    }

    #[test]
    fn webvpn_external_links_require_a_real_click() {
        assert!(is_webvpn_external_link(
            "https://d.buaa.edu.cn/https/encoded/accounting-review/article/100/6/405/13826/paper",
            "https://publications.aaahq.org/accounting-review/article-pdf/100/6/405/13826/paper.pdf",
        ));
        assert!(!is_webvpn_external_link(
            "https://d.buaa.edu.cn/https/encoded/accounting-review/article/100/6/405/13826/paper",
            "https://d.buaa.edu.cn/https/encoded/accounting-review/article-pdf/100/6/405/13826/paper.pdf",
        ));
    }

    #[test]
    fn nature_paths_never_join_cdn_host() {
        assert_eq!(
            resolve_against(
                "https://media.springernature.com/full/springer-static/pdf/x",
                "/articles/s41580-026-01022-7"
            ),
            "https://www.nature.com/articles/s41580-026-01022-7"
        );
        assert_eq!(
            resolve_against(
                "https://www.nature.com/nrm/articles?year=2026",
                "https://media.springernature.com/articles/s41580-026-01022-7"
            ),
            "https://www.nature.com/articles/s41580-026-01022-7"
        );
    }

    #[test]
    fn ds_store_does_not_block_pdf_capture() {
        let dir = std::env::temp_dir().join(format!("drission-ds-store-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join(".DS_Store"), b"mac folder metadata").unwrap();
        let mut pdf = b"%PDF-1.4\n".to_vec();
        pdf.resize(1200, b'x');
        let pdf_path = dir.join("s41580-026-01009-4.pdf");
        std::fs::write(&pdf_path, &pdf).unwrap();
        std::thread::sleep(Duration::from_millis(600));
        let found = newest_finished_file(&dir, &HashMap::new());
        let _ = std::fs::remove_dir_all(&dir);
        assert_eq!(
            found.as_ref().map(|(path, _)| path.as_path()),
            Some(pdf_path.as_path())
        );
    }

    #[test]
    fn hidden_and_html_files_are_not_treated_as_partial_downloads() {
        assert!(is_partial_download(Path::new("paper.pdf.crdownload")));
        assert!(!is_partial_download(Path::new(".DS_Store")));
        assert!(is_ignored_download(Path::new(".DS_Store")));
        assert!(is_ignored_download(Path::new("desktop.ini")));
    }

    #[test]
    fn detects_sangfor_easyconnect_utun() {
        let sample = "\
lo0: flags=8049 mtu 16384\n\
\tinet 127.0.0.1 netmask 0xff000000\n\
utun4: flags=8051 mtu 1400\n\
\tinet 10.230.32.55 --> 10.230.32.55 netmask 0xff000000\n\
en1: flags=8863 mtu 1500\n\
\tinet 192.168.31.20 netmask 0xffffff00\n";
        assert!(ifconfig_has_easyconnect_tun(sample));
        assert!(!ifconfig_has_easyconnect_tun(
            "en1: flags=8863\n\tinet 192.168.31.20 netmask 0xffffff00\n"
        ));
    }
    #[test]
    fn download_watch_is_scoped_to_task_directories() {
        let primary = std::env::temp_dir().join("springer-watch-scope");
        let (dirs, _) = watch_download_dirs(&primary, &[]);
        assert_eq!(dirs, vec![primary]);
    }

    #[test]
    fn text_logs_are_not_pdf_downloads_and_are_preserved() {
        let dir = std::env::temp_dir().join(format!("drission-log-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let log = dir.join("unrelated.pdf");
        std::fs::write(&log, vec![b'x'; 1200]).unwrap();
        std::thread::sleep(Duration::from_millis(600));
        assert!(newest_finished_file(&dir, &HashMap::new()).is_none());
        assert!(log.exists());
        std::fs::remove_dir_all(dir).unwrap();
    }

}

#[cfg(test)]
mod nature_access_tests {
    use super::*;
    #[test]
    fn requires_matching_doi_and_explicit_institution_denial() {
        let good = json!({"doi":"10.1038/s44160-026-01154-w", "preview":true,
            "message":"Access to this article via Test Institution is not available."});
        assert!(explicit_nature_denial(&good, "10.1038/s44160-026-01154-w"));
        assert!(!explicit_nature_denial(&good, "10.1038/other"));
        for message in ["Subscribe", "Log in", "Verify you are human", "403 Forbidden", ""] {
            let mut uncertain = good.clone();
            uncertain["message"] = json!(message);
            assert!(!explicit_nature_denial(&uncertain, "10.1038/s44160-026-01154-w"));
        }
        let mut loading = good;
        loading["preview"] = json!(false);
        assert!(!explicit_nature_denial(&loading, "10.1038/s44160-026-01154-w"));
    }
}
