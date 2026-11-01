//! Experimental observation adapter for an operator-owned loopback test website.
//! Not a model tool or a general network sandbox. No user profile or arbitrary JS.
use crate::{Result, ensure, now, protocol::hash_bytes};
use base64::{Engine, engine::general_purpose::STANDARD};
use reqwest::{Client, Method};
use serde_json::{Value, json};
use std::time::Duration;
use url::Url;

const MAX_RESPONSE: usize = 4 * 1024 * 1024;
const MAX_TEXT: usize = 32 * 1024;

pub struct BrowserSnapshot {
    pub url: String,
    pub title: String,
    pub text: String,
    pub truncated: bool,
    pub observed_at: i64,
    pub png: Vec<u8>,
    pub screenshot_digest: String,
    pub browser_version: String,
}

pub struct FixtureBrowser {
    client: Client,
    endpoint: Url,
    origin: Url,
    session: String,
    browser_version: String,
    usable: bool,
    closed: bool,
}

fn loopback_root(url: &Url) -> bool {
    url.scheme() == "http"
        && url.host_str() == Some("127.0.0.1")
        && url.port().is_some_and(|port| port != 0)
        && url.username().is_empty()
        && url.password().is_none()
        && url.path() == "/"
        && url.query().is_none()
        && url.fragment().is_none()
}
fn identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 512
        && !matches!(value, "." | "..")
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-:".contains(&b))
}

impl FixtureBrowser {
    /// The caller owns the driver process and must stop it on all exit paths.
    /// ChromeDriver creates a fresh temporary profile; no debugger address is accepted.
    pub async fn open(endpoint: Url, origin: Url) -> Result<Self> {
        ensure(
            loopback_root(&endpoint) && loopback_root(&origin),
            "loopback fixture roots required",
        )?;
        let client = Client::builder()
            .no_proxy()
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| crate::Error::Rejected("browser client setup failed".into()))?;
        let mut browser = Self {
            client,
            endpoint,
            origin,
            session: String::new(),
            browser_version: String::new(),
            usable: false,
            closed: false,
        };
        let value = browser.command(Method::POST, "session", Some(json!({
            "capabilities":{"alwaysMatch":{"browserName":"chrome","acceptInsecureCerts":false,
                "goog:chromeOptions":{"args":["--headless=new","--disable-background-networking",
                    "--disable-sync","--no-first-run","--no-default-browser-check","--window-size=1024,768"]}}}
        }))).await?;
        let session = value["sessionId"].as_str().unwrap_or("");
        ensure(identifier(session), "invalid browser session identity")?;
        browser.session = session.into();
        let version = value["capabilities"]["browserVersion"]
            .as_str()
            .unwrap_or("");
        if version.is_empty() || version.len() > 128 {
            let _ = browser.close().await;
            return Err(crate::Error::Rejected("missing browser version".into()));
        }
        browser.browser_version = version.into();
        browser.usable = true;
        Ok(browser)
    }
    fn allowed(&self, url: &Url) -> bool {
        url.origin() == self.origin.origin()
            && url.username().is_empty()
            && url.password().is_none()
            && url.as_str().len() <= 2048
    }
    async fn command(&self, method: Method, path: &str, body: Option<Value>) -> Result<Value> {
        let mut request = self.client.request(method, self.endpoint.join(path)?);
        if let Some(body) = body {
            request = request.json(&body);
        }
        let mut response = request
            .send()
            .await
            .map_err(|_| crate::Error::Rejected("browser command transport failed".into()))?;
        ensure(response.status().is_success(), "browser command rejected")?;
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| crate::Error::Rejected("browser response interrupted".into()))?
        {
            ensure(
                bytes.len() + chunk.len() <= MAX_RESPONSE,
                "browser response too large",
            )?;
            bytes.extend_from_slice(&chunk);
        }
        let value: Value = serde_json::from_slice(&bytes)?;
        ensure(
            value.get("value").is_some() && value["value"].get("error").is_none(),
            "invalid browser response",
        )?;
        Ok(value["value"].clone())
    }
    async fn get(&self, suffix: &str) -> Result<Value> {
        self.command(
            Method::GET,
            &format!("session/{}/{suffix}", self.session),
            None,
        )
        .await
    }
    async fn current_url(&self) -> Result<Url> {
        let value = self.get("url").await?;
        let url = Url::parse(value.as_str().unwrap_or(""))?;
        ensure(self.allowed(&url), "browser left fixture origin")?;
        Ok(url)
    }
    /// Only navigate an explicitly supplied self-hosted test origin.
    /// Redirect/subresource network isolation is a separate integration gate.
    pub async fn navigate(&mut self, url: &Url) -> Result<()> {
        ensure(
            self.usable && !self.closed && self.allowed(url),
            "browser navigation outside fixture or stopped",
        )?;
        self.usable = false;
        self.command(
            Method::POST,
            &format!("session/{}/url", self.session),
            Some(json!({"url":url.as_str()})),
        )
        .await?;
        self.current_url().await?;
        self.usable = true;
        Ok(())
    }
    pub async fn observe(&mut self) -> Result<BrowserSnapshot> {
        ensure(self.usable && !self.closed, "browser session stopped")?;
        self.usable = false;
        let result = tokio::time::timeout(Duration::from_secs(15), self.snapshot())
            .await
            .map_err(|_| crate::Error::Rejected("browser observation timed out".into()))?;
        if result.is_ok() {
            self.usable = true;
        }
        result
    }
    async fn snapshot(&self) -> Result<BrowserSnapshot> {
        let url = self.current_url().await?;
        let title = self.get("title").await?;
        let title = title
            .as_str()
            .ok_or_else(|| crate::Error::Rejected("invalid browser title".into()))?;
        ensure(title.len() <= 4096, "browser title too large")?;
        let element = self
            .command(
                Method::POST,
                &format!("session/{}/element", self.session),
                Some(json!({"using":"css selector","value":"body"})),
            )
            .await?;
        let element = element["element-6066-11e4-a52e-4f735466cecf"]
            .as_str()
            .unwrap_or("");
        ensure(identifier(element), "invalid browser element identity")?;
        let text = self.get(&format!("element/{element}/text")).await?;
        let text = text
            .as_str()
            .ok_or_else(|| crate::Error::Rejected("invalid browser text".into()))?;
        let mut end = text.len().min(MAX_TEXT);
        while !text.is_char_boundary(end) {
            end -= 1;
        }
        let screenshot = self.get("screenshot").await?;
        let png = STANDARD
            .decode(screenshot.as_str().unwrap_or(""))
            .map_err(|_| crate::Error::Rejected("invalid screenshot encoding".into()))?;
        ensure(
            png.len() >= 33 && png.starts_with(b"\x89PNG\r\n\x1a\n") && &png[12..16] == b"IHDR",
            "invalid screenshot format",
        )?;
        let width = u32::from_be_bytes(png[16..20].try_into().expect("checked PNG header"));
        let height = u32::from_be_bytes(png[20..24].try_into().expect("checked PNG header"));
        ensure(
            width > 0
                && height > 0
                && width <= 8192
                && height <= 8192
                && u64::from(width) * u64::from(height) <= 16_777_216,
            "screenshot dimensions exceed limit",
        )?;
        ensure(
            self.current_url().await? == url,
            "browser navigated during observation",
        )?;
        Ok(BrowserSnapshot {
            url: url.into(),
            title: title.into(),
            text: text[..end].into(),
            truncated: end < text.len(),
            observed_at: now(),
            screenshot_digest: hash_bytes(&png),
            png,
            browser_version: self.browser_version.clone(),
        })
    }
    pub async fn close(&mut self) -> Result<()> {
        if self.closed {
            return Ok(());
        }
        self.usable = false;
        self.command(Method::DELETE, &format!("session/{}", self.session), None)
            .await?;
        self.closed = true;
        Ok(())
    }
}
