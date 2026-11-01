//! Fixed DOM reads and fixed element commands. No script or selector comes from a model.
use crate::{Result, browser::FixtureBrowser, ensure};
use pulse_core::{
    BrowserEffect, BrowserElement, BrowserElementKind, BrowserForm, browser_identifier,
    browser_input_text,
};
use reqwest::Method;
use serde_json::{Value, json};

fn element(value: &Value) -> Result<String> {
    let id = value["element-6066-11e4-a52e-4f735466cecf"]
        .as_str()
        .unwrap_or("");
    ensure(browser_identifier(id), "invalid browser element")?;
    Ok(id.into())
}
fn text(value: Value, max: usize) -> Result<String> {
    let text = value
        .as_str()
        .ok_or_else(|| crate::Error::Rejected("browser property is not text".into()))?;
    ensure(text.len() <= max, "browser property limit")?;
    Ok(text.into())
}
impl FixtureBrowser {
    async fn control_get(&self, id: &str, suffix: &str) -> Result<Value> {
        ensure(
            self.policy.is_some() && browser_identifier(id),
            "guarded browser element required",
        )?;
        self.get(&format!("element/{id}/{suffix}")).await
    }
    async fn property(&self, id: &str, name: &str) -> Result<Value> {
        self.control_get(id, &format!("property/{name}")).await
    }
    async fn find_controls(&self, root: Option<&str>, selector: &str) -> Result<Vec<String>> {
        let suffix = if let Some(id) = root {
            ensure(browser_identifier(id), "invalid form identity")?;
            format!("element/{id}/elements")
        } else {
            "elements".into()
        };
        let value = self
            .command(
                Method::POST,
                &format!("session/{}/{suffix}", self.session),
                Some(json!({"using":"css selector","value":selector})),
            )
            .await?;
        let values = value
            .as_array()
            .ok_or_else(|| crate::Error::Rejected("invalid browser control list".into()))?;
        ensure(values.len() <= 64, "browser control count limit")?;
        values.iter().map(element).collect()
    }
    async fn form(&self, button: &str, form: &str) -> Result<BrowserForm> {
        // Fixed descendant reads cannot prove fields associated outside the form.
        // A base target can also change the destination browsing context.
        ensure(
            self.find_controls(None, "[form],base[target]")
                .await?
                .is_empty(),
            "externally associated controls or base targets are unsupported",
        )?;
        let override_value = self.control_get(button, "attribute/formaction").await?;
        let url = text(
            if override_value.is_null() {
                self.property(form, "action").await?
            } else {
                self.property(button, "formAction").await?
            },
            2048,
        )?;
        let override_value = self.control_get(button, "attribute/formmethod").await?;
        let method = text(
            if override_value.is_null() {
                self.property(form, "method").await?
            } else {
                self.property(button, "formMethod").await?
            },
            16,
        )?
        .to_ascii_lowercase();
        let override_value = self.control_get(button, "attribute/formenctype").await?;
        let encoding = text(
            if override_value.is_null() {
                self.property(form, "enctype").await?
            } else {
                self.property(button, "formEnctype").await?
            },
            128,
        )?;
        ensure(
            encoding == "application/x-www-form-urlencoded",
            "unsupported form encoding",
        )?;
        let override_value = self.control_get(button, "attribute/formtarget").await?;
        let target = text(
            if override_value.is_null() {
                self.property(form, "target").await?
            } else {
                self.property(button, "formTarget").await?
            },
            32,
        )?;
        ensure(
            target.is_empty() || target == "_self",
            "new-window forms are disabled",
        )?;
        let charset = text(self.property(form, "acceptCharset").await?, 64)?.to_ascii_lowercase();
        ensure(
            charset.is_empty() || matches!(charset.as_str(), "utf-8" | "utf8"),
            "unsupported form charset",
        )?;
        let controls = self
            .find_controls(Some(form), "input,textarea,select,button")
            .await?;
        let mut fields = Vec::new();
        for id in controls {
            if self.control_get(&id, "enabled").await? != true {
                continue;
            }
            let name = text(self.property(&id, "name").await?, 128)?;
            if name.is_empty() {
                continue;
            }
            let tag = text(self.control_get(&id, "name").await?, 32)?;
            let kind = if tag == "textarea" {
                "textarea".into()
            } else {
                text(self.property(&id, "type").await?, 32)?
            };
            if matches!(kind.as_str(), "submit" | "button" | "reset") && id != button {
                continue;
            }
            ensure(
                matches!(
                    kind.as_str(),
                    "text" | "search" | "hidden" | "textarea" | "submit"
                ),
                "unsupported form control",
            )?;
            let value = text(self.property(&id, "value").await?, 1024)?;
            ensure(
                !name.chars().any(char::is_control) && !value.chars().any(char::is_control),
                "form contains unsupported control characters",
            )?;
            fields.push((name, value));
        }
        let form = BrowserForm {
            url,
            method,
            fields,
        };
        let current = self.get("url").await?;
        let origin = current
            .as_str()
            .and_then(|u| u.parse::<url::Url>().ok())
            .ok_or_else(|| crate::Error::Rejected("browser URL missing".into()))?;
        ensure(
            form.valid(&origin),
            "form destination or data is outside scope",
        )?;
        Ok(form)
    }
    pub(crate) async fn catalog(&self) -> Result<Vec<BrowserElement>> {
        ensure(self.policy.is_some(), "guarded browser required")?;
        let ids=self.find_controls(None,"button,input[type=submit],input[type=button],input:not([type]),input[type=text],input[type=search],textarea").await?;
        let mut controls = Vec::new();
        for id in ids {
            if self.control_get(&id, "enabled").await? != true
                || self.control_get(&id, "displayed").await? != true
            {
                continue;
            }
            let mut label = text(self.control_get(&id, "computedlabel").await?, 4096)?;
            if label.len() > 256 {
                let mut n = 253;
                while !label.is_char_boundary(n) {
                    n -= 1
                }
                label.truncate(n);
                label.push('…');
            }
            let tag = text(self.control_get(&id, "name").await?, 32)?;
            let kind = if tag == "textarea" {
                "textarea".into()
            } else {
                text(self.property(&id, "type").await?, 32)?
            };
            let kind = if matches!(kind.as_str(), "text" | "search" | "textarea") {
                if self.property(&id, "readOnly").await? == true {
                    continue;
                }
                BrowserElementKind::Text {
                    value: text(self.property(&id, "value").await?, 1024)?,
                }
            } else {
                let form = if kind == "submit" {
                    let owner = self.property(&id, "form").await?;
                    if owner.is_null() {
                        None
                    } else {
                        match self.form(&id, &element(&owner)?).await {
                            Ok(form) => Some(form),
                            Err(_) => continue,
                        }
                    }
                } else {
                    None
                };
                BrowserElementKind::Button { form }
            };
            controls.push(BrowserElement { id, label, kind });
            if controls.len() == 16 {
                break;
            }
        }
        Ok(controls)
    }
    pub(crate) async fn apply_effect(&self, id: &str, effect: &BrowserEffect) -> Result<()> {
        ensure(
            self.policy.is_some() && browser_identifier(id),
            "guarded browser target required",
        )?;
        let path = format!("session/{}/element/{id}", self.session);
        match effect {
            BrowserEffect::Click => {
                self.command(Method::POST, &format!("{path}/click"), Some(json!({})))
                    .await?;
            }
            BrowserEffect::SetText { text } => {
                ensure(
                    browser_input_text(text),
                    "text contains submit or control keys",
                )?;
                self.command(Method::POST, &format!("{path}/clear"), Some(json!({})))
                    .await?;
                if !text.is_empty() {
                    self.command(
                        Method::POST,
                        &format!("{path}/value"),
                        Some(json!({"text":text})),
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }
}
