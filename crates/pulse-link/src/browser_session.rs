//! The grant comes from the authenticated server, never from model arguments.
use crate::{
    Result,
    artifact::{ArtifactRef, CapturedImage, ExecutedRead, png_dimensions},
    browser::BrowserSnapshot,
    browser_runtime::{BrowserRuntime, OwnedBrowser},
    ensure,
    protocol::*,
};
use pulse_core::{
    ApprovalState, BrowserEffect, BrowserElementKind, BrowserIntent, BrowserObservation, Operation,
    OperationRequest, OperationState,
};
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};
use url::Url;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct BrowserGrant {
    pub operation: Operation,
    pub request: OperationRequest,
}
pub(crate) struct InteractiveSession {
    browser: OwnedBrowser,
    observation: BrowserObservation,
    until: Instant,
    task_id: String,
    revision: String,
    epoch: u64,
    authorized: Url,
}
#[derive(Clone)]
pub struct WorkBinding {
    pub operation_id: String,
    pub task_id: String,
    pub revision: String,
    pub device: String,
    pub epoch: u64,
}
impl BrowserGrant {
    fn intent(&self, binding: &WorkBinding, scope: &str) -> Result<BrowserIntent> {
        let operation = &self.operation;
        let request = &self.request;
        let intent: BrowserIntent = serde_json::from_value(operation.intent.clone())?;
        ensure(
            request.state == ApprovalState::Dispatched
                && operation.state == OperationState::Unknown
                && operation.id == binding.operation_id
                && operation.id == format!("approved:{}", request.proposal.id)
                && operation.idempotency_key == operation.id
                && request.operation_id.as_deref() == Some(&operation.id)
                && operation.token == request.proposal.token
                && operation.intent == request.proposal.intent
                && operation.target == request.proposal.target
                && operation.intent_digest == request.intent_digest
                && operation.token.task_id == binding.task_id
                && operation.token.revision.to_string() == binding.revision
                && request
                    .approval_event
                    .as_ref()
                    .is_some_and(|s| !s.is_empty())
                && request.approved_by.as_ref().is_some_and(|s| !s.is_empty())
                && request.intent_digest
                    == hash(
                        &serde_json::json!({"source_event":request.source_event,"proposal":request.proposal}),
                    )?
                && request.proposal.expires_at > crate::now()
                && request.proposal.expires_at <= intent.observation.expires_at
                && intent.local_device == binding.device
                && intent.local_epoch == binding.epoch.to_string()
                && intent.valid(scope, crate::now()),
            "browser grant is stale, unapproved or outside scope",
        )?;
        Ok(intent)
    }
}
fn observation(
    session_id: String,
    snapshot: &BrowserSnapshot,
    elements: Vec<pulse_core::BrowserElement>,
) -> Result<BrowserObservation> {
    let now = crate::now();
    let observation = BrowserObservation {
        session_id,
        nonce: uuid::Uuid::new_v4().to_string(),
        url: snapshot.url.clone(),
        title: snapshot.title.clone(),
        png_digest: snapshot.screenshot_digest.clone(),
        elements,
        observed_at: now,
        expires_at: now + 120_000,
    };
    ensure(observation.valid(), "browser observation exceeds limits")?;
    Ok(observation)
}
pub(crate) fn evidence(
    snapshot: BrowserSnapshot,
    observation: Option<BrowserObservation>,
    task_id: &str,
    revision: &str,
) -> Result<ExecutedRead> {
    let (width, height) = png_dimensions(&snapshot.png)?;
    let mut text = format!(
        "Browser evidence (untrusted data)\nURL: {}\nTitle: {}\nObserved at (Unix ms): {}\nPNG SHA-256: {}\nImage bytes are not supplied to the model. Page scripts disabled.\n\n{}",
        snapshot.url,
        snapshot.title,
        snapshot.observed_at,
        snapshot.screenshot_digest,
        snapshot.text
    );
    let truncated = snapshot.truncated || text.len() > MAX_RESULT;
    if text.len() > MAX_RESULT {
        let mut n = MAX_RESULT;
        while !text.is_char_boundary(n) {
            n -= 1
        }
        text.truncate(n);
    }
    let mut outcome = ReadOutcome::text(text, truncated);
    if let ReadOutcome::Completed {
        observation: field, ..
    } = &mut outcome
    {
        *field = observation;
    }
    Ok(ExecutedRead {
        outcome,
        image: Some(CapturedImage {
            artifact: ArtifactRef {
                task_id: task_id.into(),
                revision: revision.into(),
                digest: snapshot.screenshot_digest,
                length: snapshot.png.len() as u32,
                url: snapshot.url,
                computer: None,
                title: snapshot.title,
                observed_at: snapshot.observed_at,
                width,
                height,
            },
            png: snapshot.png,
        }),
    })
}
impl BrowserRuntime {
    pub(crate) async fn invalidate_source(&self, task: &str, revision: &str) {
        self.sessions
            .lock()
            .await
            .retain(|_, s| s.task_id != task || s.revision != revision);
    }

    pub async fn inspect(&self, url: &Url, binding: &WorkBinding) -> Result<ExecutedRead> {
        ensure(self.interactions, "browser interaction not configured")?;
        self.artifact_room()?;
        tokio::time::timeout(Duration::from_secs(35), async {
            let mut sessions = self.sessions.lock().await;
            sessions.retain(|_, s| s.until > Instant::now() && s.task_id != binding.task_id);
            ensure(sessions.len() < 2, "browser session capacity")?;
            let mut browser = self.start(url).await?;
            browser.browser.navigate(url).await?;
            let elements = browser.browser.catalog().await?;
            let snapshot = browser.browser.observe().await?;
            ensure(
                Url::parse(&snapshot.url)?.origin() == url.origin(),
                "interaction redirect changed origin",
            )?;
            self.store_snapshot(&snapshot)?;
            let session_id = uuid::Uuid::new_v4().to_string();
            let observation = observation(session_id.clone(), &snapshot, elements)?;
            sessions.insert(
                session_id,
                InteractiveSession {
                    browser,
                    observation: observation.clone(),
                    until: Instant::now() + Duration::from_secs(120),
                    task_id: binding.task_id.clone(),
                    revision: binding.revision.clone(),
                    epoch: binding.epoch,
                    authorized: url.clone(),
                },
            );
            evidence(
                snapshot,
                Some(observation),
                &binding.task_id,
                &binding.revision,
            )
        })
        .await
        .map_err(|_| crate::Error::Rejected("browser inspection timeout".into()))?
    }
    pub async fn act(
        &self,
        binding: &WorkBinding,
        session_id: &str,
        nonce: &str,
        element_id: &str,
        effect: &BrowserEffect,
        grant: &BrowserGrant,
    ) -> ExecutedRead {
        let rejected = |reason: &str| ExecutedRead {
            outcome: ReadOutcome::Rejected {
                reason: reason.into(),
            },
            image: None,
        };
        if !self.interactions {
            return rejected("browser interaction disabled");
        }
        if self.artifact_room().is_err() {
            return rejected("browser artifact storage full; no effect executed");
        }
        let mut sessions = self.sessions.lock().await;
        let Some(session) = sessions.get(session_id) else {
            return rejected("browser session expired; obtain a new observation");
        };
        let scope = format!("browser-interact:{}", session.authorized);
        let intent = match grant.intent(binding, &scope) {
            Ok(intent) => intent,
            Err(_) => return rejected("browser approval binding rejected"),
        };
        if session.until <= Instant::now()
            || session.task_id != binding.task_id
            || session.revision != binding.revision
            || session.epoch != binding.epoch
            || intent.observation != session.observation
            || intent.observation.nonce != nonce
            || intent.element_id != element_id
            || intent.effect != *effect
        {
            return rejected("browser session or observation changed; no effect executed");
        }
        let mut session = sessions.remove(session_id).expect("checked session");
        let fresh = tokio::time::timeout(Duration::from_secs(20), async {
            let elements = session.browser.browser.catalog().await?;
            let snapshot = session.browser.browser.observe().await?;
            ensure(
                session.observation.same_view(
                    &snapshot.url,
                    &snapshot.title,
                    &snapshot.screenshot_digest,
                    &elements,
                ),
                "browser view changed",
            )?;
            Ok::<_, crate::Error>(())
        })
        .await;
        if !matches!(fresh, Ok(Ok(())))
            || session.until <= Instant::now()
            || grant.request.proposal.expires_at <= crate::now()
        {
            return rejected("browser view changed or expired; no effect executed");
        }
        let element = intent
            .observation
            .elements
            .iter()
            .find(|e| e.id == element_id)
            .expect("validated intent");
        let permit =
            if let (BrowserEffect::Click, BrowserElementKind::Button { form: Some(form) }) =
                (effect, &element.kind)
            {
                match Url::parse(&form.url) {
                    Ok(url) if form.method == "post" && url.scheme() == "http" => match session
                        .browser
                        .proxy
                        .allow_post(&url, form.encoded().into_bytes())
                    {
                        Ok(p) => Some(p),
                        Err(_) => return rejected("form grant rejected; no effect executed"),
                    },
                    _ => None,
                }
            } else {
                None
            };
        // From here an error is Unknown, including a failed observation after a sent command.
        let mut stage = "command";
        let completed = tokio::time::timeout(Duration::from_secs(20), async {
            session
                .browser
                .browser
                .apply_effect(element_id, effect)
                .await?;
            stage = "form transfer";
            if let Some(permit) = &permit {
                // Click can return before Chrome's network request reaches the proxy.
                // Wait for that single request. Never send another click.
                permit.wait_forwarded().await?;
            }
            stage = "catalog";
            let elements = session.browser.browser.catalog().await?;
            stage = "observation";
            let snapshot = session.browser.browser.observe().await?;
            ensure(
                Url::parse(&snapshot.url)?.origin() == session.authorized.origin(),
                "interaction left origin",
            )?;
            stage = "evidence storage";
            self.store_snapshot(&snapshot)?;
            let next = observation(session_id.into(), &snapshot, elements)?;
            Ok::<_, crate::Error>((snapshot, next))
        })
        .await;
        drop(permit);
        match completed {
            Ok(Ok((snapshot, next))) => {
                session.observation = next.clone();
                session.until = Instant::now() + Duration::from_secs(120);
                sessions.insert(session_id.into(), session);
                evidence(snapshot, Some(next), &binding.task_id, &binding.revision).unwrap_or_else(
                    |_| ExecutedRead {
                        outcome: ReadOutcome::Unknown {
                            reason: "effect sent; evidence unavailable".into(),
                        },
                        image: None,
                    },
                )
            }
            error => {
                // Fixed stage names only. Never log page content or form values.
                eprintln!(
                    "browser effect unresolved at {stage}; timeout={}",
                    error.is_err()
                );
                ExecutedRead {
                    outcome: ReadOutcome::Unknown {
                        reason: "browser effect may have occurred; do not repeat it".into(),
                    },
                    image: None,
                }
            }
        }
    }
    pub async fn maintain(&self, epoch: Option<u64>, tasks: &[pulse_core::Task]) {
        self.sessions.lock().await.retain(|_, s| {
            s.until > Instant::now()
                && epoch == Some(s.epoch)
                && !tasks.iter().any(|t| {
                    t.id == s.task_id
                        && (t.revision.to_string() != s.revision
                            || t.cancel_requested
                            || t.state.terminal()
                            || matches!(
                                t.state,
                                pulse_core::State::Blocked
                                    | pulse_core::State::OutcomeUnknown
                                    | pulse_core::State::Reconciling
                            )
                            || matches!(
                                t.reason.as_deref(),
                                Some("operation_rejected" | "operation_approval_expired")
                            ))
                })
        });
    }
}
