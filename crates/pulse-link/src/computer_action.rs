//! Local grant and exact retained-view checks before one fixed native command.
use super::*;
use crate::{artifact::ExecutedRead, browser_session::WorkBinding, protocol::*};
use pulse_core::{ApprovalState, ComputerIntent, Operation, OperationRequest, OperationState};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ComputerGrant {
    pub operation: Operation,
    pub request: OperationRequest,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct NativeActionReceipt {
    binding: ComputerBinding,
    source_nonce: String,
    state: NativeActionState,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum NativeActionState {
    NotExecuted,
    Completed,
    Unknown,
}
impl ComputerGrant {
    fn intent(&self, binding: &WorkBinding) -> Result<ComputerIntent> {
        let operation = &self.operation;
        let request = &self.request;
        let intent: ComputerIntent = serde_json::from_value(operation.intent.clone())?;
        let source = &intent.observation.binding;
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
                && intent.target().as_deref() == Some(&operation.target)
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
                && source.operation_id != binding.operation_id
                && source.task_id == binding.task_id
                && source.revision.to_string() == binding.revision
                && source.device == binding.device
                && source.epoch == binding.epoch
                && intent.valid(&operation.target, crate::now()),
            "computer grant is stale, unapproved or outside scope",
        )?;
        Ok(intent)
    }
}
impl ComputerRuntime {
    /// Trusted authenticated delivery only. The journal must mark work running before this call.
    /// No caller may retry Unknown; reopening the journal must retain that uncertainty.
    pub async fn act(&self, binding: &WorkBinding, grant: &ComputerGrant) -> ExecutedRead {
        let result = |unknown: bool, reason: &str| ExecutedRead {
            outcome: if unknown {
                ReadOutcome::Unknown {
                    reason: reason.into(),
                }
            } else {
                ReadOutcome::Rejected {
                    reason: reason.into(),
                }
            },
            image: None,
        };
        // A background capability probe may own the helper briefly. Reserve its
        // gate before consuming the view, then recheck the grant and local selection.
        let permit =
            match tokio::time::timeout(Duration::from_secs(3), self.gate.clone().acquire_owned())
                .await
            {
                Ok(Ok(permit)) => permit,
                _ => return result(false, "computer helper busy; no command sent"),
            };
        let image_lease = match self
            .images
            .existing(&binding.task_id, &binding.revision)
            .await
        {
            Ok(lease) => lease,
            Err(_) => return result(false, "computer image source unavailable; no command sent"),
        };
        let admission = (|| -> Result<_> {
            let intent = grant.intent(binding)?;
            let selection = intent.observation.selection();
            ensure(
                self.permitted(&binding.task_id, &binding.revision, &selection)
                    && self
                        .target
                        .as_ref()
                        .is_none_or(|target| *target == selection.target),
                "computer local selection unavailable",
            )?;
            image_lease.valid()?;
            // Native preflight and post-effect observation can each save one PNG.
            self.images.capacity(&self.directory, 2)?;
            let cache = image_lease.path.clone();
            let current = ComputerBinding {
                operation_id: binding.operation_id.clone(),
                task_id: binding.task_id.clone(),
                revision: number(&binding.revision)?,
                device: binding.device.clone(),
                epoch: binding.epoch,
            };
            ensure(current.valid(), "invalid computer effect binding")?;
            let request = serde_json::to_vec(
                &serde_json::json!({"kind":"act","binding":current,"intent":intent}),
            )?;
            ensure(request.len() <= 65_536, "computer action request limit")?;
            let args = self.arguments(&selection.target, &cache)?;
            // Exact retained local metadata and original private PNG are required. A server
            // cannot substitute a fabricated observation. Remove it before any child I/O.
            let mut views = self
                .observations
                .lock()
                .map_err(|_| crate::Error::Rejected("local observations unavailable".into()))?;
            let index = views
                .iter()
                .position(|view| view == &intent.observation)
                .ok_or_else(|| {
                    crate::Error::Rejected("local observation expired or consumed".into())
                })?;
            let image = cache.join(format!("{}.png", intent.observation.image.digest));
            read_image(&image, &intent.observation.image)?;
            views.remove(index);
            Ok((intent, selection, current, request, args))
        })();
        let (intent, selection, current, request, args) = match admission {
            Ok(value) => value,
            Err(_) => {
                return result(
                    false,
                    "computer approval or exact local observation unavailable; no command sent",
                );
            }
        };
        let started = crate::now();
        let mut activity = self.activity.subscribe();
        let mut check = tokio::time::interval(Duration::from_millis(20));
        let source = match SourceScope::new(&binding.task_id, &binding.revision) {
            Ok(source) => source,
            Err(_) => return result(false, "computer source rejected; no command sent"),
        };
        let call = self.call_with_permit(&args, Some(request), self.timeout, permit, Some(source));
        tokio::pin!(call);
        let response = loop {
            tokio::select! {
                response=&mut call=>break response,
                _=activity.wait_for(|live| !*live)=>return result(true,"computer command interrupted; outcome unknown"),
                _=check.tick()=>if !self.permitted(&binding.task_id,&binding.revision,&selection) || grant.request.proposal.expires_at <= crate::now() {
                    return result(true,"computer command expired or selection withdrawn; outcome unknown");
                },
            }
        };
        let Ok(response) = response else {
            return result(
                true,
                "computer child ended without a verified receipt; outcome unknown",
            );
        };
        let Some(receipt) = response.action else {
            return result(true, "computer effect receipt missing; outcome unknown");
        };
        if receipt.binding != current
            || receipt.source_nonce != intent.observation.nonce
            || response.selection.is_some()
        {
            return result(
                true,
                "computer effect receipt source mismatch; outcome unknown",
            );
        }
        match receipt.state {
            NativeActionState::NotExecuted
                if response.observation.is_none() && response.error.is_some() =>
            {
                result(
                    false,
                    "computer window changed or preflight rejected; no command sent",
                )
            }
            NativeActionState::Completed
                if response.error.is_none() && response.permissions.can_observe() =>
            {
                let Some(raw) = response.observation else {
                    return result(
                        true,
                        "computer command sent without original result image; outcome unknown",
                    );
                };
                match self
                    .validate_evidence(
                        raw,
                        &current,
                        Some(&selection),
                        &selection.target,
                        started,
                        image_lease,
                    )
                    .await
                    .and_then(|evidence| executed_observation_kind(evidence, true))
                {
                    Ok(evidence) => evidence,
                    Err(_) => result(
                        true,
                        "computer command sent but result image invalid; outcome unknown",
                    ),
                }
            }
            _ => result(true, "computer command may have executed; outcome unknown"),
        }
    }
}

#[cfg(test)]
#[path = "computer_action_tests.rs"]
mod tests;
