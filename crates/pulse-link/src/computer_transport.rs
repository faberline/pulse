//! Source checks at both sides of durable image transport. No effect or permission grant.
use crate::{Result, artifact::ArtifactRef, ensure, protocol::*};

pub(crate) fn validate_result(
    work: &ServerPayload,
    device: &str,
    epoch: u64,
    outcome: &ReadOutcome,
    artifact: Option<&ArtifactRef>,
) -> Result<()> {
    let ServerPayload::Read {
        operation_id,
        task_id,
        revision,
        operation,
    } = work
    else {
        return Err(crate::Error::Rejected(
            "image result requires read work".into(),
        ));
    };
    let ReadOutcome::Completed {
        computer,
        observation,
        ..
    } = outcome
    else {
        return ensure(artifact.is_none(), "unfinished work cannot supply an image");
    };
    match operation {
        ReadOp::ComputerInspect { selection } => {
            let computer = computer
                .as_ref()
                .ok_or_else(|| crate::Error::Rejected("computer observation missing".into()))?;
            ensure(
                observation.is_none()
                    && computer.valid_evidence()
                    && computer.observed_at <= crate::now().saturating_add(1000)
                    && computer.selection() == *selection
                    && computer.binding.operation_id == *operation_id
                    && computer.binding.task_id == *task_id
                    && computer.binding.revision.to_string() == *revision
                    && computer.binding.device == device
                    && computer.binding.epoch == epoch
                    && artifact
                        .is_some_and(|a| a.validate().is_ok() && a.matches_computer(computer)),
                "computer result source or original image mismatch",
            )
        }
        _ => ensure(
            computer.is_none() && artifact.is_none_or(|a| a.computer.is_none()),
            "computer evidence on another tool kind",
        ),
    }
}

pub(crate) fn valid_chunk_source(
    operation: &ReadOp,
    artifact: &ArtifactRef,
    operation_id: &str,
    device: &str,
    epoch: u64,
) -> bool {
    match (operation, &artifact.computer) {
        (ReadOp::ComputerInspect { selection }, Some(source)) => {
            source.selection == *selection
                && source.binding.operation_id == operation_id
                && source.binding.device == device
                && source.binding.epoch == epoch
        }
        (
            ReadOp::Browser { .. } | ReadOp::BrowserInspect { .. } | ReadOp::BrowserAction { .. },
            None,
        ) => true,
        _ => false,
    }
}
