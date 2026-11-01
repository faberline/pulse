//! Local timer and memory-change events. This loop never calls a provider.
use crate::{Result, now};
use pulse_core::Store;
use std::time::Duration;

pub async fn run(store: Store) -> Result<()> {
    loop {
        let state = store.maintain(now()).await?;
        let delay = state.next_at.saturating_sub(now()).max(0) as u64;
        tokio::select! {
            () = tokio::time::sleep(Duration::from_millis(delay)) => {},
            () = store.wait_for_maintenance_change() => {},
        }
    }
}
