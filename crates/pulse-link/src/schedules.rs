//! Event/timer wakeups only. Never call a provider from this loop.
use crate::{Result, server::Hub};
use std::time::Duration;
impl Hub {
    pub async fn schedule_ready(&self, device: &str, epoch: u64, time: i64) -> Result<()> {
        let changed=sqlx::query("UPDATE link_device SET schedule_notices=1 WHERE id=? AND role='chat' AND revoked=0 AND epoch=? AND lease_until>?")
            .bind(device).bind(epoch as i64).bind(time).execute(&self.db).await?.rows_affected();
        crate::ensure(
            changed == 1,
            "schedule notices require a current chat lease",
        )?;
        self.notify_change();
        Ok(())
    }
}
pub async fn run(hub: Hub) -> Result<()> {
    let mut changes = hub.core.schedule_changes();
    loop {
        let tick = hub.core.fire_schedule(crate::now()).await?;
        if tick.fired {
            hub.notify_change();
            continue;
        }
        let at = if tick.blocked {
            Some(crate::now().saturating_add(1000))
        } else {
            tick.next_at
        };
        tokio::select! {
            result=changes.changed()=>{if result.is_err(){return Ok(());}},
            ()=async {match at {Some(at)=>tokio::time::sleep(Duration::from_millis(at.saturating_sub(crate::now()).max(0) as u64)).await,None=>std::future::pending().await}}=>{},
        }
    }
}
