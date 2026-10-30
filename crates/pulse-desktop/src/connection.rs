//! Chat transport only. Closing this connection cannot stop an executor.
use pulse_link::{
    client::{Config, Device, NativeVault},
    protocol::{NativeCommand, NativeState},
};
use std::sync::{Arc, mpsc};
use tokio::sync::{mpsc as channel, watch};

pub(crate) struct ChatConnection {
    pub commands: channel::Sender<NativeCommand>,
    pub states: mpsc::Receiver<NativeState>,
    thread: std::thread::JoinHandle<()>,
    stop: watch::Sender<bool>,
}
impl ChatConnection {
    pub fn start(args: &[String], previous: Option<Self>) -> std::io::Result<Self> {
        let config = Config::parse_chat(args.to_vec())
            .map_err(|_| std::io::Error::other("invalid chat connection"))?;
        let (commands, receiver) = channel::channel(32);
        let (output, states) = mpsc::sync_channel(8);
        let (stop, mut stopped) = watch::channel(false);
        // Join the old chat thread here, so reconnect never races its file lock.
        // This wait runs outside the UI thread.
        let old_thread = previous.map(|old| old.close());
        let thread = std::thread::Builder::new()
            .name("pulse-chat".into())
            .spawn(move || {
                if let Some(thread) = old_thread {
                    let _ = thread.join();
                }
                let Ok(runtime) = tokio::runtime::Runtime::new() else {
                    return;
                };
                runtime.block_on(async move {
                    let (sender, mut changed) = watch::channel(NativeState::default());
                    let output_task = tokio::spawn(async move {
                        while changed.changed().await.is_ok() {
                            let _ = output.try_send(changed.borrow_and_update().clone());
                        }
                    });
                    tokio::select! {
                        _=stopped.changed()=>{},
                        _=run(config,receiver,sender)=>{}
                    }
                    output_task.abort();
                });
            })?;
        Ok(Self {
            commands,
            states,
            thread,
            stop,
        })
    }
    fn close(self) -> std::thread::JoinHandle<()> {
        self.stop.send_replace(true);
        self.thread
    }
    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }
}
// Dropping the sender cancels the chat thread. It owns no executor process.
async fn run(
    config: Config,
    receiver: channel::Receiver<NativeCommand>,
    sender: watch::Sender<NativeState>,
) -> pulse_link::Result<()> {
    Device::open_chat(config, Arc::new(NativeVault))
        .await?
        .run(receiver, sender)
        .await
}
