//! Plain-text frontend for the existing Rust chat device and server.
use crate::{
    Result,
    client::{Config, Device, Vault},
    protocol::*,
};
use std::{collections::BTreeMap, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncBufRead, AsyncBufReadExt, AsyncWrite, AsyncWriteExt},
    sync::{mpsc, watch},
};

pub const HELP: &str = "pulse-cli [--watch] --server HTTPS_ORIGIN [--data-dir PRIVATE_DIR] [--pair-code-file PRIVATE_FILE] [--dev-loopback]\nType one message per line. :quit ends after server receipt. --watch accepts no new input; existing pending messages still send.\n";

fn terminal_text(text: &str) -> String {
    text.chars()
        .filter(|c| !c.is_control() || *c == '\n' || *c == '\t')
        .collect()
}
struct Input<R> {
    reader: R,
    bytes: Vec<u8>,
}
impl<R: AsyncBufRead + Unpin> Input<R> {
    async fn line(&mut self) -> Result<Option<String>> {
        loop {
            let chunk = self.reader.fill_buf().await?;
            if chunk.is_empty() {
                break;
            }
            let end = chunk
                .iter()
                .position(|b| *b == b'\n')
                .map(|n| n + 1)
                .unwrap_or(chunk.len());
            crate::ensure(
                self.bytes.len() + end <= 4098,
                "CLI input exceeds 4096 bytes",
            )?;
            self.bytes.extend_from_slice(&chunk[..end]);
            self.reader.consume(end);
            if self.bytes.ends_with(b"\n") {
                break;
            }
        }
        let mut bytes = std::mem::take(&mut self.bytes);
        if bytes.is_empty() {
            return Ok(None);
        }
        if bytes.ends_with(b"\n") {
            bytes.pop();
        }
        if bytes.ends_with(b"\r") {
            bytes.pop();
        }
        crate::ensure(bytes.len() <= 4096, "CLI input exceeds 4096 bytes")?;
        Ok(Some(String::from_utf8(bytes).map_err(|_| {
            crate::Error::Rejected("CLI input must be UTF-8".into())
        })?))
    }
}

pub async fn run(
    config: Config,
    pairing: Option<String>,
    vault: Arc<dyn Vault>,
    input: impl AsyncBufRead + Unpin,
    mut output: impl AsyncWrite + Unpin,
    watch_only: bool,
) -> Result<()> {
    let (commands, receiver) = mpsc::channel(8);
    let (states, mut state) = watch::channel(NativeState::default());
    if let Some(code) = pairing {
        commands
            .send(NativeCommand::Pair { code })
            .await
            .map_err(|_| crate::Error::Rejected("CLI stopped".into()))?;
    }
    // Keep startup in this future. EOF or a signal must not orphan an opening device.
    let device = async {
        Device::open_chat(config, vault)
            .await?
            .run(receiver, states)
            .await
    };
    tokio::pin!(device);
    let mut device_done = false;
    let mut input = Input {
        reader: input,
        bytes: Vec::new(),
    };
    let mut ready = false;
    let mut pending: Option<String> = None;
    let mut ending = false;
    let mut shutdown_sent = false;
    let mut messages = BTreeMap::<String, String>::new();
    let mut link = None;
    let startup = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(startup);
    let finish = tokio::time::sleep(Duration::from_secs(30));
    tokio::pin!(finish);
    let signal = tokio::signal::ctrl_c();
    tokio::pin!(signal);
    let result=async {loop {tokio::select! {
        result=&mut device=>{device_done=true;return result;},
        _=&mut startup,if !ready=>return Err(crate::Error::Rejected("CLI startup unconfirmed; check pairing and Keychain".into())),
        _=&mut finish,if ending=>return Err(crate::Error::Rejected("server receipt unconfirmed; check the original private data directory before resubmitting".into())),
        _=&mut signal,if !ending=>{ending=true;finish.as_mut().reset(tokio::time::Instant::now()+Duration::from_secs(30));},
        changed=state.changed()=>{
            changed.map_err(|_|crate::Error::Rejected("CLI state stopped".into()))?;
            let value=state.borrow_and_update().clone();
            ready=value.paired;
            if link!=Some((value.paired,value.connected)) {
                link=Some((value.paired,value.connected));
                output.write_all(if value.connected {"server 已連線。\n".as_bytes()}else if value.paired {"離線；訊息會保存在本機。\n".as_bytes()}else{"尚未配對；請先產生 private 配對檔案。\n".as_bytes()}).await?;
            }
            if let Some(receipt)=&value.input_receipt
                && pending.as_ref()==Some(&receipt.source_id) {
                crate::ensure(receipt.saved,"input was not saved; check connection identity, size and queue capacity")?;
                output.write_all(format!("本機已保存：{}\n",receipt.source_id).as_bytes()).await?;
                pending=None;
            }
            for message in &value.messages {
                let digest=hash_bytes(message.body.as_bytes());
                if messages.get(&message.id)!=Some(&digest) {
                    output.write_all(format!("\n{}\n",terminal_text(&message.body)).as_bytes()).await?;
                    messages.insert(message.id.clone(),digest);
                }
            }
            // Keep only IDs and digests for the current bounded projection.
            messages.retain(|id,_|value.messages.iter().any(|m|&m.id==id));
            if ending && pending.is_none() && value.pending==0 && value.connected && !shutdown_sent {
                output.write_all("server 已收件；這不表示任務已完成。\n".as_bytes()).await?;
                commands.send(NativeCommand::Shutdown).await.map_err(|_|crate::Error::Rejected("CLI stopped before shutdown".into()))?;
                shutdown_sent=true;
            }
            output.flush().await?;
        },
        value=input.line(),if ready && pending.is_none() && !ending && !watch_only=>{
            match value? {
                None=>{ending=true;finish.as_mut().reset(tokio::time::Instant::now()+Duration::from_secs(30));},
                Some(text) if text==":quit"=>{ending=true;finish.as_mut().reset(tokio::time::Instant::now()+Duration::from_secs(30));},
                Some(text) if text.trim().is_empty()=>{},
                Some(goal)=>{
                    let source_id=uuid::Uuid::new_v4().to_string();
                    let command=NativeCommand::Action{source_id:source_id.clone(),action:HumanAction::Submit{id:uuid::Uuid::new_v4().to_string(),goal}};
                    commands.send(command).await.map_err(|_|crate::Error::Rejected("CLI input channel stopped".into()))?;
                    pending=Some(source_id);
                }
            }
        }
    }}}.await;
    if !device_done {
        let _ = commands.try_send(NativeCommand::Shutdown);
        let _ = tokio::time::timeout(Duration::from_secs(5), &mut device).await;
    }
    result
}

#[cfg(test)]
mod tests;
