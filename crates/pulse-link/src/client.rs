// Outbound device connector and private native IPC.
use crate::{
    Result, ensure,
    executor::Executor,
    journal::{DeviceLock, Journal, default_data_dir},
    protocol::*,
};
use futures_util::{SinkExt, StreamExt};
use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::{mpsc, watch};
use tokio_tungstenite::tungstenite::{Message, client::IntoClientRequest};
use url::Url;

#[derive(Clone)]
pub struct Config {
    pub server: Url,
    pub repo: PathBuf,
    pub data_dir: PathBuf,
}
impl Config {
    /// Operator convenience: the one-use code stays in a private file, not argv.
    pub fn parse_with_pairing(
        args: impl IntoIterator<Item = String>,
        role: ClientRole,
    ) -> Result<(Self, Option<String>)> {
        let mut args = args.into_iter();
        let mut connection = Vec::new();
        let mut pairing = None;
        while let Some(arg) = args.next() {
            if arg == "--pair-code-file" {
                ensure(pairing.is_none(), "duplicate pair file")?;
                let path = args
                    .next()
                    .ok_or_else(|| crate::Error::Rejected("pair file required".into()))?;
                let code = crate::journal::private_read(std::path::Path::new(&path))?;
                ensure(
                    code.len() == 64 && code.bytes().all(|b| b.is_ascii_hexdigit()),
                    "invalid pairing code",
                )?;
                pairing = Some(code);
            } else {
                connection.push(arg);
            }
        }
        Ok((Self::parse_for(connection, role)?, pairing))
    }
    pub fn parse(args: impl IntoIterator<Item = String>) -> Result<Self> {
        Self::parse_for(args, ClientRole::LocalExecutor)
    }
    pub fn parse_chat(args: impl IntoIterator<Item = String>) -> Result<Self> {
        Self::parse_for(args, ClientRole::Chat)
    }
    fn parse_for(args: impl IntoIterator<Item = String>, role: ClientRole) -> Result<Self> {
        let mut args = args.into_iter();
        let mut server = None;
        let mut repo = None;
        let mut data_dir = default_data_dir().join(role.as_str());
        let mut dev = false;
        while let Some(arg) = args.next() {
            match arg.as_str(){
            "--server"=>server=args.next(),"--repo"=>repo=args.next().map(PathBuf::from),"--data-dir"=>data_dir=PathBuf::from(args.next().ok_or_else(||crate::Error::Rejected("missing data directory".into()))?),
            "--dev-loopback"=>dev=true,_=>return Err(crate::Error::Rejected("use --server HTTPS_ORIGIN --repo PULSE_ROOT [--data-dir PRIVATE_DIR] [--dev-loopback]".into())),
        }
        }
        let server = validate_server(
            &server.ok_or_else(|| crate::Error::Rejected("server origin is required".into()))?,
            dev,
        )?;
        let repo = if role == ClientRole::Chat {
            ensure(repo.is_none(), "chat has no repository or local executor")?;
            PathBuf::new()
        } else {
            Executor::new(
                &repo
                    .ok_or_else(|| crate::Error::Rejected("repository root is required".into()))?,
            )?
            .root()
            .to_path_buf()
        };
        Ok(Self {
            server,
            repo,
            data_dir,
        })
    }
}
pub fn validate_server(value: &str, dev: bool) -> Result<Url> {
    ensure(value.len() <= 2048, "server origin size")?;
    let url = Url::parse(value)?;
    ensure(
        url.username().is_empty()
            && url.password().is_none()
            && url.query().is_none()
            && url.fragment().is_none()
            && url.path() == "/",
        "server must be an origin without user information, path, query or fragment",
    )?;
    let loopback = match url.host() {
        Some(url::Host::Ipv4(ip)) => ip.is_loopback(),
        Some(url::Host::Ipv6(ip)) => ip.is_loopback(),
        _ => false,
    };
    ensure(
        url.scheme() == "https" || (dev && loopback && url.scheme() == "http"),
        "HTTPS required; HTTP only supports explicit literal loopback development",
    )?;
    Ok(url)
}

/// Implementations must keep credentials outside application databases and IPC.
pub trait Vault: Send + Sync + 'static {
    fn load(&self, account: &str) -> Result<Option<Credential>>;
    fn save(&self, account: &str, credential: &Credential) -> Result<()>;
}
pub struct NativeVault;
#[cfg(target_os = "macos")]
impl Vault for NativeVault {
    fn load(&self, account: &str) -> Result<Option<Credential>> {
        match security_framework::passwords::get_generic_password(
            "com.faberline.pulse.device",
            account,
        ) {
            Ok(bytes) => Ok(Some(parse(&bytes)?)),
            Err(error) if error.code() == -25300 => Ok(None),
            Err(_) => Err(crate::Error::Rejected("Keychain access failed".into())),
        }
    }
    fn save(&self, account: &str, credential: &Credential) -> Result<()> {
        security_framework::passwords::set_generic_password(
            "com.faberline.pulse.device",
            account,
            &serde_json::to_vec(credential)?,
        )
        .map_err(|_| crate::Error::Rejected("Keychain save failed".into()))
    }
}
#[cfg(not(target_os = "macos"))]
impl Vault for NativeVault {
    fn load(&self, _: &str) -> Result<Option<Credential>> {
        Err(crate::Error::Rejected(
            "native credential storage is only implemented on macOS".into(),
        ))
    }
    fn save(&self, _: &str, _: &Credential) -> Result<()> {
        Err(crate::Error::Rejected(
            "native credential storage is only implemented on macOS".into(),
        ))
    }
}

#[derive(Clone, Default)]
struct Lease {
    epoch: u64,
    until: Option<Instant>,
}
impl Lease {
    fn live(&self) -> bool {
        self.until.is_some_and(|until| Instant::now() < until)
    }
}
#[derive(Clone, Default)]
struct LinkState {
    connected: bool,
}

pub struct Device {
    config: Config,
    journal: Journal,
    _lock: DeviceLock,
    executor: Option<Executor>,
    vault: Arc<dyn Vault>,
    account: String,
}
impl Device {
    pub async fn open(config: Config, vault: Arc<dyn Vault>) -> Result<Self> {
        Self::open_role(config, vault, ClientRole::LocalExecutor).await
    }
    pub async fn open_chat(config: Config, vault: Arc<dyn Vault>) -> Result<Self> {
        ensure(
            config.repo.as_os_str().is_empty(),
            "chat cannot select a repository",
        )?;
        Self::open_role(config, vault, ClientRole::Chat).await
    }
    async fn open_role(config: Config, vault: Arc<dyn Vault>, role: ClientRole) -> Result<Self> {
        let executor = if role == ClientRole::LocalExecutor {
            Some(Executor::new(&config.repo)?.with_browser(
                crate::browser_runtime::BrowserRuntime::from_environment(&config.data_dir)?,
            ))
        } else {
            None
        };
        let (journal, lock) =
            Journal::open_role(&config.data_dir, config.server.as_str(), &config.repo, role)
                .await?;
        let account = hash(&(config.server.as_str(), &journal.device, role))?;
        journal.recover().await?;
        Ok(Self {
            config,
            journal,
            _lock: lock,
            executor,
            vault,
            account,
        })
    }
    pub async fn run(
        self,
        mut commands: mpsc::Receiver<NativeCommand>,
        states: watch::Sender<NativeState>,
    ) -> Result<()> {
        let vault = self.vault.clone();
        let account = self.account.clone();
        let mut credential = tokio::task::spawn_blocking(move || vault.load(&account))
            .await
            .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
        if let Some(value) = &credential {
            ensure(
                value.device == self.journal.device
                    && value.token.len() == 64
                    && value.role == self.journal.role,
                "Keychain identity mismatch",
            )?;
        }
        let (lease_tx, lease_rx) = watch::channel(Lease::default());
        let (link_tx, link_rx) = watch::channel(LinkState::default());
        let mut connection = credential.clone().map(|c| {
            tokio::spawn(reconnect(
                self.config.clone(),
                self.journal.clone(),
                c,
                lease_tx.clone(),
                link_tx.clone(),
                self.executor.as_ref().and_then(Executor::browser_ready),
            ))
        });
        let journal = self.journal.clone();
        let executor = self.executor.clone();
        let mut worker = tokio::spawn(async move {
            if let Some(executor) = executor {
                work(journal, executor, lease_rx).await
            } else {
                std::future::pending::<Result<()>>().await
            }
        });
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        let mut message = "配對後可連線。模型尚未接入。".to_string();
        let mut pairing: Option<tokio::task::JoinHandle<Result<Credential>>> = None;
        let mut artifact_download: Option<
            tokio::task::JoinHandle<Result<crate::artifact::LocalArtifact>>,
        > = None;
        let mut artifact_loading: Option<String> = None;
        let mut artifact_failure: Option<String> = None;
        let result=async {loop {tokio::select! {
            completed=async{match artifact_download.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                artifact_download=None;
                let loaded_id=artifact_loading.take();
                match completed {
                    Ok(Ok(image))=>{match self.journal.save_artifact(&image).await {
                        Ok(())=>message="截圖已核對並保存。".into(),
                        Err(crate::Error::Rejected(_))=>message="截圖來源已更新。請重新選取。".into(),
                        Err(error)=>return Err(error),
                    }}
                    _=>{artifact_failure=loaded_id;message="截圖尚未取得。可按截圖重試；不會重新瀏覽。".into();},
                }
            }
            completed=async {match pairing.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                pairing=None;
                match completed {
                    Ok(Ok(value))=>{
                        lease_tx.send_replace(Lease::default());link_tx.send_replace(LinkState::default());
                        if let Some(task)=connection.take(){task.abort();let _=task.await;}
                        connection=Some(tokio::spawn(reconnect(self.config.clone(),self.journal.clone(),value.clone(),lease_tx.clone(),link_tx.clone(),self.executor.as_ref().and_then(Executor::browser_ready))));
                        credential=Some(value);message="配對完成。密鑰已存入 Keychain。".into();
                    }
                    _=>message="配對未完成。請核對伺服器、一次性配對碼及 Keychain 權限。若已兌換配對碼，須先由操作員撤銷再重試。".into(),
                }
            }
            _=ticker.tick()=>{
                let mut state=self.journal.view().await?;state.server=self.config.server.to_string();state.paired=credential.is_some();state.connected=link_rx.borrow().connected;state.message=message.clone();
                state.artifact_loading=artifact_loading.clone();state.artifact_failure=artifact_failure.clone();
                states.send_replace(state);
            }
            done=&mut worker=>{done.map_err(|_|crate::Error::Rejected("local worker stopped".into()))??;return Err(crate::Error::Rejected("local worker stopped".into()));}
            command=commands.recv()=>{
                match command {
                    Some(NativeCommand::LoadArtifact{operation_id,digest})=>{
                        if artifact_download.is_some(){continue;}
                        if let Some(credential)=credential.clone() {
                            match self.journal.artifact_request(&operation_id,&digest).await {
                                Ok(artifact)=>{artifact_loading=Some(operation_id.clone());artifact_failure=None;message="正在載入截圖…".into();let config=self.config.clone();artifact_download=Some(tokio::spawn(async move{crate::artifact_client::download(&config,&credential,&operation_id,&artifact).await}));}
                                Err(_)=>message="截圖要求已拒絕。請核對來源。".into(),
                            }
                        }
                    }
                    None|Some(NativeCommand::Shutdown)=>break,
                    Some(NativeCommand::KillSwitch{stopped})=>{
                        if self.journal.role == ClientRole::Chat {
                            self.journal.action(&uuid::Uuid::new_v4().to_string(), HumanAction::StopLocal{stopped}).await?;
                            message="本機工具控制已排隊。等待 server 確認。".into();
                        } else {
                            self.journal.stop(stopped).await?;
                            message=if stopped{"本機工具已停止。"}else{"本機只讀工具已啟用。"}.into();
                        }
                    }
                    Some(NativeCommand::Action{source_id,action})=>{
                        if self.journal.role != ClientRole::Chat {message="executor 不接受人類輸入。請使用對話框。".into();continue;}
                        match self.journal.action(&source_id,action).await {Ok(())=>message="已存入本機待送佇列。".into(),Err(crate::Error::Rejected(_)|crate::Error::Capacity)=>message="要求未保存：內容或佇列已超過限制。".into(),Err(e)=>return Err(e)}
                    }
                    Some(NativeCommand::Pair{code})=>{
                        if pairing.is_some(){message="配對仍在進行。".into();continue;}
                        let request=PairRequest{device:self.journal.device.clone(),code,role:self.journal.role};
                        let config=self.config.clone();let vault=self.vault.clone();let account=self.account.clone();
                        pairing=Some(tokio::spawn(async move {
                            let value=redeem(&config,&request).await?;
                            ensure(value.device==request.device&&value.token.len()==64&&value.role==request.role,"pair response identity")?;
                            let copy=value.clone();tokio::task::spawn_blocking(move||vault.save(&account,&copy)).await.map_err(|_|crate::Error::Rejected("Keychain worker failed".into()))??;
                            Ok(value)
                        }));message="正在配對。本機停止工具仍可用。".into();
                    }
                }
            }
        }}Ok(())}.await;
        lease_tx.send_replace(Lease::default());
        if let Some(task) = artifact_download {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = pairing {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = connection {
            task.abort();
            let _ = task.await;
        }
        worker.abort();
        let _ = worker.await;
        result
    }
}
async fn redeem(config: &Config, request: &PairRequest) -> Result<Credential> {
    ensure(
        request.code.len() == 64 && request.code.bytes().all(|b| b.is_ascii_hexdigit()),
        "invalid pairing code",
    )?;
    let client = reqwest::Client::builder()
        .no_proxy()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(Duration::from_secs(10))
        .build()
        .map_err(|_| crate::Error::Rejected("HTTP setup failed".into()))?;
    let mut response = client
        .post(config.server.join("v1/device-pairings/redeem")?)
        .json(request)
        .send()
        .await
        .map_err(|_| crate::Error::Rejected("pair request failed".into()))?;
    ensure(response.status().is_success(), "pair rejected")?;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|_| crate::Error::Rejected("pair response failed".into()))?
    {
        ensure(bytes.len() + chunk.len() <= 4096, "pair response size")?;
        bytes.extend_from_slice(&chunk);
    }
    parse(&bytes)
}
async fn reconnect(
    config: Config,
    journal: Journal,
    credential: Credential,
    lease: watch::Sender<Lease>,
    state: watch::Sender<LinkState>,
    browser: Option<Option<String>>,
) {
    let mut delay = 1;
    loop {
        let started = Instant::now();
        let _ = connected(
            &config,
            &journal,
            &credential,
            &lease,
            &state,
            browser.clone(),
        )
        .await;
        lease.send_replace(Lease::default());
        state.send_replace(LinkState::default());
        if started.elapsed() > Duration::from_secs(30) {
            delay = 1;
        }
        let mut random = [0u8; 1];
        let _ = getrandom::fill(&mut random);
        tokio::time::sleep(Duration::from_millis(
            delay * 1000 + u64::from(random[0]) * 2,
        ))
        .await;
        delay = (delay * 2).min(30);
    }
}
async fn connected(
    config: &Config,
    journal: &Journal,
    credential: &Credential,
    lease: &watch::Sender<Lease>,
    state: &watch::Sender<LinkState>,
    browser: Option<Option<String>>,
) -> Result<()> {
    let started = Instant::now();
    let mut url = config.server.join("v1/devices/connect")?;
    url.set_scheme(if config.server.scheme() == "https" {
        "wss"
    } else {
        "ws"
    })
    .map_err(|_| crate::Error::Rejected("socket scheme".into()))?;
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(|_| crate::Error::Rejected("socket request".into()))?;
    request.headers_mut().insert(
        "authorization",
        format!("Bearer {}", credential.token)
            .parse()
            .map_err(|_| crate::Error::Rejected("credential format".into()))?,
    );
    let ws_config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
        .max_message_size(Some(MAX_FRAME))
        .max_frame_size(Some(MAX_FRAME));
    let (mut socket, _) = tokio::time::timeout(
        Duration::from_secs(10),
        tokio_tungstenite::connect_async_with_config(request, Some(ws_config), false),
    )
    .await
    .map_err(|_| crate::Error::Rejected("connect timeout".into()))?
    .map_err(|_| crate::Error::Rejected("socket connect failed".into()))?;
    wire(
        &mut socket,
        &ClientWire::Hello {
            device: journal.device.clone(),
            cursor: journal.cursor().await?.to_string(),
        },
    )
    .await?;
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    let mut last_heartbeat = Instant::now() - Duration::from_secs(15);
    let mut pending_heartbeat = None;
    let mut epoch: Option<u64> = None;
    loop {
        tokio::select! {
            _=tick.tick()=>{
                ensure(epoch.is_some()||started.elapsed()<Duration::from_secs(10),"welcome timeout")?;
                if let Some(epoch)=epoch {
                    ensure(lease.borrow().live(),"local lease expired")?;
                    if pending_heartbeat.is_none()&&last_heartbeat.elapsed()>=Duration::from_secs(15){last_heartbeat=Instant::now();pending_heartbeat=Some(last_heartbeat);wire(&mut socket,&ClientWire::Heartbeat{epoch:epoch.to_string()}).await?;}
                    for envelope in journal.outgoing().await? {wire(&mut socket,&ClientWire::Data{envelope:Box::new(envelope)}).await?;}
                }
            }
            frame=socket.next()=>{
                let Some(Ok(frame))=frame else{return Err(crate::Error::Rejected("socket closed".into()));};
                match frame {
                    Message::Text(text)=>match parse::<ServerWire>(text.as_bytes())? {
                        ServerWire::Welcome{device,epoch:wire_epoch,lease_ms}=>{
                            ensure(epoch.is_none()&&device==journal.device&&lease_ms>0&&lease_ms<=LEASE_MS as u32,"invalid welcome")?;
                            let value=number(&wire_epoch)?;epoch=Some(value);lease.send_replace(Lease{epoch:value,until:Some(started+Duration::from_millis(lease_ms.into()))});state.send_replace(LinkState{connected:true});
                            if let Some(fixture_origin)=browser.clone(){wire(&mut socket,&ClientWire::BrowserReady{fixture_origin}).await?;}
                        }
                        ServerWire::Renewed{epoch:wire_epoch,lease_ms}=>{
                            ensure(epoch==Some(number(&wire_epoch)?)&&lease_ms>0&&lease_ms<=LEASE_MS as u32&&lease.borrow().live(),"invalid lease renewal")?;
                            let sent=pending_heartbeat.take().ok_or_else(||crate::Error::Rejected("unsolicited lease renewal".into()))?;
                            lease.send_replace(Lease{epoch:epoch.unwrap_or(0),until:Some(sent+Duration::from_millis(lease_ms.into()))});
                        }
                        ServerWire::Data{envelope,permitted}=>{
                            let current=lease.borrow().clone();ensure(epoch.is_some()&&current.live(),"data without live session")?;
                            journal.accept(&envelope,permitted,current.epoch,current.live()&&!journal.stopped().await?).await?;
                            wire(&mut socket,&ClientWire::Ack{sequence:envelope.sequence,id:envelope.id}).await?;
                        }
                        ServerWire::Ack{sequence,id}=>{ensure(epoch.is_some(),"ACK before welcome")?;journal.ack(number(&sequence)?,&id).await?;}
                        ServerWire::Error{..}=>return Err(crate::Error::Rejected("server stopped session".into())),
                    }
                    Message::Ping(data)=>{socket.send(Message::Pong(data)).await.map_err(|_|crate::Error::Rejected("socket ping failed".into()))?;}
                    Message::Pong(_)=>{},_=>return Err(crate::Error::Rejected("socket closed or unsupported frame".into())),
                }
            }
        }
    }
}
async fn wire<S>(
    socket: &mut tokio_tungstenite::WebSocketStream<S>,
    message: &ClientWire,
) -> Result<()>
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    let text = serde_json::to_string(message)?;
    ensure(text.len() <= MAX_FRAME, "outgoing frame limit")?;
    tokio::time::timeout(
        Duration::from_secs(5),
        socket.send(Message::Text(text.into())),
    )
    .await
    .map_err(|_| crate::Error::Rejected("socket send timeout".into()))?
    .map_err(|_| crate::Error::Rejected("socket send failed".into()))
}
async fn work(journal: Journal, executor: Executor, lease: watch::Receiver<Lease>) -> Result<()> {
    loop {
        if let Some(envelope) = journal.take_work().await? {
            let current = lease.borrow().clone();
            let allowed = current.live()
                && number(&envelope.epoch)? == current.epoch
                && !journal.stopped().await?;
            let outcome = if allowed {
                match &envelope.payload {
                    ServerPayload::Read {
                        operation,
                        task_id,
                        revision,
                        ..
                    } => {
                        let read = executor.execute_work(operation.clone());
                        tokio::pin!(read);
                        let mut monitor = tokio::time::interval(Duration::from_millis(100));
                        loop {
                            tokio::select! {
                                outcome = &mut read => break outcome,
                                _ = monitor.tick() => {
                                    let live = lease.borrow().clone();
                                    let view = journal.view().await?;
                                    let obsolete = view.tasks.iter().find(|t| t.id == *task_id)
                                        .is_some_and(|t| t.revision.to_string() != *revision || t.cancel_requested || t.state.terminal());
                                    if !live.live() || live.epoch != current.epoch || journal.stopped().await? || obsolete {
                                        break crate::artifact::ExecutedRead {outcome:ReadOutcome::Unknown { reason: "local read interrupted by stop, revision, cancellation or expired session".into() },image:None};
                                    }
                                }
                            }
                        }
                    }
                    _ => return Err(crate::Error::Rejected("invalid work record".into())),
                }
            } else {
                crate::artifact::ExecutedRead {
                    outcome: ReadOutcome::Unknown {
                        reason: "worker stopped or session lease is no longer valid".into(),
                    },
                    image: None,
                }
            };
            journal
                .finish_work_image(&envelope, outcome.outcome, outcome.image)
                .await?;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
