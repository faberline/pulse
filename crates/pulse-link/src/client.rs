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

#[cfg(test)]
#[path = "client/permission_tests.rs"]
mod permission_tests;

mod vault;
pub use vault::{NativeVault, Vault};
mod pairing;
#[cfg(test)]
mod pairing_tests;

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
    credential_rejected: bool,
}

pub struct Device {
    control_directory: PathBuf,
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
            Some(
                Executor::new(&config.repo)?
                    .with_browser(crate::browser_runtime::BrowserRuntime::from_environment(
                        &config.data_dir,
                    )?)
                    .with_computer(crate::computer_runtime::ComputerRuntime::from_environment(
                        &config.data_dir,
                    )?),
            )
        } else {
            None
        };
        let (journal, lock) =
            Journal::open_role(&config.data_dir, config.server.as_str(), &config.repo, role)
                .await?;
        let account = hash(&(config.server.as_str(), &journal.device, role))?;
        journal.recover().await?;
        Ok(Self {
            control_directory: crate::computer_control::control_directory(),
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
        let computer_control = if let Some(runtime) =
            self.executor.as_ref().and_then(Executor::computer_runtime)
        {
            Some(
                crate::computer_control::PickerWorker::start(self.config.server.as_str(), runtime)
                    .await?,
            )
        } else {
            None
        };
        let vault = self.vault.clone();
        let account = self.account.clone();
        let (mut credential, pending) = tokio::task::spawn_blocking(move || {
            Ok::<_, crate::Error>((vault.load(&account)?, vault.pending(&account)?))
        })
        .await
        .map_err(|_| crate::Error::Rejected("Keychain worker failed".into()))??;
        if let Some(claim) = &pending {
            ensure(
                claim.valid()
                    && claim.device == self.journal.device
                    && claim.role == self.journal.role,
                "pending pairing identity mismatch",
            )?;
        }
        if let Some(value) = &credential {
            ensure(
                value.device == self.journal.device
                    && value.token.len() == 64
                    && value.role == self.journal.role,
                "Keychain identity mismatch",
            )?;
            if !self.journal.bind_memory_credential(value).await? {
                credential = None;
            }
        }
        // Finish fallible local recovery before creating owned connection tasks.
        let mut exports = crate::text_result_client::Exports::default();
        exports.restore(&self.journal).await?;
        let (lease_tx, lease_rx) = watch::channel(Lease::default());
        let (link_tx, mut link_rx) = watch::channel(LinkState::default());
        let recovering = pending.is_some();
        let mut connection = credential.clone().filter(|_| !recovering).map(|c| {
            tokio::spawn(reconnect(
                self.config.clone(),
                self.journal.clone(),
                c,
                lease_tx.clone(),
                link_tx.clone(),
                self.executor.clone(),
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
        let mut message = if recovering {
            "正在接續已保存的配對。"
        } else {
            "配對後可在對話框提出需求。"
        }
        .to_string();
        let mut pairing: Option<tokio::task::JoinHandle<Result<Credential>>> =
            pending.map(|claim| {
                tokio::spawn(pairing::recover(
                    self.config.clone(),
                    self.journal.clone(),
                    self.vault.clone(),
                    self.account.clone(),
                    claim,
                ))
            });
        let mut artifact_download: Option<
            tokio::task::JoinHandle<Result<crate::artifact::LocalArtifact>>,
        > = None;
        let mut artifact_loading: Option<String> = None;
        let mut artifact_failure: Option<String> = None;
        let mut computer_selection = tokio::task::JoinSet::new();
        let mut computer_selecting: Option<String> = None;
        let mut computer_failure: Option<String> = None;
        let mut input_failure: Option<String> = None;
        let mut input_receipt: Option<InputReceipt> = None;
        let mut memory_sync: Option<tokio::task::JoinHandle<Result<()>>> = None;
        let mut memory_retry = Instant::now();
        let mut window_list: Option<
            tokio::task::JoinHandle<Result<crate::computer_control::WindowInventory>>,
        > = None;
        let mut windows: Option<ComputerWindowList> = None;
        let mut permission_task: Option<
            tokio::task::JoinHandle<Result<crate::computer_runtime::ComputerPermissions>>,
        > = None;
        let mut permissions: Option<ComputerPermissionState> = None;
        let file_slots = Arc::new(tokio::sync::Semaphore::new(1));
        let mut file_read: Option<
            tokio::task::JoinHandle<Result<(crate::text_input::TextInput, String)>>,
        > = None;
        let mut file_request: Option<TextInputRequest> = None;
        let mut file_selected_id: Option<String> = None;
        let result=async {loop {tokio::select! {
            changed=link_rx.changed()=>{
                changed.map_err(|_|crate::Error::Rejected("connection state stopped".into()))?;
                let denied=link_rx.borrow().credential_rejected;
                if denied {
                    input_receipt=None;
                    exports.invalidate();
                    credential=None;
                    lease_tx.send_replace(Lease::default());
                    let mut cleared=states.borrow().clone();
                    cleared.connected=false;cleared.paired=false;cleared.memories.clear();cleared.memory_synced_at=None;
                    cleared.text_input=None;cleared.text_input_request=None;
                    cleared.text_result_export=exports.state();
                    cleared.input_receipt=None;
                    cleared.message="對話身分已失效，正在清除本機記憶。請重新配對。".into();
                    states.send_replace(cleared);
                    if let Some(task)=connection.take(){task.abort();let _=task.await;}
                    if let Some(task)=memory_sync.take(){task.abort();let _=task.await;}
                    if let Some(task)=artifact_download.take(){task.abort();let _=task.await;}
                    artifact_loading=None;
                    if let Some(task)=window_list.take(){task.abort();let _=task.await;}
                    windows=None;
                    if let Some(task)=permission_task.take(){task.abort();let _=task.await;}
                    permissions=None;
                    if let Some(task)=file_read.take(){task.abort();let _=task.await;}
                    file_request=None;
                    file_selected_id=None;
                    computer_selection.abort_all();while computer_selection.join_next().await.is_some(){}
                    computer_selecting=None;
                    self.journal.deny_memory_access().await?;
                    message=if self.journal.role==ClientRole::Chat {"連線身分已失效。本機記憶與待送正文已清除，請重新配對。"} else {"連線身分已失效，請重新配對。"}.into();
                }
            }
            completed=async {match permission_task.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                permission_task=None;
                if let Some(state)=permissions.as_mut() {
                    state.loading=false;
                    match completed {Ok(Ok(value))=>{state.permissions=Some(value);state.failed=false;},_=>{state.permissions=None;state.failed=true;}}
                }
            }
            completed=exports.completed()=>{
                exports.finish(completed,self.journal.text_input_identity().ok());
            }
            completed=async {match file_read.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                file_read=None;
                let result=match completed {
                    Ok(Ok((input,identity)))=>{
                        let selected=input.id.clone();
                        let result=self.journal.store_text_input(input,&identity).await;
                        if result.is_ok(){file_selected_id=Some(selected);}
                        result
                    },
                    _=>Err(crate::Error::Rejected("file selection failed".into())),
                };
                if let Some(state)=file_request.as_mut(){state.loading=false;state.failed=result.is_err();}
                message=if result.is_ok(){"已選入文字檔案。傳送訊息後才會交給模型。"}else{"檔案未選入。請核對格式、大小與配對狀態；原選擇與草稿會保留。"}.into();
            }
            completed=async {match window_list.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                window_list=None;
                if let Some(state)=windows.as_mut() {
                    state.loading=false;
                    match completed {
                        Ok(Ok(value))=>{
                            permissions=Some(ComputerPermissionState {request_id:state.request_id.clone(),loading:false,failed:false,permissions:Some(value.permissions)});
                            match value.windows {Ok(windows)=>{state.windows=windows;state.failed=false;},Err(_)=>{state.windows.clear();state.failed=true;}}
                        },
                        _=>{state.windows.clear();state.failed=true;permissions=Some(ComputerPermissionState {request_id:state.request_id.clone(),loading:false,failed:true,permissions:None});}
                    }
                }
            }
            completed=async{match memory_sync.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                memory_sync=None;
                if matches!(completed,Ok(Err(crate::Error::CredentialRejected|crate::Error::CredentialCleanup(_)))) {
                    link_tx.send_replace(LinkState{connected:false,credential_rejected:true});
                } else if !matches!(completed,Ok(Ok(()))) {memory_retry=Instant::now()+Duration::from_secs(3);message="記憶仍待同步。已保留待送要求，重連後先核對刪除狀態。".into();}
            }
            completed=computer_selection.join_next(), if !computer_selection.is_empty()=>{
                let id=computer_selecting.take();
                match completed {
                    Some(Ok(Ok(())))=>{
                        let prepared=if let Some(id)=&id {self.journal.prepared_computer_selection(id).await?}else{false};
                        if prepared {computer_failure=None;message="視窗範圍已在本機核對。".into();}
                        else {computer_failure=id;message="任務已變更，這份視窗選擇未套用。".into();}
                    }
                    _=>{computer_failure=id;message="訊息已保存，視窗尚未連上。請核對地端連線、所選視窗與輔助使用／螢幕錄製權限。".into();}
                }
            }
            completed=async{match artifact_download.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                artifact_download=None;
                let loaded_id=artifact_loading.take();
                match completed {
                    Ok(Ok(image))=>{match self.journal.save_artifact(&image).await {
                        Ok(())=>message="截圖已核對並保存。".into(),
                        Err(crate::Error::Rejected(_))=>{artifact_failure=loaded_id;message="截圖來源已更新。請重新選取。".into();},
                        Err(error)=>return Err(error),
                    }}
                    _=>{artifact_failure=loaded_id;message="截圖尚未取得。可按截圖重試；不會重新瀏覽。".into();},
                }
            }
            completed=async {match pairing.as_mut(){Some(task)=>task.await,None=>std::future::pending().await}}=>{
                pairing=None;
                match completed {
                    Ok(Ok(value))=>{
                        lease_tx.send_replace(Lease::default());
                        if let Some(task)=connection.take(){task.abort();let _=task.await;}
                        if let Some(task)=memory_sync.take(){task.abort();let _=task.await;}
                        self.journal.activate_memory_pairing(&value).await?;
                        // Old workers must be joined before clearing their rejection signal.
                        link_tx.send_replace(LinkState::default());
                        connection=Some(tokio::spawn(reconnect(self.config.clone(),self.journal.clone(),value.clone(),lease_tx.clone(),link_tx.clone(),self.executor.clone())));
                        credential=Some(value);message="配對完成。密鑰已存入 Keychain。".into();
                    }
                    failure=>{
                        if let Some(old)=credential.as_ref() {
                            if self.journal.bind_memory_credential(old).await? {
                                connection=Some(tokio::spawn(reconnect(self.config.clone(),self.journal.clone(),old.clone(),lease_tx.clone(),link_tx.clone(),self.executor.clone())));
                            } else {credential=None;}
                        }
                        message=if matches!(failure,Ok(Err(crate::Error::PairingRejected))) {"配對已拒絕。請核對配對碼；已有使用中身分時，須先由操作員撤銷。"}else{"配對尚未完成。請檢查網路與 Keychain 權限。已保存的嘗試會在重開後核對，也可用同一配對碼重試。"}.into();
                    }
                }
            }
            _=ticker.tick()=>{
                if self.journal.role==ClientRole::Chat && credential.is_some() && link_rx.borrow().connected {
                    self.journal.offer_desktop_notification(crate::now()).await?;
                }
                let mut state=self.journal.view().await?;state.server=self.config.server.to_string();state.paired=credential.is_some();state.connected=link_rx.borrow().connected;state.message=message.clone();
                if !state.connected || !state.paired {state.desktop_notifications.current=None;}
                state.input_failure=input_failure.clone();
                if self.journal.role==ClientRole::Chat && state.connected && memory_sync.is_none() && Instant::now()>=memory_retry && self.journal.memory_dirty().await?.is_some() && let Some(credential)=credential.clone() {
                    let config=self.config.clone();let journal=self.journal.clone();memory_sync=Some(tokio::spawn(async move{crate::memory_chat::client::sync(&config,&journal,&credential).await}));
                }
                state.artifact_loading=artifact_loading.clone();state.artifact_failure=artifact_failure.clone();
                state.computer_windows=windows.clone();
                state.computer_permissions=permissions.clone();
                state.text_input_request=file_request.clone();
                state.text_result_export=exports.state();
                state.input_receipt=input_receipt.clone();
                if computer_selection.is_empty() && let Some(entry)=self.journal.take_computer_selection(crate::now()).await? {
                    computer_selecting=Some(entry.source_id.clone());let server=self.config.server.to_string();
                    computer_selection.spawn(async move {crate::computer_control::prepare(&server,entry.permit,entry.replace).await});
                }
                state.computer_selecting=computer_selecting.clone();state.computer_failure=computer_failure.clone();
                state.computer_selections=self.journal.computer_selection_statuses().await?;
                states.send_replace(state);
            }
            done=&mut worker=>{done.map_err(|_|crate::Error::Rejected("local worker stopped".into()))??;return Err(crate::Error::Rejected("local worker stopped".into()));}
            command=commands.recv()=>{
                match command {
                    Some(NativeCommand::SetDesktopNotifications{enabled})=>{
                        self.journal.set_desktop_notifications(enabled,crate::now()).await?;
                    }
                    Some(NativeCommand::DesktopNotificationResult{attempt_id,outcome})=>{
                        match self.journal.desktop_notification_result(&attempt_id,outcome).await {
                            Ok(())=>{},
                            Err(crate::Error::Rejected(_))=>message="通知回覆無法核對；提醒仍保存在對話中。".into(),
                            Err(e)=>return Err(e),
                        }
                    }
                    Some(NativeCommand::ExportTextResult{request_id,message_id,path})=>{
                        exports.start(self.journal.clone(),self.config.clone(),credential.clone(),crate::text_result_client::ExportRequest{request_id,message_id,path}).await?;
                    }
                    Some(NativeCommand::CancelTextResultExport{request_id})=>{
                        ensure(self.journal.role==ClientRole::Chat,"export requires Human chat")?;exports.cancel(&request_id);
                    }
                    Some(NativeCommand::SelectTextFile{request_id,path})=>{
                        ensure(self.journal.role==ClientRole::Chat && uuid::Uuid::parse_str(&request_id).is_ok(),"file selection requires Human chat")?;
                        if file_read.is_some(){continue;}
                        let identity=self.journal.text_input_identity();
                        let ticket=file_slots.clone().try_acquire_owned();
                        file_selected_id=None;
                        file_request=Some(TextInputRequest{request_id,loading:false,failed:true,cancelled:false});
                        if let (Ok(identity),Ok(ticket))=(identity,ticket) {
                            if let Some(state)=file_request.as_mut(){state.loading=true;state.failed=false;}
                            file_read=Some(tokio::spawn(async move {
                                let input=tokio::task::spawn_blocking(move ||{let _ticket=ticket;crate::text_input::TextInput::read(std::path::Path::new(&path))}).await
                                    .map_err(|_|crate::Error::Rejected("file reader stopped".into()))??;
                                Ok((input,identity))
                            }));
                        } else {message="請先完成配對，或等待上一份檔案讀取結束。".into();}
                    }
                    Some(NativeCommand::CancelTextFileSelection{request_id})=>{
                        ensure(self.journal.role==ClientRole::Chat,"file selection requires chat")?;
                        if file_request.as_ref().is_some_and(|s|s.request_id==request_id){
                            if let Some(task)=file_read.take(){task.abort();let _=task.await;}
                            if let Some(id)=file_selected_id.take(){self.journal.clear_text_input(Some(&id)).await?;}
                            if let Some(state)=file_request.as_mut(){state.loading=false;state.failed=false;state.cancelled=true;}
                        }
                    }
                    Some(NativeCommand::ClearTextFile{input_id})=>{
                        ensure(self.journal.role==ClientRole::Chat,"file selection requires chat")?;
                        self.journal.clear_text_input(Some(&input_id)).await?;
                    }
                    Some(NativeCommand::SubmitTextFile{source_id,id,goal,input_id})=>{
                        let receipt_id=source_id.clone();
                        let result=self.journal.submit_text_input(&source_id,id,goal,&input_id).await;
                        match result {
                            Ok(())=>{input_failure=None;message="訊息與選定的檔案副本已存入本機待送佇列。".into();},
                            Err(crate::Error::Rejected(_)|crate::Error::CredentialRejected|crate::Error::Capacity)=>{
                                input_failure=Some(source_id);message="檔案訊息未保存。請核對配對、檔案、訊息大小及待送數量；草稿已保留。".into();
                            },Err(e)=>return Err(e),
                        }
                        input_receipt=Some(InputReceipt{source_id:receipt_id,saved:input_failure.is_none()});
                    }
                    Some(NativeCommand::ComputerPermissions{request_id,request})=>{
                        ensure(self.journal.role==ClientRole::Chat && uuid::Uuid::parse_str(&request_id).is_ok(),"permission control requires Human chat")?;
                        if permissions.as_ref().is_some_and(|s|s.request_id==request_id) {continue;}
                        // Never replace or retry an in-flight OS prompt automatically.
                        if permission_task.is_some() || window_list.is_some(){continue;}
                        permissions=Some(ComputerPermissionState {request_id:request_id.clone(),loading:true,failed:false,permissions:None});
                        let server=self.config.server.to_string();let directory=self.control_directory.clone();
                        permission_task=Some(tokio::spawn(async move {crate::computer_control::permissions(&directory,&server,&request_id,request).await}));
                    }
                    Some(NativeCommand::CancelComputerPermissions{request_id})=>{
                        ensure(self.journal.role==ClientRole::Chat,"permission control requires Human chat")?;
                        if permissions.as_ref().is_some_and(|s|s.request_id==request_id) {
                            if let Some(task)=permission_task.take(){task.abort();let _=task.await;}
                            permissions=None;
                        }
                    }
                    Some(NativeCommand::CancelComputerWindows{request_id})=>{
                        ensure(self.journal.role==ClientRole::Chat,"window picker requires Human chat")?;
                        if windows.as_ref().is_some_and(|s|s.request_id==request_id) {
                            if let Some(task)=window_list.take(){task.abort();let _=task.await;}
                            windows=None;
                        }
                    }
                    Some(NativeCommand::ListComputerWindows{request_id})=>{
                        ensure(self.journal.role==ClientRole::Chat && uuid::Uuid::parse_str(&request_id).is_ok(),"window picker requires Human chat")?;
                        if windows.as_ref().is_some_and(|s|s.request_id==request_id) {continue;}
                        if let Some(task)=window_list.take(){task.abort();let _=task.await;}
                        if permission_task.is_some(){continue;}
                        windows=Some(ComputerWindowList {request_id:request_id.clone(),loading:true,failed:false,windows:vec![]});
                        let server=self.config.server.to_string();let directory=self.control_directory.clone();
                        window_list=Some(tokio::spawn(async move {crate::computer_control::inventory(&directory,&server,&request_id).await}));
                    }
                    Some(NativeCommand::SubmitComputer{source_id,id,goal,selection,selected_at})=>{
                        if crate::memory_chat::parse(&goal).is_some() {
                            match self.journal.action(&source_id,HumanAction::Submit{id,goal}).await {
                                Ok(())=>{input_failure=None;message="記憶要求已保存，等待同步。".into();},
                                Err(crate::Error::Rejected(_)|crate::Error::Capacity)=>{input_failure=Some(source_id);message="請先查詢目前記憶，核對名稱與內容長度；草稿已保留。".into();},
                                Err(e)=>return Err(e),
                            }
                            continue;
                        }
                        match self.journal.queue_computer_selection(&source_id,HumanAction::SubmitComputer{id,goal,selection},selected_at).await {
                            Ok(())=>{computer_failure=None;message="訊息已存入本機，視窗連上後會繼續。".into();},
                            Err(crate::Error::Rejected(_)|crate::Error::Capacity)=>{computer_failure=Some(source_id);message="要求未保存。請核對視窗與佇列；草稿已保留。".into();},
                            Err(error)=>return Err(error),
                        }
                    }
                    Some(NativeCommand::RebindComputer{source_id,task_id,revision,selection,selected_at})=>{
                        match self.journal.queue_computer_selection(&source_id,HumanAction::RebindComputer{task_id,revision,selection},selected_at).await {
                            Ok(())=>{computer_failure=None;message="視窗選擇已保存，等待本機核對。".into();},
                            Err(crate::Error::Rejected(_)|crate::Error::Capacity)=>{computer_failure=Some(source_id);message="視窗尚未更新。請核對目前任務與修訂；未知操作不會重做。".into();},
                            Err(error)=>return Err(error),
                        }
                    }
                    Some(NativeCommand::LoadArtifact{operation_id,digest})=>{
                        if artifact_download.is_some(){continue;}
                        if let Some(credential)=credential.clone() {
                            match self.journal.artifact_request(&operation_id,&digest).await {
                                Ok(artifact)=>{artifact_loading=Some(operation_id.clone());artifact_failure=None;message="正在載入截圖…".into();let config=self.config.clone();artifact_download=Some(tokio::spawn(async move{crate::artifact_client::download(&config,&credential,&operation_id,&artifact).await}));}
                                Err(_)=>{artifact_failure=Some(operation_id);message="截圖要求已拒絕。請核對來源。".into();},
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
                        let receipt_id=source_id.clone();
                        let memory_control=matches!(&action,HumanAction::Submit{goal,..} if crate::memory_chat::parse(goal).is_some());
                        if matches!(&action,HumanAction::Submit{..}) {computer_failure=None;}
                        match self.journal.action(&source_id,action).await {Ok(())=>{input_failure=None;message="已存入本機待送佇列。".into();},Err(crate::Error::CredentialRejected)=>{input_failure=Some(source_id);message="記憶要求未保存，草稿已保留。請重新配對。".into();},Err(crate::Error::Rejected(_)|crate::Error::Capacity)=>{input_failure=Some(source_id);message=if memory_control {"記憶要求未保存。請先查詢目前來源，並核對名稱及內容長度；草稿已保留。"}else{"要求未保存：內容或待送數量已超過限制；草稿已保留。"}.into();},Err(e)=>return Err(e)}
                        input_receipt=Some(InputReceipt{source_id:receipt_id,saved:input_failure.is_none()});
                    }
                    Some(NativeCommand::Pair{code})=>{
                        input_receipt=None;
                        exports.invalidate();
                        if pairing.is_some(){message="配對仍在進行。".into();continue;}
                        lease_tx.send_replace(Lease::default());
                        if let Some(task)=connection.take(){task.abort();let _=task.await;}
                        if let Some(task)=memory_sync.take(){task.abort();let _=task.await;}
                        if let Some(task)=permission_task.take(){task.abort();let _=task.await;}
                        permissions=None;
                        if let Some(task)=file_read.take(){task.abort();let _=task.await;}
                        file_request=None;
                        file_selected_id=None;
                        if self.journal.role==ClientRole::Chat{self.journal.clear_text_input(None).await?;}
                        link_tx.send_replace(LinkState::default());
                        pairing=Some(tokio::spawn(pairing::begin(self.config.clone(),self.journal.clone(),self.vault.clone(),self.account.clone(),code)));
                        message="正在配對。本機停止工具仍可用。".into();
                    }
                }
            }
        }}Ok(())}.await;
        lease_tx.send_replace(Lease::default());
        computer_selection.abort_all();
        while computer_selection.join_next().await.is_some() {}
        if let Some(task) = computer_control {
            task.stop().await;
        }
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
        if let Some(task) = memory_sync {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = window_list {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = permission_task {
            task.abort();
            let _ = task.await;
        }
        if let Some(task) = file_read {
            task.abort();
            let _ = task.await;
        }
        let _ = worker.await;
        result
    }
}
async fn reconnect(
    config: Config,
    journal: Journal,
    credential: Credential,
    lease: watch::Sender<Lease>,
    state: watch::Sender<LinkState>,
    executor: Option<Executor>,
) {
    let mut delay = 1;
    loop {
        let started = Instant::now();
        let result = connected(
            &config,
            &journal,
            &credential,
            &lease,
            &state,
            executor.clone(),
        )
        .await;
        lease.send_replace(Lease::default());
        if matches!(result, Err(crate::Error::CredentialRejected)) {
            state.send_replace(LinkState {
                connected: false,
                credential_rejected: true,
            });
            return;
        }
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
    executor: Option<Executor>,
) -> Result<()> {
    let browser = executor.as_ref().and_then(Executor::browser_ready);
    let interactions = executor
        .as_ref()
        .is_some_and(Executor::browser_interactions);
    let mut computer_checks = tokio::task::JoinSet::new();
    let mut computer_snapshot = None;
    let mut computer_checked = Instant::now() - Duration::from_secs(5);
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
    .map_err(|error| match error {
        tokio_tungstenite::tungstenite::Error::Http(response)
            if matches!(response.status().as_u16(), 401 | 403)
                && response
                    .headers()
                    .get("x-pulse-credential-denied")
                    .is_some_and(|v| v == "1") =>
        {
            crate::Error::CredentialRejected
        }
        _ => crate::Error::Rejected("socket connect failed".into()),
    })?;
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
    let mut projection_token: Option<String> = None;
    loop {
        tokio::select! {
            _=tick.tick()=>{
                ensure(epoch.is_some()||started.elapsed()<Duration::from_secs(10),"welcome timeout")?;
                if let Some(epoch)=epoch {
                    ensure(lease.borrow().live(),"local lease expired")?;
                    let token=journal.projection_token().await?;
                    if token!=projection_token {
                        if token.is_some(){wire(&mut socket,&ClientWire::ProjectionErasureReady).await?;}
                        projection_token=token;
                    }
                    if pending_heartbeat.is_none()&&last_heartbeat.elapsed()>=Duration::from_secs(15){last_heartbeat=Instant::now();pending_heartbeat=Some(last_heartbeat);wire(&mut socket,&ClientWire::Heartbeat{epoch:epoch.to_string()}).await?;}
                    if computer_checked.elapsed() >= Duration::from_secs(5) && !journal.stopped().await? && let Some(executor) = executor.as_ref().filter(|e| e.computer_runtime().is_some()) {
                        computer_checked = Instant::now();
                        if computer_checks.is_empty() { let executor=executor.clone(); computer_checks.spawn(async move { executor.computer_capabilities().await }); }
                    }
                    for item in journal.deliveries().await? {match item {
                        crate::worker_erasure::DeviceDelivery::Data(envelope)=>wire(&mut socket,&ClientWire::Data{envelope}).await?,
                        crate::worker_erasure::DeviceDelivery::Erased(item)=>wire(&mut socket,&ClientWire::ErasedData{header:item.header,operation_id:item.operation_id,source_id:item.source_id}).await?,
                    }}
                }
            }
            result=computer_checks.join_next(), if !computer_checks.is_empty()=>{
                if epoch.is_some() && lease.borrow().live() && !journal.stopped().await? {
                    let permits=match result {Some(Ok(permits))=>permits,_=>vec![]};
                    if !permits.is_empty() || computer_snapshot.as_ref() != Some(&permits) { wire(&mut socket,&ClientWire::ComputerReady{permits:permits.clone(),interactions:true}).await?; }
                    computer_snapshot=Some(permits);
                }
            }
            frame=socket.next()=>{
                let Some(Ok(frame))=frame else{return Err(crate::Error::Rejected("socket closed".into()));};
                match frame {
                    Message::Text(text)=>match parse::<ServerWire>(text.as_bytes())? {
                        ServerWire::Welcome{device,epoch:wire_epoch,lease_ms}=>{
                            ensure(epoch.is_none()&&device==journal.device&&lease_ms>0&&lease_ms<=LEASE_MS as u32,"invalid welcome")?;
                            let value=number(&wire_epoch)?;epoch=Some(value);lease.send_replace(Lease{epoch:value,until:Some(started+Duration::from_millis(lease_ms.into()))});state.send_replace(LinkState{connected:true,credential_rejected:false});
                            if journal.role==ClientRole::Chat {wire(&mut socket,&ClientWire::ConversationReady{version:1}).await?;wire(&mut socket,&ClientWire::ScheduleReady).await?;}
                            else {wire(&mut socket,&ClientWire::WorkerErasureReady).await?;}
                            if let Some(fixture_origin)=browser.clone(){wire(&mut socket,&ClientWire::BrowserReady{fixture_origin}).await?;}
                            if interactions && let Some(fixture_origin)=browser.clone(){wire(&mut socket,&ClientWire::BrowserInteractionReady{fixture_origin}).await?;}
                        }
                        ServerWire::Renewed{epoch:wire_epoch,lease_ms}=>{
                            ensure(epoch==Some(number(&wire_epoch)?)&&lease_ms>0&&lease_ms<=LEASE_MS as u32&&lease.borrow().live(),"invalid lease renewal")?;
                            let sent=pending_heartbeat.take().ok_or_else(||crate::Error::Rejected("unsolicited lease renewal".into()))?;
                            lease.send_replace(Lease{epoch:epoch.unwrap_or(0),until:Some(sent+Duration::from_millis(lease_ms.into()))});
                        }
                        ServerWire::Data{envelope,permitted}=>{
                            let current=lease.borrow().clone();ensure(epoch.is_some()&&current.live(),"data without live session")?;
                            journal.accept(&envelope,permitted,current.epoch,current.live()&&!journal.stopped().await?).await?;
                            if let ServerPayload::EraseWork{context}=&envelope.payload && let Some(executor)=&executor {executor.invalidate_source(context).await?;}
                            wire(&mut socket,&ClientWire::Ack{sequence:envelope.sequence,id:envelope.id}).await?;
                        }
                        ServerWire::ErasedWork{header,context}=>{
                            ensure(epoch.is_some() && lease.borrow().live(),"work erasure without live session")?;
                            journal.accept_worker_erasure(&header,&context).await?;
                            if let Some(executor)=&executor {executor.invalidate_source(&context).await?;}
                            wire(&mut socket,&ClientWire::Ack{sequence:header.sequence,id:header.id}).await?;
                        }
                        ServerWire::ErasedProjection{header}=>{
                            ensure(epoch.is_some()&&lease.borrow().live(),"erasure without live session")?;
                            journal.accept_erased_projection(&header).await?;
                            wire(&mut socket,&ClientWire::Ack{sequence:header.sequence,id:header.id}).await?;
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
        let active = lease.borrow().clone();
        let view = journal.view().await?;
        executor
            .maintain_browser(
                (active.live() && !journal.stopped().await?).then_some(active.epoch),
                &view.tasks,
            )
            .await?;
        if let Some(envelope) = journal.take_work().await? {
            let current = lease.borrow().clone();
            let allowed = current.live()
                && number(&envelope.epoch)? == current.epoch
                && !journal.stopped().await?
                && match &envelope.payload {
                    ServerPayload::Read { operation_id, .. } => {
                        !journal.work_erased(operation_id).await?
                    }
                    _ => false,
                };
            let outcome = if allowed {
                match &envelope.payload {
                    ServerPayload::Read {
                        operation,
                        task_id,
                        revision,
                        ..
                    } => {
                        let binding = crate::browser_session::WorkBinding {
                            operation_id: match &envelope.payload {
                                ServerPayload::Read { operation_id, .. } => operation_id.clone(),
                                _ => unreachable!(),
                            },
                            task_id: task_id.clone(),
                            revision: revision.clone(),
                            device: journal.device.clone(),
                            epoch: current.epoch,
                        };
                        let read = executor.execute_bound(operation.clone(), &binding);
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
                                    if !live.live() || live.epoch != current.epoch || journal.stopped().await? || journal.work_erased(&binding.operation_id).await? || obsolete || !executor.computer_permitted(operation,task_id,revision) {
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
#[cfg(test)]
pub(crate) fn test_live_worker(
    journal: Journal,
    executor: Executor,
    epoch: u64,
) -> tokio::task::JoinHandle<Result<()>> {
    let (_tx, rx) = watch::channel(Lease {
        epoch,
        until: Some(Instant::now() + Duration::from_secs(60)),
    });
    tokio::spawn(work(journal, executor, rx))
}
