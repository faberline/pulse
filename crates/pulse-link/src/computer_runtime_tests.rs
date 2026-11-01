//! Mock process fixtures verify the IPC boundary, not native macOS capture acceptance.
use super::*;
use std::{os::unix::fs::PermissionsExt, time::Instant};

const FIXTURE: &str = r#"#!/usr/bin/python3
import base64,hashlib,json,os,pathlib,sys,time
script=pathlib.Path(__file__)
mode=script.stem
# Apple's /usr/bin/python3 launcher adds SDK variables even after env -i.
assert set(os.environ)<= {'PATH','HOME','LC_CTYPE','CPATH','LIBRARY_PATH','MANPATH','SDKROOT','__CF_USER_TEXT_ENCODING'}
script.with_suffix('.stage').write_text(mode)
script.with_suffix('.pid').write_text(str(os.getpid()))
if mode=='hang' and not script.with_suffix('.allow').exists(): time.sleep(60)
if mode=='oversize': print('x'*65537);sys.exit(0)
if mode=='stderr': sys.stderr.write('x'*4097);sys.exit(0)
if mode=='exit': sys.exit(74)
if mode=='invalid': print('not-json');sys.exit(0)
response={'version':'pulse-computer-observe-4','permissions':{'accessibility':True,'screen_capture':True}}
if mode=='version':response['version']='unknown-helper'
if mode=='extra':response['grant']='forged'
if sys.argv[1:]!=['--status']:
 request=json.load(sys.stdin)
 if request['kind']=='act':
  response['action']={'binding':request['binding'],'source_nonce':request['intent']['observation']['nonce'],'state':'completed'}
  if mode=='action-rejected':
   response['action']['state']='not_executed';response['error']='target_changed';print(json.dumps(response));sys.exit(0)
  counter=script.with_suffix('.effects')
  counter.write_text(str(int(counter.read_text())+1 if counter.exists() else 1))
  if mode=='action-hang':time.sleep(60)
  if mode=='action-unknown':response['action']['state']='unknown';response['error']='accessibility_unavailable';print(json.dumps(response));sys.exit(0)
  if mode=='action-binding':response['action']['binding']['task_id']='other'
  if mode=='action-missing':del response['action']
  if mode=='action-bad-image':response['action']['state']='completed'
 if mode=='selected':assert request.get('expected_process_launched_at')=='1'
 args=dict(zip(sys.argv[1::2],sys.argv[2::2]))
 if request['kind']=='probe':
  response['selection']={'target':{'bundle_id':args['--bundle-id'],'pid':int(args['--pid']),'window_id':int(args['--window-id'])},'process_launched_at':int(request['expected_process_launched_at'])}
  if mode=='probe-other':response['selection']['target']['window_id']+=1
  if mode=='permission':response['permissions']['accessibility']=False
  print(json.dumps(response));script.with_suffix('.done').write_text('probe constructed');sys.exit(0)
 cache=pathlib.Path(args['--cache'])
 png=base64.b64decode('iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mP8/x8AAwMCAO+jRZkAAAAASUVORK5CYII=')
 digest=hashlib.sha256(png).hexdigest()
 path=cache/(digest+'.png')
 if mode=='fifo':os.mkfifo(path,0o600)
 elif mode=='symlink':
  other=cache.parent/'other.png';other.write_bytes(png);path.symlink_to(other)
 elif mode!='missing':
  path.write_bytes(png);path.chmod(0o600)
 now=int(time.time()*1000)
 observation={'binding':request['binding'],'target':{'bundle_id':args['--bundle-id'],'pid':int(args['--pid']),'window_id':int(args['--window-id'])},'process_launched_at':1,'title':'Mock window','frame':{'x':0,'y':0,'width':1,'height':1},'elements':[{'id':'window.0','role':'AXButton','label':'Count','value':None,'can_press':True,'can_set_text':False}],'truncated':False,'nonce':'dd493179-2d63-4f6f-bd52-9b8ca6c2ca38','observed_at':now,'expires_at':now+120000,'image':{'digest':digest,'length':len(png),'width':1,'height':1,'path':str(path)}}
 if mode.startswith('action-'):
  observation['elements'].append({'id':'window.1','role':'AXTextField','label':'Message','value':'before','can_press':False,'can_set_text':True})
  if request['kind']=='act' and request['intent']['effect']['kind']=='set_text':observation['elements'][1]['value']=request['intent']['effect']['text']
 if mode=='binding':observation['binding']['task_id']='other-task'
 if mode=='epoch':observation['binding']['epoch']='9'
 if mode=='target':observation['target']['window_id']+=1
 if mode=='path':observation['image']['path']=str(cache.parent/'other.png')
 if mode=='size':observation['image']['length']+=1
 if mode=='dimensions':observation['image']['width']=2
 if mode in ['digest','action-bad-image']:observation['image']['digest']='0'*64
 if mode=='mode':path.chmod(0o644)
 if mode=='tamper':path.write_bytes(bytes(len(png)))
 if mode=='stale':observation['observed_at']-=100000;observation['expires_at']-=100000
 if mode=='future':observation['observed_at']+=100000;observation['expires_at']+=100000
 if mode=='permission':response['permissions']['accessibility']=False
 if mode=='secure':observation['elements'][0]['role']='AXSecureTextField';observation['elements'][0]['can_press']=False
 if mode!='no-observation':response['observation']=observation
 if mode=='error':response['error']='permissions_required'
print(json.dumps(response))
script.with_suffix('.done').write_text('response constructed')
"#;

pub(crate) fn fixture(mode: &str) -> (tempfile::TempDir, ComputerRuntime) {
    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join(format!("{mode}.py"));
    // Resolve Apple's SDK launcher outside the measured native-child deadlines. The
    // actual Python runtime runs the fixture; production still uses its fixed helper.
    #[cfg(target_os = "macos")]
    let script = {
        static PYTHON: std::sync::OnceLock<String> = std::sync::OnceLock::new();
        let python = PYTHON.get_or_init(|| {
            let output = std::process::Command::new("/usr/bin/xcrun")
                .args(["--find", "python3"])
                .output()
                .unwrap();
            assert!(output.status.success());
            let path = String::from_utf8(output.stdout).unwrap().trim().to_owned();
            assert!(path.starts_with('/') && !path.chars().any(char::is_whitespace));
            path
        });
        FIXTURE.replacen("#!/usr/bin/python3", &format!("#!{python}"), 1)
    };
    #[cfg(not(target_os = "macos"))]
    let script = FIXTURE;
    std::fs::write(&helper, script).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let target = ComputerTarget {
        bundle_id: "com.faberline.PulseComputerFixture".into(),
        pid: 42,
        window_id: 7,
    };
    let runtime = ComputerRuntime::new(helper, dir.path().join("private"), Some(target)).unwrap();
    runtime.set_live(true); // Controlled fixture; not native pairing/lease acceptance.
    (dir, runtime)
}
fn binding() -> ComputerBinding {
    ComputerBinding {
        operation_id: "computer-1".into(),
        task_id: "owned-fixture".into(),
        revision: 1,
        device: uuid::Uuid::new_v4().to_string(),
        epoch: 2,
    }
}
pub(crate) async fn assert_reaped(runtime: &ComputerRuntime) {
    let pid = std::fs::read_to_string(runtime.helper.with_extension("pid"))
        .unwrap()
        .parse::<i32>()
        .unwrap();
    let pid = Pid::from_raw(pid).unwrap();
    for _ in 0..100 {
        if rustix::process::test_kill_process(pid).is_err_and(|e| e == rustix::io::Errno::SRCH) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("owned native fixture process was not reaped");
}

#[tokio::test]
async fn only_explicit_reselection_can_replace_a_live_permit_and_never_downgrade_revision() {
    let (_root, mut runtime) = fixture("good");
    runtime.target = None;
    let selected_at = crate::now() - 10;
    let original = pulse_core::ComputerPermit {
        task_id: "task".into(),
        revision: "1".into(),
        selection: pulse_core::ComputerSelection {
            target: ComputerTarget {
                bundle_id: "com.faberline.PulseComputerFixture".into(),
                pid: 42,
                window_id: 7,
            },
            process_launched_at: 1,
        },
        selected_at,
        expires_at: selected_at + 900_000,
    };
    runtime.permit_local(original.clone()).unwrap();
    let mut renewed = original.clone();
    renewed.selected_at += 1;
    renewed.expires_at += 1;
    assert!(runtime.permit_local(renewed.clone()).is_err());
    runtime.replace_local(renewed.clone()).unwrap();
    runtime.replace_local(renewed.clone()).unwrap();
    let mut changed = renewed.clone();
    changed.selection.target.window_id += 1;
    changed.selected_at += 1;
    changed.expires_at += 1;
    assert!(runtime.replace_local(changed.clone()).is_err());
    changed.revision = "2".into();
    runtime.replace_local(changed.clone()).unwrap();
    assert!(!runtime.permitted("task", "1", &original.selection));
    assert!(runtime.permitted("task", "2", &changed.selection));
    renewed.selected_at += 3;
    renewed.expires_at += 3;
    assert!(runtime.replace_local(renewed).is_err());
    let restarted =
        ComputerRuntime::new(runtime.helper.clone(), runtime.directory.clone(), None).unwrap();
    restarted.set_live(true);
    assert!(!restarted.permitted("task", "2", &changed.selection));
    restarted.replace_local(changed.clone()).unwrap();
    assert!(restarted.permitted("task", "2", &changed.selection));
}

#[tokio::test]
async fn local_selection_ends_on_reply_cancellation_or_revision_without_changing_task_truth() {
    let (root, runtime) = fixture("good");
    let store = pulse_core::Store::open(root.path().join("tasks.db"), "owner")
        .await
        .unwrap();
    let selection = pulse_core::ComputerSelection {
        target: runtime.configured_target().unwrap().clone(),
        process_launched_at: 1,
    };
    let event=store.ingest(pulse_core::Origin::Human,"fixture","selection",serde_json::json!({"commands":[{"kind":"create","id":"task","goal":"Read the owned window","scope":selection.scope().unwrap(),"priority":0,"requires_device":false}]}),crate::now()).await.unwrap();
    store.route(&event.event_id).await.unwrap();
    let task = store.task("task").await.unwrap();
    let selected_at = crate::now();
    let permit = pulse_core::ComputerPermit {
        task_id: "task".into(),
        revision: "1".into(),
        selection: selection.clone(),
        selected_at,
        expires_at: selected_at + 900_000,
    };
    for change in ["reply", "cancel", "revision"] {
        runtime.permit_local(permit.clone()).unwrap();
        runtime.maintain(std::slice::from_ref(&task));
        assert!(runtime.permitted("task", "1", &selection));
        let mut changed = task.clone();
        match change {
            "reply" => changed.reason = Some("model_reply_ready".into()),
            "cancel" => changed.cancel_requested = true,
            _ => changed.revision += 1,
        }
        runtime.maintain(&[changed]);
        assert!(!runtime.permitted("task", "1", &selection));
    }
    assert_eq!(store.task("task").await.unwrap().revision, 1);
    assert!(!runtime.helper.with_extension("stage").exists());
}

#[tokio::test]
async fn fixed_native_bridge_validates_source_png_and_omits_local_paths() {
    let (_dir, runtime) = fixture("good");
    let permissions = runtime.permissions().await.unwrap();
    assert!(permissions.can_observe());
    let evidence = runtime.observe(&binding()).await.unwrap();
    assert_eq!(
        evidence.png.len(),
        evidence.observation.image.length as usize
    );
    assert_eq!(hash_bytes(&evidence.png), evidence.observation.image.digest);
    assert!(
        serde_json::to_value(&evidence.observation).unwrap()["image"]
            .get("path")
            .is_none()
    );
    assert_reaped(&runtime).await;
}

#[tokio::test]
async fn bound_computer_dispatch_checks_selection_before_capture_and_returns_original_image() {
    let (dir, runtime) = fixture("selected");
    let binding = binding();
    let selection = pulse_core::ComputerSelection {
        target: runtime.configured_target().unwrap().clone(),
        process_launched_at: 1,
    };
    let executor = crate::executor::Executor::new(dir.path())
        .unwrap()
        .with_computer(Some(runtime.clone()));
    let selected_at = crate::now();
    runtime
        .permit_local(pulse_core::ComputerPermit {
            task_id: binding.task_id.clone(),
            revision: binding.revision.to_string(),
            selection: selection.clone(),
            selected_at,
            expires_at: selected_at + 900_000,
        })
        .unwrap();
    let work = crate::browser_session::WorkBinding {
        operation_id: binding.operation_id.clone(),
        task_id: binding.task_id.clone(),
        revision: binding.revision.to_string(),
        device: binding.device.clone(),
        epoch: binding.epoch,
    };
    let mut other = selection.clone();
    other.target.window_id += 1;
    let result = executor
        .execute_bound(
            crate::protocol::ReadOp::ComputerInspect { selection: other },
            &work,
        )
        .await;
    assert!(matches!(
        result.outcome,
        crate::protocol::ReadOutcome::Rejected { .. }
    ));
    assert!(!runtime.helper.with_extension("stage").exists());
    let result = executor
        .execute_bound(
            crate::protocol::ReadOp::ComputerInspect {
                selection: selection.clone(),
            },
            &work,
        )
        .await;
    let crate::protocol::ReadOutcome::Completed {
        computer: Some(observation),
        observation: None,
        ..
    } = result.outcome
    else {
        panic!("not completed")
    };
    assert_eq!(observation.selection(), selection);
    let image = result.image.unwrap();
    image.artifact.verify(&image.png).unwrap();
    assert!(image.artifact.matches_computer(&observation));
    let mut reused = selection;
    reused.process_launched_at = 2;
    let result = executor
        .execute_bound(
            crate::protocol::ReadOp::ComputerInspect { selection: reused },
            &work,
        )
        .await;
    assert!(matches!(
        result.outcome,
        crate::protocol::ReadOutcome::Rejected { .. }
    ));
}

#[tokio::test]
async fn forged_native_sources_and_bad_cache_entries_never_publish_evidence() {
    for mode in [
        "binding",
        "epoch",
        "target",
        "path",
        "size",
        "dimensions",
        "digest",
        "mode",
        "tamper",
        "stale",
        "future",
        "permission",
        "secure",
        "no-observation",
        "error",
        "extra",
        "version",
        "missing",
        "symlink",
        "fifo",
    ] {
        let (_dir, runtime) = fixture(mode);
        assert!(runtime.observe(&binding()).await.is_err(), "{mode}");
        assert!(
            runtime.helper.with_extension("done").exists(),
            "fixture failed before supplying {mode} evidence"
        );
        assert_reaped(&runtime).await;
    }
}

#[tokio::test]
async fn native_output_exit_and_timeout_are_bounded_and_reaped() {
    for mode in ["oversize", "stderr", "exit", "invalid", "hang"] {
        let (_dir, mut runtime) = fixture(mode);
        runtime.timeout = Duration::from_millis(1500);
        let start = Instant::now();
        assert!(runtime.permissions().await.is_err(), "{mode}");
        assert!(start.elapsed() < Duration::from_secs(4));
        assert_eq!(
            std::fs::read_to_string(runtime.helper.with_extension("stage")).unwrap(),
            mode
        );
        assert_reaped(&runtime).await;
    }
}

#[tokio::test]
async fn cancelled_native_wait_kills_and_reaps_its_process() {
    let (_dir, runtime) = fixture("hang");
    let worker = runtime.clone();
    let task = tokio::spawn(async move { worker.permissions().await });
    let start = Instant::now();
    while !runtime.helper.with_extension("pid").exists() {
        assert!(start.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_reaped(&runtime).await;
}

#[tokio::test]
async fn concurrent_native_work_stays_bounded_and_cancel_frees_capacity() {
    let (_dir, runtime) = fixture("hang");
    let worker = runtime.clone();
    let task = tokio::spawn(async move { worker.permissions().await });
    let start = Instant::now();
    while !runtime.helper.with_extension("pid").exists() {
        assert!(start.elapsed() < Duration::from_secs(2));
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(matches!(
        runtime.permissions().await,
        Err(crate::Error::Capacity)
    ));
    task.abort();
    assert!(task.await.unwrap_err().is_cancelled());
    assert_reaped(&runtime).await;
    std::fs::write(runtime.helper.with_extension("allow"), []).unwrap();
    assert!(runtime.permissions().await.unwrap().can_observe());
    assert_reaped(&runtime).await;
}

#[tokio::test]
async fn missing_selection_invalid_binding_and_capacity_stop_before_spawn() {
    let (_dir, mut runtime) = fixture("good");
    runtime.target = None;
    assert!(runtime.observe(&binding()).await.is_err());
    assert!(!runtime.helper.with_extension("pid").exists());
    let (_dir, runtime) = fixture("good");
    let mut invalid = binding();
    invalid.revision = 0;
    assert!(runtime.observe(&invalid).await.is_err());
    assert!(!runtime.helper.with_extension("pid").exists());
    private_dir(&runtime.directory.join("artifacts")).unwrap();
    for index in 0..128 {
        std::fs::write(
            runtime.directory.join("artifacts").join(index.to_string()),
            [],
        )
        .unwrap();
    }
    assert!(runtime.observe(&binding()).await.is_err());
    assert!(!runtime.helper.with_extension("pid").exists());
}
