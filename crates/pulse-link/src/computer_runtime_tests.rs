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
response={'version':'pulse-computer-observe-2','permissions':{'accessibility':True,'screen_capture':True}}
if mode=='version':response['version']='unknown-helper'
if mode=='extra':response['grant']='forged'
if sys.argv[1:]!=['--status']:
 request=json.load(sys.stdin)
 if mode=='selected':assert request.get('expected_process_launched_at')=='1'
 args=dict(zip(sys.argv[1::2],sys.argv[2::2]))
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
 if mode=='binding':observation['binding']['task_id']='other-task'
 if mode=='epoch':observation['binding']['epoch']='9'
 if mode=='target':observation['target']['window_id']+=1
 if mode=='path':observation['image']['path']=str(cache.parent/'other.png')
 if mode=='size':observation['image']['length']+=1
 if mode=='dimensions':observation['image']['width']=2
 if mode=='digest':observation['image']['digest']='0'*64
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

fn fixture(mode: &str) -> (tempfile::TempDir, ComputerRuntime) {
    let dir = tempfile::tempdir().unwrap();
    let helper = dir.path().join(format!("{mode}.py"));
    std::fs::write(&helper, FIXTURE).unwrap();
    std::fs::set_permissions(&helper, std::fs::Permissions::from_mode(0o700)).unwrap();
    let target = ComputerTarget {
        bundle_id: "com.faberline.PulseComputerFixture".into(),
        pid: 42,
        window_id: 7,
    };
    let runtime = ComputerRuntime::new(helper, dir.path().join("private"), Some(target)).unwrap();
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
async fn assert_reaped(runtime: &ComputerRuntime) {
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
