use std::{
    collections::BTreeMap,
    io::{Read, Write},
    path::PathBuf,
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant},
};
use tower_pool_proto::{self as wire, Frame, Launch, Reply, Request, Session, Size};

struct Pool {
    dir: tempfile::TempDir,
    child: Child,
}
impl Pool {
    fn new() -> Self {
        let dir = tempfile::Builder::new()
            .prefix("tp-")
            .tempdir_in("/tmp")
            .unwrap();
        let child = Command::new(env!("CARGO_BIN_EXE_tower-terminal-pool"))
            .args(["run"])
            .env("HOME", dir.path())
            .env("XDG_STATE_HOME", dir.path())
            .env("TOKIO_WORKER_THREADS", "2")
            .env_remove("TOWER_SOCKET")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .unwrap();
        let p = Self { dir, child };
        wait(|| {
            tower_pal::connect_blocking(&p.socket())
                .is_ok_and(|mut stream| wire::hello_sync(&mut stream).is_ok())
        });
        p
    }
    fn socket(&self) -> PathBuf {
        self.dir.path().join("tower/run/pool.sock")
    }
    fn connect(&self) -> tower_pal::BlockingIpcStream {
        let mut s = tower_pal::connect_blocking(&self.socket()).unwrap();
        wire::hello_sync(&mut s).unwrap();
        s
    }
    fn call(&self, r: Request) -> Reply {
        let mut s = self.connect();
        wire::write_sync(&mut s, &r).unwrap();
        wire::read_sync(&mut s)
            .unwrap()
            .json::<Reply>()
            .unwrap()
            .checked()
            .unwrap()
    }
    fn launch(&self, name: &str, script: &str, headless: bool) {
        assert!(matches!(
            self.call(Request::Create {
                launch: Launch {
                    name: name.into(),
                    argv: vec!["sh".into(), "-c".into(), script.into()],
                    env: BTreeMap::new(),
                    cwd: self.dir.path().to_string_lossy().into_owned(),
                    size: headless.then(Size::default),
                    clipboard: None,
                }
            }),
            Reply::Created { .. }
        ));
    }
    fn list(&self) -> Vec<Session> {
        let Reply::Sessions { sessions } = self.call(Request::List) else {
            panic!()
        };
        sessions
    }
    fn attach(&self, name: &str, size: Size) -> tower_pal::BlockingIpcStream {
        let mut s = self.connect();
        wire::write_sync(
            &mut s,
            &Request::Attach {
                name: name.into(),
                size,
                clipboard: None,
            },
        )
        .unwrap();
        assert!(matches!(
            wire::read_sync(&mut s)
                .unwrap()
                .json::<Reply>()
                .unwrap()
                .checked()
                .unwrap(),
            Reply::Attached { .. }
        ));
        s
    }
    fn snapshot(&self, name: &str) -> (Reply, Vec<u8>) {
        let mut s = self.connect();
        wire::write_sync(&mut s, &Request::Snapshot { name: name.into() }).unwrap();
        let header = wire::read_sync(&mut s)
            .unwrap()
            .json::<Reply>()
            .unwrap()
            .checked()
            .unwrap();
        let mut out = Vec::new();
        loop {
            match wire::read_sync(&mut s).unwrap() {
                Frame::Data(b) => out.extend(b),
                Frame::Json(b) => {
                    assert!(matches!(
                        serde_json::from_slice::<Reply>(&b).unwrap(),
                        Reply::End
                    ));
                    break;
                }
                _ => {}
            }
        }
        (header, out)
    }
}
impl Drop for Pool {
    fn drop(&mut self) {
        if let Ok(mut s) = tower_pal::connect_blocking(&self.socket()) {
            let _ = wire::hello_sync(&mut s);
            let _ = wire::write_sync(&mut s, &Request::Shutdown);
            let _ = wire::read_sync(&mut s);
        }
        let end = Instant::now() + Duration::from_secs(3);
        while self.child.try_wait().ok().flatten().is_none() && Instant::now() < end {
            thread::sleep(Duration::from_millis(10));
        }
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
fn wait(mut f: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !f() {
        assert!(Instant::now() < end, "condition timed out");
        thread::sleep(Duration::from_millis(10));
    }
}
fn send(s: &mut impl Write, bytes: &[u8]) {
    s.write_all(&((bytes.len() + 1) as u32).to_be_bytes())
        .unwrap();
    s.write_all(&[1]).unwrap();
    s.write_all(bytes).unwrap();
}
fn until(s: &mut impl Read, needle: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    while !out.windows(needle.len()).any(|w| w == needle) {
        match wire::read_sync(s).unwrap() {
            Frame::Data(b) => out.extend(b),
            Frame::Json(b) => {
                let r: Reply = serde_json::from_slice(&b).unwrap();
                panic!(
                    "unexpected reply {r:?}; output {}",
                    String::from_utf8_lossy(&out)
                );
            }
            _ => {}
        }
        assert!(out.len() < 2 * 1024 * 1024);
    }
    out
}
#[test]
fn lazy_spawn_two_clients_size_and_disconnect_are_independent() {
    let p = Pool::new();
    p.launch("shared", "stty raw -echo; printf READY; exec cat", false);
    assert_eq!(p.list()[0].state, "reserved");
    assert_eq!(p.list()[0].pid, None);
    let mut a = p.attach(
        "shared",
        Size {
            rows: 30,
            cols: 100,
        },
    );
    until(&mut a, b"READY");
    let pid = p.list()[0].pid.unwrap();
    let mut b = p.attach("shared", Size { rows: 40, cols: 80 });
    until(&mut b, b"READY");
    assert_eq!(p.list()[0].size, Size { rows: 30, cols: 80 });
    send(&mut a, b"FROM-A");
    until(&mut a, b"FROM-A");
    until(&mut b, b"FROM-A");
    send(&mut b, b"FROM-B");
    until(&mut a, b"FROM-B");
    until(&mut b, b"FROM-B");
    wire::write_sync(
        &mut b,
        &Request::Resize {
            size: Size { rows: 0, cols: 0 },
        },
    )
    .unwrap();
    wait(|| p.list()[0].size == Size { rows: 1, cols: 1 });
    drop(b);
    wait(|| p.list()[0].attach_count == 1);
    assert_eq!(
        p.list()[0].size,
        Size {
            rows: 30,
            cols: 100
        }
    );
    send(&mut a, b"STILL-HERE");
    until(&mut a, b"STILL-HERE");
    drop(a);
    wait(|| p.list()[0].attach_count == 0);
    assert_eq!(p.list()[0].pid, Some(pid));
    assert_eq!(
        p.list()[0].size,
        Size {
            rows: 30,
            cols: 100
        }
    );
    p.call(Request::Kill {
        name: "shared".into(),
    });
    assert_eq!(p.list()[0].state, "exited");
}
#[test]
fn control_sequences_and_paste_are_written_atomically_per_client() {
    let p = Pool::new();
    p.launch("input", "stty raw -echo; printf READY; exec cat", true);
    let mut a = p.attach("input", Size::default());
    until(&mut a, b"READY");
    let mut b = p.attach("input", Size::default());
    until(&mut b, b"READY");
    send(&mut a, b"\x1b[");
    send(&mut b, b"B");
    until(&mut b, b"B");
    send(&mut a, b"Aend");
    let result = until(&mut a, b"end");
    assert!(result.windows(7).any(|w| w == b"B\x1b[Aend"));
    send(&mut a, b"\x1b[200~paste");
    send(&mut b, b"OTHER");
    until(&mut b, b"OTHER");
    send(&mut a, b"\x1b[201~DONE");
    let result = until(&mut a, b"DONE");
    assert!(
        result
            .windows(b"OTHER\x1b[200~paste\x1b[201~DONE".len())
            .any(|w| w == b"OTHER\x1b[200~paste\x1b[201~DONE")
    );
    p.call(Request::Kill {
        name: "input".into(),
    });
}
#[test]
fn final_output_and_exit_are_delivered_and_snapshots_exclude_clipboard() {
    let p = Pool::new();
    p.launch(
        "end",
        r"stty raw -echo; printf READY; read x; printf '\033]52;c;SECRET\007FINAL'; exit 7",
        false,
    );
    let mut a = p.attach("end", Size::default());
    until(&mut a, b"READY");
    send(&mut a, b"go\n");
    let mut output = Vec::new();
    loop {
        match wire::read_sync(&mut a).unwrap() {
            Frame::Data(b) => output.extend(b),
            Frame::Json(b) => {
                let Reply::Exited { code, .. } = serde_json::from_slice(&b).unwrap() else {
                    panic!()
                };
                assert_eq!(code, 7);
                break;
            }
            _ => {}
        }
    }
    assert!(output.ends_with(b"FINAL"));
    let (_, snapshot) = p.snapshot("end");
    let text = String::from_utf8(snapshot).unwrap();
    assert!(text.contains("FINAL"));
    assert!(!text.contains("SECRET"));
}
#[test]
fn kill_reaps_child_and_terminates_its_process_group() {
    let p = Pool::new();
    p.launch("group", "sleep 600 & echo $! > grandchild; wait", true);
    wait(|| p.dir.path().join("grandchild").exists());
    let pid: u32 = std::fs::read_to_string(p.dir.path().join("grandchild"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    let identity = tower_pal::process_identity(pid).unwrap();
    p.call(Request::Kill {
        name: "group".into(),
    });
    wait(|| !tower_pal::process_identity(pid).is_ok_and(|i| i == identity));
}
#[test]
fn malformed_frames_and_incompatible_peers_do_not_stop_pool() {
    let p = Pool::new();
    let mut s = tower_pal::connect_blocking(&p.socket()).unwrap();
    wire::write_sync(&mut s, &Request::Hello { version: 999 }).unwrap();
    assert!(
        wire::read_sync(&mut s)
            .unwrap()
            .json::<Reply>()
            .unwrap()
            .checked()
            .is_err()
    );
    let mut s = p.connect();
    s.write_all(&[255, 255, 255, 255]).unwrap();
    let mut b = [0];
    assert!(!s.read(&mut b).is_ok_and(|n| n != 0));
    assert!(p.list().is_empty());
}

#[test]
fn a_stalled_reader_does_not_block_output_or_control_for_other_clients() {
    let p = Pool::new();
    p.launch("output", "stty raw -echo; printf READY; read x; head -c 4194304 /dev/zero | tr '\\000' x; printf FINISHED; read x", true);
    let mut fast = p.attach("output", Size::default());
    until(&mut fast, b"READY");
    let mut slow = p.attach("output", Size::default());
    until(&mut slow, b"READY");
    send(&mut fast, b"go\n");
    let reader = thread::spawn(move || {
        let mut count = 0;
        let mut tail = Vec::new();
        loop {
            if let Frame::Data(bytes) = wire::read_sync(&mut fast).unwrap() {
                count += bytes.len();
                tail.extend(bytes);
                if tail.windows(8).any(|w| w == b"FINISHED") {
                    break;
                }
                if tail.len() > 16 {
                    tail.drain(..tail.len() - 16);
                }
            }
        }
        assert!(count >= 4194304);
        fast
    });
    let fast = reader.join().unwrap();
    wait(|| p.list()[0].attach_count == 1);
    assert_eq!(p.list()[0].state, "running");
    let (header, buffer) = p.snapshot("output");
    assert!(matches!(
        header,
        Reply::Snapshot {
            truncated: true,
            ..
        }
    ));
    assert!(buffer.len() < wire::MAX_SNAPSHOT);
    p.call(Request::Kill {
        name: "output".into(),
    });
    drop((slow, fast));
}

#[test]
fn a_child_that_does_not_read_input_cannot_block_kill_or_other_clients() {
    let p = Pool::new();
    p.launch(
        "blocked",
        "stty raw -echo; printf READY; exec sleep 600",
        true,
    );
    let mut flood = p.attach("blocked", Size::default());
    until(&mut flood, b"READY");
    let writer = thread::spawn(move || {
        let bytes = vec![b'x'; 128 * 1024];
        for _ in 0..64 {
            if flood
                .write_all(&((bytes.len() + 1) as u32).to_be_bytes())
                .is_err()
                || flood.write_all(&[1]).is_err()
                || flood.write_all(&bytes).is_err()
            {
                return;
            }
        }
    });
    let mut other = p.attach("blocked", Size::default());
    until(&mut other, b"READY");
    let start = Instant::now();
    p.call(Request::Kill {
        name: "blocked".into(),
    });
    assert!(start.elapsed() < Duration::from_secs(3));
    writer.join().unwrap();
    assert_eq!(p.list()[0].state, "exited");
}

#[test]
fn pool_binary_runs_and_stops_without_a_daemon() {
    let mut p = Pool::new();
    assert!(!p.dir.path().join("tower/run/towerd.sock").exists());
    let duplicate = Command::new(env!("CARGO_BIN_EXE_tower-terminal-pool"))
        .args(["run"])
        .env("HOME", p.dir.path())
        .env("XDG_STATE_HOME", p.dir.path())
        .env("TOKIO_WORKER_THREADS", "2")
        .env_remove("TOWER_SOCKET")
        .output()
        .unwrap();
    assert!(!duplicate.status.success());
    assert!(p.list().is_empty());
    let stop = Command::new(env!("CARGO_BIN_EXE_tower-terminal-pool"))
        .args(["stop"])
        .env("HOME", p.dir.path())
        .env("XDG_STATE_HOME", p.dir.path())
        .env_remove("TOWER_SOCKET")
        .output()
        .unwrap();
    assert!(
        stop.status.success(),
        "{}",
        String::from_utf8_lossy(&stop.stderr)
    );
    wait(|| p.child.try_wait().unwrap().is_some());
    assert!(p.child.wait().unwrap().success());
    assert!(!p.socket().exists());
    assert!(!p.dir.path().join("tower/run/towerd.sock").exists());
}

#[test]
fn injection_requires_fresh_idle_evidence_and_cannot_replace_user_input() {
    let pool = Pool::new();
    pool.launch(
        "injection",
        r"stty raw -echo; printf '\033[?2004hREADY'; cat",
        true,
    );
    wait(|| String::from_utf8_lossy(&pool.snapshot("injection").1).contains("READY"));
    let state = pool
        .list()
        .into_iter()
        .find(|s| s.name == "injection")
        .unwrap();
    let proof = || tower_pool_proto::InputProof {
        pid: state.pid.unwrap(),
        pid_start_time: state.pid_start_time.unwrap(),
        idle_since: tower_pal::monotonic_ms(),
        checked_at: tower_pal::monotonic_ms(),
    };
    let submit = |proof, text: &str| {
        let mut stream = pool.connect();
        wire::write_sync(
            &mut stream,
            &Request::Inject {
                name: "injection".into(),
                proof,
                text: text.into(),
            },
        )
        .unwrap();
        wire::read_sync(&mut stream)
            .unwrap()
            .json::<Reply>()
            .unwrap()
    };
    let idle = proof();
    assert!(matches!(submit(idle.clone(), "MESSAGE"), Reply::Ok));
    wait(|| String::from_utf8_lossy(&pool.snapshot("injection").1).contains("MESSAGE"));
    assert!(matches!(submit(idle,"AGAIN"),Reply::Error{code,..} if code=="target_busy_pty"));
    let mut stale = proof();
    stale.pid_start_time += 1;
    assert!(matches!(submit(stale,"STALE"),Reply::Error{code,..} if code=="stale_session"));
    thread::sleep(Duration::from_millis(2));
    assert!(
        matches!(submit(proof(),"\x1b[201~unsafe"),Reply::Error{code,..} if code=="unsafe_pty_text")
    );
    let mut client = pool.attach("injection", Size::default());
    send(&mut client, b"draft");
    wait(|| String::from_utf8_lossy(&pool.snapshot("injection").1).contains("draft"));
    assert!(matches!(submit(proof(),"BLOCKED"),Reply::Error{code,..} if code=="user_typing"));
    assert!(!String::from_utf8_lossy(&pool.snapshot("injection").1).contains("BLOCKED"));
}
