//! A local connection for one companion program running beside the title, such as a multiplayer client.
//!
//! Started only when `VICTORY_BRIDGE_FILE` names a file. We listen on `127.0.0.1` on a free port and write
//! `{"bridge": 1, "port": …, "token": "…", "pid": …}` to that file; the companion reads it, connects and must
//! send the token first. Only programs that can read the file (the same user) can connect, and only one at a
//! time.
//!
//! The protocol is newline-delimited JSON, one object per line, at most 64 KB each:
//!
//! - request: `{"id": 1, "op": "hello", "args": {"token": "…", "client": "projectvi"}}`
//! - answer: `{"id": 1, "ok": true, "result": …}` or `{"id": 1, "ok": false, "error": {"code": "…", "message": "…"}}`
//! - event (unasked): `{"event": "frame", "frame": 1234}` after the title presents a frame (coalesced: a slow
//!   reader gets the latest frame, not a backlog), and `{"event": "exit", "code": 0}` when the title exits.
//!
//! Built-in operations: `hello` (must be first), `info` and `capabilities`. Everything else is looked up among
//! the game operations registered with [`register`]. None are registered today: reading the player, spawning
//! characters and the like need hooks in the title's code (`victory-game`), and an operation nobody registered
//! answers with error code `unsupported`, so a companion always knows what it can't do yet.

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader, Write};
use std::net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, SyncSender, TrySendError};
use std::sync::{Arc, Mutex, OnceLock, RwLock};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result};
use serde_json::{Value, json};

/// The protocol version in the bridge file and the `hello` answer. Bumped on incompatible changes.
pub const BRIDGE_VERSION: u32 = 1;

/// The longest line (one message) we read, in bytes.
pub const MAX_LINE: usize = 64 * 1024;

/// What `info` answers, and `hello` too.
#[derive(Debug, Clone, Default)]
pub struct Info {
    /// Our version.
    pub emulator_version: String,
    /// The title build's label, or `None` for a build we don't know.
    pub build: Option<String>,
    /// BLAKE3 of the title's executable, hex.
    pub build_hash: String,
}

/// Why a game operation failed. `code` is one of a few fixed strings a companion can act on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OpError {
    /// `"unsupported"`, `"bad_args"`, `"not_ready"` (the title isn't at a point where this works) or `"failed"`.
    pub code: &'static str,
    /// For people.
    pub message: String,
}

impl OpError {
    /// The arguments don't fit the operation.
    pub fn bad_args(message: impl Into<String>) -> Self {
        Self {
            code: "bad_args",
            message: message.into(),
        }
    }

    /// The operation exists but can't run right now (for example, before the world has loaded).
    pub fn not_ready(message: impl Into<String>) -> Self {
        Self {
            code: "not_ready",
            message: message.into(),
        }
    }

    /// The operation ran and didn't work.
    pub fn failed(message: impl Into<String>) -> Self {
        Self {
            code: "failed",
            message: message.into(),
        }
    }
}

/// A game operation: takes the request's `args`, returns its `result`. Called on the bridge's own thread;
/// an operation that must touch the title on its own thread is responsible for handing the work over.
pub type Operation = Box<dyn Fn(&Value) -> Result<Value, OpError> + Send + Sync>;

fn operations() -> &'static RwLock<BTreeMap<String, Operation>> {
    static OPS: OnceLock<RwLock<BTreeMap<String, Operation>>> = OnceLock::new();
    OPS.get_or_init(Default::default)
}

/// Makes a game operation available to companions, for example `"player.read"`. Replaces one of the same name.
/// Names of the built-in operations are refused.
pub fn register(name: &str, operation: Operation) -> bool {
    if matches!(name, "hello" | "info" | "capabilities") {
        return false;
    }
    operations()
        .write()
        .unwrap_or_else(|e| e.into_inner())
        .insert(name.to_owned(), operation);
    true
}

/// The game operations registered so far, sorted.
pub fn capabilities() -> Vec<String> {
    operations()
        .read()
        .unwrap_or_else(|e| e.into_inner())
        .keys()
        .cloned()
        .collect()
}

// The running bridge, if any, for `frame` and `exit` (called from the title's threads).
static RUNNING: OnceLock<Arc<Shared>> = OnceLock::new();

struct Shared {
    token: String,
    info: Info,
    frame: AtomicU64,
    /// Wakes the writer: something to send. Capacity 1, so frames coalesce.
    wake: Mutex<Option<SyncSender<()>>>,
    /// Answers and one-off events waiting to go out, in order.
    outbox: Mutex<Vec<String>>,
    connected: AtomicBool,
    frame_pending: AtomicBool,
}

impl Shared {
    fn push(&self, line: String) {
        self.outbox
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(line);
        self.poke();
    }

    fn poke(&self) {
        if let Some(wake) = self.wake.lock().unwrap_or_else(|e| e.into_inner()).as_ref() {
            match wake.try_send(()) {
                Ok(()) | Err(TrySendError::Full(())) | Err(TrySendError::Disconnected(())) => {}
            }
        }
    }
}

/// A started bridge. Removes its file when dropped.
pub struct Bridge {
    file: PathBuf,
    address: SocketAddr,
}

impl Bridge {
    /// Listens on a free local port and writes the bridge file. Only one bridge per process.
    pub fn start(file: &Path, info: Info) -> Result<Self> {
        let listener =
            TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).context("listening for the bridge")?;
        let address = listener.local_addr()?;
        let shared = Arc::new(Shared {
            token: random_token()?,
            info,
            frame: AtomicU64::new(0),
            wake: Mutex::new(None),
            outbox: Mutex::new(Vec::new()),
            connected: AtomicBool::new(false),
            frame_pending: AtomicBool::new(false),
        });
        RUNNING
            .set(shared.clone())
            .map_err(|_| anyhow::anyhow!("a bridge is already running in this process"))?;

        let contents = json!({
            "bridge": BRIDGE_VERSION,
            "port": address.port(),
            "token": shared.token,
            "pid": std::process::id(),
        });
        write_atomically(file, &serde_json::to_vec_pretty(&contents)?)
            .with_context(|| format!("writing the bridge file {}", file.display()))?;

        thread::Builder::new()
            .name("victory-bridge".into())
            .spawn(move || accept(listener, shared))?;
        tracing::info!(port = address.port(), file = %file.display(), "bridge listening for a companion program");
        Ok(Self {
            file: file.to_owned(),
            address,
        })
    }

    /// Where we listen.
    pub fn address(&self) -> SocketAddr {
        self.address
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.file);
    }
}

/// Call after the title presents frame number `frame`. Cheap and never blocks; does nothing without a bridge
/// or a connected companion.
pub fn frame(frame: u64) {
    if let Some(shared) = RUNNING.get()
        && shared.connected.load(Ordering::Relaxed)
    {
        shared.frame.store(frame, Ordering::Relaxed);
        shared.frame_pending.store(true, Ordering::Release);
        shared.poke();
    }
}

/// Call when the title exits, with its exit code. The companion gets an `exit` event.
pub fn exit(code: i32) {
    if let Some(shared) = RUNNING.get()
        && shared.connected.load(Ordering::Relaxed)
    {
        shared.push(json!({ "event": "exit", "code": code }).to_string());
        // Give the writer a moment to send it before the process ends.
        thread::sleep(Duration::from_millis(100));
    }
}

fn accept(listener: TcpListener, shared: Arc<Shared>) {
    for stream in listener.incoming() {
        let Ok(stream) = stream else { continue };
        // One companion at a time: a second connection is closed straight away.
        if shared.connected.swap(true, Ordering::AcqRel) {
            let _ = refuse(
                stream,
                "Another program is already connected to this bridge.",
            );
            continue;
        }
        let shared = shared.clone();
        let _ = thread::Builder::new()
            .name("victory-bridge-client".into())
            .spawn(move || {
                if let Err(e) = serve(stream, &shared) {
                    tracing::info!("bridge companion disconnected: {e:#}");
                }
                *shared.wake.lock().unwrap_or_else(|e| e.into_inner()) = None;
                shared
                    .outbox
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .clear();
                shared.connected.store(false, Ordering::Release);
            });
    }
}

fn refuse(mut stream: TcpStream, message: &str) -> std::io::Result<()> {
    let line = json!({ "id": null, "ok": false, "error": { "code": "busy", "message": message } });
    writeln!(stream, "{line}")
}

fn serve(stream: TcpStream, shared: &Arc<Shared>) -> Result<()> {
    stream.set_nodelay(true)?;
    let (wake, woken) = mpsc::sync_channel(1);
    *shared.wake.lock().unwrap_or_else(|e| e.into_inner()) = Some(wake);
    let writer = {
        let stream = stream.try_clone()?;
        let shared = shared.clone();
        thread::Builder::new()
            .name("victory-bridge-writer".into())
            .spawn(move || write_loop(stream, &shared, &woken))?
    };

    let mut reader = BufReader::new(stream.try_clone()?);
    let mut greeted = false;
    let mut line = Vec::new();
    loop {
        line.clear();
        let read =
            std::io::Read::take(&mut reader, MAX_LINE as u64 + 1).read_until(b'\n', &mut line)?;
        if read == 0 {
            break;
        }
        if line.len() > MAX_LINE {
            anyhow::bail!("a message was longer than {MAX_LINE} bytes");
        }
        let request: Value = match serde_json::from_slice(&line) {
            Ok(v) => v,
            Err(_) if !greeted => anyhow::bail!("the first message wasn't JSON"),
            Err(e) => {
                shared.push(answer(
                    &Value::Null,
                    Err(OpError::bad_args(format!("Not JSON: {e}"))),
                ));
                continue;
            }
        };
        let id = request.get("id").cloned().unwrap_or(Value::Null);
        let op = request.get("op").and_then(Value::as_str).unwrap_or("");
        let args = request.get("args").cloned().unwrap_or(Value::Null);

        if !greeted {
            let token = args.get("token").and_then(Value::as_str).unwrap_or("");
            if op != "hello" || !same(token.as_bytes(), shared.token.as_bytes()) {
                let refusal = answer(
                    &id,
                    Err(OpError {
                        code: "unauthorized",
                        message: "Send hello with the token from the bridge file first.".into(),
                    }),
                );
                shared.push(refusal);
                thread::sleep(Duration::from_millis(50));
                anyhow::bail!("refused a companion without the right token");
            }
            greeted = true;
            let client = args.get("client").and_then(Value::as_str).unwrap_or("?");
            tracing::info!(client, "bridge companion connected");
            shared.push(answer(&id, Ok(hello_result(&shared.info))));
            continue;
        }
        shared.push(answer(&id, dispatch(op, &args, &shared.info)));
    }
    let _ = stream.shutdown(std::net::Shutdown::Both);
    *shared.wake.lock().unwrap_or_else(|e| e.into_inner()) = None;
    let _ = writer.join();
    Ok(())
}

fn hello_result(info: &Info) -> Value {
    json!({
        "bridge": BRIDGE_VERSION,
        "emulator": "victory",
        "emulatorVersion": info.emulator_version,
        "build": info.build,
        "buildHash": info.build_hash,
        "capabilities": capabilities(),
    })
}

fn dispatch(op: &str, args: &Value, info: &Info) -> Result<Value, OpError> {
    match op {
        "hello" => Err(OpError::bad_args("Already greeted.")),
        "info" => Ok(hello_result(info)),
        "capabilities" => Ok(json!(capabilities())),
        _ => {
            let ops = operations().read().unwrap_or_else(|e| e.into_inner());
            match ops.get(op) {
                Some(operation) => operation(args),
                None => Err(OpError {
                    code: "unsupported",
                    message: format!("VIctory can't do \"{op}\" with this title build yet."),
                }),
            }
        }
    }
}

fn answer(id: &Value, result: Result<Value, OpError>) -> String {
    match result {
        Ok(result) => json!({ "id": id, "ok": true, "result": result }),
        Err(e) => {
            json!({ "id": id, "ok": false, "error": { "code": e.code, "message": e.message } })
        }
    }
    .to_string()
}

fn write_loop(mut stream: TcpStream, shared: &Shared, woken: &Receiver<()>) {
    while woken.recv().is_ok() {
        let lines = std::mem::take(&mut *shared.outbox.lock().unwrap_or_else(|e| e.into_inner()));
        let mut out = String::new();
        for line in lines {
            out.push_str(&line);
            out.push('\n');
        }
        if shared.frame_pending.swap(false, Ordering::AcqRel) {
            out.push_str(
                &json!({ "event": "frame", "frame": shared.frame.load(Ordering::Relaxed) })
                    .to_string(),
            );
            out.push('\n');
        }
        if !out.is_empty() && stream.write_all(out.as_bytes()).is_err() {
            break;
        }
    }
}

/// Compares without stopping at the first difference, so timing doesn't reveal the token.
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

fn write_atomically(file: &Path, bytes: &[u8]) -> std::io::Result<()> {
    if let Some(dir) = file.parent() {
        std::fs::create_dir_all(dir)?;
    }
    let temp = file.with_extension("tmp");
    std::fs::write(&temp, bytes)?;
    std::fs::rename(&temp, file)
}

fn random_token() -> Result<String> {
    let mut bytes = [0u8; 32];
    fill_random(&mut bytes)?;
    Ok(bytes.iter().map(|b| format!("{b:02x}")).collect())
}

#[cfg(windows)]
fn fill_random(bytes: &mut [u8]) -> Result<()> {
    use windows_sys::Win32::Security::Cryptography::{
        BCRYPT_USE_SYSTEM_PREFERRED_RNG, BCryptGenRandom,
    };
    // SAFETY: a valid buffer of the given length; no algorithm handle with the system-preferred flag.
    let status = unsafe {
        BCryptGenRandom(
            std::ptr::null_mut(),
            bytes.as_mut_ptr(),
            bytes.len() as u32,
            BCRYPT_USE_SYSTEM_PREFERRED_RNG,
        )
    };
    anyhow::ensure!(status == 0, "BCryptGenRandom failed ({status:#x})");
    Ok(())
}

#[cfg(not(windows))]
fn fill_random(bytes: &mut [u8]) -> Result<()> {
    use std::io::Read;
    std::fs::File::open("/dev/urandom")?.read_exact(bytes)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Companion {
        reader: BufReader<TcpStream>,
        writer: TcpStream,
    }

    impl Companion {
        fn connect(port: u16) -> Self {
            let stream = TcpStream::connect((Ipv4Addr::LOCALHOST, port)).unwrap();
            stream
                .set_read_timeout(Some(Duration::from_secs(5)))
                .unwrap();
            Self {
                reader: BufReader::new(stream.try_clone().unwrap()),
                writer: stream,
            }
        }

        fn send(&mut self, message: Value) {
            writeln!(self.writer, "{message}").unwrap();
        }

        /// The next line that isn't a frame event.
        fn next(&mut self) -> Value {
            loop {
                let v = self.next_any();
                if v.get("event").and_then(Value::as_str) != Some("frame") {
                    return v;
                }
            }
        }

        fn next_any(&mut self) -> Value {
            let mut line = String::new();
            self.reader.read_line(&mut line).unwrap();
            serde_json::from_str(&line).unwrap_or(Value::Null)
        }
    }

    // One test, because a process has one bridge.
    #[test]
    fn a_companion_connects_with_the_token_and_gets_frames_and_answers() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("bridge.json");
        let info = Info {
            emulator_version: "0.1.0".into(),
            build: None,
            build_hash: "ab".repeat(32),
        };
        let bridge = Bridge::start(&file, info).unwrap();
        assert!(Bridge::start(&dir.path().join("second.json"), Info::default()).is_err());

        let contents: Value = serde_json::from_slice(&std::fs::read(&file).unwrap()).unwrap();
        assert_eq!(contents["bridge"], BRIDGE_VERSION);
        assert_eq!(contents["port"], bridge.address().port());
        let token = contents["token"].as_str().unwrap().to_owned();
        assert_eq!(token.len(), 64);
        let port = bridge.address().port();

        // A wrong token is refused and the connection closed.
        let mut intruder = Companion::connect(port);
        intruder.send(json!({ "id": 1, "op": "hello", "args": { "token": "0".repeat(64) } }));
        let refused = intruder.next();
        assert_eq!(refused["error"]["code"], "unauthorized");
        thread::sleep(Duration::from_millis(200));

        let mut c = Companion::connect(port);
        c.send(json!({ "id": 1, "op": "hello", "args": { "token": token, "client": "test" } }));
        let hello = c.next();
        assert_eq!(hello["id"], 1);
        assert_eq!(hello["ok"], true);
        assert_eq!(hello["result"]["emulator"], "victory");
        assert_eq!(hello["result"]["build"], Value::Null);
        assert_eq!(hello["result"]["capabilities"], json!([]));

        // A second companion is turned away while the first is connected.
        let mut second = Companion::connect(port);
        assert_eq!(second.next()["error"]["code"], "busy");

        // Game operations nobody registered say so.
        c.send(json!({ "id": 2, "op": "player.read" }));
        let unsupported = c.next();
        assert_eq!(unsupported["id"], 2);
        assert_eq!(unsupported["ok"], false);
        assert_eq!(unsupported["error"]["code"], "unsupported");

        // A registered one is listed and runs.
        assert!(!register("hello", Box::new(|_| Ok(Value::Null))));
        assert!(register(
            "test.echo",
            Box::new(|args| args
                .get("n")
                .and_then(Value::as_i64)
                .map(|n| json!(n * 2))
                .ok_or_else(|| OpError::bad_args("n is required")))
        ));
        c.send(json!({ "id": 3, "op": "capabilities" }));
        assert_eq!(c.next()["result"], json!(["test.echo"]));
        c.send(json!({ "id": 4, "op": "test.echo", "args": { "n": 21 } }));
        assert_eq!(c.next()["result"], 42);
        c.send(json!({ "id": 5, "op": "test.echo" }));
        assert_eq!(c.next()["error"]["code"], "bad_args");

        // Frames coalesce: the companion sees the latest, never more than were presented.
        for n in 1..=1000 {
            frame(n);
        }
        let mut last = 0;
        while last < 1000 {
            let event = c.next_any();
            if event["event"] == "frame" {
                let n = event["frame"].as_u64().unwrap();
                assert!(n > last && n <= 1000);
                last = n;
            }
        }

        exit(3);
        assert_eq!(c.next(), json!({ "event": "exit", "code": 3 }));

        drop(bridge);
        assert!(!file.exists());
    }
}
