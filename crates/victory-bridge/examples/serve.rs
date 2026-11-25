//! Opens a bridge without a title, for developing companion programs: frame events at 30 a second and no game
//! operations (each answers `unsupported`, as with a build we can't drive yet).
//!
//! ```text
//! cargo run -p victory-bridge --example serve -- <bridge file> [seconds]
//! ```

use std::path::PathBuf;
use std::time::{Duration, Instant};

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args().skip(1);
    let file = PathBuf::from(
        args.next()
            .ok_or_else(|| anyhow::anyhow!("usage: serve <bridge file> [seconds]"))?,
    );
    let seconds: u64 = args.next().map(|s| s.parse()).transpose()?.unwrap_or(30);
    let info = victory_bridge::Info {
        emulator_version: env!("CARGO_PKG_VERSION").to_owned(),
        build: None,
        build_hash: "0".repeat(64),
    };
    let bridge = victory_bridge::Bridge::start(&file, info)?;
    println!(
        "bridge on 127.0.0.1:{} ({}) for {seconds} s",
        bridge.address().port(),
        file.display()
    );
    let start = Instant::now();
    let mut frame = 0;
    while start.elapsed() < Duration::from_secs(seconds) {
        frame += 1;
        victory_bridge::frame(frame);
        std::thread::sleep(Duration::from_millis(33));
    }
    victory_bridge::exit(0);
    println!("done after {frame} frames");
    Ok(())
}
