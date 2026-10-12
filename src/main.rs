//! vmon — the single Vibemon binary.
//!
//! - `vmon vmm …` runs the per-VM monitor (spawned by the node role).
//! - `vmon serve …` runs the node and/or driver roles.
//! - `vmon admin …` manages teams, users, and API keys in the database.

#[cfg(not(target_os = "windows"))]
mod admin;
#[cfg(not(target_os = "windows"))]
mod serve;

#[cfg(not(target_os = "windows"))]
use clap::{Parser, Subcommand};

fn main() {
	let mut args = std::env::args().collect::<Vec<_>>();
	if args.get(1).is_some_and(|arg| arg == "vmm") {
		args.drain(0..2);
		if let Err(error) = vmm::run_cli(args) {
			eprintln!("vmon: error: {error}");
			std::process::exit(1);
		}
		return;
	}
	#[cfg(target_os = "windows")]
	{
		eprintln!("vmon: error: only 'vmon vmm' is supported on Windows");
		std::process::exit(2);
	}
	#[cfg(not(target_os = "windows"))]
	if let Err(error) = run(Cli::parse()) {
		eprintln!("vmon: error: {error:#}");
		std::process::exit(1);
	}
}

/// Command-line interface of the non-VMM entry points.
#[cfg(not(target_os = "windows"))]
#[derive(Parser)]
#[command(
	name = "vmon",
	version,
	about = "Vibemon microVM platform with an E2B-compatible control plane"
)]
struct Cli {
	#[command(subcommand)]
	command: Command,
}

#[cfg(not(target_os = "windows"))]
#[derive(Subcommand)]
enum Command {
	/// Run the node and/or driver roles.
	Serve(Box<serve::ServeArgs>),
	/// Manage teams, users, and API keys (talks to the database directly).
	Admin(admin::AdminArgs),
}

#[cfg(not(target_os = "windows"))]
fn run(cli: Cli) -> anyhow::Result<()> {
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()?;
	match cli.command {
		Command::Serve(args) => runtime.block_on(serve::run(*args)),
		Command::Admin(args) => runtime.block_on(admin::run(args)),
	}
}
