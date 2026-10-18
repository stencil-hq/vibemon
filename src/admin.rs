//! `vmon admin`: operator tooling (teams, users, API keys, limits) that reads
//! and writes the driver store through the cluster KV of a running driver. Uses
//! the cluster secret to authenticate to the cluster listener and to encrypt
//! stored API keys, so it must match the secret of the running drivers.

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand};
use vmon_driver::{
	TeamLimits, admin,
	crypto::Keys,
	store::{Store, Team},
};

/// Flags shared by every `vmon admin` subcommand.
#[derive(Args, Debug)]
pub struct AdminArgs {
	/// Cluster listener URL(s) of running drivers (comma-separated); requests
	/// go to the first that answers.
	#[arg(
		long,
		env = "VMON_CLUSTER_URL",
		default_value = "http://127.0.0.1:7947",
		value_delimiter = ','
	)]
	cluster_url:    Vec<String>,
	/// Cluster secret (default: `<data-dir>/cluster-secret`).
	#[arg(long, env = "VMON_CLUSTER_SECRET", hide_env_values = true)]
	cluster_secret: Option<String>,
	/// State directory holding `cluster-secret` when `--cluster-secret` is
	/// unset.
	#[arg(long, env = "VMON_HOME")]
	data_dir:       Option<PathBuf>,
	#[command(subcommand)]
	command:        AdminCommand,
}

#[derive(Subcommand, Debug)]
enum AdminCommand {
	/// Create a team and print its first API key.
	CreateTeam {
		name:   String,
		/// Make it a system team: its public templates claim the bare aliases
		/// every team resolves (`base`, `code-interpreter-v1`, …).
		#[arg(long)]
		system: bool,
		#[command(flatten)]
		limits: LimitArgs,
	},
	/// List teams.
	ListTeams,
	/// Mint another API key for a team (id or slug).
	CreateApiKey {
		team: String,
		#[arg(long, default_value = "API key")]
		name: String,
	},
	/// Create a CLI login user, optionally in a team (id or slug) as its default
	/// team.
	CreateUser {
		email:    String,
		/// Password (reads `VMON_ADMIN_PASSWORD` when omitted).
		#[arg(long, env = "VMON_ADMIN_PASSWORD", hide_env_values = true)]
		password: String,
		#[arg(long)]
		team:     Option<String>,
	},
	/// Add an existing user to a team.
	AddMember {
		email:   String,
		team:    String,
		/// Make it the user's default team.
		#[arg(long)]
		default: bool,
	},
	/// Change a team's limits (omitted flags keep their current values).
	SetLimits {
		team:   String,
		#[command(flatten)]
		limits: LimitArgs,
	},
	/// Block a team: its credentials may still read, every change is refused
	/// with 403 `team is blocked`.
	Block { team: String },
	/// Unblock a team.
	Unblock { team: String },
	/// Make a team a system team (its public templates claim free bare
	/// aliases).
	SetSystem { team: String },
	/// Make a system team an ordinary team (releases its bare aliases).
	UnsetSystem { team: String },
}

/// Team limits (`--max-*` flags); unset flags keep the base value.
#[derive(Args, Debug)]
struct LimitArgs {
	#[arg(long = "max-concurrent-sandboxes")]
	concurrent_sandboxes: Option<u32>,
	#[arg(long = "max-concurrent-builds")]
	concurrent_builds:    Option<u32>,
	#[arg(long = "max-vcpus")]
	vcpus:                Option<u32>,
	#[arg(long = "max-memory-mb")]
	memory_mb:            Option<u64>,
	#[arg(long = "max-disk-mb")]
	disk_mb:              Option<u64>,
	#[arg(long = "max-length-hours")]
	length_hours:         Option<u32>,
}

impl LimitArgs {
	fn over(&self, base: TeamLimits) -> TeamLimits {
		TeamLimits {
			max_concurrent_sandboxes: self
				.concurrent_sandboxes
				.unwrap_or(base.max_concurrent_sandboxes),
			max_concurrent_builds:    self.concurrent_builds.unwrap_or(base.max_concurrent_builds),
			max_vcpus:                self.vcpus.unwrap_or(base.max_vcpus),
			max_memory_mb:            self.memory_mb.unwrap_or(base.max_memory_mb),
			max_disk_mb:              self.disk_mb.unwrap_or(base.max_disk_mb),
			max_length_hours:         self.length_hours.unwrap_or(base.max_length_hours),
		}
	}
}

/// Runs one `vmon admin` subcommand.
pub async fn run(args: AdminArgs) -> anyhow::Result<()> {
	let secret = cluster_secret(&args)?;
	let kv = vmon_cluster::kv::Kv::connect(args.cluster_url.clone(), secret.as_bytes())
		.context("connecting to the cluster KV")?;
	let store = Store::new(kv);
	let keys = Keys::derive(secret.as_bytes());
	match args.command {
		AdminCommand::CreateTeam { name, system, limits } => {
			let (mut team, key) =
				admin::create_team(&store, &keys, &name, limits.over(TeamLimits::default())).await?;
			if system {
				team = admin::set_team_system(&store, &team.id, true).await?;
			}
			println!("team    {} ({})", team.name, team.id);
			println!("slug    {}", team.slug);
			println!("api key {key}");
		},
		AdminCommand::ListTeams => {
			for team in admin::list_teams(&store).await? {
				print_team(&team);
			}
		},
		AdminCommand::CreateApiKey { team, name } => {
			println!("{}", admin::create_team_api_key(&store, &keys, &team, &name).await?);
		},
		AdminCommand::CreateUser { email, password, team } => {
			let user = admin::create_user(&store, &email, &password, team.as_deref()).await?;
			println!("user {} ({})", user.email, user.id);
		},
		AdminCommand::AddMember { email, team, default } => {
			admin::add_member(&store, &email, &team, default).await?;
		},
		AdminCommand::SetLimits { team, limits } => {
			let current = store
				.team_by_ref(&team)
				.await?
				.with_context(|| format!("team {team} not found"))?;
			print_team(&admin::set_team_limits(&store, &team, limits.over(current.limits)).await?);
		},
		AdminCommand::Block { team } => {
			print_team(&admin::set_team_blocked(&store, &team, true).await?);
		},
		AdminCommand::Unblock { team } => {
			print_team(&admin::set_team_blocked(&store, &team, false).await?);
		},
		AdminCommand::SetSystem { team } => {
			print_team(&admin::set_team_system(&store, &team, true).await?);
		},
		AdminCommand::UnsetSystem { team } => {
			print_team(&admin::set_team_system(&store, &team, false).await?);
		},
	}
	Ok(())
}

fn print_team(team: &Team) {
	let limits = team.limits;
	println!(
		"{}\t{}\t{}\tsandboxes={} builds={} vcpus={} memory_mb={} disk_mb={} hours={}{}{}",
		team.id,
		team.slug,
		team.name,
		limits.max_concurrent_sandboxes,
		limits.max_concurrent_builds,
		limits.max_vcpus,
		limits.max_memory_mb,
		limits.max_disk_mb,
		limits.max_length_hours,
		if team.blocked { "\tBLOCKED" } else { "" },
		if team.system { "\tSYSTEM" } else { "" },
	);
}

fn cluster_secret(args: &AdminArgs) -> anyhow::Result<String> {
	if let Some(secret) = &args.cluster_secret {
		return Ok(secret.clone());
	}
	let data_dir = args
		.data_dir
		.clone()
		.unwrap_or_else(vmon_node::config::default_data_dir);
	let path = data_dir.join("cluster-secret");
	match std::fs::read_to_string(&path) {
		Ok(secret) => Ok(secret.trim().to_owned()),
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
			bail!(
				"no cluster secret: pass --cluster-secret (VMON_CLUSTER_SECRET) or --data-dir of a \
				 server ({} missing)",
				path.display()
			)
		},
		Err(error) => Err(error).with_context(|| format!("reading {}", path.display())),
	}
}
