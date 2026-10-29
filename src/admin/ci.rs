//! `vmon admin ci`: the CI admin API (`/driver/v1/ci…` on a driver's cluster
//! listener).

use std::path::PathBuf;

use anyhow::{Context, bail};
use clap::{Args, Subcommand, ValueEnum};
use serde_json::{Value, json};
use vmon_driver::{
	admin::{ClusterRequest, Method, cluster_json, cluster_text},
	ci::api::{Account, App, Job, JobPage, Overview, Repo},
};

/// `vmon admin ci [COMMAND]`.
#[derive(Args, Debug)]
pub struct CiArgs {
	/// Print the driver's JSON reply.
	#[arg(long, global = true)]
	json:    bool,
	/// Without a command: setup problems, 7-day statistics, and the latest
	/// jobs.
	#[command(subcommand)]
	command: Option<CiCommand>,
}

#[derive(Subcommand, Debug)]
enum CiCommand {
	/// List jobs, newest first.
	Jobs {
		/// GitHub account (login).
		#[arg(long)]
		account: Option<String>,
		/// Repository (`owner/name` or `name`).
		#[arg(long)]
		repo:    Option<String>,
		#[arg(long, value_enum, default_value_t = JobFilter::All)]
		state:   JobFilter,
		#[arg(long, default_value_t = 20)]
		limit:   usize,
	},
	/// Show one job and its history.
	Job { job_id: u64 },
	/// Print a completed job's GitHub log.
	Logs { job_id: u64 },
	/// List the repositories the app can see and whether their jobs may run.
	Repos {
		#[arg(long)]
		account: Option<String>,
	},
	/// The GitHub App.
	App {
		#[command(subcommand)]
		command: AppCommand,
	},
	/// How to create and install the GitHub App (dashboard flow).
	Connect,
	/// Run an account's jobs in a team (id or slug).
	Link {
		/// GitHub account (login or installation id).
		account: String,
		#[arg(long)]
		team:    String,
	},
	/// Stop running an account's jobs (they are refused).
	Unlink { account: String },
	/// Show or change an account's public-repository policy.
	Public {
		account: String,
		/// Allow (`on`) or refuse (`off`) public repositories' jobs.
		#[arg(value_enum)]
		state:   Option<OnOff>,
		/// Allowed GitHub users (comma- or space-separated, `@` optional);
		/// both the run's actor and its triggering actor must be listed.
		#[arg(long)]
		users:   Option<String>,
		/// Allow runs by anyone (`--anyone=false` to require `--users`).
		#[arg(long, num_args = 0..=1, default_missing_value = "true")]
		anyone:  Option<bool>,
	},
	/// Re-read the app's installations from GitHub.
	Sync,
}

#[derive(Subcommand, Debug)]
enum AppCommand {
	/// Show the configured app.
	Show,
	/// Configure an existing app (checked against GitHub).
	Set {
		#[arg(long)]
		app_id:              u64,
		/// PEM private key of the app.
		#[arg(long)]
		private_key_file:    PathBuf,
		/// File holding the app's webhook secret.
		#[arg(long)]
		webhook_secret_file: PathBuf,
	},
	/// Remove the app (its jobs are no longer accepted).
	Delete,
	/// Print the webhook secret (for a webhook relay).
	Secret,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum JobFilter {
	All,
	Active,
	Failed,
	Refused,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum OnOff {
	On,
	Off,
}

/// Cluster listener access of one invocation.
struct Driver<'a> {
	urls:   &'a [String],
	secret: &'a str,
}

impl Driver<'_> {
	async fn json<T: serde::de::DeserializeOwned>(
		&self,
		method: Method,
		path: &[&str],
		query: &[(&str, String)],
		body: Option<&Value>,
	) -> anyhow::Result<T> {
		let mut full = vec!["ci"];
		full.extend_from_slice(path);
		let request = ClusterRequest { method, path: &full, query, body };
		cluster_json(self.urls, self.secret, &request).await
	}

	async fn text(
		&self,
		method: Method,
		path: &[&str],
		body: Option<&Value>,
	) -> anyhow::Result<String> {
		let mut full = vec!["ci"];
		full.extend_from_slice(path);
		let request = ClusterRequest { method, path: &full, query: &[], body };
		cluster_text(self.urls, self.secret, &request).await
	}

	async fn get<T: serde::de::DeserializeOwned>(&self, path: &[&str]) -> anyhow::Result<T> {
		self.json(Method::GET, path, &[], None).await
	}

	/// Installation id of `account` (login or id).
	async fn installation(&self, account: &str) -> anyhow::Result<u64> {
		let overview: Overview = self.get(&[]).await?;
		if let Some(found) = overview
			.accounts
			.iter()
			.find(|candidate| candidate.login.eq_ignore_ascii_case(account))
		{
			return Ok(found.installation_id);
		}
		account.parse().ok().with_context(|| {
			let known: Vec<&str> = overview.accounts.iter().map(|a| a.login.as_str()).collect();
			format!(
				"no GitHub account {account} (known: {}; vmon admin ci sync re-reads them)",
				if known.is_empty() {
					"none".to_owned()
				} else {
					known.join(", ")
				}
			)
		})
	}
}

fn print_json<T: serde::Serialize>(value: &T) -> anyhow::Result<()> {
	println!("{}", serde_json::to_string_pretty(value)?);
	Ok(())
}

fn or_dash(value: Option<&str>) -> &str {
	value.unwrap_or("-")
}

fn duration(ms: Option<i64>) -> String {
	match ms {
		None => "-".to_owned(),
		Some(ms) if ms < 60_000 => format!("{:.1}s", ms as f64 / 1000.0),
		Some(ms) => format!("{}m{:02}s", ms / 60_000, (ms % 60_000) / 1000),
	}
}

fn print_jobs(jobs: &[Job]) {
	if jobs.is_empty() {
		println!("no jobs");
		return;
	}
	println!(
		"{:<12} {:<9} {:<12} {:<28} {:<24} {:>8} {:>8}  REASON",
		"JOB", "STATE", "SIZE", "REPO", "JOB NAME", "WAIT", "RUN"
	);
	for job in jobs {
		let state = match (&job.state[..], job.conclusion.as_deref()) {
			("completed", Some(conclusion)) => conclusion.to_owned(),
			(state, _) => state.to_owned(),
		};
		println!(
			"{:<12} {:<9} {:<12} {:<28} {:<24} {:>8} {:>8}  {}",
			job.job_id,
			state,
			or_dash(job.size.as_deref()),
			job.repo,
			job.job,
			duration(job.wait_ms),
			duration(job.run_ms),
			or_dash(job.reason.as_deref()),
		);
	}
}

fn print_app(app: &App) {
	println!("app        {} ({}, id {})", app.name, app.slug, app.app_id);
	println!("owner      {}", app.owner);
	println!("page       {}", app.html_url);
	println!("install    {}", app.install_url);
	println!("webhook    {}", or_dash(app.webhook_url.as_deref()));
}

fn print_account(account: &Account) {
	let team = account
		.team
		.as_ref()
		.map_or_else(|| "(not linked)".to_owned(), |team| format!("team {}", team.slug));
	let public = match (&account.public.enabled, &account.public.anyone) {
		(false, _) => "public repos off".to_owned(),
		(true, true) => "public repos: anyone".to_owned(),
		(true, false) => format!("public repos: {}", account.public.users.join(", ")),
	};
	println!(
		"{:<20} {:<12} id {:<12} {:<20} {}{}",
		account.login,
		account.kind,
		account.installation_id,
		team,
		public,
		if account.suspended { "  SUSPENDED" } else { "" }
	);
}

/// Runs `vmon admin ci …`.
pub async fn run(args: CiArgs, urls: &[String], secret: &str) -> anyhow::Result<()> {
	let driver = Driver { urls, secret };
	let as_json = args.json;
	let Some(command) = args.command else {
		let overview: Overview = driver.get(&[]).await?;
		if as_json {
			return print_json(&overview);
		}
		match &overview.app {
			Some(app) => print_app(app),
			None => println!("app        (none)"),
		}
		println!("dashboard  {}/ci", or_dash(overview.dashboard_url.as_deref()));
		println!();
		if overview.problems.is_empty() {
			println!("no setup problems");
		} else {
			println!("problems:");
			for problem in &overview.problems {
				println!("  - {problem}");
			}
		}
		println!();
		println!("accounts:");
		for account in &overview.accounts {
			print!("  ");
			print_account(account);
		}
		let stats = &overview.stats;
		println!();
		println!(
			"last 7 days: {} jobs, {} succeeded, {} failed, {} cancelled, {} refused; wait p50 {} \
			 p95 {}; {} runner-minutes",
			stats.jobs,
			stats.succeeded,
			stats.failed,
			stats.cancelled,
			stats.refused,
			duration(stats.wait_p50_ms),
			duration(stats.wait_p95_ms),
			stats.run_seconds / 60
		);
		println!();
		print_jobs(&overview.jobs);
		return Ok(());
	};
	match command {
		CiCommand::Jobs { account, repo, state, limit } => {
			let mut query =
				vec![("limit", limit.to_string()), ("state", format!("{state:?}").to_lowercase())];
			query.extend(account.map(|account| ("account", account)));
			query.extend(repo.map(|repo| ("repo", repo)));
			let page: JobPage = driver.json(Method::GET, &["jobs"], &query, None).await?;
			if as_json {
				return print_json(&page);
			}
			print_jobs(&page.jobs);
		},
		CiCommand::Job { job_id } => {
			let job: Job = driver.get(&["jobs", &job_id.to_string()]).await?;
			if as_json {
				return print_json(&job);
			}
			println!("job        {} {} ({})", job.job_id, job.job, job.html_url);
			println!("repo       {} {} ({})", job.repo, or_dash(job.branch.as_deref()), job.sha);
			println!("workflow   {} (run {} attempt {})", job.workflow, job.run_id, job.run_attempt);
			println!("labels     {}", job.labels.join(", "));
			println!(
				"runner     size {} from {} debug {}",
				or_dash(job.size.as_deref()),
				or_dash(job.from.as_deref()),
				job.debug
			);
			println!(
				"state      {} {}",
				job.state,
				or_dash(job.conclusion.as_deref().or(job.reason.as_deref()))
			);
			println!(
				"sandbox    {} on node {} (runner {})",
				or_dash(job.sandbox_id.as_deref()),
				or_dash(job.node_id.as_deref()),
				or_dash(job.runner_name.as_deref())
			);
			println!(
				"times      queued {} started {} completed {} held until {}",
				job.queued_at,
				or_dash(job.started_at.as_deref()),
				or_dash(job.completed_at.as_deref()),
				or_dash(job.held_until.as_deref())
			);
			println!("           wait {} run {}", duration(job.wait_ms), duration(job.run_ms));
			for event in job.events.unwrap_or_default() {
				println!(
					"  {} {:<9} {}{}",
					event.at,
					event.state,
					or_dash(event.message.as_deref()),
					event
						.sandbox_id
						.map(|sandbox| format!(" (sandbox {sandbox})"))
						.unwrap_or_default()
				);
			}
		},
		CiCommand::Logs { job_id } => {
			print!(
				"{}",
				driver
					.text(Method::GET, &["jobs", &job_id.to_string(), "logs"], None)
					.await?
			);
		},
		CiCommand::Repos { account } => {
			let query: Vec<(&str, String)> = account
				.map(|account| ("account", account))
				.into_iter()
				.collect();
			let reply: Value = driver.json(Method::GET, &["repos"], &query, None).await?;
			if as_json {
				return print_json(&reply);
			}
			let repos: Vec<Repo> = serde_json::from_value(reply["repos"].clone())?;
			for repo in repos {
				println!(
					"{:<40} {:<8} {:<8} {}",
					repo.repo,
					if repo.private { "private" } else { "public" },
					if repo.allowed { "allowed" } else { "refused" },
					or_dash(repo.reason.as_deref())
				);
			}
		},
		CiCommand::App { command } => match command {
			AppCommand::Show => {
				let app: App = driver.get(&["app"]).await?;
				if as_json {
					return print_json(&app);
				}
				print_app(&app);
			},
			AppCommand::Set { app_id, private_key_file, webhook_secret_file } => {
				let private_key = std::fs::read_to_string(&private_key_file)
					.with_context(|| format!("reading {}", private_key_file.display()))?;
				let webhook_secret = std::fs::read_to_string(&webhook_secret_file)
					.with_context(|| format!("reading {}", webhook_secret_file.display()))?;
				let body = json!({
					"appId": app_id,
					"privateKey": private_key,
					"webhookSecret": webhook_secret.trim(),
				});
				let app: App = driver.json(Method::PUT, &["app"], &[], Some(&body)).await?;
				if as_json {
					return print_json(&app);
				}
				print_app(&app);
			},
			AppCommand::Delete => {
				driver.text(Method::DELETE, &["app"], None).await?;
				if !as_json {
					println!("GitHub App removed");
				}
			},
			AppCommand::Secret => {
				let reply: Value = driver.get(&["app", "webhook-secret"]).await?;
				if as_json {
					return print_json(&reply);
				}
				println!("{}", reply["webhookSecret"].as_str().unwrap_or_default());
			},
		},
		CiCommand::Connect => {
			let overview: Overview = driver.get(&[]).await?;
			let dashboard = overview.dashboard_url.unwrap_or_default();
			if as_json {
				return print_json(
					&json!({ "dashboardUrl": dashboard, "ciUrl": format!("{dashboard}/ci") }),
				);
			}
			println!("Open {dashboard}/ci (admin token required) and:");
			println!("  1. Create the GitHub App: choose the owner (account or organization) and");
			println!("     the public webhook URL (this driver's /ci/github/webhook or a relay");
			println!("     that forwards there), then confirm on GitHub.");
			println!("  2. Install the app on the accounts whose repositories should use");
			println!("     `runs-on: vmon`; GitHub returns to the dashboard to link each");
			println!(
				"     installation to a vmon team (or: vmon admin ci link <account> --team <team>)."
			);
			println!("  3. Build the `ci` template (templates/build.py ci) and check `vmon admin ci`");
			println!("     reports no setup problems.");
			println!("An existing app: vmon admin ci app set --app-id <id> --private-key-file <pem>");
			println!("                 --webhook-secret-file <file>");
		},
		CiCommand::Link { account, team } => {
			let id = driver.installation(&account).await?.to_string();
			let body = json!({ "team": team });
			let linked: Account = driver
				.json(Method::POST, &["accounts", &id, "link"], &[], Some(&body))
				.await?;
			if as_json {
				return print_json(&linked);
			}
			print_account(&linked);
		},
		CiCommand::Unlink { account } => {
			let id = driver.installation(&account).await?.to_string();
			let unlinked: Account = driver
				.json(Method::DELETE, &["accounts", &id, "link"], &[], None)
				.await?;
			if as_json {
				return print_json(&unlinked);
			}
			print_account(&unlinked);
		},
		CiCommand::Public { account, state, users, anyone } => {
			let overview: Overview = driver.get(&[]).await?;
			let Some(current) = overview.accounts.into_iter().find(|candidate| {
				candidate.login.eq_ignore_ascii_case(&account)
					|| candidate.installation_id.to_string() == account
			}) else {
				bail!("no GitHub account {account} (vmon admin ci sync re-reads them)");
			};
			let account = if state.is_none() && users.is_none() && anyone.is_none() {
				current
			} else {
				let users: Vec<String> = match users {
					Some(users) => vec![users],
					None => current.public.users.clone(),
				};
				let body = json!({
					"enabled": state.map_or(current.public.enabled, |state| matches!(state, OnOff::On)),
					"anyone": anyone.unwrap_or(current.public.anyone),
					"users": users,
				});
				let id = current.installation_id.to_string();
				driver
					.json(Method::PUT, &["accounts", &id, "public"], &[], Some(&body))
					.await?
			};
			if as_json {
				return print_json(&account);
			}
			print_account(&account);
		},
		CiCommand::Sync => {
			let reply: Value = driver
				.json(Method::POST, &["accounts", "sync"], &[], None)
				.await?;
			if as_json {
				return print_json(&reply);
			}
			let accounts: Vec<Account> = serde_json::from_value(reply["accounts"].clone())?;
			if accounts.is_empty() {
				println!("the GitHub App is not installed anywhere");
			}
			for account in &accounts {
				print_account(account);
			}
		},
	}
	Ok(())
}
