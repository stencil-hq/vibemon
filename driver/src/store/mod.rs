//! `MySQL` system of record (ARCHITECTURE.md §7).
//!
//! One [`Store`] over a [`MySqlPool`]; methods are grouped by table in the
//! child modules (each owner adds an `impl Store` block in its own file).
//! Queries are runtime-checked (`sqlx::query*` + [`sqlx::FromRow`]) so the
//! crate builds without a live database.

pub mod builds;
pub mod events;
pub mod keys;
pub mod sandboxes;
pub mod secrets;
pub mod teams;
pub mod templates;
pub mod tokens;
pub mod users;
pub mod volumes;
pub mod webhooks;

use std::{borrow::Cow, str::FromStr, time::Duration};

use anyhow::Context;
use base64::Engine;
use sqlx::{
	MySqlPool, SqlSafeStr,
	migrate::{Migration, MigrationType, Migrator},
	mysql::{MySqlConnectOptions, MySqlPoolOptions},
};

pub use self::{
	events::{EventQuery, NewSandboxEvent, SandboxEventRow, event_types},
	keys::{ApiKeyRow, NewApiKey},
	secrets::SecretRow,
	teams::Team,
	tokens::{RefreshTokenRow, Rotation},
	users::User,
	volumes::VolumeRow,
	webhooks::{DeliveryFilter, DeliveryRow, NewDelivery, StatsBucket, WebhookRow, WebhookWrite},
};

/// Maximum pooled connections per driver process.
const MAX_CONNECTIONS: u32 = 32;

/// Durable driver state in `MySQL`. Cheap to clone (the pool is an `Arc`).
#[derive(Clone, Debug)]
pub struct Store {
	pool: MySqlPool,
}

impl Store {
	/// Connects to `url` (`mysql://user:pass@host:port/db`) with `utf8mb4`
	/// and a UTC session time zone. Called once by [`crate::Driver::start`].
	pub async fn connect(url: &str) -> anyhow::Result<Self> {
		let options = MySqlConnectOptions::from_str(url)
			.with_context(|| "invalid MySQL URL")?
			.charset("utf8mb4")
			.collation("utf8mb4_0900_ai_ci")
			.timezone(Some(String::from("+00:00")));
		let pool = MySqlPoolOptions::new()
			.max_connections(MAX_CONNECTIONS)
			.min_connections(1)
			.acquire_timeout(Duration::from_secs(10))
			.idle_timeout(Duration::from_mins(10))
			.connect_with(options)
			.await
			.with_context(|| "connecting to MySQL")?;
		Ok(Self { pool })
	}

	/// Wraps an existing pool (tests, admin tooling).
	pub const fn from_pool(pool: MySqlPool) -> Self {
		Self { pool }
	}

	/// Applies the embedded migrations in `driver/migrations/`; idempotent.
	pub async fn migrate(&self) -> anyhow::Result<()> {
		migrator()
			.run(&self.pool)
			.await
			.with_context(|| "applying MySQL migrations")?;
		Ok(())
	}

	/// The underlying pool, for `impl Store` blocks in sibling modules.
	pub const fn pool(&self) -> &MySqlPool {
		&self.pool
	}
}

/// Migrations embedded at build time, in version order. (`sqlx::migrate!`
/// is unavailable: the workspace builds sqlx without `macros`.) Add new
/// files here when adding `driver/migrations/NNNN_*.sql`.
const MIGRATIONS: &[(i64, &str, &str)] = &[
	(1, "core", include_str!("../../migrations/0001_core.sql")),
	(2, "templates", include_str!("../../migrations/0002_templates.sql")),
	(3, "sandboxes", include_str!("../../migrations/0003_sandboxes.sql")),
	(4, "system_teams", include_str!("../../migrations/0004_system_teams.sql")),
	(5, "s3_mounts", include_str!("../../migrations/0005_s3_mounts.sql")),
];

fn migrator() -> Migrator {
	Migrator::with_migrations(
		MIGRATIONS
			.iter()
			.map(|(version, description, sql)| {
				Migration::new(
					*version,
					Cow::Borrowed(*description),
					MigrationType::Simple,
					sql.into_sql_str(),
					false,
				)
			})
			.collect(),
	)
}

/// Opaque keyset cursor shared by every paginated list (`nextToken` /
/// `X-Next-Token`): URL-safe base64 of `"<created_at micros>__<id>"`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cursor {
	/// Sort timestamp of the last returned row (µs since the epoch).
	pub created_at_us: i64,
	/// Id of the last returned row (tie-breaker).
	pub id:            String,
}

impl Cursor {
	/// Renders the token sent in `X-Next-Token`.
	pub fn encode(&self) -> String {
		base64::engine::general_purpose::URL_SAFE_NO_PAD
			.encode(format!("{}__{}", self.created_at_us, self.id))
	}

	/// Parses a client-supplied `nextToken`; `None` when malformed.
	pub fn decode(token: &str) -> Option<Self> {
		let raw = base64::engine::general_purpose::URL_SAFE_NO_PAD
			.decode(token.trim_end_matches('='))
			.ok()?;
		let text = String::from_utf8(raw).ok()?;
		let (ts, id) = text.split_once("__")?;
		if id.is_empty() {
			return None;
		}
		Some(Self { created_at_us: ts.parse().ok()?, id: id.to_owned() })
	}
}

/// Converts a `DATETIME(3)` value to the µs used by [`Cursor`].
pub const fn micros(at: chrono::DateTime<chrono::Utc>) -> i64 {
	at.timestamp_micros()
}

/// Inverse of [`micros`].
pub fn from_micros(us: i64) -> chrono::DateTime<chrono::Utc> {
	chrono::DateTime::from_timestamp_micros(us).unwrap_or_default()
}

#[cfg(test)]
pub(crate) mod testdb;

#[cfg(test)]
mod tests {
	use super::Cursor;

	#[test]
	fn cursor_round_trips_and_rejects_garbage() {
		let cursor =
			Cursor { created_at_us: 1_700_000_000_123_000, id: "abc__def".into() };
		assert_eq!(Cursor::decode(&cursor.encode()), Some(cursor));
		assert_eq!(Cursor::decode("not base64!"), None);
		assert_eq!(
			Cursor::decode(&base64::Engine::encode(
				&base64::engine::general_purpose::URL_SAFE_NO_PAD,
				"12__"
			)),
			None
		);
		assert_eq!(
			Cursor::decode(&base64::Engine::encode(
				&base64::engine::general_purpose::URL_SAFE_NO_PAD,
				"x__id"
			)),
			None
		);
	}
}
