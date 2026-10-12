//! Throwaway `MySQL` databases for tests gated on `VMON_MYSQL_TEST_URL`
//! (e.g. `mysql://root:vmon@127.0.0.1:13306/vmon`). Each test creates its
//! own database and drops it at the end.

use std::str::FromStr;

use sqlx::{
	ConnectOptions, Connection,
	mysql::{MySqlConnectOptions, MySqlConnection, MySqlPoolOptions},
};

use super::Store;

/// A migrated store on a private database.
pub struct TestDb {
	pub store: Store,
	admin:     MySqlConnectOptions,
	name:      String,
}

impl TestDb {
	/// `None` (test skipped) when `VMON_MYSQL_TEST_URL` is unset.
	pub async fn create() -> Option<Self> {
		let url = std::env::var("VMON_MYSQL_TEST_URL").ok()?;
		let admin = MySqlConnectOptions::from_str(&url).expect("valid VMON_MYSQL_TEST_URL");
		let name = format!("vmon_test_{}", uuid::Uuid::new_v4().simple());
		let mut conn = admin.connect().await.expect("connect to test MySQL");
		sqlx::query(sqlx::AssertSqlSafe(format!("CREATE DATABASE `{name}` CHARACTER SET utf8mb4")))
			.execute(&mut conn)
			.await
			.expect("create test database");
		conn.close().await.ok();
		let options = admin
			.clone()
			.database(&name)
			.charset("utf8mb4")
			.timezone(Some(String::from("+00:00")));
		let pool = MySqlPoolOptions::new()
			.max_connections(4)
			.connect_with(options)
			.await
			.expect("connect test db");
		let store = Store::from_pool(pool);
		store.migrate().await.expect("migrations apply");
		Some(Self { store, admin, name })
	}

	/// Drops the database.
	pub async fn drop_db(self) {
		self.store.pool().close().await;
		let mut conn: MySqlConnection = self.admin.connect().await.expect("connect to test MySQL");
		sqlx::query(sqlx::AssertSqlSafe(format!("DROP DATABASE `{}`", self.name)))
			.execute(&mut conn)
			.await
			.expect("drop test database");
	}
}
