-- Core driver tables (ARCHITECTURE.md §7): identity, API keys, CLI refresh
-- tokens, secrets, volumes, sandbox events, webhooks. All timestamps are
-- DATETIME(3) in UTC (the session time zone is forced to +00:00).

CREATE TABLE users (
	id CHAR(36) NOT NULL PRIMARY KEY,
	email VARCHAR(320) NOT NULL,
	password_hash VARCHAR(255) NOT NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	UNIQUE KEY users_email (email)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE teams (
	id CHAR(36) NOT NULL PRIMARY KEY,
	name VARCHAR(255) NOT NULL,
	slug VARCHAR(255) NOT NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	max_concurrent_sandboxes INT UNSIGNED NOT NULL DEFAULT 100,
	max_concurrent_builds INT UNSIGNED NOT NULL DEFAULT 20,
	max_vcpus INT UNSIGNED NOT NULL DEFAULT 8,
	max_memory_mb BIGINT UNSIGNED NOT NULL DEFAULT 8192,
	max_disk_mb BIGINT UNSIGNED NOT NULL DEFAULT 20480,
	max_length_hours INT UNSIGNED NOT NULL DEFAULT 24,
	blocked BOOLEAN NOT NULL DEFAULT FALSE,
	UNIQUE KEY teams_slug (slug)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE team_members (
	team_id CHAR(36) NOT NULL,
	user_id CHAR(36) NOT NULL,
	is_default BOOLEAN NOT NULL DEFAULT FALSE,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	PRIMARY KEY (team_id, user_id),
	KEY team_members_user (user_id),
	CONSTRAINT team_members_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE,
	CONSTRAINT team_members_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE api_keys (
	id CHAR(36) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	key_hash CHAR(64) NOT NULL,
	key_enc VARBINARY(256) NOT NULL,
	mask_prefix VARCHAR(16) NOT NULL,
	mask_suffix VARCHAR(16) NOT NULL,
	name VARCHAR(255) NOT NULL,
	created_by CHAR(36) NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	last_used_at DATETIME(3) NULL,
	UNIQUE KEY api_keys_hash (key_hash),
	KEY api_keys_team (team_id, created_at),
	CONSTRAINT api_keys_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE,
	CONSTRAINT api_keys_creator FOREIGN KEY (created_by) REFERENCES users (id) ON DELETE SET NULL
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE refresh_tokens (
	id CHAR(36) NOT NULL PRIMARY KEY,
	user_id CHAR(36) NOT NULL,
	token_hash CHAR(64) NOT NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	expires_at DATETIME(3) NOT NULL,
	revoked_at DATETIME(3) NULL,
	UNIQUE KEY refresh_tokens_hash (token_hash),
	KEY refresh_tokens_user (user_id),
	CONSTRAINT refresh_tokens_user FOREIGN KEY (user_id) REFERENCES users (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE secrets (
	id VARCHAR(64) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	name VARCHAR(128) NOT NULL,
	current_version BIGINT NOT NULL,
	metadata JSON NOT NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	UNIQUE KEY secrets_team_name (team_id, name),
	KEY secrets_team_created (team_id, created_at, id),
	CONSTRAINT secrets_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE secret_versions (
	secret_id VARCHAR(64) NOT NULL,
	version BIGINT NOT NULL,
	value_enc MEDIUMBLOB NOT NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	PRIMARY KEY (secret_id, version),
	CONSTRAINT secret_versions_secret FOREIGN KEY (secret_id) REFERENCES secrets (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE volumes (
	id CHAR(36) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	name VARCHAR(255) NOT NULL,
	node_id VARCHAR(16) NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	UNIQUE KEY volumes_team_name (team_id, name),
	CONSTRAINT volumes_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- `seq` gives webhook delivery a monotonic cursor; `id` is the public UUID.
CREATE TABLE sandbox_events (
	seq BIGINT UNSIGNED NOT NULL AUTO_INCREMENT PRIMARY KEY,
	id CHAR(36) NOT NULL,
	team_id CHAR(36) NOT NULL,
	sandbox_id VARCHAR(64) NOT NULL,
	execution_id VARCHAR(64) NOT NULL,
	template_id VARCHAR(64) NOT NULL,
	build_id VARCHAR(64) NOT NULL,
	type VARCHAR(64) NOT NULL,
	ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	data JSON NULL,
	UNIQUE KEY sandbox_events_id (id),
	KEY sandbox_events_team_ts (team_id, ts),
	KEY sandbox_events_team_sandbox (team_id, sandbox_id, ts),
	KEY sandbox_events_team_seq (team_id, seq)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

-- `last_event_seq` is the delivery cursor: events with a larger seq are pending.
CREATE TABLE webhooks (
	id CHAR(36) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	name VARCHAR(255) NOT NULL,
	url VARCHAR(2048) NOT NULL,
	events JSON NOT NULL,
	enabled BOOLEAN NOT NULL DEFAULT TRUE,
	signature_secret_enc VARBINARY(1024) NOT NULL,
	last_event_seq BIGINT UNSIGNED NOT NULL DEFAULT 0,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	KEY webhooks_team (team_id, created_at),
	CONSTRAINT webhooks_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;

CREATE TABLE webhook_deliveries (
	id CHAR(36) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	webhook_id CHAR(36) NOT NULL,
	event_id CHAR(36) NOT NULL,
	sandbox_id VARCHAR(64) NOT NULL,
	event_type VARCHAR(64) NOT NULL,
	status VARCHAR(16) NOT NULL,
	duration_ms INT NOT NULL,
	request_url VARCHAR(2048) NOT NULL,
	request_headers TEXT NOT NULL,
	request_body MEDIUMTEXT NOT NULL,
	response_status INT NULL,
	response_headers TEXT NULL,
	response_body TEXT NULL,
	error_class VARCHAR(32) NULL,
	error_message TEXT NULL,
	ts DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	KEY webhook_deliveries_hook_ts (webhook_id, ts),
	KEY webhook_deliveries_hook_event (webhook_id, event_id),
	CONSTRAINT webhook_deliveries_hook FOREIGN KEY (webhook_id) REFERENCES webhooks (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
