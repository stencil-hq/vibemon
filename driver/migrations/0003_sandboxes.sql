-- Sandboxes (ARCHITECTURE.md §7). One row per sandbox ever created; the KV
-- route `sbx/<id>` is the live catalog, this row is the durable record.
-- Paused sandboxes exist only here (state = 'paused', node_id = holder of
-- the paused state).
CREATE TABLE sandboxes (
	id VARCHAR(64) NOT NULL PRIMARY KEY,
	team_id CHAR(36) NOT NULL,
	template_id VARCHAR(64) NOT NULL,
	build_id CHAR(36) NOT NULL,
	alias VARCHAR(255) NULL,
	node_id CHAR(16) NOT NULL,
	state ENUM('running', 'paused', 'killed') NOT NULL,
	-- UUID of the current start/resume (sandbox_events.execution_id).
	execution_id CHAR(36) NOT NULL,
	vcpus INT UNSIGNED NOT NULL,
	memory_mb BIGINT UNSIGNED NOT NULL,
	disk_mb BIGINT UNSIGNED NOT NULL,
	metadata JSON NOT NULL,
	-- AES-GCM (data key) JSON map of the create-time envVars.
	env_enc MEDIUMBLOB NULL,
	auto_pause BOOLEAN NOT NULL,
	auto_pause_memory BOOLEAN NOT NULL,
	auto_resume BOOLEAN NOT NULL,
	allow_internet_access BOOLEAN NULL,
	-- E2B SandboxNetworkConfig without secrets (echoed by GET).
	network JSON NOT NULL,
	-- AES-GCM JSON of the normalized node NetworkPolicy (includes proxy credentials).
	egress_enc MEDIUMBLOB NOT NULL,
	volume_mounts JSON NOT NULL,
	-- {"autoResumeTimeoutS": n}
	lifecycle JSON NOT NULL,
	envd_version VARCHAR(32) NOT NULL,
	started_at DATETIME(3) NOT NULL,
	end_at DATETIME(3) NOT NULL,
	paused_at DATETIME(3) NULL,
	ended_at DATETIME(3) NULL,
	-- Latest snapshot template build captured from this sandbox.
	snapshot_build_id CHAR(36) NULL,
	created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
	KEY idx_sandboxes_team_state_started (team_id, state, started_at, id),
	KEY idx_sandboxes_node_state (node_id, state),
	CONSTRAINT fk_sandboxes_team FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_0900_ai_ci;
