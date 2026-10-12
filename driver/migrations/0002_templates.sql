-- Templates, aliases, tags, builds and build logs (ARCHITECTURE.md §7).

CREATE TABLE templates (
	id                VARCHAR(32)  NOT NULL,
	team_id           CHAR(36)     NOT NULL,
	public            BOOLEAN      NOT NULL DEFAULT FALSE,
	-- 'template' = built from steps; 'snapshot' = POST /sandboxes/{id}/snapshots;
	-- 'fork' = hidden snapshot backing POST /sandboxes/{id}/fork.
	kind              ENUM('template', 'snapshot', 'fork') NOT NULL DEFAULT 'template',
	source_sandbox_id VARCHAR(32)  NULL,
	created_at        DATETIME(3)  NOT NULL,
	updated_at        DATETIME(3)  NOT NULL,
	spawn_count       BIGINT       NOT NULL DEFAULT 0,
	last_spawned_at   DATETIME(3)  NULL,
	build_count       INT          NOT NULL DEFAULT 0,
	deleted_at        DATETIME(3)  NULL,
	PRIMARY KEY (id),
	KEY templates_team (team_id, kind, deleted_at, created_at),
	CONSTRAINT templates_team_fk FOREIGN KEY (team_id) REFERENCES teams (id) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4;

-- `namespace` is the owning team's slug, or '' for bare (legacy/public) aliases.
CREATE TABLE template_aliases (
	namespace   VARCHAR(64)  NOT NULL,
	alias       VARCHAR(128) NOT NULL,
	template_id VARCHAR(32)  NOT NULL,
	PRIMARY KEY (namespace, alias),
	KEY template_aliases_template (template_id),
	CONSTRAINT template_aliases_template_fk FOREIGN KEY (template_id) REFERENCES templates (id) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE builds (
	id               CHAR(36)     NOT NULL,
	template_id      VARCHAR(32)  NOT NULL,
	team_id          CHAR(36)     NOT NULL,
	status           ENUM('waiting', 'building', 'ready', 'error') NOT NULL,
	kind             ENUM('build', 'snapshot', 'pause') NOT NULL DEFAULT 'build',
	vcpus            INT UNSIGNED NOT NULL,
	memory_mb        BIGINT UNSIGNED NOT NULL,
	disk_mb          BIGINT UNSIGNED NOT NULL,
	min_free_disk_mb BIGINT UNSIGNED NULL,
	envd_version     VARCHAR(32)  NULL,
	vmm_version      VARCHAR(32)  NULL,
	kernel           VARCHAR(64)  NULL,
	arch             VARCHAR(16)  NULL,
	node_id          VARCHAR(16)  NULL,
	-- Requested tags, assigned to the build once it is ready.
	tags             JSON         NOT NULL,
	-- Persisted build definition (fromImage/fromTemplate/steps); never credentials.
	spec             JSON         NULL,
	template_version VARCHAR(16)  NULL,
	start_cmd        TEXT         NULL,
	ready_cmd        TEXT         NULL,
	-- `{message, step?}` once the build failed.
	reason           JSON         NULL,
	-- `ArtifactInfo` reported by the builder once ready.
	artifact         JSON         NULL,
	created_at       DATETIME(3)  NOT NULL,
	updated_at       DATETIME(3)  NOT NULL,
	started_at       DATETIME(3)  NULL,
	finished_at      DATETIME(3)  NULL,
	-- Set once the artifacts of a deleted template's build were collected.
	artifacts_gc_at  DATETIME(3)  NULL,
	PRIMARY KEY (id),
	KEY builds_template (template_id, created_at),
	KEY builds_status (status, kind),
	KEY builds_team_status (team_id, status),
	CONSTRAINT builds_template_fk FOREIGN KEY (template_id) REFERENCES templates (id) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE template_tags (
	template_id VARCHAR(32) NOT NULL,
	tag         VARCHAR(128) NOT NULL,
	build_id    CHAR(36)    NOT NULL,
	created_at  DATETIME(3) NOT NULL,
	PRIMARY KEY (template_id, tag),
	KEY template_tags_build (build_id),
	CONSTRAINT template_tags_template_fk FOREIGN KEY (template_id) REFERENCES templates (id) ON DELETE CASCADE,
	CONSTRAINT template_tags_build_fk FOREIGN KEY (build_id) REFERENCES builds (id) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4;

CREATE TABLE build_logs (
	build_id CHAR(36)    NOT NULL,
	seq      BIGINT UNSIGNED NOT NULL,
	ts       DATETIME(3) NOT NULL,
	level    ENUM('debug', 'info', 'warn', 'error') NOT NULL,
	step     VARCHAR(64) NULL,
	message  MEDIUMTEXT  NOT NULL,
	PRIMARY KEY (build_id, seq),
	KEY build_logs_ts (build_id, ts),
	CONSTRAINT build_logs_build_fk FOREIGN KEY (build_id) REFERENCES builds (id) ON DELETE CASCADE
) DEFAULT CHARSET = utf8mb4;
