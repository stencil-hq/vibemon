-- S3-backed filesystems (ARCHITECTURE.md §7, §9.6).
-- sandboxes.s3_mounts: non-secret `vmonS3Mounts` (path, uri, mode, endpoint,
-- region, credentialsSecret); s3_mounts_enc: AES-GCM JSON of the inline
-- credentials by mount index. volumes.object_store: the volume lives under
-- `volumes/<id>/` in the operator object store instead of on a node.

ALTER TABLE sandboxes ADD COLUMN s3_mounts JSON NULL;
ALTER TABLE sandboxes ADD COLUMN s3_mounts_enc VARBINARY(8192) NULL;
ALTER TABLE volumes ADD COLUMN object_store BOOLEAN NOT NULL DEFAULT FALSE;
