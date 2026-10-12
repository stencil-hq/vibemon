-- Team-less sandbox listings (`GET /admin/sandboxes`, ARCHITECTURE.md §9.2)
-- and the reconciler's `live_sandboxes()` filter on state only; this index
-- keeps them off killed rows.

ALTER TABLE sandboxes ADD KEY idx_sandboxes_state_started (state, started_at, id);
