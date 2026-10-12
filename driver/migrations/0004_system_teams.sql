-- System teams (ARCHITECTURE.md §7): the public templates of a system team
-- claim bare (non-namespaced) aliases, which every team resolves after its own
-- namespace (`base`, `code-interpreter-v1`, `desktop`, `mcp-gateway`). Bare
-- aliases are reserved for system teams; none exists yet, so earlier bare
-- aliases (claimed by ordinary teams through the v1 publish) are released.

ALTER TABLE teams ADD COLUMN is_system BOOLEAN NOT NULL DEFAULT FALSE;

DELETE FROM template_aliases WHERE namespace = '';
