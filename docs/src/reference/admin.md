# vmon admin commands

Complete reference for `vmon admin`, the operator tool for teams, users, API keys, limits, and system teams. It reads and writes the cluster's store through the cluster listener of a running driver, so at least one driver must be up; [`delete-team`](#delete-team) instead asks that driver to do the work. Task-oriented walkthroughs are in [Administration](../operate/admin.md).

```text
Usage: vmon admin [OPTIONS] <COMMAND>
```

Every invocation connects to the first reachable `--cluster-url`; a write reaches the raft leader through it, and reads are linearizable (they see every write acknowledged before them). Failures print `vmon: error: <message>` to stderr and exit with status 1; invalid arguments are reported by the argument parser with status 2. `vmon admin help [COMMAND]` prints the help of any subcommand.

## Global options

Global options go between `admin` and the subcommand. Every subcommand also accepts `-h`, `--help`; `--system`, `--if-missing`, `--force`, and `--default` are off unless supplied.

| Option | Env | Default | Description |
|---|---|---|---|
| `--cluster-url <CLUSTER_URL>` | `VMON_CLUSTER_URL` | `http://127.0.0.1:7947` | Cluster listener URL(s) of running drivers, comma-separated; requests go to the first that answers. |
| `--cluster-secret <CLUSTER_SECRET>` | `VMON_CLUSTER_SECRET` | `<data-dir>/cluster-secret` | Cluster secret. It authenticates the cluster requests, and API keys are stored encrypted with a key derived from it, so it must match the drivers' secret. |
| `--data-dir <DATA_DIR>` | `VMON_HOME` | `/var/lib/vmon` as root on Linux, else `~/.vmon` | State directory holding `cluster-secret` when `--cluster-secret` is unset. |
| `-h`, `--help` | | | Print help. |

When `--cluster-secret` is unset and `<data-dir>/cluster-secret` does not exist, every command fails with `no cluster secret: pass --cluster-secret (VMON_CLUSTER_SECRET) or --data-dir of a server (<path> missing)`. The secret file is read with surrounding whitespace trimmed. Other file-read failures report the read error. `vmon admin` never creates the file.

## Team arguments

Every `<TEAM>` argument and `--team` option accepts the team's ID (UUID) or its slug. An unknown team fails with `team <TEAM> not found`.

## Team lines

`list-teams`, `set-limits`, `block`, `unblock`, `set-system`, and `unset-system` print teams as one tab-separated line each:

```text
<id>	<slug>	<name>	sandboxes=<n> builds=<n> vcpus=<n> memory_mb=<n> disk_mb=<n> hours=<n>[	BLOCKED][	SYSTEM]
```

The fourth column holds the [limits](../concepts/auth.md#limits) separated by spaces: `max_concurrent_sandboxes`, `max_concurrent_builds`, `max_vcpus`, `max_memory_mb`, `max_disk_mb`, `max_length_hours`. `BLOCKED` and `SYSTEM` are extra tab-separated columns, present only when set.

## Limit options

`create-team` and `set-limits` take the same limit options:

| Option | Limit | Default for new teams |
|---|---|---|
| `--max-concurrent-sandboxes <CONCURRENT_SANDBOXES>` | Concurrent running sandboxes | 100 |
| `--max-concurrent-builds <CONCURRENT_BUILDS>` | Concurrent template builds | 20 |
| `--max-vcpus <VCPUS>` | vCPUs per sandbox or build | 8 |
| `--max-memory-mb <MEMORY_MB>` | Memory per sandbox or build, MiB | 8192 |
| `--max-disk-mb <DISK_MB>` | Disk per sandbox or build, MiB | 20480 |
| `--max-length-hours <LENGTH_HOURS>` | Maximum sandbox lifetime per execution, hours | 24 |

Values are unsigned integers. An omitted option means "keep the base value": the default above for `create-team`, the team's current value for `set-limits`.

## create-team

Create a team and print its first API key.

```text
Usage: vmon admin create-team [OPTIONS] <NAME>
```

| Argument / option | Description |
|---|---|
| `<NAME>` | Team name. Leading and trailing whitespace is trimmed; an empty name is an error. |
| `--system` | Make it a system team: its public templates claim the bare aliases every team resolves (`base`, `code-interpreter-v1`, …). |
| `--if-missing` | When a team already holds the slug of `<NAME>`, print that team and change nothing. |
| `--max-*` | [Limit options](#limit-options); omitted ones take the defaults. |

The slug is derived from the name: lowercase ASCII letters and digits, every other run of characters becomes one `-`, no leading or trailing `-`, and `team` when nothing remains. On a collision `-2`, `-3`, … is appended. With `--if-missing` there is no suffix: if the slug is taken, its team is printed as its `team` and `slug` lines, `team <slug> already exists; left unchanged` goes to stderr, the command exits with status 0, and neither `--system` nor the limit options are applied. Reruns therefore return the same team.

The command also mints the team's first API key, named `Default API key`, and prints:

```text
team    <name> (<id>)
slug    <slug>
api key e2b_<40 hex>
```

The plaintext key is only shown here.

## list-teams

List teams.

```text
Usage: vmon admin list-teams
```

Prints one [team line](#team-lines) per team, oldest first.

## delete-team

Delete a team (id or slug) with its API keys, templates, volumes, secrets, webhooks, and memberships; frees its slug.

```text
Usage: vmon admin delete-team [OPTIONS] <TEAM>
```

| Argument / option | Description |
|---|---|
| `<TEAM>` | Team ID or slug. |
| `--force` | Kill the team's running and paused sandboxes and cancel its unfinished builds instead of refusing; required for a system team. |

Unlike the other commands, the deletion runs on a driver, because killing sandboxes and deleting node-local volume data need the nodes. `vmon admin` sends `DELETE /driver/v1/teams/<TEAM>?force=<bool>` with `Authorization: Bearer <cluster secret>` to the first `--cluster-url` that accepts the connection. Only a driver's cluster listener serves that route; a node-only listener fails with `<url> serves no driver API (is it a driver's cluster listener?)`.

Without `--force` the command fails with status 1 and changes nothing when:

- the team is a system team: `team '<slug>' is a system team whose public templates other teams use; force the delete to remove it`;
- the team has running or paused sandboxes or unfinished builds: `team '<slug>' has running or paused sandboxes <ids> and unfinished builds <ids>; force the delete to kill and cancel them` (each list names at most 10 IDs, then `and <n> more`).

Otherwise the driver blocks the team, kills its sandboxes, cancels its unfinished builds (reason `The build was cancelled because its team was deleted.`), and removes, in order, its templates, volumes, secrets, webhooks, and API keys, and then the team with its memberships and slug. Templates are deleted like `DELETE /templates/{templateID}`: their aliases and tags are freed at once, and the build GC deletes their artifacts once no sandbox uses them. Volume data is deleted from the object store or the owning node; a volume whose node is gone loses only its record. Users are kept; a user whose default team was deleted falls back to their oldest remaining membership. If a sandbox cannot be killed, the command fails with `<n> sandboxes of team '<slug>' could not be killed; the team stays blocked, retry the delete`; any failure leaves the team blocked with part of its records removed, and rerunning the command finishes the delete. Other drivers may accept the team's API keys for up to 30 seconds, until their key cache expires. On success it prints:

```text
deleted team <slug> (<id>)
killed sandboxes=<n> cancelled builds=<n>
removed templates=<n> volumes=<n> secrets=<n> webhooks=<n> api keys=<n>
```

The admin HTTP API offers the same operation as `DELETE /admin/teams/{teamID}` ([Administration](../operate/admin.md#teams)).

## create-api-key

Mint another API key for a team (id or slug).

```text
Usage: vmon admin create-api-key [OPTIONS] <TEAM>
```

| Argument / option | Default | Description |
|---|---|---|
| `<TEAM>` | | Team ID or slug. |
| `--name <NAME>` | `API key` | Key name, shown in `GET /api-keys`. |

Prints only the new key (`e2b_` + 40 lowercase hex) on one line. The key is stored as a SHA-256 hash plus an AES-256-GCM ciphertext under the cluster secret's data key, with no creating user.

## create-user

Create a CLI login user, optionally in a team (id or slug) as its default team.

```text
Usage: vmon admin create-user [OPTIONS] --password <PASSWORD> <EMAIL>
```

| Argument / option | Env | Description |
|---|---|---|
| `<EMAIL>` | | Login email. Trimmed; must contain `@`. |
| `--password <PASSWORD>` | `VMON_ADMIN_PASSWORD` | Password, at least 8 UTF-8 bytes. Required; read from `VMON_ADMIN_PASSWORD` when the option is omitted. Stored as an Argon2id hash. |
| `--team <TEAM>` | | Team ID or slug. The user becomes a member with this team as the default. |

Fails when a user with the email already exists or the team is unknown (the team is checked before the user is created). Prints:

```text
user <email> (<id>)
```

## add-member

Add an existing user to a team.

```text
Usage: vmon admin add-member [OPTIONS] <EMAIL> <TEAM>
```

| Argument / option | Description |
|---|---|
| `<EMAIL>` | Email of an existing user; unknown users fail with `user <EMAIL> not found`. |
| `<TEAM>` | Team ID or slug. |
| `--default` | Make it the user's default team. Clears the default flag on the user's other memberships. |

If the user is already a member, the membership's default flag is set to the value of `--default`, so running without `--default` clears it. Prints nothing on success.

## set-limits

Change a team's limits (omitted flags keep their current values).

```text
Usage: vmon admin set-limits [OPTIONS] <TEAM>
```

| Argument / option | Description |
|---|---|
| `<TEAM>` | Team ID or slug. |
| `--max-*` | [Limit options](#limit-options). |

All six limit columns are written in one update. Each value comes from its option when given and from the team's current limit otherwise, so omitted options leave that limit unchanged and running with no options rewrites the current values. Prints the updated [team line](#team-lines).

## block

Block a team: its credentials may still read, every change is refused with 403 `team is blocked`.

```text
Usage: vmon admin block <TEAM>
```

Sets the team's blocked flag. All of its credentials keep authenticating, but every team-scoped request with a method other than `GET`, `HEAD`, `OPTIONS`, or `TRACE` is rejected with 403 `team is blocked`; running sandboxes are not stopped. Drivers cache API-key lookups for 30 seconds, so API-key callers see the change within that window. Prints the updated [team line](#team-lines).

## unblock

Unblock a team.

```text
Usage: vmon admin unblock <TEAM>
```

Clears the blocked flag. Prints the updated [team line](#team-lines).

## set-system

Make a team a system team (its public templates claim free bare aliases).

```text
Usage: vmon admin set-system <TEAM>
```

Sets the system flag and, in the same transaction, claims a bare alias for each name of the team's public, non-deleted templates whose bare alias no other template holds. Prints the updated [team line](#team-lines). See [System teams](../concepts/auth.md#system-teams).

## unset-system

Make a system team an ordinary team (releases its bare aliases).

```text
Usage: vmon admin unset-system <TEAM>
```

Clears the system flag and, in the same transaction, deletes every bare alias held by the team's templates. The team's namespaced aliases (`<slug>/<alias>`) are unaffected. Prints the updated [team line](#team-lines).

## ci

[vmon CI](../operate/ci.md): GitHub Actions jobs with `runs-on: vmon…` on fresh sandboxes.

```text
Usage: vmon admin ci [OPTIONS] [COMMAND]
```

Every `ci` command calls the CI admin API of a driver (`/driver/v1/ci…` with `Authorization: Bearer <cluster secret>`, the first `--cluster-url` that accepts the connection), the same API the dashboard uses under `/admin/ci`. `--json` (any position after `ci`) prints the driver's JSON reply instead of text. `<ACCOUNT>` is a GitHub account login (case-insensitive) or installation ID of the app.

| Command | Description |
|---|---|
| (none) | The app, the dashboard URL, setup problems (no app, an app or installation lacking Administration: write or Actions: read, uninstalled or unlinked accounts, team limits below a size, a team cap under 6 h 10 min, an unresolvable `--ci-template`), accounts, last-7-day statistics (jobs, succeeded, failed, cancelled, refused, wait p50/p95, runner minutes), and the newest 20 jobs. |
| `jobs [--account <ACCOUNT>] [--repo <REPO>] [--state all\|active\|failed\|refused] [--limit <N>]` | Jobs, newest first (default 20): ID, state (the conclusion once completed), size, repository, job name, wait, run time, reason. `--repo` matches `owner/name` or `name`. |
| `job <JOB_ID>` | One job: labels, size, template, state, sandbox and node, times, and its history (each state change with its reason or conclusion). |
| `logs <JOB_ID>` | The job's GitHub log; fails with `the job log is available once the job completes` while it runs. |
| `repos [--account <ACCOUNT>]` | Repositories the app can see: visibility, `allowed`/`refused`, and why. |
| `app show` | The configured app (name, slug, ID, owner, page, install URL, webhook URL). |
| `app set --app-id <ID> --private-key-file <PEM> --webhook-secret-file <FILE>` | Configure an existing app. `<FILE>` holds the secret that signs the webhooks vmon receives: the app's webhook secret, or a re-signing relay's own secret. The driver checks the key against GitHub (`GET /app`, which must report the same ID; missing permissions are reported as setup problems, not refused) and stores the key and secret encrypted. |
| `app delete` | Remove the app; webhooks then answer 503 and no runner starts. |
| `app secret` | Print the webhook secret vmon verifies with (the one `app set` stored; for a relay that passes GitHub's signature through). Only on the cluster listener. |
| `connect` | Print the dashboard URL and the steps of the app-creation flow. |
| `link <ACCOUNT> --team <TEAM>` | Run the account's jobs as sandboxes of `<TEAM>` (ID or slug). An installation not yet known is fetched from GitHub. |
| `unlink <ACCOUNT>` | Unlink the account: its jobs are refused. |
| `public <ACCOUNT> [on\|off] [--users "<a>, @<b>"] [--anyone[=<BOOL>]]` | Show or change the public-repository policy: off by default; with `on`, runs are allowed when `--anyone` is set or both the run's actor and its triggering actor are listed in `--users` (comma- or space-separated, `@` optional, checked against GitHub; unknown users fail with `unknown GitHub users: …`). Omitted parts keep their current values. |
| `sync` | Re-read the app's installations from GitHub (removes uninstalled accounts, keeps links and policies). |
| `caches [--repo <OWNER/NAME>]` | [Actions cache](../operate/ci.md#actions-cache) usage: per repository (entries, size, budget, last use), or with `--repo` that repository's entries (key, version, scope ref, state, size, last use, nodes holding a copy), most recently used first. |
| `caches delete <OWNER/NAME> [--key-prefix <PREFIX>]` | Delete a repository's cache entries, all or those whose key starts with `<PREFIX>`; prints how many and the bytes freed. Archives no entry uses any more are dropped by their nodes within 10 minutes. |

Account lines read `<login> <type> id <installation> team <slug>|(not linked) <public policy>[ SUSPENDED]`.
