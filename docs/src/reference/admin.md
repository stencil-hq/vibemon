# vmon admin commands

Complete reference for `vmon admin`, the operator tool for teams, users, API keys, limits, and system teams. It reads and writes the cluster's store through the cluster listener of a running driver, so at least one driver must be up. Task-oriented walkthroughs are in [Administration](../operate/admin.md).

```text
Usage: vmon admin [OPTIONS] <COMMAND>
```

Every invocation connects to the first reachable `--cluster-url`; a write reaches the raft leader through it, and reads are linearizable (they see every write acknowledged before them). Failures print `vmon: error: <message>` to stderr and exit with status 1; invalid arguments are reported by the argument parser with status 2. `vmon admin help [COMMAND]` prints the help of any subcommand.

## Global options

Global options go between `admin` and the subcommand. Every subcommand also accepts `-h`, `--help`; `--system` and `--default` are off unless supplied.

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
| `--max-*` | [Limit options](#limit-options); omitted ones take the defaults. |

The slug is derived from the name: lowercase ASCII letters and digits, every other run of characters becomes one `-`, no leading or trailing `-`, and `team` when nothing remains. On a collision `-2`, `-3`, … is appended.

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
