// Package freestyle provides a Freestyle-shaped VM compatibility subset over
// the vmon Go SDK.
//
// It follows the VM surface published by freestyle@0.1.63 closely enough that
// most Go ports keep the same object model, but it is deliberately not a
// complete implementation of every published Freestyle declaration.
//
// Mapping onto vmon:
//   - API keys and access tokens are vmon bearer tokens; BaseURL is a vmon DSN.
//   - Vms.Create provisions a sandbox with no wall-clock lease. An idle timeout
//     of zero disables network-idle reclaim.
//   - Creating from SnapshotID restores the captured machine state, including
//     processes and memory, rather than fresh-booting it.
//   - Vm.Stop discards in-memory state while retaining disk and identity;
//     Vm.Start resumes suspended VMs or fresh-boots stopped VMs.
//   - Vm.Fork takes a full VM snapshot and atomically forks it; the base
//     snapshot is retained.
//   - Vm.Pty.Open creates a guest-owned persistent terminal. Attachments can
//     detach and reconnect by stable session ID; sessions survive
//     suspend/resume and are inherited by memory-preserving forks.
//
// Divergences from the published declarations:
//   - Vms.Create always returns an empty Domains slice and additionally accepts
//     vmon image, CPU, memory, storage, environment, workdir, and tag options.
//   - Persistence, ActivityThresholdBytes, and terminal Exec use vmon-native
//     semantics. Ephemeral VMs discard stored state on stop or suspend; sticky
//     VMs retain state but may be evicted by storage GC in ascending priority.
//     VPC NICs support one routed NIC on Linux hosts; macOS rejects them.
//   - Filesystem stat owner and group and snapshot creation time are omitted
//     because vmon does not report them. PTY options are a simplified subset.
//   - Git, domains, identities, DNS, serverless, cron, whoami, and raw fetch
//     surfaces have no vmon backing service and are not provided.
package freestyle
