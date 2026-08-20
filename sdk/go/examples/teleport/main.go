// Command teleport demonstrates live sandbox migration between two vmon mesh
// nodes: it seeds guest state that only survives a real teleport (a tmpfs
// marker in RAM, a marker on the writable rootfs, and a live counter process),
// migrates the sandbox with one call, prints the phase latencies reported by
// the source node, and verifies every piece of state through the same client
// handle — which transparently re-routes to the new owner.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
)

func main() {
	sourceDSN := flag.String("source", "http://127.0.0.1:8081", "DSN of the source node")
	targetDSN := flag.String("target", "http://127.0.0.1:8082", "DSN of the target node")
	image := flag.String("image", "alpine:latest", "guest image")
	memMiB := flag.Uint("mem", 512, "guest RAM in MiB (larger sizes stress the delta path)")
	flag.Parse()
	token := os.Getenv("VMON_API_TOKEN")
	if token == "" {
		log.Fatal("VMON_API_TOKEN is required")
	}
	source, err := vmon.Connect(*sourceDSN, vmon.WithToken(token), vmon.WithDiscovery(true))
	if err != nil {
		log.Fatalf("source client: %v", err)
	}
	defer source.Close()
	target, err := vmon.Connect(*targetDSN, vmon.WithToken(token), vmon.WithDiscovery(true))
	if err != nil {
		log.Fatalf("target client: %v", err)
	}
	defer target.Close()
	ctx := context.Background()

	sourceMesh, err := source.Mesh.Status(ctx)
	if err != nil {
		log.Fatalf("source mesh status: %v", err)
	}
	targetMesh, err := target.Mesh.Status(ctx)
	if err != nil {
		log.Fatalf("target mesh status: %v", err)
	}
	if sourceMesh.Self.NodeID == "" || targetMesh.Self.NodeID == "" {
		log.Fatal("both nodes must be mesh members")
	}

	timeout := uint64(900)
	sandbox, err := source.Sandboxes.Create(ctx, vmon.SandboxCreateRequest{
		Image:          *image,
		Name:           fmt.Sprintf("teleport-%d", time.Now().UnixMilli()),
		Command:        []string{"sleep", "600"},
		MemoryMiB:      uint32(*memMiB),
		TimeoutSeconds: &timeout,
		HA:             "off",
	})
	if err != nil {
		log.Fatalf("create sandbox: %v", err)
	}
	defer sandbox.Remove(ctx)
	// Mesh placement may put the sandbox on either node; teleport to whichever
	// one it is NOT on, and address the (admin-only) migrate at the owner.
	owner, dest := source, targetMesh.Self
	if sandbox.Node == dest.NodeID {
		owner, dest = target, sourceMesh.Self
	}
	handle := owner.Sandboxes.Ref(sandbox.ID)
	defer handle.Remove(ctx)
	fmt.Printf("sandbox %s on node %s; teleporting to %s\n", sandbox.ID, sandbox.Node, dest.NodeID)

	// Seed state that only survives if the teleport really carries it:
	// a marker in guest RAM (tmpfs), one on the writable rootfs (disk delta),
	// and a counter whose value lives in a shell process's memory. The counter
	// is fully detached so the seeding exec's output pipe closes.
	nonce := fmt.Sprintf("nonce-%d", time.Now().UnixNano())
	sh(ctx, handle, "seed guest state", fmt.Sprintf(
		"mkdir -p /dev/shm && mount -t tmpfs tmpfs /dev/shm"+
			" && printf %%s '%s' >/dev/shm/teleport"+
			" && printf %%s '%s' >/root/teleport && sync"+
			" && (i=0; while :; do i=$((i+1)); printf %%s $i >/tmp/count; sleep 0.05; done)"+
			" </dev/null >/dev/null 2>&1 &", nonce, nonce))
	before := count(ctx, handle, "counter before teleport")
	fmt.Printf("counter before teleport: %d\n", before)

	migrated, err := handle.Migrate(ctx, dest.NodeID)
	if err != nil {
		log.Fatalf("teleport: %v", err)
	}
	timing, ok := migrated.MigrationTiming()
	if !ok {
		log.Fatal("migrate response carried no timing object")
	}
	fmt.Printf(
		"  pre-copy   %6d ms  (guest running: bulk RAM+disk streamed to target)\n"+
			"  downtime   %6d ms  (guest suspended: dirty RAM/disk delta + resume)\n"+
			"  total      %6d ms\n",
		timing.PrecopyMS, timing.DowntimeMS, timing.TotalMS)
	// The restore view has no mesh routing fields; re-fetch for the new owner.
	after, err := owner.Sandboxes.Get(ctx, sandbox.ID)
	if err != nil {
		log.Fatalf("refreshing sandbox after teleport: %v", err)
	}
	if after.Node != dest.NodeID {
		log.Fatalf("sandbox reports node %q after teleport to %s", after.Node, dest.NodeID)
	}

	// The handle re-resolved its endpoint during Migrate; verify through it.
	check("tmpfs (RAM) marker", sh(ctx, handle, "read tmpfs marker", "cat /dev/shm/teleport"), nonce)
	check("rootfs (disk) marker", sh(ctx, handle, "read disk marker", "cat /root/teleport"), nonce)
	countA := count(ctx, handle, "counter after teleport")
	countB := count(ctx, handle, "counter still advancing")
	fmt.Printf("  counter process alive: %d -> %d -> %d\n", before, countA, countB)
	if countB <= countA {
		log.Fatal("counter process did not survive the teleport")
	}
	fmt.Printf("\nteleport complete: sandbox %s is %s on %s; process, RAM, and disk intact\n",
		after.ID, after.Status, after.Node)
}

// sh runs one /bin/sh command inside the sandbox and returns trimmed stdout.
func sh(ctx context.Context, sandbox *vmon.Sandbox, what, script string) string {
	result, err := sandbox.Run(ctx, vmon.ExecRequest{Command: []string{"/bin/sh", "-c", script}})
	if err != nil {
		log.Fatalf("%s: %v", what, err)
	}
	if result.ExitCode != 0 {
		log.Fatalf("%s: exit %d stderr=%q", what, result.ExitCode, result.Stderr)
	}
	return strings.TrimSpace(string(result.Stdout))
}

// count reads the in-guest counter, waiting a beat so consecutive reads
// observe progress.
func count(ctx context.Context, sandbox *vmon.Sandbox, what string) int {
	value, err := strconv.Atoi(sh(ctx, sandbox, what, "sleep 1; cat /tmp/count"))
	if err != nil {
		log.Fatalf("%s: non-numeric counter: %v", what, err)
	}
	return value
}

func check(what, got, want string) {
	if got != want {
		log.Fatalf("%s mismatch: got %q want %q", what, got, want)
	}
	fmt.Printf("  %-22s intact (%s)\n", what, got)
}
