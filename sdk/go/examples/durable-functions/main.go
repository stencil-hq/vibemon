// Command durable-functions exercises deployed durable functions through the Go SDK.
//
// It expects a deployed CBOR echo function: the function must return the input
// object unchanged. Set VMON_SERVER_URL, VMON_API_TOKEN, VMON_REMOTE_NAMESPACE,
// and VMON_REMOTE_CBOR_NAME before running it.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
)

type echoValue struct {
	RunID string `cbor:"run_id"`
	Item  uint64 `cbor:"item"`
	Bytes []byte `cbor:"bytes"`
	Wide  uint64 `cbor:"wide"`
}

type config struct {
	namespace  string
	function   string
	revision   string
	callIDFile string
	batchSize  int
	cancelDemo bool
	timeout    time.Duration
}

func main() {
	if err := run(); err != nil {
		reportError(err)
		os.Exit(1)
	}
}

func run() (resultErr error) {
	cfg := config{}
	flag.StringVar(&cfg.namespace, "namespace", os.Getenv("VMON_REMOTE_NAMESPACE"), "deployed function namespace")
	flag.StringVar(&cfg.function, "function", os.Getenv("VMON_REMOTE_CBOR_NAME"), "deployed CBOR echo function name")
	flag.StringVar(&cfg.revision, "revision", os.Getenv("VMON_REMOTE_CBOR_REVISION"), "immutable revision to pin (defaults to the current revision)")
	flag.StringVar(&cfg.callIDFile, "call-id-file", "", "file in which to save the spawned call ID (defaults to a run-unique file in the temporary directory)")
	flag.IntVar(&cfg.batchSize, "batch-size", 8, "number of batch inputs (1..1000)")
	flag.BoolVar(&cfg.cancelDemo, "cancel-demo", false, "spawn and cancel an additional call")
	flag.DurationVar(&cfg.timeout, "timeout", 2*time.Minute, "deadline for the example")
	flag.Parse()

	serverURL := os.Getenv("VMON_SERVER_URL")
	token := os.Getenv("VMON_API_TOKEN")
	if serverURL == "" {
		return errors.New("VMON_SERVER_URL is required")
	}
	if token == "" {
		return errors.New("VMON_API_TOKEN is required")
	}
	if cfg.namespace == "" || cfg.function == "" {
		return errors.New("-namespace and -function are required (or set VMON_REMOTE_NAMESPACE and VMON_REMOTE_CBOR_NAME)")
	}
	if cfg.batchSize < 1 || cfg.batchSize > 1000 {
		return fmt.Errorf("-batch-size must be between 1 and 1000, got %d", cfg.batchSize)
	}
	if cfg.timeout <= 0 {
		return fmt.Errorf("-timeout must be positive, got %s", cfg.timeout)
	}

	runID, err := newRunID()
	if err != nil {
		return fmt.Errorf("create run ID: %w", err)
	}
	if cfg.callIDFile == "" {
		cfg.callIDFile = filepath.Join(os.TempDir(), "vmon-durable-"+runID+".call-id")
	}

	client, err := vmon.Connect(serverURL, vmon.WithToken(token))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", serverURL, err)
	}
	defer func() {
		resultErr = errors.Join(resultErr, client.Close())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), cfg.timeout)
	defer cancel()

	current, err := vmon.LookupFunction[echoValue](ctx, client, cfg.namespace, cfg.function)
	if err != nil {
		return fmt.Errorf("look up current function %s/%s: %w", cfg.namespace, cfg.function, err)
	}
	if cfg.revision == "" {
		cfg.revision = current.RevisionID()
	}
	pinned, err := vmon.LookupFunctionRevision[echoValue](ctx, client, cfg.namespace, cfg.function, cfg.revision)
	if err != nil {
		return fmt.Errorf("look up pinned function %s/%s@%s: %w", cfg.namespace, cfg.function, cfg.revision, err)
	}
	current = current.WithOptions(
		vmon.WithValueEncoding(vmon.ValueCBOR, vmon.CompressionNone),
		vmon.WithCallLabels(map[string]string{"example": "go-durable-functions", "run_id": runID}),
	)
	pinned = pinned.WithOptions(
		vmon.WithValueEncoding(vmon.ValueCBOR, vmon.CompressionNone),
		vmon.WithCallLabels(map[string]string{"example": "go-durable-functions", "run_id": runID}),
	)
	fmt.Printf("current revision: %s; pinned revision: %s\n", current.RevisionID(), pinned.RevisionID())

	portable := echoValue{RunID: runID, Item: 0, Bytes: []byte{0, 1, 255}, Wide: uint64(1) << 53}
	remoteResult, err := current.Remote(ctx, portable)
	if err != nil {
		return fmt.Errorf("remote call: %w", err)
	}
	if err := checkEcho(remoteResult, portable); err != nil {
		return fmt.Errorf("remote result: %w", err)
	}
	fmt.Printf("Remote preserved CBOR bytes and integer 2^53 for run %s\n", runID)

	// Durable execution has at-least-once attempt behavior after infrastructure
	// failures. External side effects in the deployed function must use RunID and
	// Item as stable idempotency keys rather than relying on a single attempt.
	spawned, err := pinned.Spawn(ctx, echoValue{RunID: runID, Item: 1, Bytes: []byte("durable"), Wide: portable.Wide})
	if err != nil {
		return fmt.Errorf("spawn pinned call: %w", err)
	}
	if err := os.WriteFile(cfg.callIDFile, []byte(spawned.ID()+"\n"), 0o600); err != nil {
		return fmt.Errorf("save call ID to %s: %w", cfg.callIDFile, err)
	}
	savedID, err := os.ReadFile(cfg.callIDFile)
	if err != nil {
		return fmt.Errorf("read call ID from %s: %w", cfg.callIDFile, err)
	}
	callID := string(bytes.TrimSpace(savedID))
	reconstructed, err := vmon.FunctionCallFromID[echoValue](client, callID)
	if err != nil {
		return fmt.Errorf("reconstruct call %q: %w", callID, err)
	}
	durableResult, err := reconstructed.Get(ctx)
	if err != nil {
		return fmt.Errorf("get reconstructed call %q: %w", callID, err)
	}
	if err := checkEcho(durableResult, echoValue{RunID: runID, Item: 1, Bytes: []byte("durable"), Wide: portable.Wide}); err != nil {
		return fmt.Errorf("durable result: %w", err)
	}
	fmt.Printf("reconstructed call %s from %s\n", callID, cfg.callIDFile)

	if err := printObservability(ctx, reconstructed); err != nil {
		return err
	}
	if err := runBatch(ctx, pinned, runID, cfg.batchSize, portable.Wide); err != nil {
		return err
	}
	if cfg.cancelDemo {
		if err := runCancellation(ctx, pinned, runID, portable.Wide); err != nil {
			return err
		}
	}
	return nil
}

func runBatch(ctx context.Context, function *vmon.Function[echoValue], runID string, size int, wide uint64) error {
	feedCtx, stopFeed := context.WithCancel(ctx)
	inputs := make(chan any, 4)
	go func() {
		defer close(inputs)
		for i := range size {
			value := echoValue{RunID: runID, Item: uint64(i + 2), Bytes: []byte{byte(i)}, Wide: wide}
			select {
			case inputs <- value:
			case <-feedCtx.Done():
				return
			}
		}
	}()
	batch, err := function.Map(ctx, inputs)
	stopFeed()
	if err != nil {
		return fmt.Errorf("submit bounded batch: %w", err)
	}

	results, failures := batch.CompletionResults(ctx)
	seen := make(map[uint64]bool, size)
	for result := range results {
		if result.InputIndex >= uint64(size) {
			return fmt.Errorf("batch returned out-of-range input index %d", result.InputIndex)
		}
		if seen[result.InputIndex] {
			return fmt.Errorf("batch returned input index %d twice", result.InputIndex)
		}
		seen[result.InputIndex] = true
		fmt.Printf("batch completion sequence=%d input=%d item=%d\n", result.Sequence, result.InputIndex, result.Value.Item)
	}
	if err := <-failures; err != nil {
		return fmt.Errorf("iterate batch results: %w", err)
	}
	if len(seen) != size {
		return fmt.Errorf("batch returned %d results, want %d", len(seen), size)
	}
	return nil
}

func runCancellation(ctx context.Context, function *vmon.Function[echoValue], runID string, wide uint64) error {
	call, err := function.Spawn(ctx, echoValue{RunID: runID, Item: uint64(1) << 32, Bytes: []byte("cancel"), Wide: wide})
	if err != nil {
		return fmt.Errorf("spawn cancellation demo: %w", err)
	}
	if err := call.Cancel(ctx, "Go durable-functions example requested cancellation"); err != nil {
		return fmt.Errorf("cancel call %s: %w", call.ID(), err)
	}
	_, err = call.Get(ctx)
	if err != nil && !errors.Is(err, vmon.ErrCallCancelled) {
		return fmt.Errorf("wait for cancelled call %s: %w", call.ID(), err)
	}
	status, statusErr := call.Status(ctx)
	if statusErr != nil {
		return fmt.Errorf("read cancelled call %s status: %w", call.ID(), statusErr)
	}
	if err == nil {
		fmt.Printf("cancellation raced with completion for call %s (status %s)\n", call.ID(), statusName(status))
	} else {
		fmt.Printf("cancelled call %s (status %s)\n", call.ID(), statusName(status))
	}
	return nil
}

func printObservability(ctx context.Context, call *vmon.FunctionCall[echoValue]) error {
	status, err := call.Status(ctx)
	if err != nil {
		return fmt.Errorf("read call %s status: %w", call.ID(), err)
	}
	fmt.Printf("call status: %s\n", statusName(status))

	events, failures := call.Watch(ctx, 0, false)
	for event := range events {
		if event.Kind == vmon.CallEventLog {
			fmt.Printf("call log stream=%d sequence=%d: %s\n", event.LogStream, event.Sequence, event.Log)
		}
	}
	if err := <-failures; err != nil {
		return fmt.Errorf("read call %s events: %w", call.ID(), err)
	}

	stats, err := call.Stats(ctx)
	if err != nil {
		return fmt.Errorf("read call %s stats: %w", call.ID(), err)
	}
	if stats == nil {
		fmt.Println("call stats: unavailable")
		return nil
	}
	fmt.Printf("call stats: attempts=%d queue=%dms startup=%dms execution=%dms wall=%dms cpu=%dms peak_memory=%dB\n",
		len(stats.Attempts), stats.QueueMillis, stats.StartupMillis, stats.ExecutionMillis,
		stats.WallMillis, stats.CPUMillis, stats.PeakMemoryBytes)
	return nil
}

func checkEcho(got, want echoValue) error {
	if got.RunID != want.RunID || got.Item != want.Item || got.Wide != want.Wide || !bytes.Equal(got.Bytes, want.Bytes) {
		return fmt.Errorf("echo mismatch: got %#v, want %#v", got, want)
	}
	return nil
}

func newRunID() (string, error) {
	var value [12]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	return hex.EncodeToString(value[:]), nil
}

func statusName(status vmon.CallStatus) string {
	switch status {
	case vmon.CallStatusPending:
		return "pending"
	case vmon.CallStatusQueued:
		return "queued"
	case vmon.CallStatusRunning:
		return "running"
	case vmon.CallStatusSucceeded:
		return "succeeded"
	case vmon.CallStatusFailed:
		return "failed"
	case vmon.CallStatusCancelling:
		return "cancelling"
	case vmon.CallStatusCancelled:
		return "cancelled"
	default:
		return fmt.Sprintf("unknown(%d)", status)
	}
}

func reportError(err error) {
	var remote *vmon.RemoteCallError
	if errors.As(err, &remote) {
		fmt.Fprintf(os.Stderr, "error: %v\nremote code=%q type=%q retryable=%t details=%v\n",
			err, remote.Code, remote.Type, remote.Retryable, remote.Details)
		return
	}
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
}
