package freestyle

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/url"
	"reflect"
	"testing"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testDriver struct{ endpoint string }

func (driver *testDriver) Do(context.Context, vmon.DriverRequest) (*http.Response, string, error) {
	return nil, "", errors.New("unexpected HTTP request")
}
func (driver *testDriver) Dial(context.Context, string, url.Values, string) (*vmon.WebSocketConn, string, error) {
	return nil, "", errors.New("unexpected WebSocket request")
}
func (driver *testDriver) Endpoints() []vmon.EndpointInfo {
	return []vmon.EndpointInfo{{URL: driver.endpoint, Healthy: true, Source: "test"}}
}
func (driver *testDriver) Refresh(context.Context, bool) error { return nil }
func (driver *testDriver) Close() error                        { return nil }

type sandboxStub struct {
	pb.UnimplementedSandboxServiceServer
	create         func(context.Context, *pb.CreateSandboxRequest) (*pb.JsonView, error)
	execCapture    func(context.Context, *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error)
	setIdleTimeout func(context.Context, *pb.SetIdleTimeoutRequest) (*pb.JsonView, error)
	resume         func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	resize         func(context.Context, *pb.ResizeSandboxRequest) (*pb.JsonView, error)
	terminate      func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	remove         func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	fileStat       func(context.Context, *pb.FilePathRequest) (*pb.JsonView, error)
}

func (stub *sandboxStub) Create(ctx context.Context, request *pb.CreateSandboxRequest) (*pb.JsonView, error) {
	return stub.create(ctx, request)
}
func (stub *sandboxStub) ExecCapture(ctx context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
	return stub.execCapture(ctx, request)
}
func (stub *sandboxStub) SetIdleTimeout(ctx context.Context, request *pb.SetIdleTimeoutRequest) (*pb.JsonView, error) {
	return stub.setIdleTimeout(ctx, request)
}
func (stub *sandboxStub) Resume(ctx context.Context, request *pb.SandboxRef) (*pb.JsonView, error) {
	return stub.resume(ctx, request)
}
func (stub *sandboxStub) Resize(ctx context.Context, request *pb.ResizeSandboxRequest) (*pb.JsonView, error) {
	return stub.resize(ctx, request)
}
func (stub *sandboxStub) Terminate(ctx context.Context, request *pb.SandboxRef) (*pb.JsonView, error) {
	return stub.terminate(ctx, request)
}
func (stub *sandboxStub) Remove(ctx context.Context, request *pb.SandboxRef) (*pb.JsonView, error) {
	return stub.remove(ctx, request)
}
func (stub *sandboxStub) FileStat(ctx context.Context, request *pb.FilePathRequest) (*pb.JsonView, error) {
	return stub.fileStat(ctx, request)
}

func testFreestyle(t *testing.T, stub *sandboxStub) *Freestyle {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	pb.RegisterSandboxServiceServer(server, stub)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		_ = listener.Close()
	})
	client := vmon.NewClient(&testDriver{endpoint: "http://" + listener.Addr().String()})
	t.Cleanup(func() { _ = client.Close() })
	return NewFromClient(client)
}

func TestCreateSnapshotsDefaultAndFullyConfiguredSpecs(t *testing.T) {
	var spec map[string]any
	stub := &sandboxStub{}
	stub.create = func(_ context.Context, request *pb.CreateSandboxRequest) (*pb.JsonView, error) {
		if err := json.Unmarshal([]byte(request.GetSpecJson()), &spec); err != nil {
			t.Fatal(err)
		}
		return &pb.JsonView{Json: `{"id":"vm-1","status":"running"}`}, nil
	}
	client := testFreestyle(t, stub)
	ctx := context.Background()
	created, err := client.Vms.Create(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if created.VmID != "vm-1" || created.Vm.VmID != "vm-1" || created.Domains == nil ||
		len(created.Domains) != 0 {
		t.Fatalf("created=%+v", created)
	}
	if want := map[string]any{"timeout_secs": float64(0), "block_network": false}; !reflect.DeepEqual(spec, want) {
		t.Fatalf("default spec=%#v, want %#v", spec, want)
	}

	idle := uint64(0)
	threshold := uint64(4096)
	priority := 99
	useDefault := false
	_, err = client.Vms.Create(ctx, &CreateOptions{
		Name: "mapped", IdleTimeoutSeconds: &idle, ActivityThresholdBytes: &threshold,
		Persistence: &PersistenceOptions{Type: "sticky", Priority: &priority},
		NICs: []NICOptions{
			{VPC: "vpc-1", Mode: "routed", IPv4: "10.88.0.4", Default: &useDefault},
		},
		Template: "fs-snap-1", CPU: 2, Memory: 2, Storage: 3,
		Env: map[string]string{"A": "B"}, Workdir: "/work", Tags: map[string]string{"kind": "test"},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{
		"name": "mapped", "template": "fs-snap-1", "cpus": float64(2),
		"memory": float64(2048), "disk_mb": float64(3072), "timeout_secs": float64(0),
		"block_network":     false,
		"idle_timeout_secs": float64(0), "activity_threshold_bytes": float64(4096),
		"persistence": map[string]any{"type": "sticky", "priority": float64(10)},
		"nics": []any{
			map[string]any{"vpc": "vpc-1", "ipv4": "10.88.0.4", "default": false},
		},
		"env": map[string]any{"A": "B"}, "workdir": "/work",
		"tags": map[string]any{"kind": "test"},
	}
	if !reflect.DeepEqual(spec, want) {
		t.Fatalf("fully configured spec=%#v, want %#v", spec, want)
	}
}

func TestExecUsesShellAndMapsResult(t *testing.T) {
	var captured *pb.ExecCaptureRequest
	stub := &sandboxStub{}
	stub.execCapture = func(_ context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
		captured = request
		return &pb.ExecCaptureResponse{Code: 7, Stdout: []byte("out"), Stderr: []byte("err")}, nil
	}
	client := testFreestyle(t, stub)
	vm, err := client.Vms.Ref(context.Background(), "vm-1")
	if err != nil {
		t.Fatal(err)
	}
	result, err := vm.Exec(context.Background(), ExecOptions{Command: "echo hi", TimeoutMs: 1500})
	if err != nil {
		t.Fatal(err)
	}
	if result != (ExecResult{Stdout: "out", Stderr: "err", StatusCode: 7}) {
		t.Fatalf("result=%+v", result)
	}
	if got := captured.GetExec().GetCmd(); !reflect.DeepEqual(got, []string{"sh", "-c", "echo hi"}) {
		t.Errorf("cmd=%q", got)
	}
	if captured.GetExec().GetTimeout() != 1.5 {
		t.Errorf("timeout=%v", captured.GetExec().GetTimeout())
	}
}

func TestResizeConvertsUnitsAndRejectsBeforeRPC(t *testing.T) {
	calls := 0
	var captured *pb.ResizeSandboxRequest
	stub := &sandboxStub{}
	stub.resize = func(_ context.Context, request *pb.ResizeSandboxRequest) (*pb.JsonView, error) {
		calls++
		captured = request
		return &pb.JsonView{Json: `{"id":"vm-1","status":"running"}`}, nil
	}
	client := testFreestyle(t, stub)
	vm, err := client.Vms.Ref(context.Background(), "vm-1")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := vm.Resize(context.Background(), ResizeOptions{CPU: 3}); err == nil {
		t.Fatal("non-power-of-two CPU was accepted")
	}
	if _, err := vm.Resize(context.Background(), ResizeOptions{Memory: 6}); err == nil {
		t.Fatal("non-power-of-two memory was accepted")
	}
	if calls != 0 {
		t.Fatalf("resize RPC called %d times during client validation", calls)
	}
	if _, err := vm.Resize(context.Background(), ResizeOptions{CPU: 4, Memory: 8, Storage: 12}); err != nil {
		t.Fatal(err)
	}
	if calls != 1 || captured.GetCpus() != 4 || captured.GetMemoryMib() != 8192 || captured.GetDiskMb() != 12288 {
		t.Fatalf("calls=%d request=%+v", calls, captured)
	}
}

func TestStartSetsIdlePolicyBeforeResuming(t *testing.T) {
	var calls []string
	var idleValues []float64
	stub := &sandboxStub{}
	stub.create = func(context.Context, *pb.CreateSandboxRequest) (*pb.JsonView, error) {
		return &pb.JsonView{Json: `{"id":"vm-start"}`}, nil
	}
	stub.setIdleTimeout = func(_ context.Context, request *pb.SetIdleTimeoutRequest) (*pb.JsonView, error) {
		calls = append(calls, "idle")
		idleValues = append(idleValues, request.GetIdleTimeoutSecs())
		return &pb.JsonView{Json: `{"id":"vm-start"}`}, nil
	}
	stub.resume = func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
		calls = append(calls, "resume")
		return &pb.JsonView{Json: `{"id":"vm-start"}`}, nil
	}
	client := testFreestyle(t, stub)
	created, err := client.Vms.Create(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	vm := created.Vm
	if _, err := vm.Start(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
	idle, disabled := uint64(30), uint64(0)
	if _, err := vm.Start(context.Background(), &StartOptions{IdleTimeoutSeconds: &idle}); err != nil {
		t.Fatal(err)
	}
	if _, err := vm.Start(context.Background(), &StartOptions{IdleTimeoutSeconds: &disabled}); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(calls, []string{"resume", "idle", "resume", "idle", "resume"}) {
		t.Fatalf("calls=%v", calls)
	}
	if !reflect.DeepEqual(idleValues, []float64{30, 0}) {
		t.Fatalf("idle values=%v", idleValues)
	}
}

func TestDeleteToleratesNotFound(t *testing.T) {
	removeCalls := 0
	terminateCalls := 0
	stub := &sandboxStub{}
	stub.terminate = func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
		terminateCalls++
		return &pb.JsonView{Json: `{}`}, nil
	}
	stub.remove = func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
		removeCalls++
		return nil, status.Error(codes.NotFound, "gone")
	}
	client := testFreestyle(t, stub)
	if err := client.Vms.Delete(context.Background(), "vm-gone"); err != nil {
		t.Fatal(err)
	}
	if removeCalls != 1 || terminateCalls != 0 {
		t.Fatalf("remove calls=%d terminate calls=%d", removeCalls, terminateCalls)
	}
}

func TestExistsNarrowsOnlyNotFound(t *testing.T) {
	stub := &sandboxStub{}
	stub.fileStat = func(_ context.Context, request *pb.FilePathRequest) (*pb.JsonView, error) {
		switch request.GetPath() {
		case "/missing":
			return nil, status.Error(codes.NotFound, "missing")
		case "/denied":
			return nil, status.Error(codes.PermissionDenied, "denied")
		default:
			return &pb.JsonView{Json: `{"ok":true,"type":"file","size":3,"mode":420,"mtime":1}`}, nil
		}
	}
	client := testFreestyle(t, stub)
	vm, err := client.Vms.Ref(context.Background(), "vm-1")
	if err != nil {
		t.Fatal(err)
	}
	exists, err := vm.Fs.Exists(context.Background(), "/missing")
	if err != nil || exists {
		t.Fatalf("missing exists=%v err=%v", exists, err)
	}
	exists, err = vm.Fs.Exists(context.Background(), "/present")
	if err != nil || !exists {
		t.Fatalf("present exists=%v err=%v", exists, err)
	}
	if _, err := vm.Fs.Exists(context.Background(), "/denied"); err == nil {
		t.Fatal("permission error was narrowed to absence")
	} else {
		var apiError *vmon.APIError
		if !errors.As(err, &apiError) || apiError.Code != "forbidden" {
			t.Fatalf("permission error=%T %v", err, err)
		}
	}
}
