package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// stubDriver covers the residual HTTP surface (ports proxy) and the endpoint
// roster; gRPC traffic bypasses it entirely.
type stubDriver struct {
	do        func(context.Context, DriverRequest) (*http.Response, string, error)
	endpoints []EndpointInfo
}

func (driver *stubDriver) Do(ctx context.Context, request DriverRequest) (*http.Response, string, error) {
	if driver.do == nil {
		return nil, "", io.EOF
	}
	return driver.do(ctx, request)
}
func (driver *stubDriver) Dial(context.Context, string, url.Values, string) (*WebSocketConn, string, error) {
	return nil, "", io.EOF
}
func (driver *stubDriver) Endpoints() []EndpointInfo           { return driver.endpoints }
func (driver *stubDriver) Refresh(context.Context, bool) error { return nil }
func (driver *stubDriver) Close() error                        { return nil }
func jsonResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Body: io.NopCloser(strings.NewReader(body)), Header: make(http.Header)}
}

// routingDialer routes gRPC dials to per-endpoint bufconn listeners by
// address (e.g. "a.test:80").
func routingDialer(routes map[string]*bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, address string) (net.Conn, error) {
		if listener := routes[address]; listener != nil {
			return listener.DialContext(ctx)
		}
		return nil, fmt.Errorf("no route for %s", address)
	}
}

type snapshotServiceStub struct {
	pb.UnimplementedSnapshotServiceServer
	fork   func(context.Context, *pb.ForkSnapshotRequest) (*pb.JsonView, error)
	delete func(context.Context, *pb.SnapshotRef) (*pb.Ok, error)
}

func (stub *snapshotServiceStub) Fork(ctx context.Context, request *pb.ForkSnapshotRequest) (*pb.JsonView, error) {
	if stub.fork == nil {
		return nil, status.Error(codes.Unimplemented, "fork not stubbed")
	}
	return stub.fork(ctx, request)
}

func (stub *snapshotServiceStub) Delete(ctx context.Context, ref *pb.SnapshotRef) (*pb.Ok, error) {
	if stub.delete == nil {
		return nil, status.Error(codes.Unimplemented, "delete not stubbed")
	}
	return stub.delete(ctx, ref)
}

type poolServiceStub struct {
	pb.UnimplementedPoolServiceServer
	list   func(context.Context, *pb.ListPoolsRequest) (*pb.JsonView, error)
	delete func(context.Context, *pb.PoolRef) (*pb.Ok, error)
}

func (stub *poolServiceStub) List(ctx context.Context, request *pb.ListPoolsRequest) (*pb.JsonView, error) {
	if stub.list == nil {
		return nil, status.Error(codes.Unimplemented, "list not stubbed")
	}
	return stub.list(ctx, request)

}
func (stub *poolServiceStub) Delete(ctx context.Context, ref *pb.PoolRef) (*pb.Ok, error) {
	if stub.delete == nil {
		return nil, status.Error(codes.Unimplemented, "delete not stubbed")
	}
	return stub.delete(ctx, ref)
}

func TestSandboxRecoveryAndSnapshotDelete(t *testing.T) {
	var deleted string
	sandbox := &sandboxServiceStub{
		suspend: func(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
			if ref.GetId() != "box" {
				return nil, status.Error(codes.InvalidArgument, "unexpected sandbox")
			}
			return &pb.JsonView{Json: `{"id":"box","status":"suspended","desired_state":"suspended","observed_state":"suspended","state_generation":2,"lifecycle_failure":null,"ha":"async","restart_policy":"none"}`}, nil
		},
		resume: func(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
			if ref.GetId() != "box" {
				return nil, status.Error(codes.InvalidArgument, "unexpected sandbox")
			}
			return &pb.JsonView{Json: `{"id":"box","status":"running","desired_state":"running","observed_state":"running","state_generation":3,"lifecycle_failure":null,"ha":"async","restart_policy":"none"}`}, nil
		},
		history: func(_ context.Context, ref *pb.SandboxRef) (*pb.RecoveryPointList, error) {
			if ref.GetId() != "box" {
				return nil, status.Error(codes.InvalidArgument, "unexpected sandbox")
			}
			return &pb.RecoveryPointList{Points: []*pb.RecoveryPoint{{
				Name: "point-1", Kind: "checkpoint", CreatedAtUnixMillis: 123, SizeBytes: 456,
			}}}, nil
		},
		rollback: func(_ context.Context, request *pb.RollbackSandboxRequest) (*pb.JsonView, error) {
			if request.GetId() != "box" || request.GetRecoveryPoint() != "point-1" {
				return nil, status.Error(codes.InvalidArgument, "unexpected rollback")
			}
			return &pb.JsonView{Json: `{"id":"box","status":"running","desired_state":"running","observed_state":"running","state_generation":4,"lifecycle_failure":null,"ha":"async","restart_policy":"none"}`}, nil
		},
	}
	snapshots := &snapshotServiceStub{
		delete: func(_ context.Context, ref *pb.SnapshotRef) (*pb.Ok, error) {
			deleted = ref.GetName()
			return &pb.Ok{}, nil
		},
	}
	client := bufconnClient(t, startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterSandboxServiceServer(server, sandbox)
		pb.RegisterSnapshotServiceServer(server, snapshots)
	}))
	box := client.Sandboxes.Ref("box")
	if updated, err := box.Suspend(context.Background()); err != nil || updated.Status != "suspended" {
		t.Fatalf("Suspend() = %#v, %v", updated, err)
	}
	if box.DesiredState != "suspended" || box.ObservedState != "suspended" ||
		box.StateGeneration != 2 || box.LifecycleFailure != nil ||
		box.HA != "async" || box.RestartPolicy != "none" {
		t.Fatalf("Suspend() lifecycle = %#v", box)
	}
	if updated, err := box.Resume(context.Background()); err != nil || updated.ObservedState != "running" || updated.StateGeneration != 3 {
		t.Fatalf("Resume() = %#v, %v", updated, err)
	}
	points, err := box.History(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got := points; len(got) != 1 || got[0] != (RecoveryPoint{Name: "point-1", Kind: "checkpoint", CreatedAtUnixMillis: 123, SizeBytes: 456}) {
		t.Fatalf("History() = %#v", got)
	}
	if updated, err := box.Rollback(context.Background(), "point-1"); err != nil || updated.ObservedState != "running" || updated.StateGeneration != 4 {
		t.Fatalf("Rollback() = %#v, %v", updated, err)
	}
	if err := client.Snapshots.Delete(context.Background(), "snapshot-1"); err != nil {
		t.Fatal(err)
	}
	if deleted != "snapshot-1" {
		t.Fatalf("Delete() sent %q", deleted)
	}
}

func TestClientServicesAndBoundSandbox(t *testing.T) {
	var metricsID string
	stub := &sandboxServiceStub{
		create: func(_ context.Context, request *pb.CreateSandboxRequest) (*pb.JsonView, error) {
			var spec SandboxCreateRequest
			if err := json.Unmarshal([]byte(request.GetSpecJson()), &spec); err != nil ||
				spec.Image != "alpine" || len(spec.Credentials) != 1 || spec.Credentials[0] != "github-api" {
				return nil, status.Error(codes.InvalidArgument, "unexpected create spec")
			}
			return &pb.JsonView{Json: `{"id":"box","status":"running"}`}, nil
		},
		metrics: func(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
			metricsID = ref.GetId()
			return &pb.JsonView{Json: `{"cpu":1}`}, nil
		},
	}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	if client.Sandboxes == nil || client.Snapshots == nil || client.Volumes == nil || client.Pools == nil || client.Mesh == nil {
		t.Fatal("client services were not initialized")
	}
	sandbox, err := client.Sandboxes.Create(
		context.Background(),
		SandboxCreateRequest{Image: "alpine", Credentials: []string{"github-api"}},
	)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := sandbox.Metrics(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	cpu, ok := metrics.Float64("cpu")
	if !ok || cpu != 1 || metricsID != "box" {
		t.Fatalf("metrics=%v id=%q", metrics.Values, metricsID)
	}
	if sandbox.endpoint != "http://127.0.0.1:1" {
		t.Fatalf("sandbox affinity=%q", sandbox.endpoint)
	}
}

func TestTypedResponsesRejectMalformedPayloads(t *testing.T) {
	for _, body := range []string{"{}", "[]", "null", "not-json", `{"ok":true,"bad":NaN}`} {
		t.Run("health "+body, func(t *testing.T) {
			driver := &stubDriver{do: func(context.Context, DriverRequest) (*http.Response, string, error) {
				return jsonResponse(http.StatusOK, body), "http://node", nil
			}}
			_, err := NewClient(driver).Health(context.Background())
			var protocolErr *ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("error=%T %v", err, err)
			}
		})
	}

	if _, err := decodeEvent([]byte("[]")); err == nil {
		t.Fatal("event array was accepted")
	}

	stub := &sandboxServiceStub{
		create: func(context.Context, *pb.CreateSandboxRequest) (*pb.JsonView, error) {
			return &pb.JsonView{Json: `{"id":"box","status":"running"}`}, nil
		},
		metrics: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
			return &pb.JsonView{Json: "null"}, nil
		},
	}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	sandbox, err := client.Sandboxes.Create(context.Background(), SandboxCreateRequest{})
	if err != nil {
		t.Fatal(err)
	}
	_, err = sandbox.Metrics(context.Background())
	var protocolErr *ProtocolError
	if !errors.As(err, &protocolErr) {
		t.Fatalf("error=%T %v", err, err)
	}
}

func TestSandboxGetReconnectsByStableID(t *testing.T) {
	old := &sandboxServiceStub{get: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
		return nil, status.Error(codes.NotFound, "moved")
	}}
	current := &sandboxServiceStub{get: func(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
		if ref.GetId() != "box" {
			return nil, status.Error(codes.InvalidArgument, "unexpected id")
		}
		return &pb.JsonView{Json: `{"id":"box","status":"running"}`}, nil
	}}
	dialer := routingDialer(map[string]*bufconn.Listener{
		"a.test:80": startSandboxServiceStub(t, old),
		"b.test:80": startSandboxServiceStub(t, current),
	})
	driver := &stubDriver{endpoints: []EndpointInfo{
		{URL: "http://a.test", Healthy: true},
		{URL: "http://b.test", Healthy: true},
	}}
	client := NewClient(driver, withGRPCDialer(dialer))
	defer client.Close()

	sandbox, err := client.Sandboxes.Get(context.Background(), "box")
	if err != nil {
		t.Fatal(err)
	}
	if sandbox.ID != "box" || sandbox.endpoint != "http://b.test" {
		t.Fatalf("sandbox=%#v endpoint=%q", sandbox, sandbox.endpoint)
	}
}

func TestSandboxRelocatesOnceOnNotFound(t *testing.T) {
	var mu sync.Mutex
	oldGets, newGets := 0, 0
	old := &sandboxServiceStub{get: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
		mu.Lock()
		oldGets++
		mu.Unlock()
		return nil, status.Error(codes.NotFound, "moved")
	}}
	current := &sandboxServiceStub{get: func(_ context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
		mu.Lock()
		newGets++
		mu.Unlock()
		if ref.GetId() != "box" {
			return nil, status.Error(codes.InvalidArgument, "unexpected id")
		}
		return &pb.JsonView{Json: `{"id":"box","status":"running"}`}, nil
	}}
	dialer := routingDialer(map[string]*bufconn.Listener{
		"a.test:80": startSandboxServiceStub(t, old),
		"b.test:80": startSandboxServiceStub(t, current),
	})
	driver := &stubDriver{endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}}}
	client := NewClient(driver, withGRPCDialer(dialer))
	defer client.Close()
	sandbox := client.Sandboxes.Ref("box")
	sandbox.endpoint = "http://a.test"
	if _, err := sandbox.Refresh(context.Background()); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	// Initial Get + resolve probe on the old node; resolve probe + retried Get
	// on the new one.
	if oldGets != 2 || newGets != 2 || sandbox.endpoint != "http://b.test" {
		t.Fatalf("oldGets=%d newGets=%d endpoint=%q", oldGets, newGets, sandbox.endpoint)
	}
}

func TestSandboxListOnlySkipsTransportErrors(t *testing.T) {
	t.Run("transport error", func(t *testing.T) {
		live := &sandboxServiceStub{list: func(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
			return &pb.ListSandboxesResponse{SandboxesJson: []string{
				`{"id":"wanted"}`,
			}}, nil
		}}
		// a.test has no route: its dial fails and the call fails over to b.
		dialer := routingDialer(map[string]*bufconn.Listener{"b.test:80": startSandboxServiceStub(t, live)})
		driver := &stubDriver{endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}}}
		client := NewClient(driver, withGRPCDialer(dialer))
		defer client.Close()
		sandboxes, err := client.Sandboxes.List(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if len(sandboxes) != 1 || sandboxes[0].ID != "wanted" || sandboxes[0].endpoint != "http://b.test" {
			t.Fatalf("sandboxes=%#v", sandboxes)
		}
	})

	t.Run("API error", func(t *testing.T) {
		busy := &sandboxServiceStub{list: func(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
			return nil, status.Error(codes.Aborted, "try later")
		}}
		empty := &sandboxServiceStub{list: func(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
			return &pb.ListSandboxesResponse{}, nil
		}}
		dialer := routingDialer(map[string]*bufconn.Listener{
			"a.test:80": startSandboxServiceStub(t, busy),
			"b.test:80": startSandboxServiceStub(t, empty),
		})
		driver := &stubDriver{endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}}}
		client := NewClient(driver, withGRPCDialer(dialer))
		defer client.Close()
		_, err := client.Sandboxes.List(context.Background())
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.Code != "busy" {
			t.Fatalf("error=%T %v", err, err)
		}
	})

	t.Run("protocol error", func(t *testing.T) {
		invalid := &sandboxServiceStub{list: func(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
			return &pb.ListSandboxesResponse{SandboxesJson: []string{"not json"}}, nil
		}}
		client := bufconnClient(t, startSandboxServiceStub(t, invalid))
		_, err := client.Sandboxes.List(context.Background())
		var protocolErr *ProtocolError
		if !errors.As(err, &protocolErr) {
			t.Fatalf("error=%T %v", err, err)
		}
	})
}

func TestSandboxListFansOutConcurrentlyAndEmptySuccessWins(t *testing.T) {
	var active atomic.Int32
	var peak atomic.Int32
	live := &sandboxServiceStub{list: func(_ context.Context, request *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			seen := peak.Load()
			if current <= seen || peak.CompareAndSwap(seen, current) {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
		if got := request.GetTags(); len(got) != 2 || got[0] != "a=1" || got[1] != "z=2" {
			t.Errorf("tag filters=%v", got)
		}
		return &pb.ListSandboxesResponse{}, nil
	}}
	// a.test is unreachable: that fan-out leg fails over to b concurrently.
	dialer := routingDialer(map[string]*bufconn.Listener{"b.test:80": startSandboxServiceStub(t, live)})
	driver := &stubDriver{endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}}}
	client := NewClient(driver, withGRPCDialer(dialer))
	defer client.Close()
	sandboxes, err := client.Sandboxes.List(context.Background(), SandboxListOptions{Tags: map[string]string{"z": "2", "a": "1"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(sandboxes) != 0 {
		t.Fatalf("sandboxes=%v", sandboxes)
	}
	if peak.Load() != 2 {
		t.Fatalf("peak concurrent requests=%d, want 2", peak.Load())
	}
}

func TestPoolClearListsAndDeletesEveryReference(t *testing.T) {
	var mu sync.Mutex
	deleted := make(map[string]bool)
	pools := &poolServiceStub{
		list: func(context.Context, *pb.ListPoolsRequest) (*pb.JsonView, error) {
			return &pb.JsonView{Json: `{"image:one":{"ready":1},"image/two":{"ready":2}}`}, nil
		},
		delete: func(_ context.Context, ref *pb.PoolRef) (*pb.Ok, error) {
			mu.Lock()
			deleted[ref.GetReference()] = true
			mu.Unlock()
			return &pb.Ok{}, nil
		},
	}
	listener := startGRPCServices(t, func(server *grpc.Server) { pb.RegisterPoolServiceServer(server, pools) })
	if err := bufconnClient(t, listener).Pools.Clear(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(deleted) != 2 || !deleted["image:one"] || !deleted["image/two"] {
		t.Fatalf("deleted=%v", deleted)
	}
}

// TestStrictCollectionEnvelopes covers the JsonView documents whose envelope
// the client still validates. The former snapshots/volumes list subtests are
// gone with REST: their repeated proto fields cannot be absent.
func TestStrictCollectionEnvelopes(t *testing.T) {
	tests := []struct {
		name string
		body string
		call func(*Client) error
	}{
		{name: "files list", call: func(client *Client) error {
			_, err := client.Sandboxes.Ref("box").Files.List(context.Background(), ".")
			return err
		}},
		{name: "snapshot fork", call: func(client *Client) error {
			_, err := client.Snapshots.Fork(context.Background(), "base", ForkRequest{})
			return err
		}},
		{name: "pools list null", body: "null", call: func(client *Client) error {
			_, err := client.Pools.List(context.Background())
			return err
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := test.body
			if body == "" {
				body = `{}`
			}
			view := func() (*pb.JsonView, error) { return &pb.JsonView{Json: body}, nil }
			listener := startGRPCServices(t, func(server *grpc.Server) {
				pb.RegisterSandboxServiceServer(server, &sandboxServiceStub{fileList: func(context.Context, *pb.FilePathRequest) (*pb.JsonView, error) { return view() }})
				pb.RegisterSnapshotServiceServer(server, &snapshotServiceStub{fork: func(context.Context, *pb.ForkSnapshotRequest) (*pb.JsonView, error) { return view() }})
				pb.RegisterPoolServiceServer(server, &poolServiceStub{list: func(context.Context, *pb.ListPoolsRequest) (*pb.JsonView, error) { return view() }})
			})
			err := test.call(bufconnClient(t, listener))
			var protocolErr *ProtocolError
			if !errors.As(err, &protocolErr) {
				t.Fatalf("missing envelope error=%T %v", err, err)
			}
		})
	}
}

func TestExtendPreservesSandboxIDWhenResponseOmitsIt(t *testing.T) {
	stub := &sandboxServiceStub{extend: func(_ context.Context, request *pb.ExtendSandboxRequest) (*pb.JsonView, error) {
		if request.GetId() != "box" || request.GetSecs() != 30 {
			return nil, status.Error(codes.InvalidArgument, "unexpected extend request")
		}
		return &pb.JsonView{Json: `{"status":"running"}`}, nil
	}}
	sandbox := bufconnClient(t, startSandboxServiceStub(t, stub)).Sandboxes.Ref("box")
	extended, err := sandbox.Extend(context.Background(), 30)
	if err != nil {
		t.Fatal(err)
	}
	if extended.ID != "box" || sandbox.ID != "box" {
		t.Fatalf("extended ID=%q sandbox ID=%q", extended.ID, sandbox.ID)
	}
}

func TestMigrateUpdatesSandboxNode(t *testing.T) {
	stub := &sandboxServiceStub{migrate: func(_ context.Context, request *pb.MigrateRequest) (*pb.JsonView, error) {
		if request.GetId() != "box" || request.GetTarget() != "node-b" {
			return nil, status.Error(codes.InvalidArgument, "unexpected migrate request")
		}
		return &pb.JsonView{Json: `{"id":"box","node":"node-b","status":"running","migration":{"precopy_ms":12,"downtime_ms":3,"total_ms":15}}`}, nil
	}}
	sandbox := bufconnClient(t, startSandboxServiceStub(t, stub)).Sandboxes.Ref("box")
	migrated, err := sandbox.Migrate(context.Background(), "node-b")
	if err != nil {
		t.Fatal(err)
	}
	if migrated.Node != "node-b" {
		t.Fatalf("migrated node=%q", migrated.Node)
	}
	timing, ok := migrated.MigrationTiming()
	if !ok || timing.PrecopyMS != 12 || timing.DowntimeMS != 3 || timing.TotalMS != 15 {
		t.Fatalf("migration timing=%+v ok=%t", timing, ok)
	}
}

func TestPortProxyReturnsRawDownstreamNon2xx(t *testing.T) {
	driver := &stubDriver{
		endpoints: []EndpointInfo{{URL: "node", Healthy: true}},
		do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
			if request.Path != "/v1/sandboxes/box/ports/8080/status" || !request.Stream {
				t.Fatalf("request=%+v", request)
			}
			response := jsonResponse(http.StatusTeapot, "downstream failure")
			response.Header.Set("X-Downstream", "yes")
			return response, "node", nil
		},
	}
	sandbox := NewClient(driver).Sandboxes.Ref("box")
	sandbox.endpoint = "node"
	sandbox.connectToken = "secret"
	response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodGet, Path: "status", Body: strings.NewReader("")})
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	if response.StatusCode != http.StatusTeapot || response.Header.Get("X-Downstream") != "yes" || string(body) != "downstream failure" {
		t.Fatalf("status=%d header=%q body=%q", response.StatusCode, response.Header.Get("X-Downstream"), body)
	}
}

func TestPortProxyOnlyRelocatesDaemonNotFound(t *testing.T) {
	// Relocation probes are gRPC Gets: the old node reports not_found, the
	// new one hosts the sandbox.
	newFixture := func(t *testing.T) (func(context.Context, string) (net.Conn, error), *int) {
		resolves := new(int)
		var mu sync.Mutex
		old := &sandboxServiceStub{get: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
			mu.Lock()
			*resolves++
			mu.Unlock()
			return nil, status.Error(codes.NotFound, "moved")
		}}
		dialer := routingDialer(map[string]*bufconn.Listener{
			"a.test:80": startSandboxServiceStub(t, old),
			"b.test:80": startSandboxServiceStub(t, &sandboxServiceStub{}),
		})
		return dialer, resolves
	}

	t.Run("downstream 404 is not replayed", func(t *testing.T) {
		calls := 0
		dialer, resolves := newFixture(t)
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}},
			do: func(_ context.Context, _ DriverRequest) (*http.Response, string, error) {
				calls++
				return jsonResponse(http.StatusNotFound, `{"message":"app route missing"}`), "http://a.test", nil
			},
		}
		client := NewClient(driver, withGRPCDialer(dialer))
		defer client.Close()
		sandbox := client.Sandboxes.Ref("box")
		sandbox.endpoint, sandbox.connectToken = "http://a.test", "secret"
		response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodPost, Body: strings.NewReader("side effect")})
		if err != nil {
			t.Fatal(err)
		}
		body, _ := io.ReadAll(response.Body)
		_ = response.Body.Close()
		if calls != 1 || *resolves != 0 || string(body) != `{"message":"app route missing"}` {
			t.Fatalf("calls=%d resolves=%d body=%q", calls, *resolves, body)
		}
	})

	t.Run("daemon not_found relocates once", func(t *testing.T) {
		calls := 0
		dialer, resolves := newFixture(t)
		driver := &stubDriver{
			endpoints: []EndpointInfo{{URL: "http://a.test", Healthy: true}, {URL: "http://b.test", Healthy: true}},
			do: func(_ context.Context, request DriverRequest) (*http.Response, string, error) {
				calls++
				if request.Endpoint == "http://a.test" {
					return jsonResponse(http.StatusNotFound, `{"code":"not_found","message":"moved"}`), "http://a.test", nil
				}
				return jsonResponse(http.StatusCreated, "created"), "http://b.test", nil
			},
		}
		client := NewClient(driver, withGRPCDialer(dialer))
		defer client.Close()
		sandbox := client.Sandboxes.Ref("box")
		sandbox.endpoint, sandbox.connectToken = "http://a.test", "secret"
		response, err := sandbox.Ports.HTTP(context.Background(), 8080, ProxyRequest{Method: http.MethodPost, Body: strings.NewReader("side effect")})
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if calls != 2 || *resolves != 1 || response.StatusCode != http.StatusCreated || sandbox.endpoint != "http://b.test" {
			t.Fatalf("calls=%d resolves=%d status=%d endpoint=%q", calls, *resolves, response.StatusCode, sandbox.endpoint)
		}
	})
}

func TestFilesLimitsDeleteOptionsAndPathValidation(t *testing.T) {
	var deleteRequest *pb.FileDeleteRequest
	stub := &sandboxServiceStub{
		fileRead: func(context.Context, *pb.FilePathRequest) (*pb.FileContent, error) {
			return &pb.FileContent{Data: []byte("12345")}, nil
		},
		fileDelete: func(_ context.Context, request *pb.FileDeleteRequest) (*pb.Ok, error) {
			deleteRequest = request
			return &pb.Ok{}, nil
		},
	}
	client, err := Connect("http://127.0.0.1:1", WithDiscovery(false), WithMaxResponseBytes(4), withGRPCDialer(bufconnDialer(startSandboxServiceStub(t, stub))))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	files := client.Sandboxes.Ref("box").Files
	if _, err := files.Read(context.Background(), "large"); !errors.As(err, new(*ResponseTooLargeError)) {
		t.Fatalf("large read error=%T %v", err, err)
	}
	if err := files.Delete(context.Background(), "dir", DeleteOptions{Recursive: true}); err != nil {
		t.Fatal(err)
	}
	if deleteRequest.GetPath() != "dir" || !deleteRequest.GetRecursive() {
		t.Fatalf("delete request=%v", deleteRequest)
	}
	if err := files.Write(context.Background(), "", nil); err == nil {
		t.Fatal("empty write path accepted")
	}
	if _, err := files.Stat(context.Background(), ""); err == nil {
		t.Fatal("empty stat path accepted")
	}
	if err := files.Delete(context.Background(), ""); err == nil {
		t.Fatal("empty delete path accepted")
	}
	if err := files.Mkdir(context.Background(), ""); err == nil {
		t.Fatal("empty mkdir path accepted")
	}
}

func TestWaitReadyProbes(t *testing.T) {
	t.Run("mutually exclusive", func(t *testing.T) {
		sandbox := NewClient(&stubDriver{}).Sandboxes.Ref("box")
		_, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Port: 80, Command: []string{"true"}})
		if err == nil {
			t.Fatal("accepted mutually exclusive probes")
		}
	})

	t.Run("command", func(t *testing.T) {
		stub := &sandboxServiceStub{execCapture: func(_ context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
			if cmd := request.GetExec().GetCmd(); len(cmd) != 1 || cmd[0] != "check" {
				return nil, status.Error(codes.InvalidArgument, "unexpected probe command")
			}
			return &pb.ExecCaptureResponse{Code: 0}, nil
		}}
		sandbox := bufconnClient(t, startSandboxServiceStub(t, stub)).Sandboxes.Ref("box")
		if _, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Command: []string{"check"}, Timeout: time.Second, Interval: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	})

	t.Run("port", func(t *testing.T) {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		defer listener.Close()
		go func() {
			connection, acceptErr := listener.Accept()
			if acceptErr == nil {
				_ = connection.Close()
			}
		}()
		port := listener.Addr().(*net.TCPAddr).Port
		stub := &sandboxServiceStub{tunnels: func(context.Context, *pb.SandboxRef) (*pb.JsonView, error) {
			return &pb.JsonView{Json: fmt.Sprintf(`{"tunnels":{"8080":{"host":"127.0.0.1","port":%d}}}`, port)}, nil
		}}
		sandbox := bufconnClient(t, startSandboxServiceStub(t, stub)).Sandboxes.Ref("box")
		if _, err := sandbox.WaitReady(context.Background(), WaitReadyOptions{Port: 8080, Timeout: time.Second, Interval: time.Millisecond}); err != nil {
			t.Fatal(err)
		}
	})
}

func TestAPIErrorRetryableAndActionMetadata(t *testing.T) {
	err := apiErrorFromStatus(
		status.Error(codes.Aborted, "busy details"),
		"test ops",
		metadata.Pairs(
			"vmon-code", "busy",
			"vmon-retryable", "true",
			"vmon-action", "re-register",
		),
	)
	var apiErr *APIError
	if !errors.As(err, &apiErr) {
		t.Fatalf("expected APIError, got %T: %v", err, err)
	}
	if apiErr.Code != "busy" || !apiErr.Retryable || apiErr.Action != "re-register" {
		t.Fatalf("apiErr=%#v", apiErr)
	}

	errFallback := apiErrorFromStatus(
		status.Error(codes.Aborted, "busy details"),
		"test ops",
		metadata.Pairs("vmon-code", "busy"),
	)
	if !errors.As(errFallback, &apiErr) {
		t.Fatalf("expected APIError")
	}
	if !apiErr.Retryable {
		t.Fatalf("expected fallback retryable=true")
	}
}
