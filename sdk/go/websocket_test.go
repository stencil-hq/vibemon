package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"sync"
	"testing"

	ws "github.com/coder/websocket"
	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// sandboxServiceStub is an in-process SandboxService served over bufconn.
// Handlers left nil fall back to a canned view (Get) or Unimplemented.
type sandboxServiceStub struct {
	pb.UnimplementedSandboxServiceServer
	get         func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	create      func(context.Context, *pb.CreateSandboxRequest) (*pb.JsonView, error)
	list        func(context.Context, *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error)
	metrics     func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	extend      func(context.Context, *pb.ExtendSandboxRequest) (*pb.JsonView, error)
	terminate   func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	tunnels     func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	migrate     func(context.Context, *pb.MigrateRequest) (*pb.JsonView, error)
	resume      func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	suspend     func(context.Context, *pb.SandboxRef) (*pb.JsonView, error)
	history     func(context.Context, *pb.SandboxRef) (*pb.RecoveryPointList, error)
	rollback    func(context.Context, *pb.RollbackSandboxRequest) (*pb.JsonView, error)
	execCapture func(context.Context, *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error)
	fileRead    func(context.Context, *pb.FilePathRequest) (*pb.FileContent, error)
	fileWrite   func(context.Context, *pb.FileWriteRequest) (*pb.Ok, error)
	fileDelete  func(context.Context, *pb.FileDeleteRequest) (*pb.Ok, error)
	fileList    func(context.Context, *pb.FilePathRequest) (*pb.JsonView, error)
	exec        func(grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error
	shell       func(grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error
	attach      func(*pb.SandboxRef, grpc.ServerStreamingServer[pb.ExecOutput]) error
}

func (stub *sandboxServiceStub) Get(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.get == nil {
		return &pb.JsonView{Json: `{"id":"` + ref.GetId() + `","status":"running"}`}, nil
	}
	return stub.get(ctx, ref)
}
func (stub *sandboxServiceStub) Create(ctx context.Context, request *pb.CreateSandboxRequest) (*pb.JsonView, error) {
	if stub.create == nil {
		return nil, status.Error(codes.Unimplemented, "create not stubbed")
	}
	return stub.create(ctx, request)
}
func (stub *sandboxServiceStub) List(ctx context.Context, request *pb.ListSandboxesRequest) (*pb.ListSandboxesResponse, error) {
	if stub.list == nil {
		return nil, status.Error(codes.Unimplemented, "list not stubbed")
	}
	return stub.list(ctx, request)
}
func (stub *sandboxServiceStub) Metrics(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.metrics == nil {
		return nil, status.Error(codes.Unimplemented, "metrics not stubbed")
	}
	return stub.metrics(ctx, ref)
}
func (stub *sandboxServiceStub) Extend(ctx context.Context, request *pb.ExtendSandboxRequest) (*pb.JsonView, error) {
	if stub.extend == nil {
		return nil, status.Error(codes.Unimplemented, "extend not stubbed")
	}
	return stub.extend(ctx, request)
}
func (stub *sandboxServiceStub) Terminate(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.terminate == nil {
		return nil, status.Error(codes.Unimplemented, "terminate not stubbed")
	}
	return stub.terminate(ctx, ref)
}
func (stub *sandboxServiceStub) Tunnels(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.tunnels == nil {
		return nil, status.Error(codes.Unimplemented, "tunnels not stubbed")
	}
	return stub.tunnels(ctx, ref)
}
func (stub *sandboxServiceStub) Migrate(ctx context.Context, request *pb.MigrateRequest) (*pb.JsonView, error) {
	if stub.migrate == nil {
		return nil, status.Error(codes.Unimplemented, "migrate not stubbed")
	}
	return stub.migrate(ctx, request)
}

func (stub *sandboxServiceStub) Resume(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.resume == nil {
		return nil, status.Error(codes.Unimplemented, "resume not stubbed")
	}
	return stub.resume(ctx, ref)
}

func (stub *sandboxServiceStub) Suspend(ctx context.Context, ref *pb.SandboxRef) (*pb.JsonView, error) {
	if stub.suspend == nil {
		return nil, status.Error(codes.Unimplemented, "suspend not stubbed")
	}
	return stub.suspend(ctx, ref)
}
func (stub *sandboxServiceStub) History(ctx context.Context, ref *pb.SandboxRef) (*pb.RecoveryPointList, error) {
	if stub.history == nil {
		return nil, status.Error(codes.Unimplemented, "history not stubbed")
	}
	return stub.history(ctx, ref)
}
func (stub *sandboxServiceStub) Rollback(ctx context.Context, request *pb.RollbackSandboxRequest) (*pb.JsonView, error) {
	if stub.rollback == nil {
		return nil, status.Error(codes.Unimplemented, "rollback not stubbed")
	}
	return stub.rollback(ctx, request)
}
func (stub *sandboxServiceStub) ExecCapture(ctx context.Context, request *pb.ExecCaptureRequest) (*pb.ExecCaptureResponse, error) {
	if stub.execCapture == nil {
		return nil, status.Error(codes.Unimplemented, "exec capture not stubbed")
	}
	return stub.execCapture(ctx, request)
}
func (stub *sandboxServiceStub) FileRead(ctx context.Context, request *pb.FilePathRequest) (*pb.FileContent, error) {
	if stub.fileRead == nil {
		return nil, status.Error(codes.Unimplemented, "file read not stubbed")
	}
	return stub.fileRead(ctx, request)
}
func (stub *sandboxServiceStub) FileWrite(ctx context.Context, request *pb.FileWriteRequest) (*pb.Ok, error) {
	if stub.fileWrite == nil {
		return nil, status.Error(codes.Unimplemented, "file write not stubbed")
	}
	return stub.fileWrite(ctx, request)
}
func (stub *sandboxServiceStub) FileDelete(ctx context.Context, request *pb.FileDeleteRequest) (*pb.Ok, error) {
	if stub.fileDelete == nil {
		return nil, status.Error(codes.Unimplemented, "file delete not stubbed")
	}
	return stub.fileDelete(ctx, request)
}
func (stub *sandboxServiceStub) FileList(ctx context.Context, request *pb.FilePathRequest) (*pb.JsonView, error) {
	if stub.fileList == nil {
		return nil, status.Error(codes.Unimplemented, "file list not stubbed")
	}
	return stub.fileList(ctx, request)
}
func (stub *sandboxServiceStub) Exec(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
	if stub.exec == nil {
		return status.Error(codes.Unimplemented, "exec not stubbed")
	}
	return stub.exec(stream)
}
func (stub *sandboxServiceStub) Shell(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
	if stub.shell == nil {
		return status.Error(codes.Unimplemented, "shell not stubbed")
	}
	return stub.shell(stream)
}
func (stub *sandboxServiceStub) Attach(ref *pb.SandboxRef, stream grpc.ServerStreamingServer[pb.ExecOutput]) error {
	if stub.attach == nil {
		return status.Error(codes.Unimplemented, "attach not stubbed")
	}
	return stub.attach(ref, stream)
}

// startGRPCServices serves the registered services over a fresh bufconn.
func startGRPCServices(t *testing.T, register func(*grpc.Server)) *bufconn.Listener {
	t.Helper()
	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer()
	register(server)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener
}

func startSandboxServiceStub(t *testing.T, stub *sandboxServiceStub) *bufconn.Listener {
	t.Helper()
	return startGRPCServices(t, func(server *grpc.Server) { pb.RegisterSandboxServiceServer(server, stub) })
}

func bufconnDialer(listener *bufconn.Listener) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }
}

func bufconnClient(t *testing.T, listener *bufconn.Listener) *Client {
	t.Helper()
	client, err := Connect("http://127.0.0.1:1", WithDiscovery(false), withGRPCDialer(bufconnDialer(listener)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func TestProcessFrames(t *testing.T) {
	stub := &sandboxServiceStub{exec: func(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
		input, err := stream.Recv()
		if err != nil {
			return err
		}
		start := input.GetStart()
		if start == nil || len(start.GetCmd()) != 1 || start.GetCmd()[0] != "echo" || start.GetSandboxId() != "box" {
			t.Errorf("start=%v", input)
			return status.Error(codes.InvalidArgument, "unexpected start frame")
		}
		_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Chunk{Chunk: &pb.Output{Stream: pb.Stream_STREAM_STDOUT, Data: []byte("ok\n")}}})
		_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Exit{Exit: &pb.Exit{Code: 0}}})
		return nil
	}}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	process, err := client.Sandboxes.Ref("box").Exec(context.Background(), ExecRequest{Command: []string{"echo"}})
	if err != nil {
		t.Fatal(err)
	}
	event, err := process.Receive(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if string(event.Data) != "ok\n" || event.Stream != StreamStdout {
		t.Fatalf("event=%+v", event)
	}
	exit, err := process.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exit.Code != 0 {
		t.Fatalf("exit=%+v", exit)
	}
}

func TestShellConsumesReadyFrame(t *testing.T) {
	stub := &sandboxServiceStub{shell: func(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
		input, err := stream.Recv()
		if err != nil {
			return err
		}
		var params map[string]any
		if json.Unmarshal([]byte(input.GetShellParamsJson()), &params) != nil {
			t.Errorf("shell params=%q", input.GetShellParamsJson())
			return status.Error(codes.InvalidArgument, "invalid shell params")
		}
		_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Ready{Ready: &pb.Ready{SandboxId: "box-shell"}}})
		_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Exit{Exit: &pb.Exit{Code: 7}}})
		return nil
	}}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	process, err := client.Shell(context.Background(), ShellRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if process.SandboxID != "box-shell" {
		t.Fatalf("sandbox=%q", process.SandboxID)
	}
	exit, err := process.Wait(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if exit.Code != 7 {
		t.Fatalf("exit=%d", exit.Code)
	}
}

func TestUnaryErrorsCarryVmonCode(t *testing.T) {
	stub := &sandboxServiceStub{get: func(ctx context.Context, _ *pb.SandboxRef) (*pb.JsonView, error) {
		_ = grpc.SetTrailer(ctx, metadata.Pairs(vmonCodeMetadataKey, "busy"))
		return nil, status.Error(codes.Aborted, "sandbox is busy")
	}}
	client := bufconnClient(t, startSandboxServiceStub(t, stub))
	_, err := client.Sandboxes.Get(context.Background(), "box")
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Code != "busy" || apiErr.Message != "sandbox is busy" {
		t.Fatalf("err=%T %v", err, err)
	}
}

// relocationDriver serves the ports-proxy WebSocket path of the relocation
// test; gRPC traffic bypasses it entirely.
type relocationDriver struct {
	target string
	mu     sync.Mutex
	dials  []string
}

func (driver *relocationDriver) Do(context.Context, DriverRequest) (*http.Response, string, error) {
	return nil, "", io.EOF
}
func (driver *relocationDriver) Dial(ctx context.Context, path string, query url.Values, endpoint string) (*WebSocketConn, string, error) {
	driver.mu.Lock()
	driver.dials = append(driver.dials, endpoint)
	driver.mu.Unlock()
	if endpoint == "http://old.test" {
		return nil, "http://old.test", &APIError{StatusCode: http.StatusNotFound, Code: "not_found", Message: "moved"}
	}
	target := driver.target + path
	if encoded := query.Encode(); encoded != "" {
		target += "?" + encoded
	}
	connection, response, err := ws.Dial(ctx, target, nil)
	if response != nil && response.Body != nil {
		response.Body.Close()
	}
	if err != nil {
		return nil, endpoint, err
	}
	return &WebSocketConn{conn: connection}, endpoint, nil
}
func (driver *relocationDriver) Endpoints() []EndpointInfo {
	return []EndpointInfo{{URL: "http://old.test", Healthy: true}, {URL: "http://new.test", Healthy: true}}
}
func (driver *relocationDriver) Refresh(context.Context, bool) error { return nil }
func (driver *relocationDriver) Close() error                        { return nil }

func TestSandboxWebSocketsRelocateOnceOnNotFound(t *testing.T) {
	newStub := func(t *testing.T) (*sandboxServiceStub, *sandboxServiceStub, func(context.Context, string) (net.Conn, error), *int) {
		resolves := new(int)
		var mu sync.Mutex
		old := &sandboxServiceStub{get: func(ctx context.Context, _ *pb.SandboxRef) (*pb.JsonView, error) {
			mu.Lock()
			*resolves++
			mu.Unlock()
			_ = grpc.SetTrailer(ctx, metadata.Pairs(vmonCodeMetadataKey, "not_found"))
			return nil, status.Error(codes.NotFound, "moved")
		}}
		current := &sandboxServiceStub{}
		oldListener := startSandboxServiceStub(t, old)
		newListener := startSandboxServiceStub(t, current)
		dialer := func(ctx context.Context, address string) (net.Conn, error) {
			if address == "old.test:80" {
				return oldListener.DialContext(ctx)
			}
			return newListener.DialContext(ctx)
		}
		return old, current, dialer, resolves
	}

	t.Run("exec", func(t *testing.T) {
		_, current, dialer, resolves := newStub(t)
		current.exec = func(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
			input, err := stream.Recv()
			if err != nil {
				return err
			}
			if input.GetStart().GetSandboxId() != "box" {
				return status.Error(codes.InvalidArgument, "missing sandbox id")
			}
			_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Exit{Exit: &pb.Exit{Code: 0}}})
			return nil
		}
		driver := &relocationDriver{}
		sandbox := NewClient(driver, withGRPCDialer(dialer)).Sandboxes.Ref("box")
		sandbox.endpoint = "http://old.test"
		process, err := sandbox.Exec(context.Background(), ExecRequest{Command: []string{"true"}})
		if err != nil {
			t.Fatal(err)
		}
		defer process.Close()
		exit, err := process.Wait(context.Background())
		if err != nil || exit.Code != 0 {
			t.Fatalf("exit=%+v err=%v", exit, err)
		}
		if *resolves != 1 || sandbox.endpoint != "http://new.test" {
			t.Fatalf("resolves=%d endpoint=%q", *resolves, sandbox.endpoint)
		}
	})

	t.Run("attach", func(t *testing.T) {
		_, current, dialer, resolves := newStub(t)
		current.attach = func(ref *pb.SandboxRef, stream grpc.ServerStreamingServer[pb.ExecOutput]) error {
			if ref.GetId() != "box" {
				return status.Error(codes.InvalidArgument, "unexpected sandbox id")
			}
			_ = stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Chunk{Chunk: &pb.Output{Stream: pb.Stream_STREAM_CONSOLE, Data: []byte("hi")}}})
			return nil
		}
		driver := &relocationDriver{}
		sandbox := NewClient(driver, withGRPCDialer(dialer)).Sandboxes.Ref("box")
		sandbox.endpoint = "http://old.test"
		console, err := sandbox.Attach(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		defer console.Close()
		event, err := console.Receive(context.Background())
		if err != nil || string(event.Data) != "hi" || event.Stream != StreamConsole {
			t.Fatalf("event=%+v err=%v", event, err)
		}
		if *resolves != 1 || sandbox.endpoint != "http://new.test" {
			t.Fatalf("resolves=%d endpoint=%q", *resolves, sandbox.endpoint)
		}
	})

	t.Run("port", func(t *testing.T) {
		_, _, dialer, resolves := newStub(t)
		requests := make(chan *http.Request, 1)
		server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			connection, err := ws.Accept(writer, request, nil)
			if err != nil {
				return
			}
			defer connection.CloseNow()
			requests <- request.Clone(context.Background())
			_, _, _ = connection.Read(request.Context())
		}))
		defer server.Close()
		driver := &relocationDriver{target: server.URL}
		sandbox := NewClient(driver, withGRPCDialer(dialer)).Sandboxes.Ref("box")
		sandbox.endpoint = "http://old.test"
		sandbox.connectToken = "secret"

		socket, err := sandbox.Ports.WebSocket(context.Background(), 8080, "chat", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer socket.Close()
		request := <-requests
		if request.URL.Path != "/v1/sandboxes/box/ports/8080/ws/chat" {
			t.Fatalf("path=%q", request.URL.Path)
		}
		if request.URL.Query().Get("connect_token") != "secret" {
			t.Fatalf("connect token=%q", request.URL.Query().Get("connect_token"))
		}
		driver.mu.Lock()
		dials := append([]string(nil), driver.dials...)
		driver.mu.Unlock()
		if len(dials) != 2 || dials[0] != "http://old.test" || dials[1] != "http://new.test" ||
			*resolves != 1 || sandbox.endpoint != "http://new.test" {
			t.Fatalf("dials=%v resolves=%d endpoint=%q", dials, *resolves, sandbox.endpoint)
		}
	})
}
