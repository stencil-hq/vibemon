package vmon

import (
	"context"
	"reflect"
	"testing"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type freestyleCoreSandboxStub struct {
	pb.UnimplementedSandboxServiceServer
	ptyOpen  func(grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error
	ptyList  func(context.Context, *pb.SandboxRef) (*pb.PtySessionList, error)
	ptyClose func(context.Context, *pb.PtyCloseRequest) (*pb.PtySessionCloseResponse, error)
	ptyExec  func(context.Context, *pb.PtyExecRequest) (*pb.PtyExecResponse, error)
}

func (stub *freestyleCoreSandboxStub) PtyOpen(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
	return stub.ptyOpen(stream)
}
func (stub *freestyleCoreSandboxStub) PtyList(ctx context.Context, request *pb.SandboxRef) (*pb.PtySessionList, error) {
	return stub.ptyList(ctx, request)
}
func (stub *freestyleCoreSandboxStub) PtyClose(ctx context.Context, request *pb.PtyCloseRequest) (*pb.PtySessionCloseResponse, error) {
	return stub.ptyClose(ctx, request)
}
func (stub *freestyleCoreSandboxStub) PtyExec(ctx context.Context, request *pb.PtyExecRequest) (*pb.PtyExecResponse, error) {
	return stub.ptyExec(ctx, request)
}

func TestPtyStreamFramesAndMetadata(t *testing.T) {
	stub := &freestyleCoreSandboxStub{}
	stub.ptyOpen = func(stream grpc.BidiStreamingServer[pb.ExecInput, pb.ExecOutput]) error {
		first, err := stream.Recv()
		if err != nil {
			return err
		}
		start := first.GetPtyOpen()
		if start.GetSandboxId() != "box" || start.GetSessionId() != "stable" ||
			start.GetCols() != 100 || start.GetRows() != 40 || start.GetExec() != "bash" ||
			start.GetWorkdir() != "/work" || !reflect.DeepEqual(start.GetEnv(), map[string]string{"A": "B"}) {
			return status.Errorf(codes.InvalidArgument, "start=%v", start)
		}
		exec := "bash"
		if err := stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Pty{Pty: &pb.PtySession{
			SessionId: "stable", Running: true, Cols: 100, Rows: 40, Exec: &exec,
			CreatedAtUnixMillis: 123, AttachedCount: 1,
		}}}); err != nil {
			return err
		}
		write, err := stream.Recv()
		if err != nil {
			return err
		}
		if string(write.GetStdin()) != "hello" {
			return status.Errorf(codes.InvalidArgument, "stdin=%q", write.GetStdin())
		}
		resize, err := stream.Recv()
		if err != nil {
			return err
		}
		if resize.GetResize().GetRows() != 50 || resize.GetResize().GetCols() != 120 {
			return status.Errorf(codes.InvalidArgument, "resize=%v", resize.GetResize())
		}
		if err := stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Chunk{Chunk: &pb.Output{
			Stream: pb.Stream_STREAM_STDOUT, Data: []byte("ok"),
		}}}); err != nil {
			return err
		}
		return stream.Send(&pb.ExecOutput{Output: &pb.ExecOutput_Exit{Exit: &pb.Exit{Code: 9}}})
	}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterSandboxServiceServer(server, stub)
	})
	client := bufconnClient(t, listener)
	stream, err := client.Sandboxes.Ref("box").PtyOpen(context.Background(), PtyOpenOptions{
		SessionID: "stable", Cols: 100, Rows: 40, Exec: "bash",
		Env: map[string]string{"A": "B"}, Workdir: "/work",
	})
	if err != nil {
		t.Fatal(err)
	}
	if stream.Info.SessionID != "stable" || stream.Info.CreatedAtUnixMillis != 123 {
		t.Fatalf("metadata=%+v", stream.Info)
	}
	if err := stream.Write(context.Background(), []byte("hello")); err != nil {
		t.Fatal(err)
	}
	if err := stream.Resize(context.Background(), 50, 120); err != nil {
		t.Fatal(err)
	}
	event, err := stream.Receive(context.Background())
	if err != nil || string(event.Data) != "ok" || event.Stream != StreamStdout {
		t.Fatalf("event=%+v err=%v", event, err)
	}
	exit, err := stream.Wait(context.Background())
	if err != nil || exit.Code != 9 {
		t.Fatalf("exit=%+v err=%v", exit, err)
	}
}

func TestPtyUnaryWrappers(t *testing.T) {
	exitCode := int64(4)
	timeout := float64(2.5)
	stub := &freestyleCoreSandboxStub{}
	stub.ptyList = func(_ context.Context, request *pb.SandboxRef) (*pb.PtySessionList, error) {
		if request.GetId() != "box" {
			t.Errorf("list id=%q", request.GetId())
		}
		return &pb.PtySessionList{Sessions: []*pb.PtySession{{
			SessionId: "session", Running: false, ExitCode: &exitCode, Cols: 80, Rows: 24,
		}}}, nil
	}
	stub.ptyClose = func(_ context.Context, request *pb.PtyCloseRequest) (*pb.PtySessionCloseResponse, error) {
		if request.GetId() != "box" || request.GetSessionId() != "session" {
			t.Errorf("close=%+v", request)
		}
		return &pb.PtySessionCloseResponse{SessionId: request.GetSessionId(), ExitCode: &exitCode}, nil
	}
	stub.ptyExec = func(_ context.Context, request *pb.PtyExecRequest) (*pb.PtyExecResponse, error) {
		if request.GetId() != "box" || request.GetSessionId() != "session" ||
			request.GetCommand() != "pwd" || request.GetTimeout() != timeout {
			t.Errorf("exec=%+v", request)
		}
		return &pb.PtyExecResponse{Code: 0, Stdout: []byte("/work\n")}, nil
	}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterSandboxServiceServer(server, stub)
	})
	sandbox := bufconnClient(t, listener).Sandboxes.Ref("box")
	rows, err := sandbox.PtyList(context.Background())
	if err != nil || len(rows) != 1 || rows[0].SessionID != "session" || *rows[0].ExitCode != 4 {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	closed, err := sandbox.PtyClose(context.Background(), "session")
	if err != nil || closed.SessionID != "session" || *closed.ExitCode != 4 {
		t.Fatalf("closed=%+v err=%v", closed, err)
	}
	result, err := sandbox.PtyExec(context.Background(), "session", "pwd", &timeout)
	if err != nil || result.ExitCode != 0 || string(result.Stdout) != "/work\n" {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

type freestyleCoreVpcStub struct {
	pb.UnimplementedVpcServiceServer
	created *pb.VpcCreateRequest
	deleted string
}

func (stub *freestyleCoreVpcStub) Create(_ context.Context, request *pb.VpcCreateRequest) (*pb.Vpc, error) {
	stub.created = request
	return &pb.Vpc{Id: "vpc-1", Name: request.GetName(), Cidr: request.GetCidr(), CreatedAtUnixMillis: 42}, nil
}
func (stub *freestyleCoreVpcStub) List(context.Context, *pb.ListVpcsRequest) (*pb.VpcList, error) {
	return &pb.VpcList{Vpcs: []*pb.Vpc{{Id: "vpc-1", Name: "private", Cidr: "10.1.0.0/16"}}}, nil
}
func (stub *freestyleCoreVpcStub) Delete(_ context.Context, request *pb.VpcRef) (*pb.Ok, error) {
	stub.deleted = request.GetId()
	return &pb.Ok{}, nil
}

func TestVpcServiceWrappers(t *testing.T) {
	stub := &freestyleCoreVpcStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterVpcServiceServer(server, stub)
	})
	service := bufconnClient(t, listener).Vpcs()
	created, err := service.Create(context.Background(), VPCCreateOptions{Name: "private", CIDR: "10.1.0.0/16"})
	if err != nil || created.ID != "vpc-1" || created.CreatedAtUnixMillis != 42 {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	if stub.created.GetName() != "private" || stub.created.GetCidr() != "10.1.0.0/16" {
		t.Fatalf("request=%+v", stub.created)
	}
	rows, err := service.List(context.Background())
	if err != nil || len(rows) != 1 || rows[0].ID != "vpc-1" {
		t.Fatalf("rows=%+v err=%v", rows, err)
	}
	if err := service.Delete(context.Background(), "vpc-1"); err != nil {
		t.Fatal(err)
	}
	if stub.deleted != "vpc-1" {
		t.Fatalf("deleted=%q", stub.deleted)
	}
}
