package vmon

import (
	"bytes"
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

type functionStub struct {
	pb.UnimplementedFunctionServiceServer
}

func (stub *functionStub) Get(context.Context, *pb.GetFunctionRequest) (*pb.FunctionRevision, error) {
	return &pb.FunctionRevision{Ref: &pb.RevisionRef{Function: &pb.FunctionRef{Namespace: "ns", Name: "double"}, RevisionId: "rev-1"}}, nil
}

type functionCallStub struct {
	pb.UnimplementedCallServiceServer
	mu            sync.Mutex
	created       *pb.CreateCallRequest
	watched       *pb.WatchCallRequest
	streamed      []*pb.CallInput
	cancelRequest *pb.CancelCallRequest
	cancelled     bool
}

func (stub *functionCallStub) Create(_ context.Context, request *pb.CreateCallRequest) (*pb.CallRecord, error) {
	stub.mu.Lock()
	stub.created = request
	stub.mu.Unlock()
	return &pb.CallRecord{Ref: &pb.CallRef{CallId: "call-stable"}, Status: pb.CallStatus_CALL_STATUS_PENDING}, nil
}

func (stub *functionCallStub) Get(_ context.Context, ref *pb.CallRef) (*pb.CallRecord, error) {
	return &pb.CallRecord{Ref: ref, Type: pb.CallType_CALL_TYPE_UNARY, InputCount: 1, InputsClosed: true, Status: pb.CallStatus_CALL_STATUS_SUCCEEDED}, nil
}

func (stub *functionCallStub) GetResult(context.Context, *pb.GetCallResultRequest) (*pb.CallResult, error) {
	value, _ := EncodeValue(42, ValueJSON, CompressionNone)
	return &pb.CallResult{Call: &pb.CallRef{CallId: "call-stable"}, Outcome: &pb.CallResult_Value{Value: value.wire}}, nil
}

func (stub *functionCallStub) Watch(request *pb.WatchCallRequest, stream grpc.ServerStreamingServer[pb.CallEvent]) error {
	stub.mu.Lock()
	stub.watched = request
	stub.mu.Unlock()
	return stream.Send(&pb.CallEvent{Call: request.Cursor.Call, Sequence: request.Cursor.AfterSequence + 1, Payload: &pb.CallEvent_Status{Status: &pb.StatusEvent{Status: pb.CallStatus_CALL_STATUS_SUCCEEDED}}})
}

func (stub *functionCallStub) Cancel(_ context.Context, request *pb.CancelCallRequest) (*pb.CallRecord, error) {
	stub.mu.Lock()
	stub.cancelled = true
	stub.cancelRequest = request
	stub.mu.Unlock()
	return &pb.CallRecord{Ref: &pb.CallRef{CallId: "call-stable"}, Status: pb.CallStatus_CALL_STATUS_CANCELLING}, nil
}

func (stub *functionCallStub) StreamInputs(stream grpc.BidiStreamingServer[pb.StreamCallInputsRequest, pb.StreamCallInputsResponse]) error {
	var count uint64
	for {
		frame, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if input := frame.GetInput(); input != nil {
			stub.mu.Lock()
			stub.streamed = append(stub.streamed, input)
			stub.mu.Unlock()
			count++
		}
		if err := stream.Send(&pb.StreamCallInputsResponse{Call: &pb.CallRef{CallId: "call-stable"}, CommittedInputCount: count}); err != nil {
			return err
		}
	}
}

func (stub *functionCallStub) CloseInputs(_ context.Context, request *pb.CloseCallInputsRequest) (*pb.CallRecord, error) {
	return &pb.CallRecord{Ref: &pb.CallRef{CallId: "call-stable"}, InputCount: request.ExpectedInputCount, InputsClosed: true}, nil
}

func TestDeployedFunctionLookupInvokeAndReconstruct(t *testing.T) {
	stub := &functionCallStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterFunctionServiceServer(server, &functionStub{})
		pb.RegisterCallServiceServer(server, stub)
	})
	client := bufconnClient(t, listener)
	function, err := LookupFunction[int](context.Background(), client, "ns", "double")
	if err != nil {
		t.Fatal(err)
	}
	if function.RevisionID() != "rev-1" {
		t.Fatalf("revision = %q", function.RevisionID())
	}
	call, err := function.Spawn(context.Background(), 21)
	if err != nil {
		t.Fatal(err)
	}
	if call.ID() != "call-stable" {
		t.Fatalf("id = %q", call.ID())
	}
	rebuilt, err := FunctionCallFromID[int](client, call.ID())
	if err != nil {
		t.Fatal(err)
	}
	result, err := rebuilt.Get(context.Background())
	if err != nil || result != 42 {
		t.Fatalf("result = %d, %v", result, err)
	}
	stub.mu.Lock()
	created := stub.created
	stub.mu.Unlock()
	if created == nil || created.Target.Function.RevisionId != "rev-1" || len(created.Inputs) != 1 {
		t.Fatalf("create = %#v", created)
	}
}

func TestMapUsesAcknowledgedBidiInputs(t *testing.T) {
	stub := &functionCallStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) {
		pb.RegisterFunctionServiceServer(server, &functionStub{})
		pb.RegisterCallServiceServer(server, stub)
	})
	client := bufconnClient(t, listener)
	function, err := LookupFunction[int](context.Background(), client, "ns", "double")
	if err != nil {
		t.Fatal(err)
	}
	inputs := make(chan any, 2)
	inputs <- 1
	inputs <- 2
	close(inputs)
	batch, err := function.Map(context.Background(), inputs)
	if err != nil {
		t.Fatal(err)
	}
	if batch.count != 2 {
		t.Fatalf("batch count = %d", batch.count)
	}
	stub.mu.Lock()
	streamed := append([]*pb.CallInput(nil), stub.streamed...)
	stub.mu.Unlock()
	if len(streamed) != 2 || streamed[0].InputId == "" || streamed[1].GetValue() == nil {
		t.Fatalf("streamed inputs = %#v", streamed)
	}
}

func TestWatchCursorAndCancel(t *testing.T) {
	stub := &functionCallStub{}
	listener := startGRPCServices(t, func(server *grpc.Server) { pb.RegisterCallServiceServer(server, stub) })
	client := bufconnClient(t, listener)
	call, _ := FunctionCallFromID[int](client, "call-stable")
	events, failures := call.Watch(context.Background(), 41, false)
	event := <-events
	if event.Sequence != 42 {
		t.Fatalf("sequence = %d", event.Sequence)
	}
	if err := <-failures; err != nil {
		t.Fatal(err)
	}
	if err := call.Cancel(context.Background(), "test"); err != nil {
		t.Fatal(err)
	}
	stub.mu.Lock()
	cancelled, request := stub.cancelled, stub.cancelRequest
	stub.mu.Unlock()
	if !cancelled {
		t.Fatal("cancel RPC not observed")
	}
	if request == nil || request.RequestId == "" {
		t.Fatal("cancel request omitted idempotency request_id")
	}
}

type portableCBORValue struct {
	Bytes []byte `cbor:"bytes"`
	Wide  uint64 `cbor:"wide"`
}

func TestRemoteDaemonPortableFunctions(t *testing.T) {
	if os.Getenv("VMON_GO_REMOTE_SMOKE") != "1" {
		t.Skip("set VMON_GO_REMOTE_SMOKE=1 to run real-daemon function smoke")
	}
	serverURL := requiredRemoteSmokeEnv(t, "VMON_SERVER_URL")
	token := requiredRemoteSmokeEnv(t, "VMON_API_TOKEN")
	namespace := requiredRemoteSmokeEnv(t, "VMON_REMOTE_NAMESPACE")
	jsonName := requiredRemoteSmokeEnv(t, "VMON_REMOTE_JSON_NAME")
	jsonRevision := requiredRemoteSmokeEnv(t, "VMON_REMOTE_JSON_REVISION")
	cborName := requiredRemoteSmokeEnv(t, "VMON_REMOTE_CBOR_NAME")
	cborRevision := requiredRemoteSmokeEnv(t, "VMON_REMOTE_CBOR_REVISION")

	client, err := Connect(serverURL, WithToken(token), WithDiscovery(false))
	if err != nil {
		t.Fatalf("connect to %s: %v", serverURL, err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	jsonFunction, err := LookupFunction[map[string]any](ctx, client, namespace, jsonName)
	if err != nil {
		t.Fatalf("lookup JSON function %s/%s: %v", namespace, jsonName, err)
	}
	if jsonFunction.RevisionID() != jsonRevision {
		t.Fatalf("JSON revision = %q, deployed %q", jsonFunction.RevisionID(), jsonRevision)
	}
	remoteJSON, err := jsonFunction.Remote(ctx, map[string]any{"language": "go", "portable": true})
	if err != nil {
		t.Fatalf("JSON Remote: %v", err)
	}
	if remoteJSON["language"] != "go" || remoteJSON["portable"] != true {
		t.Fatalf("JSON Remote result = %#v", remoteJSON)
	}
	spawned, err := jsonFunction.Spawn(ctx, map[string]any{"durable": "same-result"})
	if err != nil {
		t.Fatalf("JSON Spawn: %v", err)
	}
	callID := spawned.ID()
	if callID == "" {
		t.Fatal("JSON Spawn returned empty call ID")
	}
	reconstructed, err := FunctionCallFromID[map[string]any](client, callID)
	if err != nil {
		t.Fatalf("reconstruct call %q: %v", callID, err)
	}
	durableJSON, err := reconstructed.Get(ctx)
	if err != nil {
		t.Fatalf("get reconstructed call %q: %v", callID, err)
	}
	if durableJSON["durable"] != "same-result" {
		t.Fatalf("reconstructed JSON result = %#v", durableJSON)
	}

	cborFunction, err := LookupFunction[portableCBORValue](ctx, client, namespace, cborName)
	if err != nil {
		t.Fatalf("lookup CBOR function %s/%s: %v", namespace, cborName, err)
	}
	if cborFunction.RevisionID() != cborRevision {
		t.Fatalf("CBOR revision = %q, deployed %q", cborFunction.RevisionID(), cborRevision)
	}
	cborFunction = cborFunction.WithOptions(WithValueEncoding(ValueCBOR, CompressionNone))
	portable := portableCBORValue{Bytes: []byte{0, 1, 255}, Wide: uint64(1) << 53}
	remoteCBOR, err := cborFunction.Remote(ctx, portable)
	if err != nil {
		t.Fatalf("CBOR Remote: %v", err)
	}
	if !bytes.Equal(remoteCBOR.Bytes, portable.Bytes) || remoteCBOR.Wide != portable.Wide {
		t.Fatalf("CBOR Remote result = %#v, want %#v", remoteCBOR, portable)
	}
}

func requiredRemoteSmokeEnv(t *testing.T, name string) string {
	t.Helper()
	value := os.Getenv(name)
	if value == "" {
		t.Fatalf("%s is required when VMON_GO_REMOTE_SMOKE=1", name)
	}
	return value
}
