package vmon

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

// ErrCallCancelled reports a distinct durable cancellation outcome.
var ErrCallCancelled = errors.New("vmon: remote call cancelled")

// FunctionOption alters durable call creation without mutating a function handle.
type FunctionOption func(*callOptions)
type callOptions struct {
	labels      map[string]string
	resultTTL   *uint64
	codec       ValueCodec
	compression ValueCompression
}

// WithCallLabels attaches lookup metadata to subsequently created calls.
func WithCallLabels(labels map[string]string) FunctionOption {
	return func(o *callOptions) { o.labels = cloneStrings(labels) }
}

// WithResultTTLMillis overrides durable result retention.
func WithResultTTLMillis(milliseconds uint64) FunctionOption {
	return func(o *callOptions) { o.resultTTL = &milliseconds }
}

// WithValueEncoding selects portable argument serialization and compression.
func WithValueEncoding(codec ValueCodec, compression ValueCompression) FunctionOption {
	return func(o *callOptions) { o.codec, o.compression = codec, compression }
}

// Function is a deployed logical function pinned to an immutable revision.
type Function[T any] struct {
	client   *Client
	ref      *pb.RevisionRef
	endpoint string
	options  callOptions
}

// LookupFunction resolves the current deployed revision.
func LookupFunction[T any](ctx context.Context, client *Client, namespace, name string) (*Function[T], error) {
	selector := &pb.FunctionSelector{Selection: &pb.FunctionSelector_Current{Current: &pb.FunctionRef{Namespace: namespace, Name: name}}}
	return lookupFunction[T](ctx, client, selector)
}

// LookupFunctionRevision resolves an exact immutable deployed revision.
func LookupFunctionRevision[T any](ctx context.Context, client *Client, namespace, name, revisionID string) (*Function[T], error) {
	selector := &pb.FunctionSelector{Selection: &pb.FunctionSelector_Pinned{Pinned: &pb.RevisionRef{Function: &pb.FunctionRef{Namespace: namespace, Name: name}, RevisionId: revisionID}}}
	return lookupFunction[T](ctx, client, selector)
}

func lookupFunction[T any](ctx context.Context, client *Client, selector *pb.FunctionSelector) (*Function[T], error) {
	if client == nil {
		return nil, errors.New("vmon: nil client")
	}
	var revision *pb.FunctionRevision
	endpoint, err := client.unary(ctx, "", "get function", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		revision, err = pb.NewFunctionServiceClient(conn).Get(ctx, &pb.GetFunctionRequest{Function: selector}, opts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	if revision == nil || revision.Ref == nil {
		return nil, errors.New("vmon: function lookup returned no revision")
	}
	return &Function[T]{client: client, ref: revision.Ref, endpoint: endpoint, options: callOptions{codec: ValueJSON}}, nil
}

// WithOptions derives a handle with call options; the pinned revision is unchanged.
func (function *Function[T]) WithOptions(options ...FunctionOption) *Function[T] {
	copy := *function
	copy.options.labels = cloneStrings(function.options.labels)
	for _, option := range options {
		if option != nil {
			option(&copy.options)
		}
	}
	return &copy
}

// RevisionID returns the immutable deployed revision identifier.
func (function *Function[T]) RevisionID() string {
	if function == nil || function.ref == nil {
		return ""
	}
	return function.ref.RevisionId
}

// Spawn durably creates a unary call and returns immediately.
func (function *Function[T]) Spawn(ctx context.Context, input any) (*FunctionCall[T], error) {
	return function.spawn(ctx, pb.CallType_CALL_TYPE_UNARY, []any{input}, true)
}

// Remote invokes a deployed function and waits for its durable result.
func (function *Function[T]) Remote(ctx context.Context, input any) (T, error) {
	call, err := function.spawn(ctx, pb.CallType_CALL_TYPE_UNARY, []any{input}, true)
	if err != nil {
		var zero T
		return zero, err
	}
	return call.Get(ctx)
}

func (function *Function[T]) spawn(ctx context.Context, kind pb.CallType, values []any, closed bool) (*FunctionCall[T], error) {
	if function == nil || function.client == nil || function.ref == nil {
		return nil, errors.New("vmon: invalid function handle")
	}
	inputs := make([]*pb.CallInput, len(values))
	codec := function.options.codec
	if codec == 0 {
		codec = ValueJSON
	}
	for index, value := range values {
		envelope, err := EncodeValue(value, codec, function.options.compression)
		if err != nil {
			return nil, err
		}
		inputs[index] = &pb.CallInput{Index: uint64(index), InputId: fmt.Sprintf("%d", index), Payload: &pb.CallInput_Value{Value: envelope.wire}}
	}
	requestID, err := randomID()
	if err != nil {
		return nil, err
	}
	request := &pb.CreateCallRequest{Type: kind, Target: &pb.CallTarget{Function: function.ref}, Inputs: inputs, InputsClosed: closed, RequestId: requestID, Labels: cloneStrings(function.options.labels)}
	if function.options.resultTTL != nil {
		request.ResultTtlMillisPresence = &pb.CreateCallRequest_ResultTtlMillis{ResultTtlMillis: *function.options.resultTTL}
	}
	var record *pb.CallRecord
	endpoint, err := function.client.unary(ctx, function.endpoint, "create call", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		record, err = pb.NewCallServiceClient(conn).Create(ctx, request, opts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	if record == nil || record.Ref == nil {
		return nil, errors.New("vmon: create call returned no id")
	}
	return &FunctionCall[T]{client: function.client, id: record.Ref.CallId, endpoint: endpoint}, nil
}

// FunctionCall is a durable call handle reconstructible from its stable ID.
type FunctionCall[T any] struct {
	client     *Client
	id         string
	endpointMu sync.RWMutex
	endpoint   string
}

// CallStatus is the public durable call lifecycle state.
type CallStatus uint8

const (
	CallStatusPending CallStatus = iota + 1
	CallStatusQueued
	CallStatusRunning
	CallStatusSucceeded
	CallStatusFailed
	CallStatusCancelling
	CallStatusCancelled
)

func publicCallStatus(status pb.CallStatus) CallStatus { return CallStatus(status) }

// AttemptStatus identifies one durable execution-attempt transition.
type AttemptStatus uint8

const (
	// AttemptStatusUnspecified means the attempt state is unavailable.
	AttemptStatusUnspecified AttemptStatus = iota
	// AttemptStatusStarted means a worker accepted the attempt.
	AttemptStatusStarted
	// AttemptStatusSucceeded means the attempt committed its result.
	AttemptStatusSucceeded
	// AttemptStatusFailed means the attempt ended with an error.
	AttemptStatusFailed
	// AttemptStatusCancelled means the attempt stopped due to cancellation.
	AttemptStatusCancelled
)

// StartupKind identifies how the worker for an attempt was prepared.
type StartupKind uint8

const (
	// StartupUnspecified means startup information is unavailable.
	StartupUnspecified StartupKind = iota
	// StartupCold means a new worker was created and booted.
	StartupCold
	// StartupWarm means an initialized worker was reused.
	StartupWarm
	// StartupSnapshot means a worker was restored from a snapshot.
	StartupSnapshot
)

// AttemptFailureKind classifies why an execution attempt ended.
type AttemptFailureKind uint8

const (
	// AttemptFailureUnspecified means the attempt did not fail or the class is unavailable.
	AttemptFailureUnspecified AttemptFailureKind = iota
	// AttemptFailureUser identifies a policy-visible user-code failure.
	AttemptFailureUser
	// AttemptFailureInfrastructure identifies a transparent platform failure.
	AttemptFailureInfrastructure
	// AttemptFailureCancelled identifies cooperative or forced cancellation.
	AttemptFailureCancelled
)

// AttemptStats contains durable per-attempt timing and resource usage.
type AttemptStats struct {
	AttemptID                                                                string
	UserAttempt, InfraAttempt                                                uint32
	Startup                                                                  StartupKind
	FailureKind                                                              AttemptFailureKind
	QueuedMillis, StartupMillis, ExecutionMillis, CPUMillis, PeakMemoryBytes uint64
}

// CallStats contains durable aggregate execution statistics.
type CallStats struct {
	QueueMillis, StartupMillis, ExecutionMillis, WallMillis, CPUMillis, PeakMemoryBytes uint64
	Attempts                                                                            []AttemptStats
}

// ParentEdge identifies one exact parent call and input.
type ParentEdge struct{ CallID, InputID string }

// CallGraph contains durable call ancestry with optional root presence.
type CallGraph struct {
	Parents    []ParentEdge
	RootCallID *string
}

// FunctionCallFromID reconstructs a durable call handle in any process.
func FunctionCallFromID[T any](client *Client, id string) (*FunctionCall[T], error) {
	if client == nil || id == "" {
		return nil, errors.New("vmon: client and call id are required")
	}
	call := &FunctionCall[T]{client: client, id: id}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := call.record(ctx); err != nil {
		return nil, err
	}
	return call, nil
}

// ID returns the stable server-assigned call identifier.
func (call *FunctionCall[T]) ID() string {
	if call == nil {
		return ""
	}
	return call.id
}

// RemoteCallError is a structured language-neutral execution failure.
type RemoteCallError struct {
	Code, Message, Type string
	Retryable           bool
	Details             map[string]string
	Frames              []ErrorFrame
	Cause               *RemoteCallError
}

// ErrorFrame is one language-neutral remote stack frame.
// Unwrap exposes the structured remote cause.
func (err *RemoteCallError) Unwrap() error {
	if err == nil || err.Cause == nil {
		return nil
	}
	return err.Cause
}

type ErrorFrame struct {
	File     string
	Line     uint32
	Function string
	Code     *string
}

func (err *RemoteCallError) Error() string {
	if err == nil {
		return "<nil>"
	}
	return fmt.Sprintf("vmon remote call (%s): %s", err.Code, err.Message)
}
func callError(value *pb.CallError) error {
	if value == nil {
		return errors.New("vmon: remote call failed")
	}
	result := &RemoteCallError{Code: value.Code, Message: value.Message, Type: value.Type, Retryable: value.Retryable, Details: cloneStrings(value.Details)}
	for _, frame := range value.Frames {
		item := ErrorFrame{File: frame.File, Line: frame.Line, Function: frame.Function}
		if frame.GetCodePresence() != nil {
			code := frame.GetCode()
			item.Code = &code
		}
		result.Frames = append(result.Frames, item)
	}
	if value.GetCause() != nil {
		result.Cause = callError(value.GetCause()).(*RemoteCallError)
	}
	return result
}
func (call *FunctionCall[T]) Status(ctx context.Context) (CallStatus, error) {
	record, err := call.record(ctx)
	if err != nil {
		return 0, err
	}
	return publicCallStatus(record.Status), nil
}

// Stats returns durable aggregate execution statistics.
func (call *FunctionCall[T]) Stats(ctx context.Context) (*CallStats, error) {
	record, err := call.record(ctx)
	if err != nil {
		return nil, err
	}
	if record.Stats == nil {
		return nil, nil
	}
	source := record.Stats
	result := &CallStats{QueueMillis: source.QueueMillis, StartupMillis: source.StartupMillis, ExecutionMillis: source.ExecutionMillis, WallMillis: source.WallMillis, CPUMillis: source.CpuMillis, PeakMemoryBytes: source.PeakMemoryBytes}
	for _, attempt := range source.Attempts {
		result.Attempts = append(result.Attempts, AttemptStats{AttemptID: attempt.AttemptId, UserAttempt: attempt.UserAttempt, InfraAttempt: attempt.InfraAttempt, Startup: StartupKind(attempt.Startup), FailureKind: AttemptFailureKind(attempt.FailureKind), QueuedMillis: attempt.QueuedMillis, StartupMillis: attempt.StartupMillis, ExecutionMillis: attempt.ExecutionMillis, CPUMillis: attempt.CpuMillis, PeakMemoryBytes: attempt.PeakMemoryBytes})
	}
	return result, nil
}

// Graph returns a copy of the durable parent/root relationship.
func (call *FunctionCall[T]) Graph(ctx context.Context) (*CallGraph, error) {
	record, err := call.record(ctx)
	if err != nil {
		return nil, err
	}
	if record.Graph == nil {
		return nil, nil
	}
	result := &CallGraph{}
	for _, parent := range record.Graph.Parents {
		result.Parents = append(result.Parents, ParentEdge{CallID: parent.CallId, InputID: parent.InputId})
	}
	if record.Graph.GetRootCallIdPresence() != nil {
		root := record.Graph.GetRootCallId()
		result.RootCallID = &root
	}
	return result, nil
}
func (call *FunctionCall[T]) endpointHint() string {
	call.endpointMu.RLock()
	defer call.endpointMu.RUnlock()
	return call.endpoint
}
func (call *FunctionCall[T]) setEndpoint(endpoint string) {
	call.endpointMu.Lock()
	call.endpoint = endpoint
	call.endpointMu.Unlock()
}

func (call *FunctionCall[T]) record(ctx context.Context) (*pb.CallRecord, error) {
	var record *pb.CallRecord
	endpoint, err := call.client.unary(ctx, call.endpointHint(), "get call", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		record, err = pb.NewCallServiceClient(conn).Get(ctx, &pb.CallRef{CallId: call.id}, opts...)
		return err
	})
	if err == nil {
		call.setEndpoint(endpoint)
	}
	return record, err
}

// Cancel durably requests cancellation.
func (call *FunctionCall[T]) Cancel(ctx context.Context, reason string) error {
	requestID, idErr := randomID()
	if idErr != nil {
		return idErr
	}
	endpoint, err := call.client.unary(ctx, call.endpointHint(), "cancel call", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		_, err := pb.NewCallServiceClient(conn).Cancel(ctx, &pb.CancelCallRequest{Call: &pb.CallRef{CallId: call.id}, Reason: reason, RequestId: requestID}, opts...)
		return err
	})
	if err == nil {
		call.setEndpoint(endpoint)
	}
	return err
}

// CallEventKind identifies the durable event payload variant.
type CallEventKind uint8

const (
	CallEventStatus CallEventKind = iota + 1
	CallEventLog
	CallEventYield
	CallEventResult
	CallEventAttempt
	CallEventError
	CallEventInputClosed
	CallEventCancelRequested
)

// CallLogStream identifies a call log payload source.
type CallLogStream uint8

// AttemptEvent describes one execution attempt transition.
type AttemptEvent struct {
	AttemptID                 string
	UserAttempt, InfraAttempt uint32
	Status                    AttemptStatus
	Startup                   StartupKind
	FailureKind               AttemptFailureKind
	WorkerID                  string
	Error                     *RemoteCallError
}

// ResultEvent identifies one committed result without decoding its payload.
type ResultEvent struct {
	InputID              string
	InputIndex, Sequence uint64
	YieldIndex           *uint64
	Error                *RemoteCallError
}

// InputClosedEvent reports the committed input frontier.
type InputClosedEvent struct {
	CommittedInputCount  uint64
	LastInputID          *string
	LastInputIndex       *uint64
	MaxInputsOutstanding uint32
}

// CallEvent is a reconnectable durable event and its complete typed payload.
type CallEvent struct {
	Sequence            uint64
	CreatedAtUnixMillis uint64
	Kind                CallEventKind
	Status              CallStatus
	Log                 []byte
	LogStream           CallLogStream
	InputID             *string
	InputIndex          *uint64
	YieldIndex          *uint64
	AttemptID           *string
	AttemptEvent        *AttemptEvent
	Result              *ResultEvent
	InputClosed         *InputClosedEvent
	CancelReason        *string
	Error               *RemoteCallError
}

func (call *FunctionCall[T]) watchRequest(afterSequence uint64, follow bool) *pb.WatchCallRequest {
	return &pb.WatchCallRequest{Cursor: &pb.EventCursor{Call: &pb.CallRef{CallId: call.id}, AfterSequence: afterSequence}, Follow: follow}
}

// Watch streams observable events and resumes after transport failover.
func (call *FunctionCall[T]) Watch(ctx context.Context, afterSequence uint64, follow bool) (<-chan CallEvent, <-chan error) {
	return call.watch(ctx, afterSequence, follow)
}
func (call *FunctionCall[T]) watch(ctx context.Context, afterSequence uint64, follow bool) (<-chan CallEvent, <-chan error) {
	events := make(chan CallEvent, 16)
	failures := make(chan error, 1)
	go func() {
		defer close(events)
		defer close(failures)
		cursor := afterSequence
		hint := call.endpointHint()
		for {
			endpoint, err := call.client.grpcEndpoint(hint)
			if err != nil {
				failures <- err
				return
			}
			conn, err := call.client.conn(endpoint)
			if err != nil {
				failures <- err
				return
			}
			request := call.watchRequest(cursor, follow)
			stream, err := pb.NewCallServiceClient(conn).Watch(ctx, request)
			if err == nil {
				call.setEndpoint(endpoint)
				hint = endpoint
				for {
					event, recvErr := stream.Recv()
					if recvErr != nil {
						err = recvErr
						break
					}
					if event.Sequence <= cursor {
						continue
					}
					cursor = event.Sequence
					item := projectCallEvent(event)
					select {
					case events <- item:
					case <-ctx.Done():
						failures <- ctx.Err()
						return
					}
					if item.Status == CallStatusSucceeded || item.Status == CallStatusFailed || item.Status == CallStatusCancelled {
						return
					}
				}
			}
			if err != nil && !errors.Is(err, io.EOF) {
				mapped := apiErrorFromStatus(err, "watch call")
				var transportErr *TransportError
				if !errors.As(mapped, &transportErr) {
					failures <- mapped
					return
				}
				if health, ok := call.client.driver.(interface{ markFailed(string) }); ok {
					health.markFailed(endpoint)
				}
			} else if err == nil {
				if health, ok := call.client.driver.(interface{ markSuccess(string) }); ok {
					health.markSuccess(endpoint)
				}
			}
			if !follow && err == io.EOF {
				return
			}
			if ctx.Err() != nil {
				failures <- ctx.Err()
				return
			}
			hint = ""
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				failures <- ctx.Err()
				return
			}
		}
	}()
	return events, failures
}

func projectCallEvent(event *pb.CallEvent) CallEvent {
	item := CallEvent{Sequence: event.Sequence, CreatedAtUnixMillis: event.CreatedAtUnixMillis, Kind: CallEventKind(event.Type)}
	if event.GetInputIdPresence() != nil {
		value := event.GetInputId()
		item.InputID = &value
	}
	if event.GetInputIndexPresence() != nil {
		value := event.GetInputIndex()
		item.InputIndex = &value
	}
	if event.GetAttemptIdPresence() != nil {
		value := event.GetAttemptId()
		item.AttemptID = &value
	}
	if status := event.GetStatus(); status != nil {
		item.Status = publicCallStatus(status.Status)
	}
	if log := event.GetLog(); log != nil {
		item.Log = append([]byte(nil), log.Data...)
		item.LogStream = CallLogStream(log.Stream)
	}
	if failure := event.GetError(); failure != nil {
		item.Error = callError(failure).(*RemoteCallError)
	}
	if attempt := event.GetAttemptEvent(); attempt != nil {
		info := &AttemptEvent{AttemptID: attempt.AttemptId, UserAttempt: attempt.UserAttempt, InfraAttempt: attempt.InfraAttempt, Status: AttemptStatus(attempt.Status), Startup: StartupKind(attempt.Startup), FailureKind: AttemptFailureKind(attempt.FailureKind), WorkerID: attempt.WorkerId}
		if attempt.GetError() != nil {
			info.Error = callError(attempt.GetError()).(*RemoteCallError)
		}
		item.AttemptEvent = info
	}
	setResult := func(result *pb.CallResult) {
		inputID := result.GetInputId()
		item.InputID = &inputID
		inputIndex := result.GetInputIndex()
		item.InputIndex = &inputIndex
		info := &ResultEvent{InputID: inputID, InputIndex: inputIndex, Sequence: result.Sequence}
		if result.GetYieldIndexPresence() != nil {
			yieldIndex := result.GetYieldIndex()
			item.YieldIndex = &yieldIndex
			info.YieldIndex = &yieldIndex
		}
		if result.GetError() != nil {
			info.Error = callError(result.GetError()).(*RemoteCallError)
		}
		item.Result = info
	}
	if result := event.GetResult(); result != nil {
		setResult(result)
	}
	if result := event.GetYieldResult(); result != nil {
		setResult(result)
	}
	if closed := event.GetInputClosed(); closed != nil {
		info := &InputClosedEvent{CommittedInputCount: closed.CommittedInputCount, MaxInputsOutstanding: closed.MaxInputsOutstanding}
		if closed.GetLastInputPresence() != nil {
			last := closed.GetLastInput()
			id := last.InputId
			index := last.InputIndex
			info.LastInputID = &id
			info.LastInputIndex = &index
		}
		item.InputClosed = info
	}
	if cancel := event.GetCancelRequested(); cancel != nil {
		reason := cancel.Reason
		item.CancelReason = &reason
	}
	return item
}

// Get waits for terminal completion and decodes result index zero.
func (call *FunctionCall[T]) Get(ctx context.Context) (T, error) {
	var zero T
	events, failures := call.watch(ctx, 0, true)
	for event := range events {
		if event.Status == CallStatusFailed {
			record, err := call.record(ctx)
			if err != nil {
				return zero, err
			}
			return zero, callError(record.GetError())
		}
		if event.Status == CallStatusCancelled {
			return zero, ErrCallCancelled
		}
		if event.Status == CallStatusSucceeded {
			return call.Result(ctx, 0)
		}
	}
	if err := <-failures; err != nil {
		return zero, err
	}
	return zero, errors.New("vmon: call event stream ended before completion")
}

// Result retrieves and decodes one durable indexed result.
func (call *FunctionCall[T]) Result(ctx context.Context, index uint64) (T, error) {
	var zero T
	var result *pb.CallResult
	endpoint, err := call.client.unary(ctx, call.endpointHint(), "get call result", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		result, err = pb.NewCallServiceClient(conn).GetResult(ctx, &pb.GetCallResultRequest{Call: &pb.CallRef{CallId: call.id}, Index: index}, opts...)
		return err
	})
	if err != nil {
		return zero, err
	}
	call.setEndpoint(endpoint)
	if result.GetError() != nil {
		return zero, callError(result.GetError())
	}
	if result.GetValue() == nil {
		return zero, errors.New("vmon: result has no value")
	}
	err = envelopeFromWire(result.GetValue()).Decode(&zero, func(ref *ArtifactReference) ([]byte, error) { return call.loadArtifact(ctx, ref) })
	return zero, err
}

func (call *FunctionCall[T]) loadArtifact(ctx context.Context, ref *ArtifactReference) ([]byte, error) {
	endpoint, err := call.client.grpcEndpoint(call.endpointHint())
	if err != nil {
		return nil, err
	}
	conn, err := call.client.conn(endpoint)
	if err != nil {
		return nil, err
	}
	stream, err := pb.NewArtifactServiceClient(conn).Get(ctx, &pb.GetArtifactRequest{Artifact: &pb.ArtifactRef{Digest: &pb.Digest{Algorithm: pb.DigestAlgorithm_DIGEST_ALGORITHM_SHA256, Value: append([]byte(nil), ref.Digest...)}}})
	if err != nil {
		return nil, err
	}
	var data []byte
	var offset uint64
	for {
		chunk, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		if chunk.Offset != offset {
			return nil, fmt.Errorf("vmon: artifact chunk offset %d, expected %d", chunk.Offset, offset)
		}
		data = append(data, chunk.Data...)
		offset += uint64(len(chunk.Data))
		if chunk.Eof {
			break
		}
	}
	call.setEndpoint(endpoint)
	return data, nil
}

// BatchCall is a durable batch handle with indexed result retrieval.
type BatchCall[T any] struct {
	*FunctionCall[T]
	count uint64
}

// BatchCallFromID reconstructs a batch and obtains its committed input count.
func BatchCallFromID[T any](ctx context.Context, client *Client, id string) (*BatchCall[T], error) {
	call, err := FunctionCallFromID[T](client, id)
	if err != nil {
		return nil, err
	}
	record, err := call.record(ctx)
	if err != nil {
		return nil, err
	}
	if record.Type != pb.CallType_CALL_TYPE_BATCH {
		return nil, fmt.Errorf("vmon: call %q is not a batch", id)
	}
	return &BatchCall[T]{FunctionCall: call, count: record.InputCount}, nil
}

// SpawnMap creates a closed durable batch without owning workers or result buffers.
func (function *Function[T]) SpawnMap(ctx context.Context, inputs []any) (*BatchCall[T], error) {
	call, err := function.spawn(ctx, pb.CallType_CALL_TYPE_BATCH, inputs, true)
	if err != nil {
		return nil, err
	}
	return &BatchCall[T]{FunctionCall: call, count: uint64(len(inputs))}, nil
}

// Map commits a channel of inputs with bounded, one-at-a-time submission pressure.
// It returns after the input channel closes and the server acknowledges closure.
func (function *Function[T]) Map(ctx context.Context, inputs <-chan any) (*BatchCall[T], error) {
	call, err := function.spawn(ctx, pb.CallType_CALL_TYPE_BATCH, nil, false)
	if err != nil {
		return nil, err
	}
	completed := false
	defer func() {
		if !completed {
			boundedCancel(call, "input submission failed")
		}
	}()
	open := func(hint string) (grpc.BidiStreamingClient[pb.StreamCallInputsRequest, pb.StreamCallInputsResponse], string, uint64, error) {
		endpoint, err := function.client.grpcEndpoint(hint)
		if err != nil {
			return nil, "", 0, err
		}
		conn, err := function.client.conn(endpoint)
		if err != nil {
			return nil, "", 0, err
		}
		stream, err := pb.NewCallServiceClient(conn).StreamInputs(ctx)
		if err != nil {
			return nil, "", 0, err
		}
		if err := stream.Send(&pb.StreamCallInputsRequest{Frame: &pb.StreamCallInputsRequest_Call{Call: &pb.CallRef{CallId: call.id}}}); err != nil {
			return nil, "", 0, err
		}
		ack, err := stream.Recv()
		if err != nil {
			return nil, "", 0, err
		}
		return stream, endpoint, ack.CommittedInputCount, nil
	}
	stream, endpoint, count, err := open(call.endpointHint())
	if err != nil {
		return nil, err
	}
	for {
		select {
		case <-ctx.Done():
			boundedCancel(call, ctx.Err().Error())
			return nil, ctx.Err()
		case input, ok := <-inputs:
			if !ok {
				_ = stream.CloseSend()
				var record *pb.CallRecord
				_, err = function.client.unary(ctx, endpoint, "close call inputs", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
					var closeErr error
					record, closeErr = pb.NewCallServiceClient(conn).CloseInputs(ctx, &pb.CloseCallInputsRequest{Call: &pb.CallRef{CallId: call.id}, ExpectedInputCount: count}, opts...)
					return closeErr
				})
				if err != nil {
					return nil, err
				}
				_ = record
				completed = true
				call.setEndpoint(endpoint)
				return &BatchCall[T]{FunctionCall: call, count: count}, nil
			}
			codec := function.options.codec
			if codec == 0 {
				codec = ValueJSON
			}
			envelope, err := EncodeValue(input, codec, function.options.compression)
			if err != nil {
				return nil, err
			}
			inputID := fmt.Sprintf("%d", count)
			frame := &pb.StreamCallInputsRequest{Frame: &pb.StreamCallInputsRequest_Input{Input: &pb.CallInput{Index: count, InputId: inputID, Payload: &pb.CallInput_Value{Value: envelope.wire}}}}
			for {
				sendErr := stream.Send(frame)
				var ack *pb.StreamCallInputsResponse
				if sendErr == nil {
					ack, sendErr = stream.Recv()
				}
				if sendErr == nil {
					if ack.CommittedInputCount != count+1 {
						return nil, fmt.Errorf("vmon: committed input count %d, expected %d", ack.CommittedInputCount, count+1)
					}
					count = ack.CommittedInputCount
					break
				}
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				var committed uint64
				stream, endpoint, committed, err = open("")
				if err != nil {
					return nil, err
				}
				if committed > count+1 {
					return nil, fmt.Errorf("vmon: committed input frontier advanced from %d to %d", count, committed)
				}
				if committed == count+1 {
					count = committed
					break
				}
				if committed != count {
					return nil, fmt.Errorf("vmon: committed input frontier regressed from %d to %d", count, committed)
				}
			}
		}
	}
}

// Results streams committed values lazily in durable result-sequence order.
func (batch *BatchCall[T]) Results(ctx context.Context) (<-chan T, <-chan error) {
	values := make(chan T, 16)
	failures := make(chan error, 1)
	entries, entryErrors := batch.CompletionResults(ctx)
	go func() {
		defer close(values)
		defer close(failures)
		for entry := range entries {
			select {
			case values <- entry.Value:
			case <-ctx.Done():
				failures <- ctx.Err()
				return
			}
		}
		if err := <-entryErrors; err != nil {
			failures <- err
		}
	}()
	return values, failures
}

// IndexedResult is one durable batch result in completion order.
type IndexedResult[T any] struct {
	InputID    string
	InputIndex uint64
	Sequence   uint64
	Value      T
}

// CompletionResults pages durable results lazily and resumes from result sequence.
func (batch *BatchCall[T]) CompletionResults(ctx context.Context) (<-chan IndexedResult[T], <-chan error) {
	results := make(chan IndexedResult[T], 16)
	failures := make(chan error, 1)
	go func() {
		defer close(results)
		defer close(failures)
		cursor := &pb.ResultCursor{Call: &pb.CallRef{CallId: batch.id}}
		for {
			var page *pb.ListCallResultsResponse
			endpoint, err := batch.client.unary(ctx, batch.endpointHint(), "list call results", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
				var callErr error
				page, callErr = pb.NewCallServiceClient(conn).ListResults(ctx, &pb.ListCallResultsRequest{Cursor: cursor, PageSize: 256}, opts...)
				return callErr
			})
			if err != nil {
				failures <- err
				return
			}
			batch.setEndpoint(endpoint)
			for _, result := range page.Results {
				if result.GetError() != nil {
					failures <- callError(result.GetError())
					return
				}
				var value T
				if result.GetValue() == nil {
					failures <- errors.New("vmon: batch result has no value")
					return
				}
				if err := envelopeFromWire(result.GetValue()).Decode(&value, func(ref *ArtifactReference) ([]byte, error) { return batch.loadArtifact(ctx, ref) }); err != nil {
					failures <- err
					return
				}
				entry := IndexedResult[T]{InputID: result.GetInputId(), InputIndex: result.GetInputIndex(), Sequence: result.Sequence, Value: value}
				select {
				case results <- entry:
				case <-ctx.Done():
					failures <- ctx.Err()
					return
				}
			}
			if page.NextCursor != nil {
				cursor = page.NextCursor
			}
			if !page.End {
				continue
			}
			record, err := batch.record(ctx)
			if err != nil {
				failures <- err
				return
			}
			batch.count = record.InputCount
			switch record.Status {
			case pb.CallStatus_CALL_STATUS_SUCCEEDED:
				return
			case pb.CallStatus_CALL_STATUS_FAILED:
				failures <- callError(record.GetError())
				return
			case pb.CallStatus_CALL_STATUS_CANCELLED:
				failures <- ErrCallCancelled
				return
			}
			select {
			case <-time.After(50 * time.Millisecond):
			case <-ctx.Done():
				boundedCancel(batch.FunctionCall, ctx.Err().Error())
				failures <- ctx.Err()
				return
			}
		}
	}()
	return results, failures
}

// GatherCalls waits for durable calls in argument order and returns the first error.
func GatherCalls[T any](ctx context.Context, calls ...*FunctionCall[T]) ([]T, error) {
	results := make([]T, len(calls))
	for index, call := range calls {
		result, err := call.Get(ctx)
		if err != nil {
			return nil, err
		}
		results[index] = result
	}
	return results, nil
}

// App identifies a deployed application revision and its pinned function bindings.
type App struct {
	Namespace, Name, RevisionID string
	client                      *Client
	endpoint                    string
	bindings                    map[string]*pb.RevisionRef
}

// LookupApp resolves the current application revision.
func LookupApp(ctx context.Context, client *Client, namespace, name string) (*App, error) {
	selector := &pb.AppSelector{Selection: &pb.AppSelector_Current{Current: &pb.AppRef{Namespace: namespace, Name: name}}}
	return lookupApp(ctx, client, namespace, name, selector)
}

// LookupAppRevision resolves an exact immutable application revision.
func LookupAppRevision(ctx context.Context, client *Client, namespace, name, revisionID string) (*App, error) {
	selector := &pb.AppSelector{Selection: &pb.AppSelector_Pinned{Pinned: &pb.AppRevisionRef{App: &pb.AppRef{Namespace: namespace, Name: name}, RevisionId: revisionID}}}
	return lookupApp(ctx, client, namespace, name, selector)
}
func lookupApp(ctx context.Context, client *Client, namespace, name string, selector *pb.AppSelector) (*App, error) {
	if client == nil {
		return nil, errors.New("vmon: nil client")
	}
	var revision *pb.AppRevision
	endpoint, err := client.unary(ctx, "", "get app", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var err error
		revision, err = pb.NewFunctionServiceClient(conn).GetApp(ctx, &pb.GetAppRequest{App: selector}, opts...)
		return err
	})
	if err != nil {
		return nil, err
	}
	ref := revision.GetRef()
	if ref == nil {
		return nil, errors.New("vmon: app lookup returned no revision")
	}
	app := &App{Namespace: namespace, Name: name, RevisionID: ref.RevisionId, client: client, endpoint: endpoint, bindings: make(map[string]*pb.RevisionRef, len(revision.Functions))}
	for _, binding := range revision.Functions {
		if binding != nil && binding.Revision != nil {
			app.bindings[binding.Name] = binding.Revision
		}
	}
	return app, nil
}

// AppFunction returns one application-local pinned function binding.
func AppFunction[T any](app *App, name string) (*Function[T], error) {
	if app == nil || app.client == nil {
		return nil, errors.New("vmon: invalid app handle")
	}
	revision := app.bindings[name]
	if revision == nil {
		return nil, fmt.Errorf("vmon: app function binding %q not found", name)
	}
	return &Function[T]{client: app.client, ref: revision, endpoint: app.endpoint, options: callOptions{codec: ValueJSON}}, nil
}

func boundedCancel[T any](call *FunctionCall[T], reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = call.Cancel(ctx, reason)
}

func randomID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("vmon: generate request id: %w", err)
	}
	return hex.EncodeToString(random), nil
}

func cloneStrings(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	copy := make(map[string]string, len(values))
	for key, value := range values {
		copy[key] = value
	}
	return copy
}
