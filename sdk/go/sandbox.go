package vmon

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

func sandboxPath(id string) string { return "/v1/sandboxes/" + escapePathSegment(id) }
func (client *Client) bindSandbox(sandbox *Sandbox, endpoint, operation string) (*Sandbox, error) {
	if sandbox == nil || sandbox.ID == "" {
		return nil, &ProtocolError{Operation: operation, Message: "sandbox response has no id"}
	}
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}
func (sandbox *Sandbox) initServices() {
	sandbox.Files = &Files{sandbox: sandbox}
	sandbox.Ports = &Ports{sandbox: sandbox}
}

// invoke runs one SandboxService unary RPC with endpoint affinity, retrying
// once through mesh relocation when the pinned node no longer hosts the
// sandbox.
func (sandbox *Sandbox) invoke(ctx context.Context, operation string, call func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error) error {
	if sandbox == nil || sandbox.client == nil {
		return errors.New("vmon: sandbox is not bound to a client")
	}
	execute := func() error {
		endpoint, err := sandbox.client.unary(ctx, sandbox.endpoint, operation, func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
			return call(ctx, pb.NewSandboxServiceClient(conn), opts...)
		})
		if endpoint != "" {
			sandbox.endpoint = endpoint
		}
		return err
	}
	err := execute()
	if err == nil || !isNotFoundAPIError(err) || len(sandbox.client.driver.Endpoints()) <= 1 {
		return err
	}
	endpoint, resolveErr := sandbox.client.resolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if resolveErr != nil {
		return resolveErr
	}
	sandbox.endpoint = endpoint
	return execute()
}

// view runs a JsonView-returning sandbox RPC and yields the raw document.
func (sandbox *Sandbox) view(ctx context.Context, operation string, call func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error)) ([]byte, error) {
	var view *pb.JsonView
	err := sandbox.invoke(ctx, operation, func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		var callErr error
		view, callErr = call(ctx, service, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	return []byte(view.GetJson()), nil
}

// pinEndpoint returns the endpoint sandbox streams should be opened against,
// resolving mesh relocation up front when several endpoints are known.
func (sandbox *Sandbox) pinEndpoint(ctx context.Context) (string, error) {
	if sandbox == nil || sandbox.client == nil || sandbox.client.driver == nil {
		return "", errors.New("vmon: sandbox is not bound to a client")
	}
	if len(sandbox.client.driver.Endpoints()) <= 1 {
		if sandbox.endpoint != "" {
			return sandbox.endpoint, nil
		}
		return sandbox.client.grpcEndpoint("")
	}
	endpoint, err := sandbox.client.resolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if err != nil {
		return "", err
	}
	sandbox.endpoint = endpoint
	return endpoint, nil
}

// streamConn returns the gRPC connection sandbox streams should use.
func (sandbox *Sandbox) streamConn(ctx context.Context) (*grpc.ClientConn, error) {
	endpoint, err := sandbox.pinEndpoint(ctx)
	if err != nil {
		return nil, err
	}
	return sandbox.client.conn(endpoint)
}

// Refresh updates the sandbox state by fetching metadata from the server.
func (sandbox *Sandbox) Refresh(ctx context.Context) (*Sandbox, error) {
	body, err := sandbox.view(ctx, "get sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Get(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
	if err != nil {
		return nil, err
	}
	var out Sandbox
	if err := decodeJSONView(body, "get sandbox", &out); err != nil {
		return nil, err
	}
	endpoint, client := sandbox.endpoint, sandbox.client
	*sandbox = out
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}

func execStartProto(request ExecRequest) *pb.ExecStart {
	start := &pb.ExecStart{Cmd: request.Command, Env: request.Env, Tty: request.TTY}
	if request.Workdir != "" {
		workdir := request.Workdir
		start.Workdir = &workdir
	}
	if request.Timeout != nil {
		timeout := *request.Timeout
		start.Timeout = &timeout
	}
	return start
}

// Run executes a command and captures its output.
func (sandbox *Sandbox) Run(ctx context.Context, request ExecRequest) (ExecResult, error) {
	if len(request.Command) == 0 {
		return ExecResult{}, errors.New("vmon: exec command must not be empty")
	}
	var out *pb.ExecCaptureResponse
	err := sandbox.invoke(ctx, "run", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		var callErr error
		out, callErr = service.ExecCapture(ctx, &pb.ExecCaptureRequest{Id: sandbox.ID, Exec: execStartProto(request)}, opts...)
		return callErr
	})
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{ExitCode: out.GetCode(), Stdout: out.GetStdout(), Stderr: out.GetStderr()}, nil
}

// Logs retrieves the current standard output and standard error logs of the sandbox.
func (sandbox *Sandbox) Logs(ctx context.Context) (string, error) {
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return "", err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := pb.NewSandboxServiceClient(conn).Logs(streamCtx, &pb.LogsRequest{Id: sandbox.ID, Follow: false})
	if err != nil {
		return "", apiErrorFromStatus(err, "get logs")
	}
	var builder strings.Builder
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return builder.String(), nil
		}
		if err != nil {
			return "", apiErrorFromStatus(err, "get logs", stream.Trailer())
		}
		builder.Write(chunk.GetData())
	}
}

// LogStream incrementally decodes follow-log chunks.
type LogStream struct {
	cancel context.CancelFunc
	stream grpc.ServerStreamingClient[pb.LogChunk]
}

// Next returns the next decoded log chunk.
func (stream *LogStream) Next() ([]byte, error) {
	if stream == nil || stream.stream == nil {
		return nil, errors.New("vmon: log stream is not open")
	}
	chunk, err := stream.stream.Recv()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		return nil, apiErrorFromStatus(err, "follow logs", stream.stream.Trailer())
	}
	return chunk.GetData(), nil
}

// Close closes the underlying log stream.
func (stream *LogStream) Close() error {
	if stream == nil || stream.cancel == nil {
		return nil
	}
	stream.cancel()
	return nil
}

// FollowLogs opens a stream to receive real-time sandbox logs.
func (sandbox *Sandbox) FollowLogs(ctx context.Context) (*LogStream, error) {
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := pb.NewSandboxServiceClient(conn).Logs(streamCtx, &pb.LogsRequest{Id: sandbox.ID, Follow: true})
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, "follow logs")
	}
	return &LogStream{cancel: cancel, stream: stream}, nil
}

// Metrics retrieves resource utilization metrics for the sandbox.
func (sandbox *Sandbox) Metrics(ctx context.Context) (SandboxMetrics, error) {
	body, err := sandbox.view(ctx, "get metrics", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Metrics(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
	if err != nil {
		return SandboxMetrics{}, err
	}
	var out SandboxMetrics
	err = decodeJSONView(body, "get metrics", &out)
	return out, err
}
func (sandbox *Sandbox) action(ctx context.Context, operation string, call func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error)) (*Sandbox, error) {
	body, err := sandbox.view(ctx, operation, call)
	if err != nil {
		return nil, err
	}
	var out Sandbox
	if err := decodeJSONView(body, operation, &out); err != nil {
		return nil, err
	}
	endpoint := sandbox.endpoint
	client := sandbox.client
	if out.ID == "" {
		out.ID = sandbox.ID
	}
	if out.Name == "" {
		out.Name = sandbox.Name
	}
	*sandbox = out
	sandbox.client = client
	sandbox.endpoint = endpoint
	sandbox.initServices()
	return sandbox, nil
}

// Stop halts the execution of the sandbox.
func (sandbox *Sandbox) Stop(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, "stop sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Stop(ctx, &pb.StopSandboxRequest{Id: sandbox.ID}, opts...)
	})
}

// Terminate immediately halts the sandbox and releases all its resources.
func (sandbox *Sandbox) Terminate(ctx context.Context) error {
	return sandbox.invoke(ctx, "terminate sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		_, err := service.Terminate(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
		return err
	})
}

// Remove deletes the sandbox record; separately managed volumes remain.
func (sandbox *Sandbox) Remove(ctx context.Context) error {
	return sandbox.invoke(ctx, "remove sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		_, err := service.Remove(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
		return err
	})
}

// Pause quiesces the sandbox's virtual CPUs in memory.
func (sandbox *Sandbox) Pause(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, "pause sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Pause(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
}

// Resume reactivates a paused sandbox or restores a durably suspended sandbox.
func (sandbox *Sandbox) Resume(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, "resume sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Resume(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
}

// Suspend durably checkpoints and releases the live VM while preserving its identity.
func (sandbox *Sandbox) Suspend(ctx context.Context) (*Sandbox, error) {
	return sandbox.action(ctx, "suspend sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Suspend(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
}

// Resize changes CPU or memory and grows the root disk.
func (sandbox *Sandbox) Resize(ctx context.Context, options ResizeOptions) (*Sandbox, error) {
	request := &pb.ResizeSandboxRequest{Id: sandbox.ID}
	if options.CPUs != 0 {
		request.Cpus = &options.CPUs
	}
	if options.MemoryMiB != 0 {
		request.MemoryMib = &options.MemoryMiB
	}
	if options.DiskMB != 0 {
		request.DiskMb = &options.DiskMB
	}
	return sandbox.action(ctx, "resize sandbox", func(
		ctx context.Context,
		service pb.SandboxServiceClient,
		opts ...grpc.CallOption,
	) (*pb.JsonView, error) {
		return service.Resize(ctx, request, opts...)
	})
}

// PtyList lists persistent terminal sessions owned by this sandbox.
func (sandbox *Sandbox) PtyList(ctx context.Context) ([]PtySessionInfo, error) {
	var response *pb.PtySessionList
	err := sandbox.invoke(ctx, "list pty sessions", func(
		ctx context.Context,
		service pb.SandboxServiceClient,
		opts ...grpc.CallOption,
	) error {
		var callErr error
		response, callErr = service.PtyList(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	sessions := response.GetSessions()
	out := make([]PtySessionInfo, 0, len(sessions))
	for _, session := range sessions {
		out = append(out, ptySessionInfo(session))
	}
	return out, nil
}

// PtyClose terminates one persistent terminal session.
func (sandbox *Sandbox) PtyClose(ctx context.Context, sessionID string) (PtyCloseResult, error) {
	if sessionID == "" {
		return PtyCloseResult{}, errors.New("vmon: pty session id must not be empty")
	}
	var response *pb.PtySessionCloseResponse
	err := sandbox.invoke(ctx, "close pty session", func(
		ctx context.Context,
		service pb.SandboxServiceClient,
		opts ...grpc.CallOption,
	) error {
		var callErr error
		response, callErr = service.PtyClose(
			ctx,
			&pb.PtyCloseRequest{Id: sandbox.ID, SessionId: sessionID},
			opts...,
		)
		return callErr
	})
	if err != nil {
		return PtyCloseResult{}, err
	}
	return PtyCloseResult{SessionID: response.GetSessionId(), ExitCode: response.ExitCode}, nil
}

// PtyExec runs a captured command as a sibling in a persistent terminal context.
func (sandbox *Sandbox) PtyExec(
	ctx context.Context,
	sessionID string,
	command string,
	timeout *float64,
) (ExecResult, error) {
	if sessionID == "" {
		return ExecResult{}, errors.New("vmon: pty session id must not be empty")
	}
	if command == "" {
		return ExecResult{}, errors.New("vmon: pty exec command must not be empty")
	}
	var response *pb.PtyExecResponse
	err := sandbox.invoke(ctx, "exec in pty session", func(
		ctx context.Context,
		service pb.SandboxServiceClient,
		opts ...grpc.CallOption,
	) error {
		var callErr error
		response, callErr = service.PtyExec(ctx, &pb.PtyExecRequest{
			Id: sandbox.ID, SessionId: sessionID, Command: command, Timeout: timeout,
		}, opts...)
		return callErr
	})
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		ExitCode: response.GetCode(),
		Stdout:   response.GetStdout(),
		Stderr:   response.GetStderr(),
	}, nil
}

func ptySessionInfo(session *pb.PtySession) PtySessionInfo {
	return PtySessionInfo{
		SessionID:           session.GetSessionId(),
		Running:             session.GetRunning(),
		ExitCode:            session.ExitCode,
		Cols:                session.GetCols(),
		Rows:                session.GetRows(),
		Exec:                session.Exec,
		CreatedAtUnixMillis: session.GetCreatedAtUnixMillis(),
		AttachedCount:       session.GetAttachedCount(),
		Suspended:           session.GetSuspended(),
	}
}

// History lists immutable disk and checkpoint recovery points from oldest to newest.
func (sandbox *Sandbox) History(ctx context.Context) ([]RecoveryPoint, error) {
	var response *pb.RecoveryPointList
	err := sandbox.invoke(ctx, "sandbox history", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		var callErr error
		response, callErr = service.History(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	points := response.GetPoints()
	out := make([]RecoveryPoint, 0, len(points))
	for _, point := range points {
		out = append(out, RecoveryPoint{
			Name:                point.GetName(),
			Kind:                point.GetKind(),
			CreatedAtUnixMillis: point.GetCreatedAtUnixMillis(),
			SizeBytes:           point.GetSizeBytes(),
		})
	}
	return out, nil
}

// Rollback restores this sandbox identity after its replacement is ready to cut over.
func (sandbox *Sandbox) Rollback(ctx context.Context, recoveryPoint string) (*Sandbox, error) {
	if recoveryPoint == "" {
		return nil, errors.New("vmon: recovery point is required")
	}
	return sandbox.action(ctx, "rollback sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Rollback(ctx, &pb.RollbackSandboxRequest{Id: sandbox.ID, RecoveryPoint: recoveryPoint}, opts...)
	})
}

// Extend increases the lease duration of the sandbox by the specified seconds.
func (sandbox *Sandbox) Extend(ctx context.Context, seconds uint64) (*Sandbox, error) {
	return sandbox.action(ctx, "extend sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Extend(ctx, &pb.ExtendSandboxRequest{Id: sandbox.ID, Secs: seconds}, opts...)
	})
}

// SetIdleTimeout replaces and rearms the network-idle suspension policy; zero disables it.
func (sandbox *Sandbox) SetIdleTimeout(ctx context.Context, seconds float64) (*Sandbox, error) {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) || seconds < 0 {
		return nil, errors.New("vmon: idle timeout seconds must be non-negative")
	}
	return sandbox.action(ctx, "set sandbox idle timeout", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.SetIdleTimeout(ctx, &pb.SetIdleTimeoutRequest{Id: sandbox.ID, IdleTimeoutSecs: &seconds}, opts...)
	})
}

// Migrate moves the sandbox to a mesh node and re-pins its serving endpoint.
func (sandbox *Sandbox) Migrate(ctx context.Context, target string) (*Sandbox, error) {
	if target == "" {
		return nil, errors.New("vmon: target node id is required")
	}
	updated, err := sandbox.action(ctx, "migrate sandbox", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Migrate(ctx, &pb.MigrateRequest{Id: sandbox.ID, Target: target}, opts...)
	})
	if err != nil {
		return nil, err
	}
	if _, err := sandbox.pinEndpoint(ctx); err != nil {
		return nil, err
	}
	return updated, nil
}

// Snapshot captures the current filesystem and memory state of the sandbox.
func (sandbox *Sandbox) Snapshot(ctx context.Context, request SnapshotRequest) (string, error) {
	message := &pb.SnapshotRequest{Id: sandbox.ID, Stop: request.Stop}
	if request.Name != "" {
		name := request.Name
		message.Name = &name
	}
	body, err := sandbox.view(ctx, "snapshot", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Snapshot(ctx, message, opts...)
	})
	if err != nil {
		return "", err
	}
	var out SnapshotResult
	if err := decodeJSONView(body, "snapshot", &out); err != nil {
		return "", err
	}
	if out.Snapshot == "" {
		return "", &ProtocolError{Operation: "snapshot", Message: "response did not include snapshot"}
	}
	return out.Snapshot, nil
}

// SnapshotFilesystem captures the current filesystem image of the sandbox.
func (sandbox *Sandbox) SnapshotFilesystem(ctx context.Context, request FilesystemSnapshotRequest) (string, error) {
	message := &pb.SnapshotFsRequest{Id: sandbox.ID}
	if request.Name != "" {
		name := request.Name
		message.Name = &name
	}
	body, err := sandbox.view(ctx, "filesystem snapshot", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.SnapshotFs(ctx, message, opts...)
	})
	if err != nil {
		return "", err
	}
	var out FilesystemSnapshotResult
	if err := decodeJSONView(body, "filesystem snapshot", &out); err != nil {
		return "", err
	}
	if out.Image == "" {
		return "", &ProtocolError{Operation: "filesystem snapshot", Message: "response did not include image"}
	}
	return out.Image, nil
}

// Network retrieves the active network state and policy for the sandbox.
func (sandbox *Sandbox) Network(ctx context.Context) (NetworkState, error) {
	var out NetworkState
	body, err := sandbox.view(ctx, "get network", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.NetworkGet(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
	if err != nil {
		return out, err
	}
	err = decodeJSONView(body, "get network", &out)
	return out, err
}

// SetNetwork updates the network policy of the sandbox.
func (sandbox *Sandbox) SetNetwork(ctx context.Context, policy NetworkPolicy) (NetworkState, error) {
	var out NetworkState
	request := &pb.NetworkSetRequest{Id: sandbox.ID, BlockNetwork: policy.BlockNetwork}
	if policy.CIDRAllow != nil {
		request.CidrAllow = &pb.StringList{Values: *policy.CIDRAllow}
	}
	if policy.DomainAllow != nil {
		request.DomainAllow = &pb.StringList{Values: *policy.DomainAllow}
	}
	body, err := sandbox.view(ctx, "set network", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.NetworkSet(ctx, request, opts...)
	})
	if err != nil {
		return out, err
	}
	err = decodeJSONView(body, "set network", &out)
	return out, err
}

// Tunnels retrieves active proxy tunnel endpoints and updates the connect token.
func (sandbox *Sandbox) Tunnels(ctx context.Context) (TunnelSet, error) {
	var out TunnelSet
	body, err := sandbox.view(ctx, "get tunnels", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.Tunnels(ctx, &pb.SandboxRef{Id: sandbox.ID}, opts...)
	})
	if err != nil {
		return out, err
	}
	if err = decodeJSONView(body, "get tunnels", &out); err != nil {
		return out, err
	}
	if out.ConnectToken != "" {
		sandbox.connectToken = out.ConnectToken
	}
	return out, nil
}

// WaitReadyOptions configures lifecycle polling and an optional readiness probe.
type WaitReadyOptions struct {
	Timeout  time.Duration
	Interval time.Duration
	Port     uint16
	Command  []string
}

// WaitReady blocks until the sandbox is running and its optional probe succeeds.
func (sandbox *Sandbox) WaitReady(ctx context.Context, options ...WaitReadyOptions) (*Sandbox, error) {
	settings := WaitReadyOptions{Timeout: 5 * time.Minute, Interval: 250 * time.Millisecond}
	if len(options) > 0 {
		settings = options[0]
		if settings.Timeout <= 0 {
			settings.Timeout = 5 * time.Minute
		}
		if settings.Interval <= 0 {
			settings.Interval = 250 * time.Millisecond
		}
	}
	if settings.Port != 0 && len(settings.Command) != 0 {
		return nil, errors.New("vmon: readiness port and command probes are mutually exclusive")
	}
	waitCtx, cancel := context.WithTimeout(ctx, settings.Timeout)
	defer cancel()
	ticker := time.NewTicker(settings.Interval)
	defer ticker.Stop()
	for {
		current, err := sandbox.Refresh(waitCtx)
		if err != nil {
			return nil, err
		}
		switch current.Status {
		case "failed", "terminated", "exited":
			return nil, &APIError{StatusCode: http.StatusConflict, Code: "not_running", Message: "sandbox entered " + current.Status}
		case "running", "ready":
			ready := settings.Port == 0 && len(settings.Command) == 0
			if settings.Port != 0 {
				tunnels, probeErr := sandbox.Tunnels(waitCtx)
				if probeErr == nil {
					if target, exists := tunnels.Tunnels[settings.Port]; exists {
						address := net.JoinHostPort(target.Host, strconv.Itoa(int(target.Port)))
						connection, dialErr := (&net.Dialer{Timeout: settings.Interval}).DialContext(waitCtx, "tcp", address)
						if dialErr == nil {
							_ = connection.Close()
							ready = true
						}
					}
				}
			}
			if len(settings.Command) != 0 {
				result, probeErr := sandbox.Run(waitCtx, ExecRequest{Command: settings.Command})
				ready = probeErr == nil && result.ExitCode == 0
			}
			if ready {
				return current, nil
			}
		}
		select {
		case <-waitCtx.Done():
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, &APIError{StatusCode: http.StatusRequestTimeout, Code: "timeout", Message: "sandbox readiness timed out"}
		case <-ticker.C:
		}
	}
}

// Files manages file-system operations on the guest sandbox.
type Files struct{ sandbox *Sandbox }

// Open retrieves a stream to read the file at the specified guest path.
func (files *Files) Open(ctx context.Context, path string) (io.ReadCloser, error) {
	data, err := files.Read(ctx, path)
	if err != nil {
		return nil, err
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

// Read reads and returns the full contents of the file at the specified guest path.
func (files *Files) Read(ctx context.Context, path string) ([]byte, error) {
	if path == "" {
		return nil, errors.New("vmon: guest path must not be empty")
	}
	var out *pb.FileContent
	err := files.sandbox.invoke(ctx, "read file", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		var callErr error
		out, callErr = service.FileRead(ctx, &pb.FilePathRequest{Id: files.sandbox.ID, Path: path}, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	data := out.GetData()
	if limit := files.sandbox.client.maxResponseBytes; limit > 0 && int64(len(data)) > limit {
		return nil, &ResponseTooLargeError{Limit: limit}
	}
	return data, nil
}

// Write writes data to the file at the specified guest path.
func (files *Files) Write(ctx context.Context, path string, data []byte) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	return files.sandbox.invoke(ctx, "write file", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		_, err := service.FileWrite(ctx, &pb.FileWriteRequest{Id: files.sandbox.ID, Path: path, Data: data}, opts...)
		return err
	})
}

// List retrieves metadata for files and directories inside the specified guest path.
func (files *Files) List(ctx context.Context, path string) ([]FileInfo, error) {
	if path == "" {
		path = "."
	}
	body, err := files.sandbox.view(ctx, "list files", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.FileList(ctx, &pb.FilePathRequest{Id: files.sandbox.ID, Path: path}, opts...)
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		Entries []FileInfo `json:"entries"`
	}
	if err := decodeJSONView(body, "list files", &out); err != nil {
		return nil, err
	}
	if out.Entries == nil {
		return nil, &ProtocolError{Operation: "list files", Message: "response did not include entries"}
	}
	return out.Entries, nil
}

// Stat retrieves file information/metadata for the specified guest path.
func (files *Files) Stat(ctx context.Context, path string) (FileInfo, error) {
	if path == "" {
		return FileInfo{}, errors.New("vmon: guest path must not be empty")
	}
	var out FileInfo
	body, err := files.sandbox.view(ctx, "stat file", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return service.FileStat(ctx, &pb.FilePathRequest{Id: files.sandbox.ID, Path: path}, opts...)
	})
	if err != nil {
		return out, err
	}
	err = decodeJSONView(body, "stat file", &out)
	return out, err
}

// DeleteOptions configures guest filesystem deletion.
type DeleteOptions struct {
	Recursive bool
}

// Delete removes the file or directory at the specified guest path.
func (files *Files) Delete(ctx context.Context, path string, options ...DeleteOptions) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	recursive := len(options) > 0 && options[0].Recursive
	return files.sandbox.invoke(ctx, "delete file", func(ctx context.Context, service pb.SandboxServiceClient, opts ...grpc.CallOption) error {
		_, err := service.FileDelete(ctx, &pb.FileDeleteRequest{Id: files.sandbox.ID, Path: path, Recursive: recursive}, opts...)
		return err
	})
}

// Mkdir creates a directory (and any necessary parent directories) inside the guest.
func (files *Files) Mkdir(ctx context.Context, path string) error {
	if path == "" {
		return errors.New("vmon: guest path must not be empty")
	}
	result, err := files.sandbox.Run(ctx, ExecRequest{Command: []string{"mkdir", "-p", "--", path}})
	if err != nil {
		return err
	}
	if result.ExitCode != 0 {
		return fmt.Errorf("vmon: mkdir failed: %s", strings.TrimSpace(string(result.Stderr)))
	}
	return nil
}

// Ports manages HTTP proxying and port forwarding to the sandbox.
type Ports struct{ sandbox *Sandbox }

func (ports *Ports) token(ctx context.Context) (string, error) {
	if ports.sandbox.connectToken == "" {
		if _, err := ports.sandbox.Tunnels(ctx); err != nil {
			return "", err
		}
	}
	if ports.sandbox.connectToken == "" {
		return "", &ProtocolError{Operation: "port proxy", Message: "server did not return a connect token"}
	}
	return ports.sandbox.connectToken, nil
}
func sanitizeProxyQuery(query url.Values) url.Values {
	query = cloneValues(query)
	query.Del("connect_token")
	query.Del("token")
	query.Del("access_token")
	return query
}

type replayReadCloser struct {
	io.Reader
	closer io.Closer
}

func (body *replayReadCloser) Close() error {
	return body.closer.Close()
}

func daemonNotFoundResponse(response *http.Response, limit int64) (bool, error) {
	original := response.Body
	prefix, err := io.ReadAll(io.LimitReader(original, limit+1))
	if err != nil {
		return false, err
	}
	response.Body = &replayReadCloser{Reader: io.MultiReader(bytes.NewReader(prefix), original), closer: original}
	if int64(len(prefix)) > limit {
		return false, nil
	}
	var envelope struct {
		Code string `json:"code"`
	}
	if err := json.Unmarshal(prefix, &envelope); err != nil {
		return false, nil
	}
	return envelope.Code == "not_found", nil
}

// HTTP proxies an incoming HTTP request to the specified guest port.
func (ports *Ports) HTTP(ctx context.Context, port uint16, proxy ProxyRequest) (*http.Response, error) {
	if proxy.Method == "" {
		closeProxyBody(proxy.Body)
		return nil, errors.New("vmon: proxy method must not be empty")
	}
	content, err := io.ReadAll(proxy.Body)
	closeProxyBody(proxy.Body)
	if err != nil {
		return nil, err
	}
	token, err := ports.token(ctx)
	if err != nil {
		return nil, err
	}
	query := sanitizeProxyQuery(proxy.Query)
	query.Set("connect_token", token)
	request := DriverRequest{
		Method:   proxy.Method,
		Path:     sandboxPath(ports.sandbox.ID) + "/ports/" + strconv.Itoa(int(port)) + "/" + escapeRestPath(proxy.Path),
		Query:    query,
		Content:  content,
		Headers:  proxy.Header,
		Stream:   true,
		Endpoint: ports.sandbox.endpoint,
	}
	response, endpoint, err := ports.sandbox.client.driver.Do(ctx, request)
	if endpoint != "" {
		ports.sandbox.endpoint = endpoint
	}
	if err != nil || response.StatusCode != http.StatusNotFound || len(ports.sandbox.client.driver.Endpoints()) <= 1 {
		return response, err
	}
	daemonNotFound, inspectErr := daemonNotFoundResponse(response, ports.sandbox.client.maxResponseBytes)
	if inspectErr != nil {
		_ = response.Body.Close()
		return nil, inspectErr
	}
	if !daemonNotFound {
		return response, nil
	}
	_ = response.Body.Close()
	endpoint, err = ports.sandbox.client.resolveSandbox(ctx, ports.sandbox.ID, ports.sandbox.endpoint)
	if err != nil {
		return nil, err
	}
	ports.sandbox.endpoint = endpoint
	request.Endpoint = endpoint
	response, used, err := ports.sandbox.client.driver.Do(ctx, request)
	if used != "" {
		ports.sandbox.endpoint = used
	}
	return response, err
}
