package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"sync"

	ws "github.com/coder/websocket"
	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

// WebSocketMessageType identifies a text or binary WebSocket message.
type WebSocketMessageType int

const (
	// WebSocketTextMessage identifies a UTF-8 text message.
	WebSocketTextMessage WebSocketMessageType = 1
	// WebSocketBinaryMessage identifies a binary message.
	WebSocketBinaryMessage WebSocketMessageType = 2
)

// WebSocketConn is a bounded, context-aware WebSocket used by port proxies.
type WebSocketConn struct {
	conn      *ws.Conn
	writeMu   sync.Mutex
	closeOnce sync.Once
	closeErr  error
}

// Read waits for one text or binary message.
func (socket *WebSocketConn) Read(ctx context.Context) (WebSocketMessageType, []byte, error) {
	if socket == nil || socket.conn == nil {
		return 0, nil, errors.New("vmon: websocket is not open")
	}
	messageType, data, err := socket.conn.Read(ctx)
	if err != nil {
		return 0, nil, normalizeWebSocketError(ctx, err)
	}
	switch messageType {
	case ws.MessageText:
		return WebSocketTextMessage, data, nil
	case ws.MessageBinary:
		return WebSocketBinaryMessage, data, nil
	default:
		return 0, nil, &ProtocolError{Operation: "read websocket", Message: "unsupported message type"}
	}
}

// Write sends one complete text or binary message.
func (socket *WebSocketConn) Write(
	ctx context.Context,
	messageType WebSocketMessageType,
	data []byte,
) error {
	if socket == nil || socket.conn == nil {
		return errors.New("vmon: websocket is not open")
	}
	var nativeType ws.MessageType
	switch messageType {
	case WebSocketTextMessage:
		nativeType = ws.MessageText
	case WebSocketBinaryMessage:
		nativeType = ws.MessageBinary
	default:
		return fmt.Errorf("vmon: unsupported websocket message type %d", messageType)
	}
	socket.writeMu.Lock()
	defer socket.writeMu.Unlock()
	if err := socket.conn.Write(ctx, nativeType, data); err != nil {
		return normalizeWebSocketError(ctx, err)
	}
	return nil
}

// Close performs a normal WebSocket close and releases the connection.
func (socket *WebSocketConn) Close() error {
	if socket == nil {
		return nil
	}
	socket.closeOnce.Do(func() {
		if socket.conn != nil {
			socket.closeErr = normalizeWebSocketCloseError(
				socket.conn.Close(ws.StatusNormalClosure, ""),
			)
		}
	})
	return socket.closeErr
}

func normalizeWebSocketCloseError(err error) error {
	if err == nil || errors.Is(err, io.EOF) {
		return nil
	}
	status := ws.CloseStatus(err)
	if status == ws.StatusNormalClosure || status == ws.StatusGoingAway {
		return nil
	}
	return err
}

func normalizeWebSocketError(ctx context.Context, err error) error {
	if contextErr := ctx.Err(); contextErr != nil {
		return contextErr
	}
	status := ws.CloseStatus(err)
	if status == ws.StatusNormalClosure || status == ws.StatusGoingAway {
		return io.EOF
	}
	return fmt.Errorf("vmon: websocket I/O failed: %w", err)
}

func (client *Client) dialWebSocket(ctx context.Context, path string, query url.Values, endpoint string) (*WebSocketConn, string, error) {
	if client == nil || client.driver == nil {
		return nil, "", errors.New("vmon: client has no driver")
	}
	return client.driver.Dial(ctx, path, query, endpoint)
}

// dial opens a ports-proxy WebSocket with endpoint affinity, relocating once
// when the pinned node no longer hosts the sandbox.
func (sandbox *Sandbox) dial(ctx context.Context, path string, query url.Values) (*WebSocketConn, string, error) {
	fullPath := sandboxPath(sandbox.ID) + path
	socket, endpoint, err := sandbox.client.dialWebSocket(ctx, fullPath, query, sandbox.endpoint)
	if endpoint != "" {
		sandbox.endpoint = endpoint
	}
	if err == nil || !isNotFoundAPIError(err) || len(sandbox.client.driver.Endpoints()) <= 1 {
		return socket, endpoint, err
	}
	endpoint, resolveErr := sandbox.client.resolveSandbox(ctx, sandbox.ID, sandbox.endpoint)
	if resolveErr != nil {
		return nil, "", resolveErr
	}
	sandbox.endpoint = endpoint
	socket, used, err := sandbox.client.dialWebSocket(ctx, fullPath, query, endpoint)
	if used != "" {
		sandbox.endpoint = used
	}
	return socket, used, err
}

// Process is a live streaming command or shell.
type Process struct {
	stream      grpc.BidiStreamingClient[pb.ExecInput, pb.ExecOutput]
	cancel      context.CancelFunc
	sendMu      sync.Mutex
	stateMu     sync.Mutex
	stdinClosed bool
	done        bool
	// SandboxID is set for shell processes after the ready frame.
	SandboxID string
}

// Shell opens an existing-sandbox or ephemeral shell and consumes its ready frame.
func (client *Client) Shell(ctx context.Context, request ShellRequest) (*Process, error) {
	endpoint, err := client.grpcEndpoint("")
	if err != nil {
		return nil, err
	}
	conn, err := client.conn(endpoint)
	if err != nil {
		return nil, err
	}
	encoded, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("vmon: encode shell request: %w", err)
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := pb.NewSandboxServiceClient(conn).Shell(streamCtx)
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, "shell setup")
	}
	process := &Process{stream: stream, cancel: cancel}
	// A cancelled caller context aborts the setup handshake.
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if err := process.send(&pb.ExecInput{Input: &pb.ExecInput_ShellParamsJson{ShellParamsJson: string(encoded)}}, "shell setup"); err != nil {
		_ = process.closeWithState()
		return nil, err
	}
	output, err := stream.Recv()
	if err != nil {
		_ = process.closeWithState()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, apiErrorFromStatus(err, "shell setup", stream.Trailer())
	}
	name := output.GetReady().GetSandboxId()
	if name == "" {
		_ = process.closeWithState()
		return nil, &ProtocolError{Operation: "shell setup", Message: "missing ready sandbox name"}
	}
	process.SandboxID = name
	return process, nil
}

// Exec opens a streaming command in this sandbox.
func (sandbox *Sandbox) Exec(ctx context.Context, request ExecRequest) (*Process, error) {
	if len(request.Command) == 0 {
		return nil, errors.New("vmon: exec command must not be empty")
	}
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := pb.NewSandboxServiceClient(conn).Exec(streamCtx)
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, "exec")
	}
	start := execStartProto(request)
	start.SandboxId = sandbox.ID
	process := &Process{stream: stream, cancel: cancel}
	if err := process.send(&pb.ExecInput{Input: &pb.ExecInput_Start{Start: start}}, "exec"); err != nil {
		_ = process.closeWithState()
		return nil, err
	}
	return process, nil
}

// PtyStream is one client attachment to a server-persistent terminal session.
// Detaching closes only this stream and leaves the guest process running.
type PtyStream struct {
	process *Process
	Info    PtySessionInfo
}

// PtyOpen creates a persistent terminal session and consumes its metadata frame.
func (sandbox *Sandbox) PtyOpen(ctx context.Context, options PtyOpenOptions) (*PtyStream, error) {
	start := &pb.PtyOpenStart{
		SandboxId: sandbox.ID,
		SessionId: options.SessionID,
		Cols:      options.Cols,
		Rows:      options.Rows,
		Env:       options.Env,
	}
	if options.Exec != "" {
		start.Exec = &options.Exec
	}
	if options.Workdir != "" {
		start.Workdir = &options.Workdir
	}
	return sandbox.openPtyStream(ctx, "open pty session", func(
		service pb.SandboxServiceClient,
		streamCtx context.Context,
	) (grpc.BidiStreamingClient[pb.ExecInput, pb.ExecOutput], *pb.ExecInput, error) {
		stream, err := service.PtyOpen(streamCtx)
		return stream, &pb.ExecInput{Input: &pb.ExecInput_PtyOpen{PtyOpen: start}}, err
	})
}

// PtyAttach reconnects to a persistent terminal session by stable identifier.
func (sandbox *Sandbox) PtyAttach(
	ctx context.Context,
	sessionID string,
	options PtyAttachOptions,
) (*PtyStream, error) {
	if sessionID == "" {
		return nil, errors.New("vmon: pty session id must not be empty")
	}
	start := &pb.PtyAttachStart{SandboxId: sandbox.ID, SessionId: sessionID}
	if options.Cols != 0 {
		start.Cols = &options.Cols
	}
	if options.Rows != 0 {
		start.Rows = &options.Rows
	}
	return sandbox.openPtyStream(ctx, "attach pty session", func(
		service pb.SandboxServiceClient,
		streamCtx context.Context,
	) (grpc.BidiStreamingClient[pb.ExecInput, pb.ExecOutput], *pb.ExecInput, error) {
		stream, err := service.PtyAttach(streamCtx)
		return stream, &pb.ExecInput{Input: &pb.ExecInput_PtyAttach{PtyAttach: start}}, err
	})
}

func (sandbox *Sandbox) openPtyStream(
	ctx context.Context,
	operation string,
	open func(
		pb.SandboxServiceClient,
		context.Context,
	) (grpc.BidiStreamingClient[pb.ExecInput, pb.ExecOutput], *pb.ExecInput, error),
) (*PtyStream, error) {
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, first, err := open(pb.NewSandboxServiceClient(conn), streamCtx)
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, operation)
	}
	process := &Process{stream: stream, cancel: cancel}
	stop := context.AfterFunc(ctx, cancel)
	defer stop()
	if err := process.send(first, operation); err != nil {
		_ = process.closeWithState()
		return nil, err
	}
	output, err := stream.Recv()
	if err != nil {
		_ = process.closeWithState()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		return nil, apiErrorFromStatus(err, operation, stream.Trailer())
	}
	session := output.GetPty()
	if session == nil || session.GetSessionId() == "" {
		_ = process.closeWithState()
		return nil, &ProtocolError{Operation: operation, Message: "missing pty session metadata"}
	}
	return &PtyStream{process: process, Info: ptySessionInfo(session)}, nil
}

// Receive waits for one output or exit frame.
func (stream *PtyStream) Receive(ctx context.Context) (ExecEvent, error) {
	if stream == nil || stream.process == nil {
		return ExecEvent{}, errors.New("vmon: pty stream is not open")
	}
	return stream.process.Receive(ctx)
}

// Write writes bytes to the terminal.
func (stream *PtyStream) Write(ctx context.Context, data []byte) error {
	if stream == nil || stream.process == nil {
		return errors.New("vmon: pty stream is not open")
	}
	return stream.process.WriteStdin(ctx, data)
}

// Resize changes the terminal dimensions.
func (stream *PtyStream) Resize(ctx context.Context, rows, columns uint16) error {
	if stream == nil || stream.process == nil {
		return errors.New("vmon: pty stream is not open")
	}
	return stream.process.Resize(ctx, rows, columns)
}

// Wait blocks until the persistent terminal process exits.
func (stream *PtyStream) Wait(ctx context.Context) (ExecExit, error) {
	if stream == nil || stream.process == nil {
		return ExecExit{}, errors.New("vmon: pty stream is not open")
	}
	return stream.process.Wait(ctx)
}

// Detach disconnects this client while leaving the guest terminal session running.
func (stream *PtyStream) Detach(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if stream == nil || stream.process == nil {
		return nil
	}
	return stream.process.Close()
}

// send serializes one client frame onto the stream.
func (process *Process) send(input *pb.ExecInput, operation string) error {
	process.sendMu.Lock()
	defer process.sendMu.Unlock()
	err := process.stream.Send(input)
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return &ProtocolError{Operation: operation, Message: "process stream closed"}
	}
	return apiErrorFromStatus(err, operation, process.stream.Trailer())
}

// Receive waits for one decoded stream or exit frame.
func (process *Process) Receive(ctx context.Context) (ExecEvent, error) {
	if process == nil || process.stream == nil {
		return ExecEvent{}, errors.New("vmon: process is not open")
	}
	process.stateMu.Lock()
	done := process.done
	process.stateMu.Unlock()
	if done {
		return ExecEvent{}, io.EOF
	}
	stop := context.AfterFunc(ctx, func() { _ = process.closeWithState() })
	defer stop()
	output, err := process.stream.Recv()
	if err != nil {
		_ = process.closeWithState()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ExecEvent{}, ctxErr
		}
		if errors.Is(err, io.EOF) {
			return ExecEvent{}, io.EOF
		}
		return ExecEvent{}, apiErrorFromStatus(err, "process", process.stream.Trailer())
	}
	switch payload := output.GetOutput().(type) {
	case *pb.ExecOutput_Chunk:
		var stream StreamName
		switch payload.Chunk.GetStream() {
		case pb.Stream_STREAM_STDOUT:
			stream = StreamStdout
		case pb.Stream_STREAM_STDERR:
			stream = StreamStderr
		case pb.Stream_STREAM_CONSOLE:
			stream = StreamConsole
		default:
			_ = process.closeWithState()
			return ExecEvent{}, &ProtocolError{Operation: "process", Message: "unknown stream name"}
		}
		return ExecEvent{Stream: stream, Data: payload.Chunk.GetData()}, nil
	case *pb.ExecOutput_Exit:
		exit := &ExecExit{Code: payload.Exit.GetCode()}
		if payload.Exit.Signal != nil {
			signal := int(payload.Exit.GetSignal())
			exit.Signal = &signal
		}
		_ = process.closeWithState()
		return ExecEvent{Exit: exit}, nil
	default:
		_ = process.closeWithState()
		return ExecEvent{}, &ProtocolError{Operation: "process", Message: "unrecognized frame"}
	}
}

// WriteStdin writes a chunk of bytes to the standard input of the process.
func (process *Process) WriteStdin(ctx context.Context, data []byte) error {
	if len(data) == 0 {
		return nil
	}
	process.stateMu.Lock()
	closed := process.stdinClosed || process.done
	process.stateMu.Unlock()
	if closed {
		return errors.New("vmon: process stdin is closed")
	}
	return process.writeFrame(ctx, &pb.ExecInput{Input: &pb.ExecInput_Stdin{Stdin: data}})
}

// CloseStdin sends an EOF signal to the standard input of the process.
func (process *Process) CloseStdin(ctx context.Context) error {
	process.stateMu.Lock()
	if process.stdinClosed {
		process.stateMu.Unlock()
		return nil
	}
	if process.done {
		process.stateMu.Unlock()
		return errors.New("vmon: process is closed")
	}
	process.stdinClosed = true
	process.stateMu.Unlock()
	return process.writeFrame(ctx, &pb.ExecInput{Input: &pb.ExecInput_Eof{Eof: &pb.Eof{}}})
}

// Resize updates the terminal size (rows and columns) of the process.
func (process *Process) Resize(ctx context.Context, rows, columns uint16) error {
	return process.writeFrame(ctx, &pb.ExecInput{Input: &pb.ExecInput_Resize{Resize: &pb.Resize{Rows: uint32(rows), Cols: uint32(columns)}}})
}
func (process *Process) writeFrame(ctx context.Context, input *pb.ExecInput) error {
	if process == nil || process.stream == nil {
		return errors.New("vmon: process is not open")
	}
	stop := context.AfterFunc(ctx, func() { _ = process.closeWithState() })
	defer stop()
	if err := process.send(input, "process"); err != nil {
		_ = process.closeWithState()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return err
	}
	return nil
}

// Copy streams stdout and stderr of the process to the provided writers until the process exits.
func (process *Process) Copy(ctx context.Context, stdout, stderr io.Writer) (ExecExit, error) {
	if stdout == nil {
		stdout = io.Discard
	}
	if stderr == nil {
		stderr = io.Discard
	}
	for {
		event, err := process.Receive(ctx)
		if err != nil {
			return ExecExit{}, err
		}
		if event.Exit != nil {
			return *event.Exit, nil
		}
		writer := stdout
		if event.Stream == StreamStderr {
			writer = stderr
		}
		n, err := writer.Write(event.Data)
		if err != nil {
			return ExecExit{}, err
		}
		if n != len(event.Data) {
			return ExecExit{}, io.ErrShortWrite
		}
	}
}

// Wait blocks until the process exits, discarding its output, and returns the exit status.
func (process *Process) Wait(ctx context.Context) (ExecExit, error) {
	return process.Copy(ctx, io.Discard, io.Discard)
}

// Close terminates the process session and releases its resources.
func (process *Process) Close() error { return process.closeWithState() }
func (process *Process) closeWithState() error {
	if process == nil {
		return nil
	}
	process.stateMu.Lock()
	process.done = true
	process.stateMu.Unlock()
	if process.cancel != nil {
		process.cancel()
	}
	return nil
}

// ConsoleStream is a live read-only sandbox console.
type ConsoleStream struct {
	stream grpc.ServerStreamingClient[pb.ExecOutput]
	cancel context.CancelFunc
}

// Attach connects to the live interactive serial console of the sandbox.
func (sandbox *Sandbox) Attach(ctx context.Context) (*ConsoleStream, error) {
	conn, err := sandbox.streamConn(ctx)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stream, err := pb.NewSandboxServiceClient(conn).Attach(streamCtx, &pb.SandboxRef{Id: sandbox.ID})
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, "attach")
	}
	return &ConsoleStream{stream: stream, cancel: cancel}, nil
}

// Receive reads and decodes the next console event.
func (stream *ConsoleStream) Receive(ctx context.Context) (StreamEvent, error) {
	if stream == nil || stream.stream == nil {
		return StreamEvent{}, errors.New("vmon: console stream is not open")
	}
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	output, err := stream.stream.Recv()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return StreamEvent{}, ctxErr
		}
		if errors.Is(err, io.EOF) {
			return StreamEvent{}, io.EOF
		}
		return StreamEvent{}, apiErrorFromStatus(err, "attach", stream.stream.Trailer())
	}
	chunk := output.GetChunk()
	if chunk == nil || chunk.GetStream() != pb.Stream_STREAM_CONSOLE {
		return StreamEvent{}, &ProtocolError{Operation: "attach", Message: "unknown stream frame"}
	}
	return StreamEvent{Stream: StreamConsole, Data: chunk.GetData()}, nil
}

// Close terminates the console stream session.
func (stream *ConsoleStream) Close() error {
	if stream == nil || stream.cancel == nil {
		return nil
	}
	stream.cancel()
	return nil
}

// WebSocket opens a connect-token-authenticated guest port websocket.
func (ports *Ports) WebSocket(ctx context.Context, port uint16, path string, query url.Values) (*WebSocketConn, error) {
	token, err := ports.token(ctx)
	if err != nil {
		return nil, err
	}
	query = sanitizeProxyQuery(query)
	query.Set("connect_token", token)
	socket, endpoint, err := ports.sandbox.dial(ctx, "/ports/"+strconv.FormatUint(uint64(port), 10)+"/ws/"+escapeRestPath(path), query)
	if endpoint != "" {
		ports.sandbox.endpoint = endpoint
	}
	return socket, err
}
