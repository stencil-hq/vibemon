package freestyle

import (
	"context"
	"errors"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
)

// PtySessionInfo describes one persistent terminal session.
type PtySessionInfo struct {
	SessionID     string
	Running       bool
	ExitCode      *int64
	Cols          uint32
	Rows          uint32
	Exec          *string
	CreatedAtMs   int64
	AttachedCount uint32
	Suspended     bool
}

// PtyOpenOptions configures a persistent guest terminal.
type PtyOpenOptions struct {
	Cols      uint32
	Rows      uint32
	Exec      string
	Env       map[string]string
	Workdir   string
	SessionID string
}

// PtyAttachOptions configures reattachment and an optional resize.
type PtyAttachOptions struct {
	SessionID string
	Cols      uint32
	Rows      uint32
}

// VmPty provides persistent terminal operations for one VM.
type VmPty struct{ sandbox *vmon.Sandbox }

// Open creates a server-persistent terminal session.
func (pty *VmPty) Open(ctx context.Context, options *PtyOpenOptions) (*PtySession, error) {
	var core vmon.PtyOpenOptions
	if options != nil {
		core = vmon.PtyOpenOptions{
			SessionID: options.SessionID,
			Cols:      options.Cols,
			Rows:      options.Rows,
			Exec:      options.Exec,
			Env:       options.Env,
			Workdir:   options.Workdir,
		}
	}
	stream, err := pty.sandbox.PtyOpen(ctx, core)
	if err != nil {
		return nil, err
	}
	return newPtySession(pty.sandbox, stream), nil
}

// Attach reconnects to a persistent terminal by stable session ID.
func (pty *VmPty) Attach(ctx context.Context, options PtyAttachOptions) (*PtySession, error) {
	stream, err := pty.sandbox.PtyAttach(ctx, options.SessionID, vmon.PtyAttachOptions{
		Cols: options.Cols, Rows: options.Rows,
	})
	if err != nil {
		return nil, err
	}
	return newPtySession(pty.sandbox, stream), nil
}

// List returns persistent sessions, including recently exited sessions retained by the server.
func (pty *VmPty) List(ctx context.Context) ([]PtySessionInfo, error) {
	rows, err := pty.sandbox.PtyList(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]PtySessionInfo, 0, len(rows))
	for _, row := range rows {
		result = append(result, ptyInfo(row))
	}
	return result, nil
}

// PtyCloseResult identifies the closed session and its exit code when known.
type PtyCloseResult struct {
	SessionID string
	ExitCode  *int64
}

// Close terminates one persistent terminal session.
func (pty *VmPty) Close(ctx context.Context, sessionID string) (PtyCloseResult, error) {
	result, err := pty.sandbox.PtyClose(ctx, sessionID)
	if err != nil {
		return PtyCloseResult{}, err
	}
	return PtyCloseResult{SessionID: result.SessionID, ExitCode: result.ExitCode}, nil
}

// PtySession is one client attachment to a persistent guest terminal.
type PtySession struct {
	SessionID string

	sandbox *vmon.Sandbox
	stream  *vmon.PtyStream
}

func newPtySession(sandbox *vmon.Sandbox, stream *vmon.PtyStream) *PtySession {
	return &PtySession{SessionID: stream.Info.SessionID, sandbox: sandbox, stream: stream}
}

// Info returns the session metadata observed during attachment.
func (session *PtySession) Info(ctx context.Context) (PtySessionInfo, error) {
	if err := ctx.Err(); err != nil {
		return PtySessionInfo{}, err
	}
	if session == nil || session.stream == nil {
		return PtySessionInfo{}, errors.New("freestyle: PTY session is not attached")
	}
	return ptyInfo(session.stream.Info), nil
}

// Write sends bytes to the terminal.
func (session *PtySession) Write(ctx context.Context, data []byte) error {
	if session == nil || session.stream == nil {
		return errors.New("freestyle: PTY session is not attached")
	}
	return session.stream.Write(ctx, data)
}

// Resize updates terminal columns and rows.
func (session *PtySession) Resize(ctx context.Context, cols, rows uint16) error {
	if session == nil || session.stream == nil {
		return errors.New("freestyle: PTY session is not attached")
	}
	return session.stream.Resize(ctx, rows, cols)
}

// Signal supports SIGINT and SIGKILL.
func (session *PtySession) Signal(ctx context.Context, signal string) error {
	switch signal {
	case "SIGINT":
		return session.Write(ctx, []byte{0x03})
	case "SIGKILL":
		if session == nil || session.sandbox == nil {
			return errors.New("freestyle: PTY session is not attached")
		}
		_, err := session.sandbox.PtyClose(ctx, session.SessionID)
		return err
	default:
		return errors.New("freestyle: PTY signal must be SIGINT or SIGKILL")
	}
}

// Receive waits for one terminal output or exit frame.
func (session *PtySession) Receive(ctx context.Context) (vmon.ExecEvent, error) {
	if session == nil || session.stream == nil {
		return vmon.ExecEvent{}, errors.New("freestyle: PTY session is not attached")
	}
	return session.stream.Receive(ctx)
}

// Wait blocks until the terminal process exits.
func (session *PtySession) Wait(ctx context.Context) (vmon.ExecExit, error) {
	if session == nil || session.stream == nil {
		return vmon.ExecExit{}, errors.New("freestyle: PTY session is not attached")
	}
	return session.stream.Wait(ctx)
}

// Detach closes only this client stream and leaves the guest session running.
func (session *PtySession) Detach(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if session == nil || session.stream == nil {
		return nil
	}
	return session.stream.Detach(ctx)
}

func ptyInfo(info vmon.PtySessionInfo) PtySessionInfo {
	return PtySessionInfo{
		SessionID:     info.SessionID,
		Running:       info.Running,
		ExitCode:      info.ExitCode,
		Cols:          info.Cols,
		Rows:          info.Rows,
		Exec:          info.Exec,
		CreatedAtMs:   info.CreatedAtUnixMillis,
		AttachedCount: info.AttachedCount,
		Suspended:     info.Suspended,
	}
}
