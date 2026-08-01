package freestyle

import (
	"context"
	"errors"

	vmon "github.com/can1357/vibemon/sdk/go"
)

// Vm is a Freestyle-shaped handle bound to one vmon sandbox.
type Vm struct {
	VmID    string
	Sandbox *vmon.Sandbox
	Fs      *VmFs
	Pty     *VmPty

	client *vmon.Client
}

func newVm(client *vmon.Client, sandbox *vmon.Sandbox) *Vm {
	vm := &Vm{VmID: sandbox.ID, Sandbox: sandbox, client: client}
	vm.Fs = &VmFs{sandbox: sandbox}
	vm.Pty = &VmPty{sandbox: sandbox}
	return vm
}

// StartOptions configures the idle lease applied after starting a VM.
type StartOptions struct {
	IdleTimeoutSeconds *uint64
}

// Start wakes a suspended VM or fresh-boots a stopped VM.
func (vm *Vm) Start(ctx context.Context, options *StartOptions) (*vmon.Sandbox, error) {
	if options != nil && options.IdleTimeoutSeconds != nil {
		if _, err := vm.Sandbox.SetIdleTimeout(ctx, float64(*options.IdleTimeoutSeconds)); err != nil {
			return nil, err
		}
	}
	return vm.Sandbox.Resume(ctx)
}

// Stop discards in-memory state while retaining the VM disk and identity.
func (vm *Vm) Stop(ctx context.Context) (*vmon.Sandbox, error) {
	return vm.Sandbox.Stop(ctx)
}

// SnapshotOptions configures a full-state snapshot.
type SnapshotOptions struct{ Name string }

// SnapshotResult identifies a captured full-state snapshot and its source VM.
type SnapshotResult struct {
	SnapshotID string
	SourceVmID string
}

// Snapshot captures memory, processes, and disk. Restoring resumes that state.
func (vm *Vm) Snapshot(ctx context.Context, options *SnapshotOptions) (SnapshotResult, error) {
	var request vmon.SnapshotRequest
	if options != nil {
		request.Name = options.Name
	}
	name, err := vm.Sandbox.Snapshot(ctx, request)
	if err != nil {
		return SnapshotResult{}, err
	}
	return SnapshotResult{SnapshotID: name, SourceVmID: vm.VmID}, nil
}

// ForkOptions configures the number of full-state clones.
type ForkOptions struct{ Count uint32 }

// Forked is one forked VM and its identifier.
type Forked struct {
	Vm   *Vm
	VmID string
}

// ForkResult contains the ordered VM forks.
type ForkResult struct{ Forks []Forked }

// Fork snapshots the current state and atomically creates Count clones.
func (vm *Vm) Fork(ctx context.Context, options ForkOptions) (ForkResult, error) {
	if options.Count == 0 {
		return ForkResult{}, errors.New("freestyle: fork count must be greater than zero")
	}
	snapshot, err := vm.Sandbox.Snapshot(ctx, vmon.SnapshotRequest{})
	if err != nil {
		return ForkResult{}, err
	}
	clones, err := vm.client.Snapshots.Fork(ctx, snapshot, vmon.ForkRequest{Count: options.Count})
	if err != nil {
		return ForkResult{}, err
	}
	result := ForkResult{Forks: make([]Forked, 0, len(clones))}
	for _, clone := range clones {
		fork := newVm(vm.client, clone)
		result.Forks = append(result.Forks, Forked{Vm: fork, VmID: clone.ID})
	}
	return result, nil
}

// Delete terminates and permanently removes this VM; missing VMs are accepted.
func (vm *Vm) Delete(ctx context.Context) error { return deleteSandbox(ctx, vm.Sandbox) }

// ExecOptions configures one captured command.
type ExecOptions struct {
	Command   string
	Terminal  string
	TimeoutMs uint64
}

// ExecResult is captured command output and its exit status.
type ExecResult struct {
	Stdout     string
	Stderr     string
	StatusCode int
}

// Exec runs Command through sh -c, or in a persistent terminal context.
func (vm *Vm) Exec(ctx context.Context, options ExecOptions) (ExecResult, error) {
	if options.Command == "" {
		return ExecResult{}, errors.New("freestyle: exec command must not be empty")
	}
	var timeout *float64
	if options.TimeoutMs != 0 {
		seconds := float64(options.TimeoutMs) / 1000
		timeout = &seconds
	}
	var result vmon.ExecResult
	var err error
	if options.Terminal != "" {
		result, err = vm.Sandbox.PtyExec(ctx, options.Terminal, options.Command, timeout)
	} else {
		result, err = vm.Sandbox.Run(ctx, vmon.ExecRequest{
			Command: []string{"sh", "-c", options.Command}, Timeout: timeout,
		})
	}
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		Stdout: string(result.Stdout), Stderr: string(result.Stderr), StatusCode: int(result.ExitCode),
	}, nil
}

// ExecCommand is shorthand for Exec with only a command string.
func (vm *Vm) ExecCommand(ctx context.Context, command string) (ExecResult, error) {
	return vm.Exec(ctx, ExecOptions{Command: command})
}

// ResizeOptions configures Freestyle units: memory and storage are in GB.
type ResizeOptions struct {
	CPU     uint32
	Memory  uint32
	Storage uint32
}

// Resize changes CPU or memory and grows the root disk. CPU and memory must be powers of two.
func (vm *Vm) Resize(ctx context.Context, options ResizeOptions) (*vmon.Sandbox, error) {
	if options.CPU != 0 && !powerOfTwo(options.CPU) {
		return nil, errors.New("freestyle: CPU must be a power of two")
	}
	if options.Memory != 0 && !powerOfTwo(options.Memory) {
		return nil, errors.New("freestyle: memory must be a power of two")
	}
	return vm.Sandbox.Resize(ctx, vmon.ResizeOptions{
		CPUs: options.CPU, MemoryMiB: options.Memory * 1024, DiskMB: uint64(options.Storage) * 1024,
	})
}

func powerOfTwo(value uint32) bool { return value != 0 && value&(value-1) == 0 }
