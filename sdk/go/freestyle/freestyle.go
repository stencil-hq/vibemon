package freestyle

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	vmon "github.com/stencil-hq/vibemon/sdk/go"
)

// Option configures a lazily connected Freestyle client.
type Option func(*config)

type config struct {
	apiKey      string
	accessToken string
	baseURL     string
}

// WithAPIKey sets the vmon bearer token through Freestyle's API-key slot.
func WithAPIKey(value string) Option { return func(config *config) { config.apiKey = value } }

// WithAccessToken sets the vmon bearer token and takes precedence over APIKey.
func WithAccessToken(value string) Option {
	return func(config *config) { config.accessToken = value }
}

// WithBaseURL sets the vmon DSN. An empty value uses VMON_DSN or VMON_CONTEXT.
func WithBaseURL(value string) Option { return func(config *config) { config.baseURL = value } }

// Freestyle is a lazily connected Freestyle-shaped vmon client.
type Freestyle struct {
	Vms *Vms
	Vpc *Vpc

	config      config
	once        sync.Once
	clientValue *vmon.Client
	clientErr   error
}

// New constructs a lazy client. No transport is opened until an operation runs.
func New(options ...Option) *Freestyle {
	freestyle := &Freestyle{}
	for _, option := range options {
		if option != nil {
			option(&freestyle.config)
		}
	}
	freestyle.initNamespaces()
	return freestyle
}

// NewFromClient binds the facade to an existing vmon client.
func NewFromClient(client *vmon.Client) *Freestyle {
	freestyle := &Freestyle{clientValue: client}
	freestyle.once.Do(func() {})
	freestyle.initNamespaces()
	return freestyle
}

func (freestyle *Freestyle) initNamespaces() {
	freestyle.Vms = &Vms{freestyle: freestyle}
	freestyle.Vms.Snapshots = &VmSnapshots{freestyle: freestyle}
	freestyle.Vpc = &Vpc{freestyle: freestyle}
}

func (freestyle *Freestyle) client() (*vmon.Client, error) {
	if freestyle == nil {
		return nil, errors.New("freestyle: nil client")
	}
	freestyle.once.Do(func() {
		token := freestyle.config.accessToken
		if token == "" {
			token = freestyle.config.apiKey
		}
		var options []vmon.Option
		if token != "" {
			options = append(options, vmon.WithToken(token))
		}
		freestyle.clientValue, freestyle.clientErr = vmon.Connect(freestyle.config.baseURL, options...)
	})
	if freestyle.clientValue == nil && freestyle.clientErr == nil {
		return nil, errors.New("freestyle: no vmon client")
	}
	return freestyle.clientValue, freestyle.clientErr
}

// Close releases the underlying vmon client if it was connected.
func (freestyle *Freestyle) Close() error {
	if freestyle == nil || freestyle.clientValue == nil {
		return nil
	}
	return freestyle.clientValue.Close()
}

// PersistenceOptions selects stored-state retention.
type PersistenceOptions struct {
	Type     string
	Priority *int
}

// NICOptions configures the single routed VPC NIC supported by vmon.
type NICOptions struct {
	Default *bool
	VPC     string
	Mode    string
	IPv4    any
}

// CreateOptions configures VM creation or full-state snapshot restoration.
type CreateOptions struct {
	SnapshotID             string
	Name                   string
	IdleTimeoutSeconds     *uint64
	ActivityThresholdBytes *uint64
	Persistence            *PersistenceOptions
	NICs                   []NICOptions
	Image                  string
	Template               string
	CPU                    uint32
	Memory                 uint32
	Storage                uint32
	Env                    map[string]string
	Workdir                string
	Tags                   map[string]string
}

// Created is the result of creating or restoring a VM.
type Created struct {
	Vm      *Vm
	VmID    string
	Domains []string
}

// VmState is a Freestyle lifecycle-state name.
type VmState string

const (
	VmStarting   VmState = "starting"
	VmRunning    VmState = "running"
	VmSuspending VmState = "suspending"
	VmSuspended  VmState = "suspended"
	VmStopped    VmState = "stopped"
	VmLost       VmState = "lost"
	VmBuilding   VmState = "building"
)

// VmListEntry is one VM collection row.
type VmListEntry struct {
	ID                  string
	State               VmState
	CreatedAt           *time.Time
	LastNetworkActivity *time.Time
	SnapshotID          *string
	Deleted             bool
}

// VmList is a VM collection and its state counts.
type VmList struct {
	Vms            []VmListEntry
	TotalCount     int
	RunningCount   int
	StartingCount  int
	SuspendedCount int
	StoppedCount   int
}

// Vms provides VM collection operations.
type Vms struct {
	Snapshots *VmSnapshots
	freestyle *Freestyle
}

// Create provisions a fresh VM or restores a full-state snapshot.
func (vms *Vms) Create(ctx context.Context, options *CreateOptions) (*Created, error) {
	if options == nil {
		options = &CreateOptions{}
	}
	if len(options.NICs) > 1 {
		return nil, errors.New("freestyle: vmon VMs have a single NIC")
	}
	for _, nic := range options.NICs {
		if nic.Mode != "routed" {
			return nil, errors.New(`freestyle: vmon VPC NICs require mode "routed"`)
		}
		if nic.VPC == "" {
			return nil, errors.New("freestyle: NIC VPC must not be empty")
		}
		switch value := nic.IPv4.(type) {
		case string:
			if value == "" {
				return nil, errors.New("freestyle: NIC IPv4 string must not be empty")
			}
		case bool:
			if !value {
				return nil, errors.New("freestyle: NIC IPv4 boolean must be true")
			}
		default:
			return nil, errors.New("freestyle: NIC IPv4 must be a string or true")
		}
	}
	client, err := vms.freestyle.client()
	if err != nil {
		return nil, err
	}
	persistence, err := persistencePolicy(options.Persistence)
	if err != nil {
		return nil, err
	}
	var sandbox *vmon.Sandbox
	if options.SnapshotID != "" {
		if options.CPU != 0 || options.Memory != 0 || options.Storage != 0 {
			return nil, errors.New("freestyle: CPU, memory, and storage are fixed by the snapshot")
		}
		overrides := map[string]any{
			"timeout_secs":  uint64(0),
			"block_network": false,
		}
		putOverride(overrides, "env", options.Env)
		putOverride(overrides, "workdir", options.Workdir)
		putOverride(overrides, "tags", options.Tags)
		if options.IdleTimeoutSeconds != nil {
			overrides["idle_timeout_secs"] = *options.IdleTimeoutSeconds
		}
		if options.ActivityThresholdBytes != nil {
			overrides["activity_threshold_bytes"] = *options.ActivityThresholdBytes
		}
		if persistence != nil {
			overrides["persistence"] = persistence
		}
		sandbox, err = client.Snapshots.Restore(ctx, options.SnapshotID, vmon.RestoreRequest{
			Name: options.Name, Overrides: overrides,
		})
	} else {
		zero := uint64(0)
		request := vmon.SandboxCreateRequest{
			Name: options.Name, Image: options.Image, Template: options.Template,
			CPUs: options.CPU, MemoryMiB: options.Memory * 1024, DiskMiB: options.Storage * 1024,
			TimeoutSeconds: &zero, IdleTimeoutSeconds: options.IdleTimeoutSeconds,
			ActivityThresholdBytes: options.ActivityThresholdBytes, Persistence: persistence,
			Env: options.Env, Workdir: options.Workdir, Tags: options.Tags,
		}
		for _, nic := range options.NICs {
			isDefault := true
			if nic.Default != nil {
				isDefault = *nic.Default
			}
			request.NICs = append(request.NICs, vmon.NIC{
				VPC: nic.VPC, IPv4: nic.IPv4, Default: isDefault,
			})
		}
		sandbox, err = client.Sandboxes.Create(ctx, request)
	}
	if err != nil {
		return nil, err
	}
	vm := newVm(client, sandbox)
	return &Created{Vm: vm, VmID: vm.VmID, Domains: []string{}}, nil
}

func persistencePolicy(options *PersistenceOptions) (*vmon.PersistencePolicy, error) {
	if options == nil {
		return nil, nil
	}
	policy := &vmon.PersistencePolicy{Type: options.Type}
	switch options.Type {
	case "persistent", "ephemeral":
		return policy, nil
	case "sticky":
		priority := 5
		if options.Priority != nil {
			priority = *options.Priority
		}
		if priority < 0 {
			priority = 0
		} else if priority > 10 {
			priority = 10
		}
		value := uint32(priority)
		policy.Priority = &value
		return policy, nil
	default:
		return nil, errors.New("freestyle: persistence type must be persistent, sticky, or ephemeral")
	}
}

func putOverride(values map[string]any, key string, value any) {
	switch typed := value.(type) {
	case string:
		if typed != "" {
			values[key] = typed
		}
	case map[string]string:
		if typed != nil {
			values[key] = typed
		}
	}
}

// List returns every reachable VM and Freestyle-compatible state counts.
func (vms *Vms) List(ctx context.Context) (*VmList, error) {
	client, err := vms.freestyle.client()
	if err != nil {
		return nil, err
	}
	sandboxes, err := client.Sandboxes.List(ctx)
	if err != nil {
		return nil, err
	}
	result := &VmList{Vms: make([]VmListEntry, 0, len(sandboxes)), TotalCount: len(sandboxes)}
	for _, sandbox := range sandboxes {
		entry := VmListEntry{ID: sandbox.ID, State: vmState(sandbox)}
		if sandbox.CreatedAt != 0 {
			created := time.UnixMilli(int64(sandbox.CreatedAt * 1000)).UTC()
			entry.CreatedAt = &created
		}
		activity := sandbox.LastActive
		if raw, ok := sandbox.Details["last_network_active"]; ok {
			_ = json.Unmarshal(raw, &activity)
		}
		if activity != 0 {
			last := time.UnixMilli(int64(activity * 1000)).UTC()
			entry.LastNetworkActivity = &last
		}
		result.Vms = append(result.Vms, entry)
		switch entry.State {
		case VmRunning:
			result.RunningCount++
		case VmStarting:
			result.StartingCount++
		case VmSuspended, VmSuspending:
			result.SuspendedCount++
		default:
			result.StoppedCount++
		}
	}
	return result, nil
}

// Get reconnects to an existing VM.
func (vms *Vms) Get(ctx context.Context, vmID string) (*Vm, error) {
	client, err := vms.freestyle.client()
	if err != nil {
		return nil, err
	}
	sandbox, err := client.Sandboxes.Get(ctx, vmID)
	if err != nil {
		return nil, err
	}
	return newVm(client, sandbox), nil
}

// Ref creates an unfetched VM reference.
func (vms *Vms) Ref(ctx context.Context, vmID string) (*Vm, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	client, err := vms.freestyle.client()
	if err != nil {
		return nil, err
	}
	if vmID == "" {
		return nil, errors.New("freestyle: VM ID must not be empty")
	}
	return newVm(client, client.Sandboxes.Ref(vmID)), nil
}

// Delete permanently removes a VM. An already missing VM is accepted.
func (vms *Vms) Delete(ctx context.Context, vmID string) error {
	client, err := vms.freestyle.client()
	if err != nil {
		return err
	}
	return deleteSandbox(ctx, client.Sandboxes.Ref(vmID))
}

func deleteSandbox(ctx context.Context, sandbox *vmon.Sandbox) error {
	if err := sandbox.Remove(ctx); err != nil && !isNotFound(err) {
		return err
	}
	return nil
}

func isNotFound(err error) bool {
	var apiError *vmon.APIError
	return errors.As(err, &apiError) &&
		(apiError.Code == "not_found" || apiError.StatusCode == http.StatusNotFound)
}

func vmState(sandbox *vmon.Sandbox) VmState {
	state := sandbox.ObservedState
	if state == "" {
		state = sandbox.Status
	}
	switch state {
	case "running":
		return VmRunning
	case "starting", "booting", "creating", "pending":
		return VmStarting
	case "suspending":
		return VmSuspending
	case "suspended", "paused":
		return VmSuspended
	case "lost":
		return VmLost
	case "building":
		return VmBuilding
	default:
		return VmStopped
	}
}

// VmSnapshot is one ready full-state snapshot.
type VmSnapshot struct {
	SnapshotID string
	State      string
}

// VmSnapshots provides snapshot collection operations.
type VmSnapshots struct{ freestyle *Freestyle }

// List returns ready snapshot identifiers.
func (snapshots *VmSnapshots) List(ctx context.Context) ([]VmSnapshot, error) {
	client, err := snapshots.freestyle.client()
	if err != nil {
		return nil, err
	}
	names, err := client.Snapshots.List(ctx)
	if err != nil {
		return nil, err
	}
	result := make([]VmSnapshot, 0, len(names))
	for _, name := range names {
		result = append(result, VmSnapshot{SnapshotID: name, State: "ready"})
	}
	return result, nil
}

// Get returns a ready snapshot or a not_found APIError.
func (snapshots *VmSnapshots) Get(ctx context.Context, snapshotID string) (VmSnapshot, error) {
	rows, err := snapshots.List(ctx)
	if err != nil {
		return VmSnapshot{}, err
	}
	for _, row := range rows {
		if row.SnapshotID == snapshotID {
			return row, nil
		}
	}
	return VmSnapshot{}, &vmon.APIError{
		StatusCode: http.StatusNotFound,
		Code:       "not_found",
		Message:    fmt.Sprintf("snapshot %s does not exist", snapshotID),
	}
}
