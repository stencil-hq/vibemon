package vmon

import (
	"encoding/json"
	"fmt"
)

// Health is the response from the daemon health endpoint.
type Health struct {
	// OK reports whether the daemon considers itself healthy.
	OK bool `json:"ok"`
}

// UnmarshalJSON requires the daemon's boolean health field.
func (health *Health) UnmarshalJSON(data []byte) error {
	var wire struct {
		OK *bool `json:"ok"`
	}
	if err := json.Unmarshal(data, &wire); err != nil {
		return err
	}
	if wire.OK == nil {
		return fmt.Errorf("health response has no boolean ok field")
	}
	health.OK = *wire.OK
	return nil
}

// ServerInfo describes the daemon build and host capabilities.
type ServerInfo struct {
	// Version is the daemon version.
	Version string `json:"version"`
	// Platform is the daemon operating system.
	Platform string `json:"platform"`
	// Arch is the daemon architecture.
	Arch string `json:"arch"`
	// Backend is the active virtualization backend.
	Backend string `json:"backend"`
	// Capabilities reports feature support by capability name.
	Capabilities map[string]bool `json:"capabilities"`
}

// UnmarshalJSON requires the daemon version and validates typed capability values.
func (info *ServerInfo) UnmarshalJSON(data []byte) error {
	type serverInfoWire ServerInfo
	var decoded serverInfoWire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	if decoded.Version == "" {
		return fmt.Errorf("server info response has no version")
	}
	*info = ServerInfo(decoded)
	return nil
}

// SandboxMetrics contains open runtime counters as lossless JSON values.
type SandboxMetrics struct {
	// Values contains each metric keyed by its daemon-defined name.
	Values map[string]json.RawMessage
}

// UnmarshalJSON requires an object while retaining arbitrary nested metrics.
func (metrics *SandboxMetrics) UnmarshalJSON(data []byte) error {
	var values map[string]json.RawMessage
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	if values == nil {
		return fmt.Errorf("sandbox metrics response is not an object")
	}
	metrics.Values = values
	return nil
}

// Float64 decodes one numeric metric.
func (metrics SandboxMetrics) Float64(name string) (float64, bool) {
	raw, found := metrics.Values[name]
	if !found {
		return 0, false
	}
	var value float64
	if err := json.Unmarshal(raw, &value); err != nil {
		return 0, false
	}
	return value, true
}

// Sandbox is a typed view of a daemon-owned sandbox.
type Sandbox struct {
	// ID is the stable sandbox identifier.
	ID string `json:"id"`
	// Name is the human-facing sandbox name.
	Name string `json:"name"`
	// Status is the serving status summary.
	Status string `json:"status"`
	// DesiredState is the durable lifecycle target.
	DesiredState string `json:"desired_state"`
	// ObservedState is the lifecycle state materialized by the owner.
	ObservedState string `json:"observed_state"`
	// StateGeneration identifies the accepted lifecycle transition.
	StateGeneration uint64 `json:"state_generation"`
	// LifecycleFailure describes the latest failed transition.
	LifecycleFailure *string `json:"lifecycle_failure"`
	// PID is the monitor process identifier when one is available.
	PID *int32 `json:"pid,omitempty"`
	// Source is the image, template, fork, or restore source.
	Source *string `json:"source,omitempty"`
	// CreatedAt is the Unix creation timestamp.
	CreatedAt float64 `json:"created_at"`
	// LastActive is the Unix timestamp of the last activity.
	LastActive float64 `json:"last_active"`
	// ExpiresAt is the Unix idle-timeout deadline when armed.
	ExpiresAt *float64 `json:"expires_at"`
	// TerminatedAt is the Unix termination timestamp when terminated.
	TerminatedAt *float64 `json:"terminated_at"`
	// ErrorMessage is the terminal daemon error, when present.
	ErrorMessage *string `json:"error"`
	// Tags contains the sandbox's string tags.
	Tags map[string]string `json:"tags"`
	// ReturnCode is the foreground process exit code when known.
	ReturnCode *int64 `json:"returncode"`
	// Node is the mesh node that reported the latest sandbox view.
	Node string `json:"node,omitempty"`
	// HA is the selected durability tier.
	HA string `json:"ha,omitempty"`
	// RestartPolicy is the server-derived owner-loss behavior.
	RestartPolicy string `json:"restart_policy,omitempty"`
	// Details retains non-canonical response fields as raw JSON.
	Details map[string]json.RawMessage `json:"-"`

	client       *Client
	endpoint     string
	connectToken string
	Files        *Files
	Ports        *Ports
}

// UnmarshalJSON decodes canonical fields while preserving open sandbox detail fields.
func (sandbox *Sandbox) UnmarshalJSON(data []byte) error {
	type sandboxAlias Sandbox
	var decoded sandboxAlias
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	var details map[string]json.RawMessage
	if err := json.Unmarshal(data, &details); err != nil {
		return err
	}
	for _, key := range []string{
		"id", "name", "status", "desired_state", "observed_state", "state_generation",
		"lifecycle_failure", "pid", "source", "created_at", "last_active", "expires_at",
		"terminated_at", "error", "tags", "returncode", "node", "ha", "restart_policy",
	} {
		delete(details, key)
	}
	*sandbox = Sandbox(decoded)
	sandbox.Details = details
	return nil
}

// MigrationTiming reports the source-side phases of a completed migration.
type MigrationTiming struct {
	// PrecopyMS is time spent copying guest state while the source kept running.
	PrecopyMS uint64 `json:"precopy_ms"`
	// DowntimeMS is time from source suspension until the target resumed.
	DowntimeMS uint64 `json:"downtime_ms"`
	// TotalMS is the complete migration duration.
	TotalMS uint64 `json:"total_ms"`
}

// MigrationTiming returns timing data carried by a migration response.
func (sandbox *Sandbox) MigrationTiming() (MigrationTiming, bool) {
	var timing MigrationTiming
	if sandbox == nil {
		return timing, false
	}
	raw, ok := sandbox.Details["migration"]
	if !ok || json.Unmarshal(raw, &timing) != nil {
		return MigrationTiming{}, false
	}
	return timing, true
}

// SandboxCreateRequest is the stable request body for creating a sandbox.
type SandboxCreateRequest struct {
	// Image is an image reference.
	Image string `json:"image,omitempty"`
	// Template is a cached template reference.
	Template string `json:"template,omitempty"`
	// Dockerfile is inline Dockerfile content.
	Dockerfile string `json:"dockerfile,omitempty"`
	// Context is the image build context.
	Context string `json:"context,omitempty"`
	// Name requests a sandbox name.
	Name string `json:"name,omitempty"`
	// CPUs is the virtual CPU count.
	CPUs uint32 `json:"cpus,omitempty"`
	// MemoryMiB is guest memory in MiB.
	MemoryMiB uint32 `json:"memory,omitempty"`
	// DiskMiB is guest disk size in MiB.
	DiskMiB uint32 `json:"disk_mb,omitempty"`
	// Timeout is the request timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
	// TimeoutSeconds is the sandbox idle timeout in seconds.
	TimeoutSeconds *uint64 `json:"timeout_secs,omitempty"`
	// IdleTimeoutSeconds is the network-idle reclaim timeout. A pointer to zero disables reclaim.
	IdleTimeoutSeconds *uint64 `json:"idle_timeout_secs,omitempty"`
	// ActivityThresholdBytes is the guest NIC byte threshold that still counts as idle.
	ActivityThresholdBytes *uint64 `json:"activity_threshold_bytes,omitempty"`
	// Persistence controls stored-state retention and storage-GC behavior.
	Persistence *PersistencePolicy `json:"persistence,omitempty"`
	// NICs attaches the sandbox to at most one routed VPC.
	NICs []NIC `json:"nics,omitempty"`
	// Workdir is the default guest working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env contains non-secret environment variables.
	Env map[string]string `json:"env,omitempty"`
	// Secrets contains request-scoped secret bundles.
	Secrets []Secret `json:"secrets,omitempty"`
	// Credentials contains host-brokered credential names; credential values never enter this request.
	Credentials []string `json:"credentials,omitempty"`
	// Volumes maps guest mountpoints to named volume mounts.
	Volumes map[string]VolumeMount `json:"volumes,omitempty"`
	// S3Mounts maps guest mountpoints to S3 bucket or prefix mounts.
	S3Mounts map[string]S3Mount `json:"s3_mounts,omitempty"`
	// Tags contains sandbox metadata tags.
	Tags map[string]string `json:"tags,omitempty"`
	// FilesystemDirectory selects a host filesystem source where supported.
	FilesystemDirectory string `json:"fs_dir,omitempty"`
	// BlockNetwork disables guest network access.
	BlockNetwork bool `json:"block_network"`
	// Ports lists guest TCP ports to expose.
	Ports []uint16 `json:"ports,omitempty"`
	// EgressAllow lists allowed destination CIDRs.
	EgressAllow []string `json:"egress_allow,omitempty"`
	// EgressAllowDomains lists allowed destination domains.
	EgressAllowDomains []string `json:"egress_allow_domains,omitempty"`
	// InboundCIDRAllowlist lists source CIDRs allowed to reach exposed ports.
	InboundCIDRAllowlist []string `json:"inbound_cidr_allowlist,omitempty"`
	// ReadinessProbe is the daemon-defined readiness probe value.
	ReadinessProbe any `json:"readiness_probe,omitempty"`
	// PoolSize requests a warm pool size.
	PoolSize uint32 `json:"pool_size,omitempty"`
	// HA selects the high-availability policy.
	HA string `json:"ha,omitempty"`
	// Arch selects the guest architecture.
	Arch string `json:"arch,omitempty"`
	// IdempotencyKey makes repeated creates resolve to the same request.
	IdempotencyKey string `json:"idempotency_key,omitempty"`
	// Command overrides the foreground entrypoint.
	Command []string `json:"command,omitempty"`
}

// RecoveryPoint describes one immutable disk or checkpoint capture.
type RecoveryPoint struct {
	// Name is the server-assigned recovery-point identifier.
	Name string
	// Kind is "disk" for a cold-boot disk capture or "checkpoint" for saved VM execution state.
	Kind string
	// CreatedAtUnixMillis is the creation time in Unix milliseconds.
	CreatedAtUnixMillis uint64
	// SizeBytes is the stored recovery archive size in bytes.
	SizeBytes uint64
}

// S3Mount configures one S3 bucket or prefix mount for a sandbox.
type S3Mount struct {
	URI          string `json:"uri"`
	Endpoint     string `json:"endpoint,omitempty"`
	Region       string `json:"region,omitempty"`
	ReadOnly     bool   `json:"read_only,omitempty"`
	AccessKey    string `json:"access_key,omitempty"`
	SecretKey    string `json:"secret_key,omitempty"`
	SessionToken string `json:"session_token,omitempty"`
}

// SandboxListOptions filters sandbox list requests.
type SandboxListOptions struct {
	// Tags requires each key/value tag pair to match.
	Tags map[string]string
}

// SandboxPoll is one non-blocking sandbox lifecycle observation.
type SandboxPoll struct {
	// Sandbox is the latest view, or nil when the resource no longer exists.
	Sandbox *Sandbox
	// Exists reports whether the sandbox still exists.
	Exists bool
	// Done reports whether the sandbox reached a terminal state or no longer exists.
	Done bool
	// ExitCode is the foreground exit code when the daemon reported one.
	ExitCode *int64
}

// ExtendResult is the daemon response after extending a sandbox idle deadline.
type ExtendResult struct {
	// DeadlineUnix is the updated absolute Unix deadline.
	DeadlineUnix int64 `json:"deadline_unix"`
}

// MeshNode is one member of a mesh.
type MeshNode struct {
	NodeID    string          `json:"node_id"`
	Advertise string          `json:"advertise"`
	Region    string          `json:"region"`
	Raw       json.RawMessage `json:"-"`
}

// UnmarshalJSON decodes known mesh-node fields while preserving the original payload.
func (node *MeshNode) UnmarshalJSON(data []byte) error {
	type wire MeshNode
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*node = MeshNode(decoded)
	node.Raw = append(node.Raw[:0], data...)
	return nil
}

// MeshStatus is a node's typed mesh membership summary.
type MeshStatus struct {
	Self            MeshNode        `json:"self"`
	Peers           []MeshNode      `json:"peers"`
	ReplicasHeld    uint64          `json:"replicas_held"`
	ExpectedMembers uint64          `json:"expected_members"`
	Quorum          uint64          `json:"quorum"`
	Raw             json.RawMessage `json:"-"`
}

// UnmarshalJSON decodes known mesh-status fields while preserving the original payload.
func (status *MeshStatus) UnmarshalJSON(data []byte) error {
	type wire MeshStatus
	var decoded wire
	if err := json.Unmarshal(data, &decoded); err != nil {
		return err
	}
	*status = MeshStatus(decoded)
	status.Raw = append(status.Raw[:0], data...)
	return nil
}

// ExecRequest describes a command to run inside a sandbox.
type ExecRequest struct {
	// Command is the non-empty command argument vector sent as cmd.
	Command []string `json:"cmd"`
	// Workdir is the guest working directory.
	Workdir string `json:"workdir,omitempty"`
	// Env contains per-process environment variables.
	Env map[string]string `json:"env,omitempty"`
	// Timeout is the server-side timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
	// TTY requests a pseudoterminal.
	TTY bool `json:"tty,omitempty"`
}

// PersistencePolicy controls sandbox stored-state retention.
type PersistencePolicy struct {
	// Type is persistent, sticky, or ephemeral.
	Type string `json:"type"`
	// Priority is used by sticky storage GC. Lower priorities are evicted first.
	Priority *uint32 `json:"priority,omitempty"`
}

// NIC describes one routed VPC attachment.
type NIC struct {
	// VPC is the VPC identifier.
	VPC string `json:"vpc"`
	// IPv4 is either a requested address string or true for automatic assignment.
	IPv4 any `json:"ipv4"`
	// Default installs this NIC's default route.
	Default bool `json:"default"`
}

// ResizeOptions describes optional sandbox resource changes.
type ResizeOptions struct {
	// CPUs is the new whole-vCPU count. Zero leaves it unchanged.
	CPUs uint32
	// MemoryMiB is the new memory size in MiB. Zero leaves it unchanged.
	MemoryMiB uint32
	// DiskMB is the new root disk size in MB. Zero leaves it unchanged.
	DiskMB uint64
}

// PtyOpenOptions configures a persistent guest-owned terminal session.
type PtyOpenOptions struct {
	SessionID string
	Cols      uint32
	Rows      uint32
	Exec      string
	Env       map[string]string
	Workdir   string
}

// PtyAttachOptions configures reattachment and an optional terminal resize.
type PtyAttachOptions struct {
	Cols uint32
	Rows uint32
}

// PtySessionInfo describes one server-persistent terminal session.
type PtySessionInfo struct {
	SessionID           string
	Running             bool
	ExitCode            *int64
	Cols                uint32
	Rows                uint32
	Exec                *string
	CreatedAtUnixMillis int64
	AttachedCount       uint32
	Suspended           bool
}

// PtyCloseResult reports a closed terminal session.
type PtyCloseResult struct {
	SessionID string
	ExitCode  *int64
}

// VPC describes one routed private network.
type VPC struct {
	ID                  string
	Name                string
	CIDR                string
	CreatedAtUnixMillis int64
}

// VPCCreateOptions configures a routed private network.
type VPCCreateOptions struct {
	Name string
	CIDR string
}

// ExecResult is the decoded captured-exec response.
type ExecResult struct {
	// ExitCode is the process exit code.
	ExitCode int64
	// Stdout is the captured standard output.
	Stdout []byte
	// Stderr is the captured standard error.
	Stderr []byte
}

// ExecExit describes the terminal event of a streaming exec or shell.
type ExecExit struct {
	// Code is the process exit code.
	Code int64
	// Signal is the terminating signal when one was reported.
	Signal *int
}

// StreamName identifies an exec or attach output stream.
type StreamName string

const (
	// StreamStdout identifies standard output.
	StreamStdout StreamName = "stdout"
	// StreamStderr identifies standard error.
	StreamStderr StreamName = "stderr"
	// StreamConsole identifies attached console output.
	StreamConsole StreamName = "console"
)

// ExecEvent is one output or exit event from a streaming exec.
type ExecEvent struct {
	// Stream identifies Data's source; it is empty for an exit event.
	Stream StreamName
	// Data contains decoded stream bytes.
	Data []byte
	// Exit is non-nil for the terminal event.
	Exit *ExecExit
}

// StreamEvent is one decoded output event from an attach stream.
type StreamEvent struct {
	// Stream identifies Data's source.
	Stream StreamName
	// Data contains decoded stream bytes.
	Data []byte
}

// FileInfo describes one guest filesystem entry.
type FileInfo struct {
	// OK is set by stat responses.
	OK bool `json:"ok,omitempty"`
	// Name is populated by directory-list responses.
	Name string `json:"name,omitempty"`
	// Type is the daemon's file type string.
	Type string `json:"type"`
	// Size is the file size in bytes.
	Size uint64 `json:"size"`
	// Mode is the guest file mode.
	Mode uint32 `json:"mode"`
	// ModTime is the guest Unix modification time.
	ModTime int64 `json:"mtime"`
}

// NetworkPolicy is the update body for a sandbox network policy.
type NetworkPolicy struct {
	// BlockNetwork changes whether all guest network access is blocked.
	BlockNetwork *bool `json:"block_network,omitempty"`
	// CIDRAllow non-nil replaces the allowed destination CIDRs, including with an empty list.
	CIDRAllow *[]string `json:"cidr_allow,omitempty"`
	// DomainAllow non-nil replaces the allowed destination domains, including with an empty list.
	DomainAllow *[]string `json:"domain_allow,omitempty"`
}

// NetworkState describes the effective persisted sandbox network settings.
type NetworkState struct {
	// BlockNetwork reports whether network access is blocked.
	BlockNetwork *bool `json:"block_network"`
	// CIDRAllow contains response-native allowed CIDRs when supplied by a peer.
	CIDRAllow []string `json:"cidr_allow,omitempty"`
	// DomainAllow contains response-native allowed domains when supplied by a peer.
	DomainAllow []string `json:"domain_allow,omitempty"`
	// EgressAllow contains persisted allowed destination CIDRs.
	EgressAllow []string `json:"egress_allow"`
	// EgressAllowDomains contains persisted allowed destination domains.
	EgressAllowDomains []string `json:"egress_allow_domains"`
	// InboundCIDRAllowlist contains persisted inbound source CIDRs.
	InboundCIDRAllowlist []string `json:"inbound_cidr_allowlist"`
}

// TunnelTarget identifies the daemon-side target for one exposed guest port.
type TunnelTarget struct {
	// Host is the target host address.
	Host string `json:"host"`
	// Port is the target TCP port.
	Port uint16 `json:"port"`
}

// TunnelSet contains exposed ports and the short-lived proxy connection token.
type TunnelSet struct {
	// Tunnels maps guest port numbers to daemon-side targets.
	Tunnels map[uint16]TunnelTarget `json:"tunnels"`
	// ConnectToken authenticates HTTP and WebSocket port proxy requests.
	ConnectToken string `json:"connect_token"`
}

// SnapshotRequest is the request body for a full sandbox snapshot.
type SnapshotRequest struct {
	// Name requests a snapshot name.
	Name string `json:"name,omitempty"`
	// Stop stops the sandbox while taking the snapshot.
	Stop bool `json:"stop,omitempty"`
}

// SnapshotResult is the response from a full sandbox snapshot.
type SnapshotResult struct {
	// Snapshot is the created snapshot name.
	Snapshot string `json:"snapshot"`
}

// FilesystemSnapshotRequest is the request body for a filesystem-only snapshot.
type FilesystemSnapshotRequest struct {
	// Name requests an image name.
	Name string `json:"name,omitempty"`
}

// FilesystemSnapshotResult is the response from a filesystem-only snapshot.
type FilesystemSnapshotResult struct {
	// Image is the created filesystem image name.
	Image string `json:"image"`
}

// RestoreRequest describes a snapshot restore and optional runtime overrides.
type RestoreRequest struct {
	// Name requests a sandbox name.
	Name string
	// Agent controls whether restore waits for the guest agent.
	Agent *bool
	// Overrides contains environment, tags, timeout, secrets, S3, readiness, or command fields.
	Overrides map[string]any
}

// MarshalJSON merges restore fields and overrides into the daemon request shape.
func (request RestoreRequest) MarshalJSON() ([]byte, error) {
	for _, key := range []string{"name", "agent"} {
		if _, exists := request.Overrides[key]; exists {
			return nil, fmt.Errorf("vmon: override %q conflicts with a typed request field", key)
		}
	}
	known := make(map[string]any, 2)
	if request.Name != "" {
		known["name"] = request.Name
	}
	if request.Agent != nil {
		known["agent"] = *request.Agent
	}
	return marshalWithExtras(known, request.Overrides)
}

// ForkRequest describes an ordered batch of clones from a snapshot.
type ForkRequest struct {
	// Count is the number of clones to create atomically, from 1 through 32.
	Count uint32
	// Overrides contains runtime-only fields shared by every clone.
	Overrides map[string]any
}

// MarshalJSON merges the fork count and runtime overrides.
func (request ForkRequest) MarshalJSON() ([]byte, error) {
	return marshalWithExtras(map[string]any{"count": request.Count}, request.Overrides)
}

// ForkResult contains the sandbox views created by a snapshot fork.
type ForkResult struct {
	// Clones is the ordered list of created sandbox views.
	Clones []*Sandbox `json:"clones"`
}

// PoolRequest describes a warm-pool size and template overrides.
type PoolRequest struct {
	// Size is the desired warm-pool size.
	Size uint32
	// Template contains image-pipeline template arguments.
	Template map[string]any
}

// MarshalJSON merges the pool size and template arguments.
func (request PoolRequest) MarshalJSON() ([]byte, error) {
	return marshalWithExtras(map[string]any{"size": request.Size}, request.Template)
}

// PoolStats is one warm-pool statistics snapshot.
type PoolStats struct {
	// Ready is the number of ready sandboxes.
	Ready uint64 `json:"ready"`
	// Hits is the number of successful pool acquisitions.
	Hits uint64 `json:"hits"`
	// Misses is the number of pool misses.
	Misses uint64 `json:"misses"`
	// Size is the configured pool size.
	Size uint64 `json:"size"`
}

// ShellRequest describes a WebSocket shell session.
type ShellRequest struct {
	// Reference names an existing sandbox, template, or image.
	Reference string `json:"ref,omitempty"`
	// Image explicitly selects an image for an ephemeral shell.
	Image string `json:"image,omitempty"`
	// Command is the shell command argument vector.
	Command []string `json:"cmd,omitempty"`
	// CPUs is the ephemeral shell virtual CPU count.
	CPUs uint32 `json:"cpus,omitempty"`
	// MemoryMiB is the ephemeral shell memory in MiB.
	MemoryMiB uint32 `json:"mem,omitempty"`
	// DiskMiB is the ephemeral shell disk size in MiB.
	DiskMiB uint32 `json:"disk_mb,omitempty"`
	// Timeout is the ephemeral shell timeout in seconds.
	Timeout *float64 `json:"timeout,omitempty"`
}

// Event is one open-ended lifecycle event from the daemon.
type Event struct {
	// Fields retains each event field as lossless JSON.
	Fields map[string]json.RawMessage
}

// UnmarshalJSON requires an event object.
func (event *Event) UnmarshalJSON(data []byte) error {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	if fields == nil {
		return fmt.Errorf("event is not an object")
	}
	event.Fields = fields
	return nil
}

// Value returns one raw event field.
func (event Event) Value(name string) (json.RawMessage, bool) {
	value, found := event.Fields[name]
	return value, found
}

func marshalWithExtras(known map[string]any, extras map[string]any) ([]byte, error) {
	for key, value := range extras {
		if _, reserved := known[key]; reserved {
			return nil, fmt.Errorf("vmon: override %q conflicts with a typed request field", key)
		}
		known[key] = value
	}
	return json.Marshal(known)
}
