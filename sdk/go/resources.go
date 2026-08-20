package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
)

// Health retrieves the health status of the vmon daemon.
func (client *Client) Health(ctx context.Context) (Health, error) {
	var out Health
	err := client.doJSON(ctx, http.MethodGet, "/healthz", nil, nil, &out)
	return out, err
}

// Info retrieves information about the vmon server.
func (client *Client) Info(ctx context.Context) (ServerInfo, error) {
	var out ServerInfo
	_, view, err := client.unaryView(ctx, "", "get info", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewSystemServiceClient(conn).Info(ctx, &pb.InfoRequest{}, opts...)
	})
	if err != nil {
		return out, err
	}
	err = decodeJSONView(view, "get info", &out)
	return out, err
}

// Metrics retrieves Prometheus metrics from the daemon.
func (client *Client) Metrics(ctx context.Context) (string, error) {
	response, _, err := client.request(ctx, DriverRequest{Method: http.MethodGet, Path: "/metrics", Headers: http.Header{"Accept": {"text/plain"}}})
	if err != nil {
		return "", err
	}
	body, err := client.readResponse(response)
	return string(body), err
}

// EventStream incrementally decodes daemon lifecycle events.
type EventStream struct {
	cancel    context.CancelFunc
	stream    grpc.ServerStreamingClient[pb.JsonView]
	readMu    sync.Mutex
	closeOnce sync.Once
}

// Events opens a stream to receive lifecycle events from the daemon.
func (client *Client) Events(ctx context.Context) (*EventStream, error) {
	endpoint, err := client.grpcEndpoint("")
	if err != nil {
		return nil, err
	}
	conn, err := client.conn(endpoint)
	if err != nil {
		return nil, err
	}
	streamCtx, cancel := context.WithCancel(ctx)
	stream, err := pb.NewSystemServiceClient(conn).Events(streamCtx, &pb.EventsRequest{})
	if err != nil {
		cancel()
		return nil, apiErrorFromStatus(err, "open events")
	}
	return &EventStream{cancel: cancel, stream: stream}, nil
}

// Next block-reads and decodes the next event from the stream.
func (stream *EventStream) Next(ctx context.Context) (Event, error) {
	if stream == nil || stream.stream == nil {
		return Event{}, errors.New("vmon: event stream is not open")
	}
	stream.readMu.Lock()
	defer stream.readMu.Unlock()
	stop := context.AfterFunc(ctx, func() { _ = stream.Close() })
	defer stop()
	view, err := stream.stream.Recv()
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return Event{}, ctxErr
		}
		if errors.Is(err, io.EOF) {
			return Event{}, io.EOF
		}
		return Event{}, apiErrorFromStatus(err, "read events", stream.stream.Trailer())
	}
	return decodeEvent([]byte(view.GetJson()))
}

// Close terminates the event stream.
func (stream *EventStream) Close() error {
	if stream == nil {
		return nil
	}
	stream.closeOnce.Do(func() {
		if stream.cancel != nil {
			stream.cancel()
		}
	})
	return nil
}
func decodeEvent(data []byte) (Event, error) {
	var event Event
	if err := json.Unmarshal(data, &event); err != nil {
		return Event{}, &ProtocolError{Operation: "read events", Message: "invalid event JSON", Err: err}
	}
	return event, nil
}

// SandboxService manages sandbox resources.
type SandboxService struct{ client *Client }

// Create provisions a new sandbox.
func (service *SandboxService) Create(ctx context.Context, request SandboxCreateRequest) (*Sandbox, error) {
	spec, err := json.Marshal(request)
	if err != nil {
		return nil, fmt.Errorf("vmon: encode create request: %w", err)
	}
	endpoint, view, err := service.client.unaryView(ctx, "", "create sandbox", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewSandboxServiceClient(conn).Create(ctx, &pb.CreateSandboxRequest{SpecJson: string(spec)}, opts...)
	})
	if err != nil {
		return nil, err
	}
	var sandbox Sandbox
	if err = decodeJSONView(view, "create sandbox", &sandbox); err != nil {
		return nil, err
	}
	return service.client.bindSandbox(&sandbox, endpoint, "create sandbox")
}

// Get reconnects to an existing sandbox by stable ID.
func (service *SandboxService) Get(ctx context.Context, id string) (*Sandbox, error) {
	if err := requireIdentifier("sandbox id", id); err != nil {
		return nil, err
	}
	fetch := func(hint string) (string, []byte, error) {
		return service.client.unaryView(ctx, hint, "get sandbox", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
			return pb.NewSandboxServiceClient(conn).Get(ctx, &pb.SandboxRef{Id: id}, opts...)
		})
	}
	endpoint, view, err := fetch("")
	if err != nil && isNotFoundAPIError(err) && len(service.client.driver.Endpoints()) > 1 {
		endpoint, err = service.client.resolveSandbox(ctx, id, "")
		if err == nil {
			endpoint, view, err = fetch(endpoint)
		}
	}
	if err != nil {
		return nil, err
	}
	var sandbox Sandbox
	if err = decodeJSONView(view, "get sandbox", &sandbox); err != nil {
		return nil, err
	}
	return service.client.bindSandbox(&sandbox, endpoint, "get sandbox")
}

// Ref returns a local reference to a sandbox without calling the server.
func (service *SandboxService) Ref(id string) *Sandbox {
	sandbox := &Sandbox{ID: id, client: service.client}
	sandbox.initServices()
	return sandbox
}

// List lists sandboxes matching the optional filters.
func (service *SandboxService) List(ctx context.Context, options ...SandboxListOptions) ([]*Sandbox, error) {
	var filter SandboxListOptions
	if len(options) > 0 {
		filter = options[0]
	}
	endpoints := service.client.driver.Endpoints()
	healthy := make([]EndpointInfo, 0, len(endpoints))
	for _, entry := range endpoints {
		if entry.Healthy {
			healthy = append(healthy, entry)
		}
	}
	if len(healthy) == 0 {
		healthy = append(healthy, EndpointInfo{})
	}

	type listResult struct {
		index    int
		endpoint string
		rows     []*Sandbox
		err      error
	}
	results := make(chan listResult, len(healthy))
	filters := tagFilters(filter.Tags)
	for index, entry := range healthy {
		go func(index int, entry EndpointInfo) {
			var response *pb.ListSandboxesResponse
			endpoint, err := service.client.unary(ctx, entry.URL, "list sandboxes", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
				var callErr error
				response, callErr = pb.NewSandboxServiceClient(conn).List(ctx, &pb.ListSandboxesRequest{Tags: filters}, opts...)
				return callErr
			})
			if err != nil {
				results <- listResult{index: index, err: err}
				return
			}
			rows, err := decodeSandboxRows(response.GetSandboxesJson())
			results <- listResult{index: index, endpoint: endpoint, rows: rows, err: err}
		}(index, entry)
	}

	ordered := make([]listResult, len(healthy))
	for range healthy {
		result := <-results
		ordered[result.index] = result
	}
	values := make([]*Sandbox, 0)
	seen := make(map[string]bool)
	successes := 0
	var lastTransport error
	for _, result := range ordered {
		if result.err != nil {
			var transportErr *TransportError
			if errors.As(result.err, &transportErr) {
				lastTransport = result.err
				continue
			}
			return nil, result.err
		}
		successes++
		for _, sandbox := range result.rows {
			if sandbox == nil || seen[sandbox.ID] {
				continue
			}
			bound, err := service.client.bindSandbox(sandbox, result.endpoint, "list sandboxes")
			if err != nil {
				return nil, err
			}
			seen[sandbox.ID] = true
			values = append(values, bound)
		}
	}
	if successes == 0 && lastTransport != nil {
		return nil, lastTransport
	}
	sort.SliceStable(values, func(i, j int) bool { return values[i].ID < values[j].ID })
	return values, nil
}
func decodeSandboxRows(rowsJSON []string) ([]*Sandbox, error) {
	rows := make([]*Sandbox, 0, len(rowsJSON))
	for _, raw := range rowsJSON {
		var sandbox Sandbox
		if err := json.Unmarshal([]byte(raw), &sandbox); err != nil {
			return nil, &ProtocolError{Operation: "list sandboxes", Message: "invalid sandbox view JSON", Err: err}
		}
		rows = append(rows, &sandbox)
	}
	return rows, nil
}

// SnapshotService manages filesystem and memory snapshots.
type SnapshotService struct{ client *Client }

// List retrieves the names of all snapshots.
func (s *SnapshotService) List(ctx context.Context) ([]string, error) {
	var out *pb.SnapshotList
	_, err := s.client.unary(ctx, "", "list snapshots", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var callErr error
		out, callErr = pb.NewSnapshotServiceClient(conn).List(ctx, &pb.ListSnapshotsRequest{}, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	snapshots := out.GetSnapshots()
	if snapshots == nil {
		snapshots = []string{}
	}
	return snapshots, nil
}

// Restore reverts a sandbox to a specific snapshot.
func (s *SnapshotService) Restore(ctx context.Context, name string, request RestoreRequest) (*Sandbox, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	endpoint, view, err := s.client.unaryView(ctx, "", "restore snapshot", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewSnapshotServiceClient(conn).Restore(ctx, &pb.RestoreSnapshotRequest{Name: name, BodyJson: string(body)}, opts...)
	})
	if err != nil {
		return nil, err
	}
	var out Sandbox
	if err = decodeJSONView(view, "restore snapshot", &out); err != nil {
		return nil, err
	}
	return s.client.bindSandbox(&out, endpoint, "restore snapshot")
}

// Fork creates one or more clones from a snapshot.
func (s *SnapshotService) Fork(ctx context.Context, name string, request ForkRequest) ([]*Sandbox, error) {
	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}
	endpoint, view, err := s.client.unaryView(ctx, "", "fork snapshot", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewSnapshotServiceClient(conn).Fork(ctx, &pb.ForkSnapshotRequest{Name: name, BodyJson: string(body)}, opts...)
	})
	if err != nil {
		return nil, err
	}
	var out struct {
		Clones []*Sandbox `json:"clones"`
	}
	if err = decodeJSONView(view, "fork snapshot", &out); err != nil {
		return nil, err
	}
	if out.Clones == nil {
		return nil, &ProtocolError{Operation: "fork snapshot", Message: "response did not include clones"}
	}
	rows := out.Clones
	for _, sandbox := range rows {
		if _, err = s.client.bindSandbox(sandbox, endpoint, "fork snapshot"); err != nil {
			return nil, err
		}
	}
	return rows, nil
}

// Delete permanently removes a named snapshot.
func (s *SnapshotService) Delete(ctx context.Context, name string) error {
	if err := requireIdentifier("snapshot name", name); err != nil {
		return err
	}
	_, err := s.client.unary(ctx, "", "delete snapshot", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		_, callErr := pb.NewSnapshotServiceClient(conn).Delete(ctx, &pb.SnapshotRef{Name: name}, opts...)
		return callErr
	})
	return err
}

// VolumeService manages persistent storage volumes.
type VolumeService struct{ client *Client }

// List retrieves all persistent volumes.
func (s *VolumeService) List(ctx context.Context) ([]Volume, error) {
	var out *pb.VolumeList
	_, err := s.client.unary(ctx, "", "list volumes", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var callErr error
		out, callErr = pb.NewVolumeServiceClient(conn).List(ctx, &pb.ListVolumesRequest{}, opts...)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	names := out.GetVolumes()
	values := make([]Volume, 0, len(names))
	for _, name := range names {
		value, err := NewVolume(name)
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

// Create provisions a new persistent volume.
func (s *VolumeService) Create(ctx context.Context, name string) (Volume, error) {
	value, err := NewVolume(name)
	if err != nil {
		return Volume{}, err
	}
	_, err = s.client.unary(ctx, "", "create volume", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		_, callErr := pb.NewVolumeServiceClient(conn).Create(ctx, &pb.VolumeRef{Name: name}, opts...)
		return callErr
	})
	return value, err
}

// Delete removes a persistent volume by name.
func (s *VolumeService) Delete(ctx context.Context, name string) error {
	if _, err := NewVolume(name); err != nil {
		return err
	}
	_, err := s.client.unary(ctx, "", "delete volume", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		_, callErr := pb.NewVolumeServiceClient(conn).Delete(ctx, &pb.VolumeRef{Name: name}, opts...)
		return callErr
	})
	return err
}

// PoolService manages sandbox resource pools.
type PoolService struct{ client *Client }

// List retrieves resource usage stats for all pools.
func (s *PoolService) List(ctx context.Context) (map[string]PoolStats, error) {
	var out map[string]PoolStats
	_, view, err := s.client.unaryView(ctx, "", "list pools", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewPoolServiceClient(conn).List(ctx, &pb.ListPoolsRequest{}, opts...)
	})
	if err != nil {
		return nil, err
	}
	if err = decodeJSONView(view, "list pools", &out); err != nil {
		return nil, err
	}
	if out == nil {
		return nil, &ProtocolError{Operation: "list pools", Message: "response is not an object"}
	}
	return out, nil
}

// Set updates the resource allocation or parameters for a pool.
func (s *PoolService) Set(ctx context.Context, reference string, request PoolRequest) (PoolStats, error) {
	var out PoolStats
	body, err := json.Marshal(request)
	if err != nil {
		return out, err
	}
	_, view, err := s.client.unaryView(ctx, "", "set pool", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewPoolServiceClient(conn).Set(ctx, &pb.PoolSetRequest{Reference: reference, BodyJson: string(body)}, opts...)
	})
	if err != nil {
		return out, err
	}
	err = decodeJSONView(view, "set pool", &out)
	return out, err
}

// Delete removes a pool allocation.
func (s *PoolService) Delete(ctx context.Context, reference string) error {
	_, err := s.client.unary(ctx, "", "delete pool", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		_, callErr := pb.NewPoolServiceClient(conn).Delete(ctx, &pb.PoolRef{Reference: reference}, opts...)
		return callErr
	})
	return err
}

// Clear removes all configured pools.
func (s *PoolService) Clear(ctx context.Context) error {
	pools, err := s.List(ctx)
	if err != nil {
		return err
	}
	for reference := range pools {
		if err := s.Delete(ctx, reference); err != nil {
			return err
		}
	}
	return nil
}

// VpcService manages routed private networks.
type VpcService struct{ client *Client }

// Vpcs returns the routed private-network service.
func (client *Client) Vpcs() *VpcService { return &VpcService{client: client} }

// Create provisions a routed private network.
func (service *VpcService) Create(ctx context.Context, options VPCCreateOptions) (VPC, error) {
	var response *pb.Vpc
	_, err := service.client.unary(ctx, "", "create vpc", func(
		ctx context.Context,
		conn grpc.ClientConnInterface,
		opts ...grpc.CallOption,
	) error {
		var callErr error
		response, callErr = pb.NewVpcServiceClient(conn).Create(ctx, &pb.VpcCreateRequest{
			Name: options.Name,
			Cidr: options.CIDR,
		}, opts...)
		return callErr
	})
	if err != nil {
		return VPC{}, err
	}
	return vpcFromProto(response), nil
}

// List lists routed private networks.
func (service *VpcService) List(ctx context.Context) ([]VPC, error) {
	var response *pb.VpcList
	_, err := service.client.unary(ctx, "", "list vpcs", func(
		ctx context.Context,
		conn grpc.ClientConnInterface,
		opts ...grpc.CallOption,
	) error {
		var callErr error
		response, callErr = pb.NewVpcServiceClient(conn).List(
			ctx,
			&pb.ListVpcsRequest{},
			opts...,
		)
		return callErr
	})
	if err != nil {
		return nil, err
	}
	vpcs := response.GetVpcs()
	out := make([]VPC, 0, len(vpcs))
	for _, vpc := range vpcs {
		out = append(out, vpcFromProto(vpc))
	}
	return out, nil
}

// Delete removes an unattached routed private network.
func (service *VpcService) Delete(ctx context.Context, id string) error {
	if id == "" {
		return errors.New("vmon: vpc id must not be empty")
	}
	_, err := service.client.unary(ctx, "", "delete vpc", func(
		ctx context.Context,
		conn grpc.ClientConnInterface,
		opts ...grpc.CallOption,
	) error {
		_, callErr := pb.NewVpcServiceClient(conn).Delete(ctx, &pb.VpcRef{Id: id}, opts...)
		return callErr
	})
	return err
}

func vpcFromProto(vpc *pb.Vpc) VPC {
	return VPC{
		ID:                  vpc.GetId(),
		Name:                vpc.GetName(),
		CIDR:                vpc.GetCidr(),
		CreatedAtUnixMillis: vpc.GetCreatedAtUnixMillis(),
	}
}

// MeshService monitors mesh cluster topology.
type MeshService struct{ client *Client }

// Status returns the current mesh membership and replication status.
func (s *MeshService) Status(ctx context.Context) (MeshStatus, error) {
	var out MeshStatus
	raw, err := s.client.meshStatusJSON(ctx)
	if err != nil {
		return out, err
	}
	err = decodeJSONView(raw, "mesh status", &out)
	return out, err
}

// Nodes lists all known nodes in the mesh topology.
func (s *MeshService) Nodes(ctx context.Context) ([]MeshNode, error) {
	status, err := s.Status(ctx)
	if err != nil {
		return nil, err
	}
	return append([]MeshNode{status.Self}, status.Peers...), nil
}

func tagFilters(tags map[string]string) []string {
	keys := make([]string, 0, len(tags))
	for key := range tags {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	filters := make([]string, 0, len(keys))
	for _, key := range keys {
		filters = append(filters, key+"="+tags[key])
	}
	return filters
}
