package vmon

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	pb "github.com/stencil-hq/vibemon/sdk/go/internal/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const (
	defaultMaxResponseBytes int64 = 64 << 20
	maxErrorResponseBytes   int64 = 64 << 10
	maxErrorMessageBytes          = 8 << 10
	// grpcMaxMessageBytes mirrors the daemon's 64 MiB gRPC message limit.
	grpcMaxMessageBytes = 64 << 20
	// vmonCodeMetadataKey carries the stable daemon error code on gRPC statuses.
	vmonCodeMetadataKey = "vmon-code"
)

// Option configures Connect or NewClient.
type Option func(*clientConfig)

type clientConfig struct {
	httpClient       *http.Client
	token            string
	userAgent        string
	maxResponseBytes int64
	discovery        *bool
	timeout          time.Duration
	grpcDialer       func(context.Context, string) (net.Conn, error)
}

// WithToken sets the bearer token.
func WithToken(token string) Option { return func(c *clientConfig) { c.token = token } }

// WithHTTPClient sets the HTTP client used by the mesh driver.
func WithHTTPClient(client *http.Client) Option {
	return func(c *clientConfig) {
		if client != nil {
			c.httpClient = client
		}
	}
}

// WithUserAgent sets the HTTP User-Agent header.
func WithUserAgent(value string) Option { return func(c *clientConfig) { c.userAgent = value } }

// WithMaxResponseBytes limits buffered successful response bodies.
func WithMaxResponseBytes(value int64) Option {
	return func(c *clientConfig) {
		if value > 0 {
			c.maxResponseBytes = value
		}
	}
}

// WithDiscovery overrides DSN mesh discovery.
func WithDiscovery(enabled bool) Option { return func(c *clientConfig) { c.discovery = &enabled } }

// WithTimeout overrides the DSN request timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(c *clientConfig) {
		if timeout > 0 {
			c.timeout = timeout
		}
	}
}

// withGRPCDialer overrides the raw connection dialer used for gRPC endpoints (tests).
func withGRPCDialer(dialer func(context.Context, string) (net.Conn, error)) Option {
	return func(c *clientConfig) { c.grpcDialer = dialer }
}

func defaultClientConfig() clientConfig {
	return clientConfig{httpClient: &http.Client{}, maxResponseBytes: defaultMaxResponseBytes}
}

// Connect parses dsn, constructs its mesh driver, and returns a ready client.
func Connect(dsn string, options ...Option) (*Client, error) {
	config, err := ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	settings := defaultClientConfig()
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	if settings.discovery != nil {
		config.Discover = *settings.discovery
	}
	if settings.timeout > 0 {
		config.Timeout = settings.timeout
	}
	settings.token = firstNonempty(settings.token, config.Token)
	driver, err := newMeshDriver(config, withMeshHTTPClient(settings.httpClient), withMeshToken(settings.token), withMeshUserAgent(settings.userAgent), withMeshMaxResponseBytes(settings.maxResponseBytes))
	if err != nil {
		return nil, err
	}
	return newClient(driver, settings), nil
}

func firstNonempty(values ...string) string {
	for _, value := range values {
		if value != "" {
			return value
		}
	}
	return ""
}

// Client is a driver-backed vmon API client.
type Client struct {
	driver           Driver
	maxResponseBytes int64
	token            string
	grpcDialer       func(context.Context, string) (net.Conn, error)
	connMu           sync.Mutex
	conns            map[string]*grpc.ClientConn
	Sandboxes        *SandboxService
	Snapshots        *SnapshotService
	Volumes          *VolumeService
	Pools            *PoolService
	Mesh             *MeshService
}

// NewClient binds the service object model to an existing driver.
func NewClient(driver Driver, options ...Option) *Client {
	settings := defaultClientConfig()
	for _, option := range options {
		if option != nil {
			option(&settings)
		}
	}
	return newClient(driver, settings)
}

func newClient(driver Driver, settings clientConfig) *Client {
	client := &Client{
		driver:           driver,
		maxResponseBytes: settings.maxResponseBytes,
		token:            settings.token,
		grpcDialer:       settings.grpcDialer,
		conns:            make(map[string]*grpc.ClientConn),
	}
	client.Sandboxes = &SandboxService{client: client}
	client.Snapshots = &SnapshotService{client: client}
	client.Volumes = &VolumeService{client: client}
	client.Pools = &PoolService{client: client}
	client.Mesh = &MeshService{client: client}
	if mesh, ok := driver.(*meshDriver); ok {
		mesh.meshStatus = client.meshStatusJSON
	}
	return client
}

// Driver returns the transport backing this client.
func (client *Client) Driver() Driver { return client.driver }

// Close releases all resources associated with the client.
func (client *Client) Close() error {
	if client == nil {
		return nil
	}
	client.connMu.Lock()
	conns := client.conns
	client.conns = nil
	client.connMu.Unlock()
	for _, conn := range conns {
		_ = conn.Close()
	}
	if client.driver == nil {
		return nil
	}
	return client.driver.Close()
}

// APIError is a structured daemon error.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	Truncated  bool
	Retryable  bool
	Action     string
}

// Error returns the string representation of the API error.
func (err *APIError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.StatusCode != 0 {
		return fmt.Sprintf("vmon API error %d (%s): %s", err.StatusCode, err.Code, err.Message)
	}
	return fmt.Sprintf("vmon API error (%s): %s", err.Code, err.Message)
}

// ProtocolError represents a failure to communicate with the daemon or parse its response.
type ProtocolError struct {
	Operation string
	Message   string
	Err       error
}

// Error returns the string representation of the protocol error.
func (err *ProtocolError) Error() string {
	if err == nil {
		return "<nil>"
	}
	if err.Operation == "" {
		return "vmon protocol error: " + err.Message
	}
	return "vmon protocol error during " + err.Operation + ": " + err.Message
}

// Unwrap returns the underlying error, if any.
func (err *ProtocolError) Unwrap() error {
	if err == nil {
		return nil
	}
	return err.Err
}

// ResponseTooLargeError indicates that the server response exceeded the configured byte limit.
type ResponseTooLargeError struct{ Limit int64 }

// Error returns the string representation of the ResponseTooLargeError.
func (err *ResponseTooLargeError) Error() string {
	return fmt.Sprintf("vmon: response exceeds %d-byte limit", err.Limit)
}

func escapePathSegment(value string) string {
	if value == "." {
		return "%2E"
	}
	if value == ".." {
		return "%2E%2E"
	}
	return url.PathEscape(value)
}
func escapeRestPath(value string) string {
	value = strings.TrimLeft(value, "/")
	if value == "" {
		return ""
	}
	parts := strings.Split(value, "/")
	for i := range parts {
		parts[i] = escapePathSegment(parts[i])
	}
	return strings.Join(parts, "/")
}
func cloneValues(values url.Values) url.Values {
	result := make(url.Values, len(values))
	for key, entries := range values {
		result[key] = append([]string(nil), entries...)
	}
	return result
}
func requireIdentifier(kind, value string) error {
	if value == "" {
		return fmt.Errorf("vmon: %s must not be empty", kind)
	}
	return nil
}

// ---- gRPC transport core ----

// grpcTarget maps a normalized driver endpoint to a gRPC dial target.
func grpcTarget(endpoint string) (string, error) {
	if path, ok := strings.CutPrefix(endpoint, "vmon+unix://"); ok {
		return "unix://" + path, nil
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("vmon: invalid endpoint %q: %w", endpoint, err)
	}
	host := parsed.Host
	if host == "" {
		return "", fmt.Errorf("vmon: invalid endpoint %q", endpoint)
	}
	if parsed.Port() == "" {
		port := "80"
		if parsed.Scheme == "https" {
			port = "443"
		}
		host = net.JoinHostPort(parsed.Hostname(), port)
	}
	return "passthrough:///" + host, nil
}

func (client *Client) authContext(ctx context.Context) context.Context {
	if client.token == "" {
		return ctx
	}
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+client.token)
}

func (client *Client) unaryAuthInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, request, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		return invoker(client.authContext(ctx), method, request, reply, cc, opts...)
	}
}

func (client *Client) streamAuthInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return streamer(client.authContext(ctx), desc, cc, method, opts...)
	}
}

// conn returns the cached gRPC connection for endpoint, dialing it lazily.
func (client *Client) conn(endpoint string) (*grpc.ClientConn, error) {
	client.connMu.Lock()
	defer client.connMu.Unlock()
	if client.conns == nil {
		return nil, errors.New("vmon: client is closed")
	}
	if existing := client.conns[endpoint]; existing != nil {
		return existing, nil
	}
	target, err := grpcTarget(endpoint)
	if err != nil {
		return nil, err
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(grpcMaxMessageBytes),
			grpc.MaxCallSendMsgSize(grpcMaxMessageBytes),
		),
		grpc.WithUnaryInterceptor(client.unaryAuthInterceptor()),
		grpc.WithStreamInterceptor(client.streamAuthInterceptor()),
	}
	if client.grpcDialer != nil {
		options = append(options, grpc.WithContextDialer(client.grpcDialer))
	}
	conn, err := grpc.NewClient(target, options...)
	if err != nil {
		return nil, &TransportError{Endpoint: endpoint, Err: err}
	}
	client.conns[endpoint] = conn
	return conn, nil
}

// grpcCandidates orders endpoints for one RPC: affinity hint first, then
// healthy roster entries, then the rest.
func (client *Client) grpcCandidates(hint string) []string {
	endpoints := client.driver.Endpoints()
	ordered := make([]string, 0, len(endpoints)+1)
	seen := make(map[string]bool, len(endpoints)+1)
	add := func(endpoint string) {
		if endpoint != "" && !seen[endpoint] {
			seen[endpoint] = true
			ordered = append(ordered, endpoint)
		}
	}
	add(hint)
	for _, entry := range endpoints {
		if entry.Healthy {
			add(entry.URL)
		}
	}
	for _, entry := range endpoints {
		add(entry.URL)
	}
	return ordered
}

// grpcEndpoint picks the endpoint an RPC or stream should be opened against.
func (client *Client) grpcEndpoint(hint string) (string, error) {
	if client == nil || client.driver == nil {
		return "", errors.New("vmon: client has no driver")
	}
	candidates := client.grpcCandidates(hint)
	if len(candidates) == 0 {
		return "", &TransportError{Err: errors.New("no vmon endpoints are currently available")}
	}
	return candidates[0], nil
}

var grpcCodeToVmonCode = map[codes.Code]string{
	codes.NotFound:           "not_found",
	codes.InvalidArgument:    "invalid",
	codes.Unauthenticated:    "unauthorized",
	codes.PermissionDenied:   "forbidden",
	codes.FailedPrecondition: "not_running",
	codes.Aborted:            "busy",
	codes.Unimplemented:      "unsupported",
	codes.Unavailable:        "engine_error",
}

var vmonCodeToHTTPStatus = map[string]int{
	"not_found":    http.StatusNotFound,
	"invalid":      http.StatusBadRequest,
	"unauthorized": http.StatusUnauthorized,
	"forbidden":    http.StatusForbidden,
	"not_running":  http.StatusConflict,
	"busy":         http.StatusConflict,
	"unsupported":  http.StatusNotImplemented,
	"engine":       http.StatusBadGateway,
	"engine_error": http.StatusBadGateway,
}

func vmonCodeFromMetadata(responseMetadata []metadata.MD) string {
	for _, md := range responseMetadata {
		if values := md.Get(vmonCodeMetadataKey); len(values) != 0 && values[0] != "" {
			return values[0]
		}
	}
	return ""
}

func vmonRetryableFromMetadata(responseMetadata []metadata.MD) (bool, bool) {
	for _, md := range responseMetadata {
		if values := md.Get("vmon-retryable"); len(values) != 0 && values[0] != "" {
			return strings.ToLower(values[0]) == "true", true
		}
	}
	return false, false
}

func vmonActionFromMetadata(responseMetadata []metadata.MD) string {
	for _, md := range responseMetadata {
		if values := md.Get("vmon-action"); len(values) != 0 && values[0] != "" {
			return values[0]
		}
	}
	return ""
}

// apiErrorFromStatus converts a gRPC call error into the SDK error taxonomy.
// The stable daemon code is read from `vmon-code` response metadata (trailers
// preferred, then headers); the gRPC status code is the fallback. Connection
// failures become TransportError so endpoint failover keeps working.
func apiErrorFromStatus(err error, operation string, responseMetadata ...metadata.MD) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) {
		return io.EOF
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	statusErr, ok := status.FromError(err)
	if !ok {
		return &TransportError{Err: fmt.Errorf("%s: %w", operation, err)}
	}
	code := vmonCodeFromMetadata(responseMetadata)
	switch statusErr.Code() {
	case codes.Canceled:
		if code == "" {
			return context.Canceled
		}
	case codes.DeadlineExceeded:
		if code == "" {
			return context.DeadlineExceeded
		}
	case codes.Unavailable:
		// UNAVAILABLE without a daemon code is a connection failure.
		if code == "" {
			return &TransportError{Err: fmt.Errorf("%s: %w", operation, err)}
		}
	}
	if code == "" {
		code = grpcCodeToVmonCode[statusErr.Code()]
	}
	if code == "" {
		code = "internal"
	}
	message := statusErr.Message()
	if message == "" {
		message = statusErr.Code().String()
	}
	retry, ok := vmonRetryableFromMetadata(responseMetadata)
	if !ok {
		retry = code == "busy" || code == "ha_unavailable" || code == "unavailable_secret"
	}
	action := vmonActionFromMetadata(responseMetadata)
	return &APIError{StatusCode: vmonCodeToHTTPStatus[code], Code: code, Message: message, Retryable: retry, Action: action}
}

func isNotFoundAPIError(err error) bool {
	var apiErr *APIError
	return errors.As(err, &apiErr) && (apiErr.Code == "not_found" || apiErr.StatusCode == http.StatusNotFound)
}

// unary runs one unary RPC with endpoint affinity and transport failover,
// returning the endpoint that served it.
func (client *Client) unary(ctx context.Context, hint, operation string, call func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error) (string, error) {
	if client == nil || client.driver == nil {
		return "", errors.New("vmon: client has no driver")
	}
	var lastTransport *TransportError
	for _, endpoint := range client.grpcCandidates(hint) {
		conn, err := client.conn(endpoint)
		if err != nil {
			var transportErr *TransportError
			if errors.As(err, &transportErr) {
				lastTransport = transportErr
				continue
			}
			return "", err
		}
		var header, trailer metadata.MD
		callErr := call(ctx, conn, grpc.Header(&header), grpc.Trailer(&trailer))
		if callErr == nil {
			return endpoint, nil
		}
		mapped := apiErrorFromStatus(callErr, operation, trailer, header)
		var transportErr *TransportError
		if errors.As(mapped, &transportErr) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			lastTransport = &TransportError{Endpoint: endpoint, Err: transportErr.Err}
			continue
		}
		return endpoint, mapped
	}
	if lastTransport != nil {
		return "", lastTransport
	}
	return "", &TransportError{Err: errors.New("no vmon endpoints are currently available")}
}

// unaryView runs a JsonView-returning unary RPC and yields the raw document.
func (client *Client) unaryView(ctx context.Context, hint, operation string, call func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error)) (string, []byte, error) {
	var view *pb.JsonView
	endpoint, err := client.unary(ctx, hint, operation, func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) error {
		var callErr error
		view, callErr = call(ctx, conn, opts...)
		return callErr
	})
	if err != nil {
		return endpoint, nil, err
	}
	return endpoint, []byte(view.GetJson()), nil
}

func decodeJSONView(body []byte, operation string, out any) error {
	if len(body) == 0 {
		return &ProtocolError{Operation: operation, Message: "empty JSON response"}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return &ProtocolError{Operation: operation, Message: "invalid JSON response", Err: err}
	}
	return nil
}

// resolveSandbox locates the mesh endpoint currently hosting a sandbox.
func (client *Client) resolveSandbox(ctx context.Context, id, hint string) (string, error) {
	if client == nil || client.driver == nil {
		return "", errors.New("vmon: client has no driver")
	}
	var originalNotFound error
	var lastTransport *TransportError
	for _, endpoint := range client.grpcCandidates(hint) {
		conn, err := client.conn(endpoint)
		if err != nil {
			var transportErr *TransportError
			if errors.As(err, &transportErr) {
				lastTransport = transportErr
				continue
			}
			return "", err
		}
		var header, trailer metadata.MD
		_, callErr := pb.NewSandboxServiceClient(conn).Get(ctx, &pb.SandboxRef{Id: id}, grpc.Header(&header), grpc.Trailer(&trailer))
		if callErr == nil {
			return endpoint, nil
		}
		mapped := apiErrorFromStatus(callErr, "resolve sandbox", trailer, header)
		var transportErr *TransportError
		if errors.As(mapped, &transportErr) {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return "", ctxErr
			}
			lastTransport = transportErr
			continue
		}
		if isNotFoundAPIError(mapped) {
			if originalNotFound == nil {
				originalNotFound = mapped
			}
			continue
		}
		return "", mapped
	}
	if originalNotFound != nil {
		return "", originalNotFound
	}
	if lastTransport != nil {
		return "", lastTransport
	}
	return "", &APIError{StatusCode: http.StatusNotFound, Code: "not_found", Message: "sandbox was not found"}
}

// meshStatusJSON fetches the mesh roster document for driver discovery.
func (client *Client) meshStatusJSON(ctx context.Context) ([]byte, error) {
	_, view, err := client.unaryView(ctx, "", "mesh status", func(ctx context.Context, conn grpc.ClientConnInterface, opts ...grpc.CallOption) (*pb.JsonView, error) {
		return pb.NewSystemServiceClient(conn).MeshStatus(ctx, &pb.MeshStatusRequest{}, opts...)
	})
	if err != nil {
		return nil, err
	}
	return view, nil
}

// ---- residual HTTP plumbing (healthz, /metrics, ports proxy) ----

func (client *Client) request(ctx context.Context, request DriverRequest) (*http.Response, string, error) {
	if client == nil || client.driver == nil {
		return nil, "", errors.New("vmon: client has no driver")
	}
	response, endpoint, err := client.driver.Do(ctx, request)
	if err != nil {
		return nil, "", err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, endpoint, apiErrorFromResponse(response)
	}
	return response, endpoint, nil
}
func (client *Client) doJSON(ctx context.Context, method, path string, query url.Values, body, out any) error {
	response, _, err := client.request(ctx, DriverRequest{Method: method, Path: path, Query: query, JSON: body})
	if err != nil {
		return err
	}
	if out == nil {
		_, err = client.readResponse(response)
		return err
	}
	return client.decodeJSONResponse(response, method+" "+path, out)
}
func (client *Client) decodeJSONResponse(response *http.Response, operation string, out any) error {
	body, err := client.readResponse(response)
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return &ProtocolError{Operation: operation, Message: "empty JSON response"}
	}
	if err := json.Unmarshal(body, out); err != nil {
		return &ProtocolError{Operation: operation, Message: "invalid JSON response", Err: err}
	}
	return nil
}
func (client *Client) readResponse(response *http.Response) ([]byte, error) {
	if response == nil || response.Body == nil {
		return nil, &ProtocolError{Operation: "read response", Message: "response has no body"}
	}
	defer response.Body.Close()
	limit := client.maxResponseBytes
	if limit <= 0 {
		limit = defaultMaxResponseBytes
	}
	body, err := io.ReadAll(io.LimitReader(response.Body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, &ResponseTooLargeError{Limit: limit}
	}
	return body, nil
}
func apiErrorFromResponse(response *http.Response) error {
	if response == nil {
		return &APIError{Message: "empty response"}
	}
	defer response.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(response.Body, maxErrorResponseBytes+1))
	truncated := int64(len(body)) > maxErrorResponseBytes
	if truncated {
		body = body[:maxErrorResponseBytes]
	}
	var wire struct {
		Code      string `json:"code"`
		Error     string `json:"error"`
		Message   string `json:"message"`
		Retryable any    `json:"retryable"`
		Action    string `json:"action"`
	}
	_ = json.Unmarshal(body, &wire)
	message := firstNonempty(wire.Message, wire.Error, strings.TrimSpace(string(body)))
	if len(message) > maxErrorMessageBytes {
		message = message[:maxErrorMessageBytes]
		truncated = true
	}
	if readErr != nil && message == "" {
		message = readErr.Error()
	}
	var retry bool
	if b, ok := wire.Retryable.(bool); ok {
		retry = b
	} else if s, ok := wire.Retryable.(string); ok {
		retry = strings.ToLower(s) == "true"
	} else {
		retry = wire.Code == "busy" || wire.Code == "ha_unavailable" || wire.Code == "unavailable_secret"
	}
	return &APIError{StatusCode: response.StatusCode, Code: wire.Code, Message: message, Truncated: truncated, Retryable: retry, Action: wire.Action}
}
