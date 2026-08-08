package rivet

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
)

const maxClientResponseBytes = 16 << 20

var (
	// ErrActorNotFound is returned when actor resolution produces no actor.
	ErrActorNotFound = errors.New("rivet actor not found")
	// ErrSelfCall is returned when an actor-scoped client attempts to call the
	// actor generation that owns it. Actors process actions serially, so waiting
	// for such a call would deadlock.
	ErrSelfCall = errors.New("rivet actor cannot call itself")
)

// ClientConfig configures an external actor client. Empty endpoint, namespace,
// and runner name values use the same defaults as Serve.
type ClientConfig struct {
	Endpoint   string
	Namespace  string
	RunnerName string
	Token      string
	Headers    http.Header
	HTTPClient *http.Client
}

// Client resolves, creates, and calls actors through Rivet Engine's HTTP API.
// A Client is immutable after construction and safe for concurrent use.
type Client struct {
	endpoint      *url.URL
	namespace     string
	runnerName    string
	token         string
	headers       http.Header
	httpClient    *http.Client
	sourceActorID string
}

// CrashPolicy controls what Rivet Engine does after an actor process failure.
type CrashPolicy string

const (
	CrashPolicySleep   CrashPolicy = "sleep"
	CrashPolicyRestart CrashPolicy = "restart"
	CrashPolicyDestroy CrashPolicy = "destroy"
)

// CreateOptions controls actor creation. Input is delivered byte-for-byte by
// Context.Input. A nil Key creates an unkeyed actor; a non-nil empty key is the
// valid Rivet key with zero segments.
type CreateOptions struct {
	Key         []string
	Input       []byte
	Region      string
	CrashPolicy CrashPolicy
	// RunnerName overrides ClientConfig.RunnerName for this creation.
	RunnerName string
}

// ActorMetadata is the Engine metadata captured when an actor is resolved or
// created. Timestamp fields contain Engine Unix millisecond values.
type ActorMetadata struct {
	ID                   string
	Name                 string
	Key                  []string
	FormattedKey         string
	NamespaceID          string
	RunnerNameSelector   string
	CreateTimestamp      int64
	ConnectableTimestamp *int64
	DestroyTimestamp     *int64
	SleepTimestamp       *int64
	StartTimestamp       *int64
	Error                json.RawMessage
}

// ActorHandle identifies one actor and sends action calls to it. Handles are
// immutable and safe for concurrent use.
type ActorHandle struct {
	client   *Client
	metadata ActorMetadata
}

// ActorErrorDetails identifies the actor generation attached to a structured
// Engine error, when available.
type ActorErrorDetails struct {
	ActorID    string `json:"actorId"`
	Generation uint64 `json:"generation"`
	Key        string `json:"key,omitempty"`
}

// ClientError is a structured non-success response from Rivet Engine.
type ClientError struct {
	StatusCode int
	Group      string
	Code       string
	Message    string
	Metadata   json.RawMessage
	Actor      *ActorErrorDetails
	RayID      string
	Body       string
}

func (e *ClientError) Error() string {
	if e == nil {
		return "rivet client error"
	}
	label := strings.Trim(strings.Join([]string{e.Group, e.Code}, "/"), "/")
	message := e.Message
	if message == "" {
		message = strings.TrimSpace(e.Body)
	}
	if message == "" {
		message = http.StatusText(e.StatusCode)
	}
	if label != "" {
		return fmt.Sprintf("rivet client: HTTP %d %s: %s", e.StatusCode, label, message)
	}
	return fmt.Sprintf("rivet client: HTTP %d: %s", e.StatusCode, message)
}

// Is lets callers use errors.Is(err, ErrActorNotFound) for structured
// actor/not_found responses as well as empty actor-list resolutions.
func (e *ClientError) Is(target error) bool {
	return target == ErrActorNotFound && e != nil &&
		e.Group == "actor" && e.Code == "not_found"
}

// NewClient constructs an immutable actor client.
func NewClient(config ClientConfig) (*Client, error) {
	endpoint := config.Endpoint
	if endpoint == "" {
		endpoint = defaultEndpoint
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("parse actor client endpoint: %w", err)
	}
	if (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
		return nil, errors.New("actor client endpoint must be an absolute HTTP or HTTPS URL")
	}
	if parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, errors.New("actor client endpoint must not contain a query or fragment")
	}
	if parsed.User != nil {
		return nil, errors.New("actor client endpoint must not contain user information; use Token instead")
	}
	parsed.Path = strings.TrimRight(parsed.Path, "/")
	parsed.RawPath = strings.TrimRight(parsed.RawPath, "/")

	namespace := config.Namespace
	if namespace == "" {
		namespace = defaultNamespace
	}
	if strings.TrimSpace(namespace) == "" {
		return nil, errors.New("actor client namespace must not be blank")
	}
	runnerName := config.RunnerName
	if runnerName == "" {
		runnerName = defaultRunnerName
	}
	if strings.TrimSpace(runnerName) == "" {
		return nil, errors.New("actor client runner name must not be blank")
	}
	httpClient := config.HTTPClient
	if httpClient == nil {
		httpClient = http.DefaultClient
	}

	headers := config.Headers.Clone()
	if headers == nil {
		headers = make(http.Header)
	}
	return &Client{
		endpoint:   parsed,
		namespace:  namespace,
		runnerName: runnerName,
		token:      config.Token,
		headers:    headers,
		httpClient: httpClient,
	}, nil
}

// Get resolves an actor by ID.
func (c *Client) Get(ctx context.Context, actorID string) (*ActorHandle, error) {
	if strings.TrimSpace(actorID) == "" {
		return nil, errors.New("actor ID must not be empty")
	}
	query := url.Values{"actor_ids": []string{actorID}}
	actors, err := c.list(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get actor %q: %w", actorID, err)
	}
	for _, actor := range actors {
		if actor.ActorID == actorID {
			return c.newHandle(actor), nil
		}
	}
	return nil, fmt.Errorf("get actor %q: %w", actorID, ErrActorNotFound)
}

// GetByKey resolves an actor by registered name and segmented key.
func (c *Client) GetByKey(ctx context.Context, name string, key []string) (*ActorHandle, error) {
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("actor name must not be empty")
	}
	formattedKey := SerializeActorKey(key)
	query := url.Values{
		"name": []string{name},
		"key":  []string{formattedKey},
	}
	actors, err := c.list(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("get actor %q by key: %w", name, err)
	}
	for _, actor := range actors {
		if actor.Name == name && actor.Key == formattedKey {
			return c.newHandle(actor), nil
		}
	}
	return nil, fmt.Errorf("get actor %q by key: %w", name, ErrActorNotFound)
}

// Create creates a new actor.
func (c *Client) Create(ctx context.Context, name string, options CreateOptions) (*ActorHandle, error) {
	request, err := c.createRequest(name, options)
	if err != nil {
		return nil, err
	}
	var response struct {
		Actor actorMetadataJSON `json:"actor"`
	}
	if err := c.doJSON(ctx, http.MethodPost, "/actors", nil, request, &response); err != nil {
		return nil, fmt.Errorf("create actor %q: %w", name, err)
	}
	if response.Actor.ActorID == "" {
		return nil, fmt.Errorf("create actor %q: Engine returned an empty actor ID", name)
	}
	return c.newHandle(response.Actor), nil
}

// GetOrCreate resolves the actor with name and key, atomically creating it if
// it does not exist. created reports whether this request performed creation.
func (c *Client) GetOrCreate(
	ctx context.Context,
	name string,
	key []string,
	options CreateOptions,
) (actor *ActorHandle, created bool, err error) {
	options.Key = key
	request, err := c.createRequest(name, options)
	if err != nil {
		return nil, false, err
	}
	// PUT requires a key even when the key has zero segments.
	formattedKey := SerializeActorKey(key)
	request.Key = &formattedKey
	var response struct {
		Actor   actorMetadataJSON `json:"actor"`
		Created bool              `json:"created"`
	}
	if err := c.doJSON(ctx, http.MethodPut, "/actors", nil, request, &response); err != nil {
		return nil, false, fmt.Errorf("get or create actor %q: %w", name, err)
	}
	if response.Actor.ActorID == "" {
		return nil, false, fmt.Errorf("get or create actor %q: Engine returned an empty actor ID", name)
	}
	return c.newHandle(response.Actor), response.Created, nil
}

// ID returns the Engine actor ID.
func (h *ActorHandle) ID() string {
	if h == nil {
		return ""
	}
	return h.metadata.ID
}

// Metadata returns a defensive copy of the metadata captured for this handle.
func (h *ActorHandle) Metadata() ActorMetadata {
	if h == nil {
		return ActorMetadata{}
	}
	metadata := h.metadata
	metadata.Key = append([]string(nil), h.metadata.Key...)
	metadata.Error = append(json.RawMessage(nil), h.metadata.Error...)
	metadata.ConnectableTimestamp = cloneInt64(h.metadata.ConnectableTimestamp)
	metadata.DestroyTimestamp = cloneInt64(h.metadata.DestroyTimestamp)
	metadata.SleepTimestamp = cloneInt64(h.metadata.SleepTimestamp)
	metadata.StartTimestamp = cloneInt64(h.metadata.StartTimestamp)
	return metadata
}

// Call marshals arguments as the Rivet action argument array and returns the
// raw JSON action output. Use the package-level Call function for typed output.
func (h *ActorHandle) Call(
	ctx context.Context,
	action string,
	arguments ...any,
) (json.RawMessage, error) {
	args := append([]any{}, arguments...)
	encoded, err := json.Marshal(args)
	if err != nil {
		return nil, fmt.Errorf("encode action %q arguments: %w", action, err)
	}
	return h.CallRaw(ctx, action, encoded)
}

// CallRaw sends an exact JSON array as the Rivet action arguments and returns
// the exact JSON value in the action response's output field.
func (h *ActorHandle) CallRaw(
	ctx context.Context,
	action string,
	arguments json.RawMessage,
) (json.RawMessage, error) {
	if h == nil || h.client == nil {
		return nil, errors.New("actor handle is unavailable")
	}
	if strings.TrimSpace(action) == "" {
		return nil, errors.New("action name must not be empty")
	}
	trimmed := bytes.TrimSpace(arguments)
	if !json.Valid(trimmed) || len(trimmed) < 2 || trimmed[0] != '[' || trimmed[len(trimmed)-1] != ']' {
		return nil, errors.New("raw action arguments must be a valid JSON array")
	}
	if h.client.sourceActorID != "" && h.client.sourceActorID == h.metadata.ID {
		return nil, fmt.Errorf("call actor %q action %q: %w", h.metadata.ID, action, ErrSelfCall)
	}
	body := make([]byte, 0, len(trimmed)+9)
	body = append(body, `{"args":`...)
	body = append(body, trimmed...)
	body = append(body, '}')
	var response struct {
		Output json.RawMessage `json:"output"`
	}
	path := "/gateway/" + url.PathEscape(h.metadata.ID) + "/action/" + url.PathEscape(action)
	if err := h.client.doJSONBytes(ctx, http.MethodPost, path, nil, body, &response); err != nil {
		return nil, fmt.Errorf("call actor %q action %q: %w", h.metadata.ID, action, err)
	}
	if response.Output == nil {
		return nil, fmt.Errorf("call actor %q action %q: Engine response has no output", h.metadata.ID, action)
	}
	return append(json.RawMessage(nil), response.Output...), nil
}

// Call invokes an actor action and decodes its output into R.
func Call[R any](
	ctx context.Context,
	actor *ActorHandle,
	action string,
	arguments ...any,
) (R, error) {
	var result R
	output, err := actor.Call(ctx, action, arguments...)
	if err != nil {
		return result, err
	}
	if err := json.Unmarshal(output, &result); err != nil {
		return result, fmt.Errorf("decode action %q output: %w", action, err)
	}
	return result, nil
}

// SerializeActorKey encodes segmented actor keys using RivetKit's stable wire
// format. An empty key encodes as "/"; empty segments encode as "\\0".
func SerializeActorKey(key []string) string {
	if len(key) == 0 {
		return "/"
	}
	parts := make([]string, len(key))
	for index, part := range key {
		if part == "" {
			parts[index] = `\0`
			continue
		}
		part = strings.ReplaceAll(part, `\`, `\\`)
		parts[index] = strings.ReplaceAll(part, "/", `\/`)
	}
	return strings.Join(parts, "/")
}

// DeserializeActorKey decodes RivetKit's stable actor-key wire format.
func DeserializeActorKey(formatted string) []string {
	if formatted == "" || formatted == "/" {
		return []string{}
	}
	parts := make([]string, 0, strings.Count(formatted, "/")+1)
	var current strings.Builder
	escaping := false
	emptyMarker := false
	for _, char := range formatted {
		if escaping {
			if char == '0' {
				emptyMarker = true
			} else {
				current.WriteRune(char)
			}
			escaping = false
			continue
		}
		switch char {
		case '\\':
			escaping = true
		case '/':
			if emptyMarker {
				parts = append(parts, "")
				emptyMarker = false
			} else {
				parts = append(parts, current.String())
			}
			current.Reset()
		default:
			current.WriteRune(char)
		}
	}
	if escaping {
		parts = append(parts, current.String()+`\`)
	} else if emptyMarker {
		parts = append(parts, "")
	} else if current.Len() > 0 {
		parts = append(parts, current.String())
	}
	return parts
}

type actorMetadataJSON struct {
	ActorID              string          `json:"actor_id"`
	Name                 string          `json:"name"`
	Key                  string          `json:"key"`
	NamespaceID          string          `json:"namespace_id"`
	RunnerNameSelector   string          `json:"runner_name_selector"`
	CreateTimestamp      int64           `json:"create_ts"`
	ConnectableTimestamp *int64          `json:"connectable_ts"`
	DestroyTimestamp     *int64          `json:"destroy_ts"`
	SleepTimestamp       *int64          `json:"sleep_ts"`
	StartTimestamp       *int64          `json:"start_ts"`
	Error                json.RawMessage `json:"error"`
}

type actorCreateRequest struct {
	Region             string  `json:"datacenter,omitempty"`
	Name               string  `json:"name"`
	RunnerNameSelector string  `json:"runner_name_selector"`
	CrashPolicy        string  `json:"crash_policy"`
	Key                *string `json:"key,omitempty"`
	Input              *string `json:"input,omitempty"`
}

func (c *Client) createRequest(name string, options CreateOptions) (actorCreateRequest, error) {
	if c == nil {
		return actorCreateRequest{}, errors.New("actor client is nil")
	}
	if strings.TrimSpace(name) == "" {
		return actorCreateRequest{}, errors.New("actor name must not be empty")
	}
	runnerName := options.RunnerName
	if runnerName == "" {
		runnerName = c.runnerName
	}
	if strings.TrimSpace(runnerName) == "" {
		return actorCreateRequest{}, errors.New("actor runner name must not be blank")
	}
	crashPolicy := options.CrashPolicy
	if crashPolicy == "" {
		crashPolicy = CrashPolicySleep
	}
	request := actorCreateRequest{
		Region:             options.Region,
		Name:               name,
		RunnerNameSelector: runnerName,
		CrashPolicy:        string(crashPolicy),
	}
	if options.Key != nil {
		key := SerializeActorKey(options.Key)
		request.Key = &key
	}
	if options.Input != nil {
		input := base64.StdEncoding.EncodeToString(options.Input)
		request.Input = &input
	}
	return request, nil
}

func (c *Client) list(ctx context.Context, query url.Values) ([]actorMetadataJSON, error) {
	if c == nil {
		return nil, errors.New("actor client is nil")
	}
	var response struct {
		Actors []actorMetadataJSON `json:"actors"`
	}
	if err := c.doJSON(ctx, http.MethodGet, "/actors", query, nil, &response); err != nil {
		return nil, err
	}
	return response.Actors, nil
}

func (c *Client) newHandle(actor actorMetadataJSON) *ActorHandle {
	return &ActorHandle{
		client: c,
		metadata: ActorMetadata{
			ID:                   actor.ActorID,
			Name:                 actor.Name,
			Key:                  DeserializeActorKey(actor.Key),
			FormattedKey:         actor.Key,
			NamespaceID:          actor.NamespaceID,
			RunnerNameSelector:   actor.RunnerNameSelector,
			CreateTimestamp:      actor.CreateTimestamp,
			ConnectableTimestamp: cloneInt64(actor.ConnectableTimestamp),
			DestroyTimestamp:     cloneInt64(actor.DestroyTimestamp),
			SleepTimestamp:       cloneInt64(actor.SleepTimestamp),
			StartTimestamp:       cloneInt64(actor.StartTimestamp),
			Error:                append(json.RawMessage(nil), actor.Error...),
		},
	}
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	copy := *value
	return &copy
}

func (c *Client) withSourceActor(actorID string) *Client {
	if c == nil {
		return nil
	}
	clone := *c
	clone.sourceActorID = actorID
	return &clone
}

func (c *Client) doJSON(
	ctx context.Context,
	method, path string,
	query url.Values,
	input, output any,
) error {
	var body []byte
	var err error
	if input != nil {
		body, err = json.Marshal(input)
		if err != nil {
			return fmt.Errorf("encode request: %w", err)
		}
	}
	return c.doJSONBytes(ctx, method, path, query, body, output)
}

func (c *Client) doJSONBytes(
	ctx context.Context,
	method, path string,
	query url.Values,
	body []byte,
	output any,
) error {
	if c == nil {
		return errors.New("actor client is nil")
	}
	if ctx == nil {
		return errors.New("actor client context is nil")
	}
	requestURL := *c.endpoint
	escapedPath := strings.TrimRight(c.endpoint.EscapedPath(), "/") + path
	unescapedPath, err := url.PathUnescape(escapedPath)
	if err != nil {
		return fmt.Errorf("build request path: %w", err)
	}
	requestURL.Path = unescapedPath
	requestURL.RawPath = escapedPath
	requestQuery := requestURL.Query()
	requestQuery.Set("namespace", c.namespace)
	for name, values := range query {
		for _, value := range values {
			requestQuery.Add(name, value)
		}
	}
	requestURL.RawQuery = requestQuery.Encode()

	var reader io.Reader
	if body != nil {
		reader = bytes.NewReader(body)
	}
	request, err := http.NewRequestWithContext(ctx, method, requestURL.String(), reader)
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	request.Header = c.headers.Clone()
	request.Header.Set("Accept", "application/json")
	request.Header.Set("User-Agent", "rivet-go")
	if body != nil {
		request.Header.Set("Content-Type", "application/json")
	}
	if c.token != "" {
		request.Header.Set("Authorization", "Bearer "+c.token)
	}

	response, err := c.httpClient.Do(request)
	if err != nil {
		return fmt.Errorf("send request: %w", err)
	}
	defer response.Body.Close()
	responseBody, readErr := io.ReadAll(io.LimitReader(response.Body, maxClientResponseBytes+1))
	if readErr != nil {
		return fmt.Errorf("read response: %w", readErr)
	}
	tooLarge := len(responseBody) > maxClientResponseBytes
	if tooLarge {
		responseBody = responseBody[:maxClientResponseBytes]
	}
	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		return decodeClientError(response, responseBody, tooLarge)
	}
	if tooLarge {
		return fmt.Errorf("response exceeds %d bytes", maxClientResponseBytes)
	}
	if output == nil || response.StatusCode == http.StatusNoContent {
		return nil
	}
	if len(responseBody) == 0 {
		return errors.New("Engine returned an empty response body")
	}
	if err := json.Unmarshal(responseBody, output); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	return nil
}

func decodeClientError(response *http.Response, body []byte, truncated bool) error {
	var payload struct {
		Group    string             `json:"group"`
		Code     string             `json:"code"`
		Message  string             `json:"message"`
		Metadata json.RawMessage    `json:"metadata"`
		Actor    *ActorErrorDetails `json:"actor"`
	}
	_ = json.Unmarshal(body, &payload)
	bodyText := string(body)
	if truncated {
		bodyText += " [truncated]"
	}
	return &ClientError{
		StatusCode: response.StatusCode,
		Group:      payload.Group,
		Code:       payload.Code,
		Message:    payload.Message,
		Metadata:   append(json.RawMessage(nil), payload.Metadata...),
		Actor:      payload.Actor,
		RayID:      response.Header.Get("X-Rivet-Ray-ID"),
		Body:       bodyText,
	}
}
