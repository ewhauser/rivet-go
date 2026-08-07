package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const maxEngineJSONResponse = 2 << 20

type engineAPI struct {
	endpoint string
	client   *http.Client
}

func newEngineAPI(endpoint string) *engineAPI {
	return &engineAPI{
		endpoint: endpoint,
		client: &http.Client{
			Timeout: 45 * time.Second,
			Transport: &http.Transport{
				MaxIdleConns:        64,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     30 * time.Second,
			},
		},
	}
}

func (a *engineAPI) close() {
	if transport, ok := a.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

type envoyRecord struct {
	PoolName   string `json:"pool_name"`
	StopTS     *int64 `json:"stop_ts"`
	LastPingTS int64  `json:"last_ping_ts"`
}

type actorRecord struct {
	ActorID       string `json:"actor_id"`
	ConnectableTS *int64 `json:"connectable_ts"`
	SleepTS       *int64 `json:"sleep_ts"`
	DestroyTS     *int64 `json:"destroy_ts"`
}

func (a *engineAPI) waitActorSleeping(ctx context.Context, actorID string) error {
	return waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		query := url.Values{}
		query.Set("namespace", "default")
		query.Set("actor_id", actorID)
		request, err := http.NewRequestWithContext(
			ctx,
			http.MethodGet,
			a.endpoint+"/actors?"+query.Encode(),
			nil,
		)
		if err != nil {
			return false, err
		}
		request.Header.Set("Authorization", "Bearer dev")
		response, err := a.client.Do(request)
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
			return false, fmt.Errorf("list actor %s: %s: %s", actorID, response.Status, body)
		}
		var payload struct {
			Actors []actorRecord `json:"actors"`
		}
		if err := decodeBoundedJSON(response.Body, &payload); err != nil {
			return false, err
		}
		for _, actor := range payload.Actors {
			if actor.ActorID != actorID || actor.ConnectableTS != nil || actor.SleepTS == nil || actor.DestroyTS != nil {
				continue
			}
			// v2.3.10 needs a brief post-checkpoint settlement before a
			// hibernated gateway message can reliably allocate the next
			// generation. This is the same state-based gate as conformance.
			return !time.Now().Before(time.UnixMilli(*actor.SleepTS).Add(2 * time.Second)), nil
		}
		return false, nil
	})
}

func (a *engineAPI) waitRunnerPingAfter(ctx context.Context, runnerName string, timestamp int64) error {
	return waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			a.endpoint+"/envoys?namespace=default&name="+url.QueryEscape(runnerName), nil)
		if err != nil {
			return false, err
		}
		request.Header.Set("Authorization", "Bearer dev")
		response, err := a.client.Do(request)
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
			return false, fmt.Errorf("list envoys for ping: %s: %s", response.Status, body)
		}
		var payload struct {
			Envoys []envoyRecord `json:"envoys"`
		}
		if err := decodeBoundedJSON(response.Body, &payload); err != nil {
			return false, err
		}
		for _, envoy := range payload.Envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil && envoy.LastPingTS >= timestamp {
				return true, nil
			}
		}
		return false, nil
	})
}

func (a *engineAPI) waitRunner(ctx context.Context, runnerName string, present bool) error {
	return waitUntil(ctx, 100*time.Millisecond, func() (bool, error) {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			a.endpoint+"/envoys?namespace=default&name="+url.QueryEscape(runnerName), nil)
		if err != nil {
			return false, err
		}
		request.Header.Set("Authorization", "Bearer dev")
		response, err := a.client.Do(request)
		if err != nil {
			return false, err
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
			return false, fmt.Errorf("list envoys: %s: %s", response.Status, body)
		}
		var payload struct {
			Envoys []envoyRecord `json:"envoys"`
		}
		if err := decodeBoundedJSON(response.Body, &payload); err != nil {
			return false, fmt.Errorf("decode envoys: %w", err)
		}
		for _, envoy := range payload.Envoys {
			if envoy.PoolName == runnerName && envoy.StopTS == nil {
				return present, nil
			}
		}
		return !present, nil
	})
}

func (a *engineAPI) createActor(
	ctx context.Context,
	name, runnerName, key string,
) (string, error) {
	payload := struct {
		Name               string  `json:"name"`
		RunnerNameSelector string  `json:"runner_name_selector"`
		CrashPolicy        string  `json:"crash_policy"`
		Key                *string `json:"key,omitempty"`
		Input              *string `json:"input,omitempty"`
	}{
		Name:               name,
		RunnerNameSelector: runnerName,
		CrashPolicy:        "destroy",
	}
	if key != "" {
		payload.Key = &key
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return "", fmt.Errorf("encode actor create: %w", err)
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost,
		a.endpoint+"/actors?namespace=default", bytes.NewReader(encoded))
	if err != nil {
		return "", err
	}
	request.Header.Set("Authorization", "Bearer dev")
	request.Header.Set("Content-Type", "application/json")
	response, err := a.client.Do(request)
	if err != nil {
		return "", fmt.Errorf("create actor %s: %w", name, err)
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 8<<10))
		return "", fmt.Errorf("create actor %s: %s: %s", name, response.Status, body)
	}
	var result struct {
		Actor struct {
			ActorID string `json:"actor_id"`
		} `json:"actor"`
	}
	if err := decodeBoundedJSON(response.Body, &result); err != nil {
		return "", fmt.Errorf("decode actor create: %w", err)
	}
	if result.Actor.ActorID == "" {
		return "", fmt.Errorf("create actor %s returned an empty actor ID", name)
	}
	return result.Actor.ActorID, nil
}

type actionFailure struct {
	Group   string `json:"group"`
	Code    string `json:"code"`
	Message string `json:"message"`
}

type actionFailureError struct {
	action  string
	failure actionFailure
}

func (e *actionFailureError) Error() string {
	return fmt.Sprintf("action %s failed: %s/%s: %s", e.action, e.failure.Group, e.failure.Code, e.failure.Message)
}

func isActionFailure(err error, group, code string) bool {
	var failure *actionFailureError
	return errors.As(err, &failure) && failure.failure.Group == group && failure.failure.Code == code
}

type actionStatusError struct {
	action string
	status int
	body   string
}

func (e *actionStatusError) Error() string {
	return fmt.Sprintf("action %s returned %d: %s", e.action, e.status, e.body)
}

func isActionStatus(err error, status int, body string) bool {
	var failure *actionStatusError
	return errors.As(err, &failure) && failure.status == status && failure.body == body
}

func gatewayAction[T any](
	ctx context.Context,
	api *engineAPI,
	actorID, action string,
	argument any,
) (T, error) {
	var output T
	status, body, err := gatewayActionResponse(ctx, api, actorID, action, argument)
	if err != nil {
		return output, err
	}
	return decodeGatewayAction[T](action, status, body)
}

// gatewayActionEventually behaves like gatewayAction but keeps retrying while
// the engine is unreachable, answers plain 503 "Actor not found", or reports
// a transient actor transition, bounded by ctx. After abrupt engine
// replacement, v2.3.10's
// replacement process completes workflow-worker failover roughly 30 seconds
// after start — beyond the 22-second generation liveness window — and until
// then a request that must wake a sleeping actor parks in gateway route
// dispatch. At the failover boundary it can spuriously fail with 503 "Actor
// not found" or with actor/stopping against the mid-teardown stale
// generation, even though the actor's persisted state is intact and it wakes
// moments later. Any other failure is returned immediately.
func gatewayActionEventually[T any](
	ctx context.Context,
	api *engineAPI,
	actorID, action string,
	argument any,
) (T, error) {
	var output T
	var terminal error
	if err := waitUntil(ctx, 250*time.Millisecond, func() (bool, error) {
		status, body, err := gatewayActionResponse(ctx, api, actorID, action, argument)
		if err != nil {
			return false, err
		}
		if status == http.StatusServiceUnavailable && strings.TrimSpace(string(body)) == "Actor not found" {
			return false, fmt.Errorf("action %s returned %d: %s", action, status, body)
		}
		if status != http.StatusOK {
			var failure actionFailure
			if json.Unmarshal(body, &failure) == nil && transientActionFailure(failure) {
				return false, fmt.Errorf("action %s transiently failed: %s/%s: %s",
					action, failure.Group, failure.Code, failure.Message)
			}
		}
		output, terminal = decodeGatewayAction[T](action, status, body)
		return true, nil
	}); err != nil {
		return output, err
	}
	return output, terminal
}

// transientActionFailure reports the engine failure codes observed when a
// wake-requiring request resolves at the post-replacement failover boundary:
// the gateway can hand the wake to a generation that is mid-teardown
// (actor/stopping) or not yet rehydrated (actor/not_ready). Both clear once
// failover finishes; every other code stays terminal.
func transientActionFailure(failure actionFailure) bool {
	if failure.Group != "actor" {
		return false
	}
	return failure.Code == "stopping" || failure.Code == "not_ready"
}

func decodeGatewayAction[T any](action string, status int, body []byte) (T, error) {
	var output T
	if status != http.StatusOK {
		var failure actionFailure
		if decodeErr := json.Unmarshal(body, &failure); decodeErr != nil {
			return output, &actionStatusError{
				action: action,
				status: status,
				body:   strings.TrimSpace(string(body)),
			}
		}
		return output, &actionFailureError{action: action, failure: failure}
	}
	var response struct {
		Output T `json:"output"`
	}
	if err := json.Unmarshal(body, &response); err != nil {
		return output, fmt.Errorf("decode action %s output: %w: %s", action, err, body)
	}
	return response.Output, nil
}

func gatewayActionResponse(
	ctx context.Context,
	api *engineAPI,
	actorID, action string,
	argument any,
) (int, []byte, error) {
	payload, err := json.Marshal(struct {
		Args []any `json:"args"`
	}{Args: []any{argument}})
	if err != nil {
		return 0, nil, fmt.Errorf("encode action %s: %w", action, err)
	}
	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		api.endpoint+"/gateway/"+url.PathEscape(actorID)+"/action/"+url.PathEscape(action),
		bytes.NewReader(payload),
	)
	if err != nil {
		return 0, nil, err
	}
	request.Header.Set("Authorization", "Bearer dev")
	request.Header.Set("Content-Type", "application/json")
	response, err := api.client.Do(request)
	if err != nil {
		return 0, nil, fmt.Errorf("call action %s: %w", action, err)
	}
	defer response.Body.Close()
	body, err := readBounded(response.Body, maxEngineJSONResponse)
	if err != nil {
		return response.StatusCode, nil, fmt.Errorf("read action %s response: %w", action, err)
	}
	return response.StatusCode, body, nil
}

func decodeBoundedJSON(reader io.Reader, output any) error {
	body, err := readBounded(reader, maxEngineJSONResponse)
	if err != nil {
		return err
	}
	return json.Unmarshal(body, output)
}

func readBounded(reader io.Reader, limit int64) ([]byte, error) {
	body, err := io.ReadAll(io.LimitReader(reader, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(body)) > limit {
		return nil, fmt.Errorf("engine response exceeded %d bytes", limit)
	}
	return body, nil
}

func expectPanicAction(
	ctx context.Context,
	api *engineAPI,
	actorID string,
) error {
	status, body, err := gatewayActionResponse(ctx, api, actorID, "panic", struct{}{})
	if err != nil {
		return err
	}
	if status != http.StatusInternalServerError {
		return fmt.Errorf("panic action status=%d body=%s", status, body)
	}
	var failure actionFailure
	if err := json.Unmarshal(body, &failure); err != nil {
		return fmt.Errorf("decode panic action error: %w: %s", err, body)
	}
	if failure.Group != "actor" || failure.Code != "handler_panic" || failure.Message == "" {
		return fmt.Errorf("panic action error=%#v", failure)
	}
	return nil
}
