package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	if status != http.StatusOK {
		var failure actionFailure
		if decodeErr := json.Unmarshal(body, &failure); decodeErr != nil {
			return output, fmt.Errorf("action %s returned %d: %s", action, status, body)
		}
		return output, fmt.Errorf("action %s failed: %s/%s: %s", action, failure.Group, failure.Code, failure.Message)
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
