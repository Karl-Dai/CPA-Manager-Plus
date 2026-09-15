package runtime

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
)

const (
	embeddedRuntimeProtocolVersion = "v1"
	embeddedRuntimeStatusPath      = "/v1/runtime/status"
	embeddedRuntimeRequestTimeout  = 30 * time.Second
)

// EmbeddedClient observes CPA through the authenticated Runtime Supervisor
// protocol.
type EmbeddedClient struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

func NewEmbeddedClient(baseURL string, token string) *EmbeddedClient {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	return &EmbeddedClient{
		baseURL: strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		token:   token,
		httpClient: &http.Client{
			Transport: transport,
			Timeout:   embeddedRuntimeRequestTimeout,
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}
}

type embeddedStatusResponse struct {
	ProtocolVersion    string                    `json:"protocolVersion"`
	RuntimeIdentity    string                    `json:"runtimeIdentity"`
	RuntimeGeneration  uint64                    `json:"runtimeGeneration"`
	State              string                    `json:"state"`
	CPAObservedVersion string                    `json:"cpaObservedVersion"`
	Capabilities       []string                  `json:"capabilities"`
	Recovery           *embeddedRecoveryResponse `json:"recovery"`
}

type embeddedRecoveryResponse struct {
	State             string `json:"state"`
	AttemptsRemaining int    `json:"attemptsRemaining"`
}

func (c *EmbeddedClient) Status(ctx context.Context) (model.RuntimeObservedStatus, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+embeddedRuntimeStatusPath, nil)
	if err != nil {
		return model.RuntimeObservedStatus{}, fmt.Errorf("create embedded Runtime status request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Bearer "+c.token)

	res, err := c.httpClient.Do(req)
	if err != nil {
		return model.RuntimeObservedStatus{}, fmt.Errorf("observe embedded Runtime status: %w", err)
	}
	defer res.Body.Close()
	if res.StatusCode < http.StatusOK || res.StatusCode >= http.StatusMultipleChoices {
		return model.RuntimeObservedStatus{}, fmt.Errorf("observe embedded Runtime status: unexpected HTTP status %s", res.Status)
	}

	var response embeddedStatusResponse
	decoder := json.NewDecoder(res.Body)
	if err := decoder.Decode(&response); err != nil {
		return model.RuntimeObservedStatus{}, fmt.Errorf("decode embedded Runtime status: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return model.RuntimeObservedStatus{}, fmt.Errorf("decode embedded Runtime status: %w", err)
	}
	if response.ProtocolVersion != embeddedRuntimeProtocolVersion {
		return model.RuntimeObservedStatus{}, fmt.Errorf("observe embedded Runtime status: unsupported protocol version %q", response.ProtocolVersion)
	}

	capabilities := make(model.RuntimeCapabilities, len(response.Capabilities))
	for i, capability := range response.Capabilities {
		capabilities[i] = model.RuntimeCapability(capability)
	}
	status := model.RuntimeObservedStatus{
		Identity:           model.RuntimeIdentity(response.RuntimeIdentity),
		Generation:         model.RuntimeGeneration(response.RuntimeGeneration),
		ProtocolVersion:    model.RuntimeProtocolVersion(response.ProtocolVersion),
		State:              model.RuntimeState(response.State),
		CPAObservedVersion: model.CPAObservedVersion(response.CPAObservedVersion),
		Capabilities:       capabilities,
	}
	if response.Recovery != nil {
		status.Recovery = &model.RuntimeRecoveryObservation{
			State:             model.RuntimeRecoveryState(response.Recovery.State),
			AttemptsRemaining: response.Recovery.AttemptsRemaining,
		}
	}
	if err := status.Validate(); err != nil {
		return model.RuntimeObservedStatus{}, fmt.Errorf("validate embedded Runtime status: %w", err)
	}
	return status, nil
}

var _ RuntimeClient = (*EmbeddedClient)(nil)
