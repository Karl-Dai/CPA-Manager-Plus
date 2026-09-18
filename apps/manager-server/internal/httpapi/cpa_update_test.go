package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/collector"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/config"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
	cpaupdateservice "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/service/cpaupdate"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/store"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/testutil"
)

const cpaUpdateTestArtifactID = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

type cpaUpdateRuntimeStub struct {
	status model.RuntimeObservedStatus
	err    error
}

func (s *cpaUpdateRuntimeStub) Status(context.Context) (model.RuntimeObservedStatus, error) {
	return s.status, s.err
}

func (*cpaUpdateRuntimeStub) Start(context.Context, model.RuntimeMutationRequest) (model.RuntimeOperationResult, error) {
	panic("unexpected Runtime Start mutation")
}

func (*cpaUpdateRuntimeStub) Stop(context.Context, model.RuntimeMutationRequest) (model.RuntimeOperationResult, error) {
	panic("unexpected Runtime Stop mutation")
}

func (*cpaUpdateRuntimeStub) Restart(context.Context, model.RuntimeMutationRequest) (model.RuntimeOperationResult, error) {
	panic("unexpected Runtime Restart mutation")
}

func (*cpaUpdateRuntimeStub) PrepareUpdate(context.Context, model.RuntimePrepareUpdateRequest) (model.RuntimeOperationResult, error) {
	panic("unexpected Runtime PrepareUpdate mutation")
}

func (*cpaUpdateRuntimeStub) ActivateUpdate(context.Context, model.RuntimeActivateUpdateRequest) (model.RuntimeOperationResult, error) {
	panic("unexpected Runtime ActivateUpdate mutation")
}

func newCPAUpdateServer(t *testing.T, runtimeClient *cpaUpdateRuntimeStub) *Server {
	t.Helper()
	cfg := config.Config{
		DBPath:      filepath.Join(t.TempDir(), "usage.sqlite"),
		Queue:       "usage",
		PopSide:     "right",
		CORSOrigins: []string{"*"},
	}
	database, err := store.Open(cfg.DBPath)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	testutil.EnsureAdminCredential(t, database)

	checkedAt := time.Now().UTC()
	state, err := json.Marshal(cpaupdateservice.DiscoveryState{
		SchemaVersion: 1,
		LastAttemptAt: checkedAt,
		LastSuccessAt: checkedAt,
		TargetVersion: "7.3.4",
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := database.SaveCPAUpdateCheck(t.Context(), state); err != nil {
		t.Fatalf("seed CPA update state: %v", err)
	}

	server := New(cfg, database, collector.NewManager(cfg, database))
	server.AppContext().CPAUpdateService = cpaupdateservice.New(database, runtimeClient, model.RuntimeModeEmbedded)
	return server
}

func cpaUpdateReadyStatus() model.RuntimeObservedStatus {
	return model.RuntimeObservedStatus{
		Identity:           "runtime-http-test",
		Generation:         1,
		ProtocolVersion:    "v1",
		State:              model.RuntimeStateReady,
		CPAObservedVersion: "7.3.3",
		ActiveGatewayArtifact: &model.ActiveGatewayArtifact{
			Engine:     "cpa",
			ArtifactID: cpaUpdateTestArtifactID,
			Version:    "7.3.3",
		},
		Capabilities: model.RuntimeCapabilities{
			model.RuntimeCapabilityPrepareUpdate,
			model.RuntimeCapabilityActivateUpdate,
		},
	}
}

func TestCPAUpdateEndpointsRequirePanelAuthAndDisableCaching(t *testing.T) {
	server := newCPAUpdateServer(t, &cpaUpdateRuntimeStub{status: cpaUpdateReadyStatus()})
	for _, test := range []struct {
		method string
		path   string
	}{
		{method: http.MethodGet, path: "/usage-service/runtime/updates"},
		{method: http.MethodPost, path: "/usage-service/runtime/updates/check"},
	} {
		t.Run(test.method+" "+test.path, func(t *testing.T) {
			unauthorized := httptest.NewRecorder()
			server.Handler().ServeHTTP(unauthorized, httptest.NewRequest(test.method, test.path, nil))
			if unauthorized.Code != http.StatusUnauthorized {
				t.Fatalf("unauthorized status = %d", unauthorized.Code)
			}

			request := httptest.NewRequest(test.method, test.path, nil)
			request.Header.Set("Authorization", "Bearer "+testutil.AdminKey)
			response := httptest.NewRecorder()
			server.Handler().ServeHTTP(response, request)
			if response.Code != http.StatusOK {
				t.Fatalf("status = %d, body = %s", response.Code, response.Body.String())
			}
			if response.Header().Get("Cache-Control") != "no-store" {
				t.Fatalf("Cache-Control = %q", response.Header().Get("Cache-Control"))
			}
			if !strings.Contains(response.Body.String(), `"state":"update_available"`) ||
				!strings.Contains(response.Body.String(), `"active_artifact_id":"`+cpaUpdateTestArtifactID+`"`) {
				t.Fatalf("response body = %s", response.Body.String())
			}
		})
	}
}

func TestCPAUpdateEndpointReturns503WithoutLeakingRuntimeError(t *testing.T) {
	server := newCPAUpdateServer(t, &cpaUpdateRuntimeStub{err: errors.New("secret Runtime token failure")})
	request := httptest.NewRequest(http.MethodGet, "/usage-service/runtime/updates", nil)
	request.Header.Set("Authorization", "Bearer "+testutil.AdminKey)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)

	if response.Code != http.StatusServiceUnavailable || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response = %d, headers=%v, body=%s", response.Code, response.Header(), response.Body.String())
	}
	if strings.Contains(response.Body.String(), "secret Runtime token") {
		t.Fatalf("response leaked Runtime error: %s", response.Body.String())
	}
}

func TestCPAUpdateEndpointRejectsRequestBody(t *testing.T) {
	server := newCPAUpdateServer(t, &cpaUpdateRuntimeStub{status: cpaUpdateReadyStatus()})
	request := httptest.NewRequest(http.MethodPost, "/usage-service/runtime/updates/check", strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer "+testutil.AdminKey)
	response := httptest.NewRecorder()
	server.Handler().ServeHTTP(response, request)
	if response.Code != http.StatusBadRequest || response.Header().Get("Cache-Control") != "no-store" {
		t.Fatalf("response = %d, headers=%v, body=%s", response.Code, response.Header(), response.Body.String())
	}
}
