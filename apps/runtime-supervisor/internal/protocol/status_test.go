package protocol

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync/atomic"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/artifact"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/readiness"
)

func TestStatusObserverRunsOnlyAfterAuthentication(t *testing.T) {
	for _, test := range []struct {
		name, method, path, token string
		code, observations        int
	}{
		{"missing token", http.MethodGet, statusPath, "", http.StatusUnauthorized, 0},
		{"wrong token", http.MethodGet, statusPath, "wrong", http.StatusUnauthorized, 0},
		{"query token", http.MethodGet, statusPath + "?token=" + testRuntimeToken, "", http.StatusUnauthorized, 0},
		{"handshake", http.MethodGet, handshakePath, testRuntimeToken, http.StatusOK, 0},
		{"wrong method", http.MethodPost, statusPath, testRuntimeToken, http.StatusMethodNotAllowed, 0},
		{"authenticated status", http.MethodGet, statusPath, testRuntimeToken, http.StatusOK, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			observations := 0
			recoveryObservations := 0
			h, err := NewHandler(Config{
				RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
				Status: statusObserverFunc(func(context.Context) readiness.State {
					observations++
					return readiness.Ready
				}),
				Recovery: recoveryStatusObserverFunc(func() lifecycle.RecoveryStatus {
					recoveryObservations++
					return lifecycle.RecoveryStatus{State: lifecycle.RecoveryStateArmed, AttemptsRemaining: 3}
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			response := request(t, h, test.method, test.path, test.token)
			if response.Code != test.code || observations != test.observations || recoveryObservations != test.observations {
				t.Fatalf("HTTP %d, observations %d/%d; want %d, %d", response.Code, observations, recoveryObservations, test.code, test.observations)
			}
		})
	}
}

func TestStatusReportsRecoveryWithoutChangingAvailabilityOrCapabilities(t *testing.T) {
	for _, recovery := range []lifecycle.RecoveryStatus{
		{State: lifecycle.RecoveryStateInactive, AttemptsRemaining: 0},
		{State: lifecycle.RecoveryStateArmed, AttemptsRemaining: 3},
		{State: lifecycle.RecoveryStateRecovering, AttemptsRemaining: 2},
		{State: lifecycle.RecoveryStateManualIntervention, AttemptsRemaining: 0},
	} {
		t.Run(string(recovery.State), func(t *testing.T) {
			executor := lifecycleFunc{}
			h, err := NewHandler(Config{
				RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
				Start: executor, Stop: executor, Restart: executor,
				Status:   statusObserverFunc(func(context.Context) readiness.State { return readiness.Ready }),
				Recovery: recoveryStatusObserverFunc(func() lifecycle.RecoveryStatus { return recovery }),
			})
			if err != nil {
				t.Fatal(err)
			}
			response := request(t, h, http.MethodGet, statusPath, testRuntimeToken)
			var got statusResponse
			decodeResponse(t, response, &got)
			if response.Code != http.StatusOK || got.State != string(readiness.Ready) || got.Recovery == nil ||
				got.Recovery.State != string(recovery.State) || got.Recovery.AttemptsRemaining != recovery.AttemptsRemaining ||
				!reflect.DeepEqual(got.Capabilities, []string{"start", "stop", "restart"}) {
				t.Fatalf("status response = %+v", got)
			}
		})
	}
}

func TestStatusStatesPreserveRuntimeMetadataAndOptionalVersion(t *testing.T) {
	activeArtifact := artifact.Observation{
		Engine:     artifact.EngineCPA,
		ArtifactID: artifact.ID("sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"),
		Version:    "7.3.3",
	}
	for _, state := range []readiness.State{readiness.Unknown, readiness.Offline, readiness.Starting, readiness.Ready} {
		t.Run(string(state), func(t *testing.T) {
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			h, err := NewHandler(Config{
				RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
				Status: statusObserverFunc(func(got context.Context) readiness.State {
					if got != ctx {
						t.Error("status lost caller context")
					}
					return state
				}),
				Artifact: artifactStatusObserverFunc(func() *artifact.Observation {
					observed := activeArtifact
					return &observed
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, statusPath, nil).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
			req.Header.Set(ArtifactObservationHeader, ArtifactObservationFeature)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("observed state returned HTTP %d", response.Code)
			}
			var got statusResponse
			decodeResponse(t, response, &got)
			if got.ProtocolVersion != "v1" || got.RuntimeIdentity != "runtime-01" || got.RuntimeGeneration != 7 ||
				got.State != string(state) || got.CPAObservedVersion != "7.3.3" ||
				got.ActiveGatewayArtifact == nil || *got.ActiveGatewayArtifact != activeArtifact || len(got.Capabilities) != 0 {
				t.Fatalf("status changed authority or invented facts: %+v", got)
			}
		})
	}
}

func TestStatusArtifactObservationIsOptInAndRuntime14Compatible(t *testing.T) {
	activeArtifact := artifact.Observation{
		Engine:     artifact.EngineCPA,
		ArtifactID: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Version:    "7.3.3",
	}
	var observations int
	h, err := NewHandler(Config{
		RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
		Artifact: artifactStatusObserverFunc(func() *artifact.Observation {
			observations++
			observed := activeArtifact
			return &observed
		}),
	})
	if err != nil {
		t.Fatal(err)
	}

	legacyRequest := httptest.NewRequest(http.MethodGet, statusPath, nil)
	legacyRequest.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	legacyResponse := httptest.NewRecorder()
	h.ServeHTTP(legacyResponse, legacyRequest)
	// This is the exact strict status shape consumed by the Runtime14 Manager
	// at v2@132f47ae. It intentionally has no activeGatewayArtifact field.
	type runtime14StatusResponse struct {
		ProtocolVersion    string            `json:"protocolVersion"`
		RuntimeIdentity    string            `json:"runtimeIdentity"`
		RuntimeGeneration  uint64            `json:"runtimeGeneration"`
		State              string            `json:"state"`
		CPAObservedVersion string            `json:"cpaObservedVersion"`
		Capabilities       []string          `json:"capabilities"`
		Recovery           *recoveryResponse `json:"recovery"`
	}
	var legacy runtime14StatusResponse
	decoder := json.NewDecoder(legacyResponse.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&legacy); err != nil {
		t.Fatalf("Runtime14 strict Manager decode failed: %v", err)
	}
	if legacyResponse.Code != http.StatusOK || legacy.ProtocolVersion != Version ||
		legacy.RuntimeIdentity != "runtime-01" || legacy.RuntimeGeneration != 7 ||
		legacy.CPAObservedVersion != "" || observations != 0 {
		t.Fatalf("legacy v1 status = %d, %+v, artifact reads %d", legacyResponse.Code, legacy, observations)
	}

	enhancedRequest := httptest.NewRequest(http.MethodGet, statusPath, nil)
	enhancedRequest.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	enhancedRequest.Header.Set(ArtifactObservationHeader, "future-feature, "+ArtifactObservationFeature)
	enhancedResponse := httptest.NewRecorder()
	h.ServeHTTP(enhancedResponse, enhancedRequest)
	var enhanced statusResponse
	decodeResponse(t, enhancedResponse, &enhanced)
	if enhancedResponse.Code != http.StatusOK || enhanced.CPAObservedVersion != "7.3.3" ||
		enhanced.ActiveGatewayArtifact == nil || *enhanced.ActiveGatewayArtifact != activeArtifact || observations != 1 {
		t.Fatalf("opted-in v1 status = %d, %+v, artifact reads %d", enhancedResponse.Code, enhanced, observations)
	}
}

func TestStatusCallerCannotOverrideProbeOrForwardCredentials(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		if r.Method != http.MethodHead || r.URL.String() != "/healthz" {
			t.Errorf("caller changed probe method/path: %s %s", r.Method, r.URL)
		}
		for _, name := range []string{"Authorization", "X-Management-Key", "Cookie", "X-CPA-Addr"} {
			if r.Header.Get(name) != "" {
				t.Errorf("probe forwarded caller header %s", name)
			}
		}
		w.Header().Set("X-CPA-Version", "not-a-safe-version-source")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	observer, err := readiness.New(statusProcess{}, server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(Config{
		RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken, Status: observer,
	})
	if err != nil {
		t.Fatal(err)
	}
	request(t, h, http.MethodGet, statusPath, "wrong-token")
	request(t, h, http.MethodGet, handshakePath, testRuntimeToken)
	if probes.Load() != 0 {
		t.Fatal("unauthorized status or handshake caused network access")
	}
	req := httptest.NewRequest(http.MethodGet, statusPath+"?target=http://remote.invalid/&path=/v0/management/config", nil)
	req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	req.Header.Set("X-Management-Key", "caller-secret")
	req.Header.Set("Cookie", "caller-secret")
	req.Header.Set("X-CPA-Addr", "remote.invalid:80")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	var got map[string]any
	decodeResponse(t, response, &got)
	if response.Code != http.StatusOK || got["state"] != "ready" || got["cpaObservedVersion"] != "" || probes.Load() != 1 {
		t.Fatalf("status = %d, %+v, probes %d", response.Code, got, probes.Load())
	}
	if len(got) != 6 {
		t.Fatalf("status exposed additional process or probe facts: %+v", got)
	}
}

func TestStatusFailsClosedForInvalidArtifactObservation(t *testing.T) {
	h, err := NewHandler(Config{
		RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
		Artifact: artifactStatusObserverFunc(func() *artifact.Observation {
			return &artifact.Observation{Engine: artifact.EngineCPA, ArtifactID: "sha256:UPPERCASE", Version: "7.3.3"}
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodGet, statusPath, nil)
	req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	req.Header.Set(ArtifactObservationHeader, ArtifactObservationFeature)
	response := httptest.NewRecorder()
	h.ServeHTTP(response, req)
	var got statusResponse
	decodeResponse(t, response, &got)
	if got.ActiveGatewayArtifact != nil || got.CPAObservedVersion != "" {
		t.Fatalf("status exposed invalid artifact observation: %+v", got)
	}
}

type statusObserverFunc func(context.Context) readiness.State

func (f statusObserverFunc) Observe(ctx context.Context) readiness.State { return f(ctx) }

type recoveryStatusObserverFunc func() lifecycle.RecoveryStatus

func (f recoveryStatusObserverFunc) RecoveryStatus() lifecycle.RecoveryStatus { return f() }

type artifactStatusObserverFunc func() *artifact.Observation

func (f artifactStatusObserverFunc) Observation() *artifact.Observation { return f() }

type statusProcess struct{}

func (statusProcess) Observe() cpaprocess.Observation {
	return cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 1, PID: 123}
}
