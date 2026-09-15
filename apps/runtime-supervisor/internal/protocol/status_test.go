package protocol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
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
			h, err := NewHandler(Config{
				RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken,
				Status: statusObserverFunc(func(context.Context) readiness.State {
					observations++
					return readiness.Ready
				}),
			})
			if err != nil {
				t.Fatal(err)
			}
			response := request(t, h, test.method, test.path, test.token)
			if response.Code != test.code || observations != test.observations {
				t.Fatalf("HTTP %d, observations %d; want %d, %d", response.Code, observations, test.code, test.observations)
			}
		})
	}
}

func TestStatusStatesPreserveRuntimeMetadataAndOptionalVersion(t *testing.T) {
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
			})
			if err != nil {
				t.Fatal(err)
			}
			req := httptest.NewRequest(http.MethodGet, statusPath, nil).WithContext(ctx)
			req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
			response := httptest.NewRecorder()
			h.ServeHTTP(response, req)
			if response.Code != http.StatusOK {
				t.Fatalf("observed state returned HTTP %d", response.Code)
			}
			var got statusResponse
			decodeResponse(t, response, &got)
			if got.ProtocolVersion != "v1" || got.RuntimeIdentity != "runtime-01" || got.RuntimeGeneration != 7 ||
				got.State != string(state) || got.CPAObservedVersion != "" || len(got.Capabilities) != 0 {
				t.Fatalf("status changed authority or invented facts: %+v", got)
			}
		})
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

type statusObserverFunc func(context.Context) readiness.State

func (f statusObserverFunc) Observe(ctx context.Context) readiness.State { return f(ctx) }

type statusProcess struct{}

func (statusProcess) Observe() cpaprocess.Observation {
	return cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 1, PID: 123}
}
