package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
)

const testRuntimeToken = "test-runtime-token"

func TestEmbeddedClientStatusMapsSupervisorObservation(t *testing.T) {
	tests := []struct {
		name         string
		response     string
		wantState    model.RuntimeState
		wantVersion  model.CPAObservedVersion
		wantCaps     model.RuntimeCapabilities
		wantSupports model.RuntimeCapability
	}{
		{
			name:      "unknown before CPA observation",
			response:  `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"unknown","cpaObservedVersion":"","capabilities":[]}`,
			wantState: model.RuntimeStateUnknown,
			wantCaps:  model.RuntimeCapabilities{},
		},
		{
			name:         "ready with CPA version and capabilities",
			response:     `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"ready","cpaObservedVersion":"v7.2.130","capabilities":["capability-a","capability-b"]}`,
			wantState:    model.RuntimeStateReady,
			wantVersion:  "v7.2.130",
			wantCaps:     model.RuntimeCapabilities{"capability-a", "capability-b"},
			wantSupports: "capability-b",
		},
		{
			name:      "authoritative offline observation",
			response:  `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"offline","cpaObservedVersion":"","capabilities":[]}`,
			wantState: model.RuntimeStateOffline,
			wantCaps:  model.RuntimeCapabilities{},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requestCount int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requestCount++
				if r.Method != http.MethodGet {
					t.Errorf("method = %s, want GET", r.Method)
				}
				if r.URL.Path != embeddedRuntimeStatusPath || r.URL.RawQuery != "" {
					t.Errorf("URL = %q, want %q without query", r.URL.String(), embeddedRuntimeStatusPath)
				}
				if got := r.Header.Get("Authorization"); got != "Bearer "+testRuntimeToken {
					t.Errorf("Authorization = %q", got)
				}
				if got := r.Header.Get("Accept"); got != "application/json" {
					t.Errorf("Accept = %q", got)
				}
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.response))
			}))
			defer server.Close()

			status, err := NewEmbeddedClient(server.URL+"/", testRuntimeToken).Status(t.Context())
			if err != nil {
				t.Fatalf("Status() error = %v", err)
			}
			if requestCount != 1 {
				t.Fatalf("request count = %d, want 1", requestCount)
			}
			if status.ProtocolVersion != "v1" || status.Identity != "runtime-01" || status.Generation != 7 {
				t.Fatalf("Status() protocol metadata = %#v", status)
			}
			if status.State != test.wantState || status.CPAObservedVersion != test.wantVersion {
				t.Fatalf("Status() = %#v", status)
			}
			if !reflect.DeepEqual(status.Capabilities, test.wantCaps) {
				t.Fatalf("Status() capabilities = %#v, want %#v", status.Capabilities, test.wantCaps)
			}
			if test.wantSupports != "" && !status.Capabilities.Supports(test.wantSupports) {
				t.Fatalf("Status() capabilities do not support %q", test.wantSupports)
			}
		})
	}
}

func TestEmbeddedClientStatusRejectsHTTPFailureWithoutLeakingToken(t *testing.T) {
	tests := []struct {
		name     string
		status   int
		location string
	}{
		{name: "unauthorized", status: http.StatusUnauthorized},
		{name: "non-2xx", status: http.StatusInternalServerError},
		{name: "redirect", status: http.StatusFound, location: "/redirected"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requestCount int
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				requestCount++
				if test.location != "" {
					w.Header().Set("Location", test.location)
				}
				http.Error(w, "rejected "+testRuntimeToken, test.status)
			}))
			defer server.Close()

			status, err := NewEmbeddedClient(server.URL, testRuntimeToken).Status(t.Context())
			assertZeroStatusError(t, status, err)
			if strings.Contains(err.Error(), testRuntimeToken) {
				t.Fatalf("Status() error leaked runtime token: %v", err)
			}
			if requestCount != 1 {
				t.Fatalf("request count = %d, want 1", requestCount)
			}
		})
	}
}

func TestEmbeddedClientStatusUsesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	status, err := NewEmbeddedClient("http://127.0.0.1:1", testRuntimeToken).Status(ctx)
	assertZeroStatusError(t, status, err)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Status() error = %v, want context canceled", err)
	}
}

func TestEmbeddedClientStatusTimesOut(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
	defer server.Close()

	client := NewEmbeddedClient(server.URL, testRuntimeToken)
	client.httpClient.Timeout = 20 * time.Millisecond
	status, err := client.Status(t.Context())
	assertZeroStatusError(t, status, err)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Status() error = %v, want deadline exceeded", err)
	}
}

func TestEmbeddedClientStatusReturnsErrorWhenSupervisorIsUnavailable(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	serverURL := server.URL
	server.Close()

	status, err := NewEmbeddedClient(serverURL, testRuntimeToken).Status(t.Context())
	assertZeroStatusError(t, status, err)
	if status.State == model.RuntimeStateOffline {
		t.Fatal("Status() converted transport failure to offline")
	}
}

func TestEmbeddedClientStatusRejectsInvalidProtocolResponse(t *testing.T) {
	tests := []struct {
		name     string
		response string
	}{
		{name: "malformed JSON", response: `{"protocolVersion":`},
		{name: "multiple JSON values", response: `{}` + `{}`},
		{name: "missing protocol metadata", response: `{"state":"unknown","capabilities":[]}`},
		{name: "missing identity", response: `{"protocolVersion":"v1","runtimeGeneration":7,"state":"unknown","capabilities":[]}`},
		{name: "missing generation", response: `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","state":"unknown","capabilities":[]}`},
		{name: "unsupported protocol version", response: `{"protocolVersion":"v2","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"unknown","capabilities":[]}`},
		{name: "ready without CPA version", response: `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"ready","capabilities":[]}`},
		{name: "invalid runtime state", response: `{"protocolVersion":"v1","runtimeIdentity":"runtime-01","runtimeGeneration":7,"state":"broken","capabilities":[]}`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				_, _ = w.Write([]byte(test.response))
			}))
			defer server.Close()

			status, err := NewEmbeddedClient(server.URL, testRuntimeToken).Status(t.Context())
			assertZeroStatusError(t, status, err)
			if strings.Contains(err.Error(), testRuntimeToken) {
				t.Fatalf("Status() error leaked runtime token: %v", err)
			}
		})
	}
}

func assertZeroStatusError(t *testing.T, status model.RuntimeObservedStatus, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("Status() error = nil")
	}
	if !reflect.DeepEqual(status, model.RuntimeObservedStatus{}) {
		t.Fatalf("Status() = %#v, want zero value", status)
	}
}
