package main

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	values := map[string]string{
		"CPAMP_RUNTIME_IDENTITY": " runtime-01 ",
		"CPAMP_RUNTIME_TOKEN":    "runtime-token",
	}
	cfg, err := loadConfig(func(key string) string { return values[key] }, fixedGeneration(7))
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.addr != defaultRuntimeAddr || cfg.runtimeIdentity != "runtime-01" || cfg.runtimeGeneration != 7 || cfg.token != "runtime-token" {
		t.Fatalf("config = %#v", cfg)
	}

	values["CPAMP_RUNTIME_ADDR"] = ":28318"
	values["CPAMP_RUNTIME_GENERATION"] = "99"
	cfg, err = loadConfig(func(key string) string { return values[key] }, fixedGeneration(8))
	if err != nil {
		t.Fatalf("loadConfig() custom address error = %v", err)
	}
	if cfg.addr != ":28318" {
		t.Fatalf("address = %q", cfg.addr)
	}
	if cfg.runtimeGeneration != 8 {
		t.Fatalf("runtime generation = %d, want source value 8", cfg.runtimeGeneration)
	}
}

func TestLoadConfigRejectsInvalidRequiredValues(t *testing.T) {
	valid := map[string]string{
		"CPAMP_RUNTIME_IDENTITY": "runtime-01",
		"CPAMP_RUNTIME_TOKEN":    "runtime-token",
	}
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "identity", key: "CPAMP_RUNTIME_IDENTITY", want: "CPAMP_RUNTIME_IDENTITY is required"},
		{name: "token", key: "CPAMP_RUNTIME_TOKEN", want: "CPAMP_RUNTIME_TOKEN is required"},
		{name: "token whitespace", key: "CPAMP_RUNTIME_TOKEN", value: "token value", want: "CPAMP_RUNTIME_TOKEN must not contain whitespace"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := make(map[string]string, len(valid))
			for key, value := range valid {
				values[key] = value
			}
			values[test.key] = test.value
			_, err := loadConfig(func(key string) string { return values[key] }, fixedGeneration(1))
			if err == nil || err.Error() != test.want {
				t.Fatalf("loadConfig() error = %v, want %q", err, test.want)
			}
		})
	}
}

func TestLoadConfigResamplesZeroGeneration(t *testing.T) {
	values := validConfigValues()
	generations := []uint64{0, 42}
	var calls int
	cfg, err := loadConfig(func(key string) string { return values[key] }, func() (uint64, error) {
		generation := generations[calls]
		calls++
		return generation, nil
	})
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.runtimeGeneration != 42 || calls != 2 {
		t.Fatalf("runtime generation = %d after %d calls, want 42 after 2 calls", cfg.runtimeGeneration, calls)
	}
}

func TestLoadConfigFailsWhenGenerationSourceFails(t *testing.T) {
	values := validConfigValues()
	sourceErr := errors.New("entropy unavailable")
	cfg, err := loadConfig(func(key string) string { return values[key] }, func() (uint64, error) {
		return 0, sourceErr
	})
	if !errors.Is(err, sourceErr) || !strings.Contains(err.Error(), "generate runtime generation") {
		t.Fatalf("loadConfig() error = %v, want wrapped generation error", err)
	}
	if !reflect.DeepEqual(cfg, config{}) {
		t.Fatalf("config = %#v, want zero value", cfg)
	}
}

func TestRuntimeGenerationIsStableForSupervisorIncarnation(t *testing.T) {
	cfg := loadTestConfig(t, fixedGeneration(41))
	handler, err := newRuntimeHandler(cfg)
	if err != nil {
		t.Fatalf("newRuntimeHandler() error = %v", err)
	}

	requests := []struct {
		path      string
		wantState string
	}{
		{path: "/v1/runtime/handshake"},
		{path: "/v1/runtime/status", wantState: "unknown"},
		{path: "/v1/runtime/status", wantState: "unknown"},
	}
	for _, request := range requests {
		response := requestRuntime(t, handler, request.path)
		if response.RuntimeGeneration != 41 || response.RuntimeIdentity != "runtime-01" || response.ProtocolVersion != "v1" {
			t.Fatalf("%s protocol metadata = %#v", request.path, response)
		}
		if response.State != request.wantState || response.CPAObservedVersion != "" || len(response.Capabilities) != 0 {
			t.Fatalf("%s response = %#v", request.path, response)
		}
	}
}

func TestSeparateSupervisorInitializationsUseIndependentGenerations(t *testing.T) {
	first := loadTestConfig(t, fixedGeneration(41))
	second := loadTestConfig(t, fixedGeneration(42))
	if first.runtimeGeneration != 41 || second.runtimeGeneration != 42 {
		t.Fatalf("runtime generations = %d and %d, want 41 and 42", first.runtimeGeneration, second.runtimeGeneration)
	}
}

func TestHTTPServerHasBoundedTimeouts(t *testing.T) {
	server := newHTTPServer(http.NewServeMux())
	if server.ReadHeaderTimeout != 5*time.Second ||
		server.ReadTimeout != 10*time.Second ||
		server.WriteTimeout != 10*time.Second ||
		server.IdleTimeout != 60*time.Second ||
		server.MaxHeaderBytes != 16<<10 {
		t.Fatalf("HTTP server limits = %#v", server)
	}
}

func TestServeStopsCleanlyWhenContextIsCanceled(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		result <- serve(ctx, listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusNoContent)
		}))
	}()

	response, err := http.Get("http://" + listener.Addr().String())
	if err != nil {
		t.Fatalf("GET running server: %v", err)
	}
	_ = response.Body.Close()
	cancel()

	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("serve() error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("serve() did not stop after context cancellation")
	}
}

func TestServeReportsListenerFailure(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	if err := listener.Close(); err != nil {
		t.Fatalf("close listener: %v", err)
	}
	err = serve(context.Background(), listener, http.NewServeMux())
	if err == nil || !strings.Contains(err.Error(), "closed network connection") {
		t.Fatalf("serve() error = %v", err)
	}
}

func TestServeForcesCloseAfterShutdownTimeout(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	requestStarted := make(chan struct{})
	releaseRequest := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	serveResult := make(chan error, 1)
	go func() {
		serveResult <- serveWithShutdownTimeout(ctx, listener, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			close(requestStarted)
			<-releaseRequest
			w.WriteHeader(http.StatusNoContent)
		}), 10*time.Millisecond)
	}()
	requestResult := make(chan error, 1)
	go func() {
		response, err := http.Get("http://" + listener.Addr().String())
		if err == nil {
			_ = response.Body.Close()
		}
		requestResult <- err
	}()

	select {
	case <-requestStarted:
	case <-time.After(time.Second):
		t.Fatal("request did not reach handler")
	}
	cancel()
	select {
	case err := <-serveResult:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("serveWithShutdownTimeout() error = %v, want deadline exceeded", err)
		}
	case <-time.After(time.Second):
		t.Fatal("serveWithShutdownTimeout() remained blocked after shutdown timeout")
	}
	close(releaseRequest)
	select {
	case <-requestResult:
	case <-time.After(time.Second):
		t.Fatal("forced-close request did not finish")
	}
}

type runtimeResponse struct {
	ProtocolVersion    string   `json:"protocolVersion"`
	RuntimeIdentity    string   `json:"runtimeIdentity"`
	RuntimeGeneration  uint64   `json:"runtimeGeneration"`
	State              string   `json:"state"`
	CPAObservedVersion string   `json:"cpaObservedVersion"`
	Capabilities       []string `json:"capabilities"`
}

func validConfigValues() map[string]string {
	return map[string]string{
		"CPAMP_RUNTIME_IDENTITY": "runtime-01",
		"CPAMP_RUNTIME_TOKEN":    "runtime-token",
	}
}

func fixedGeneration(generation uint64) generationSource {
	return func() (uint64, error) {
		return generation, nil
	}
}

func loadTestConfig(t *testing.T, source generationSource) config {
	t.Helper()
	values := validConfigValues()
	cfg, err := loadConfig(func(key string) string { return values[key] }, source)
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	return cfg
}

func requestRuntime(t *testing.T, handler http.Handler, path string) runtimeResponse {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.Header.Set("Authorization", "Bearer runtime-token")
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, req)
	if recorder.Code != http.StatusOK {
		t.Fatalf("GET %s status = %d, body = %s", path, recorder.Code, recorder.Body.String())
	}
	var response runtimeResponse
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatalf("decode GET %s response: %v", path, err)
	}
	return response
}
