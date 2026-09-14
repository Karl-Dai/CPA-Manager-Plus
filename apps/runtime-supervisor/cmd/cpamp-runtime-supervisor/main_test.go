package main

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	values := map[string]string{
		"CPAMP_RUNTIME_IDENTITY":   " runtime-01 ",
		"CPAMP_RUNTIME_GENERATION": "7",
		"CPAMP_RUNTIME_TOKEN":      "runtime-token",
	}
	cfg, err := loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("loadConfig() error = %v", err)
	}
	if cfg.addr != defaultRuntimeAddr || cfg.runtimeIdentity != "runtime-01" || cfg.runtimeGeneration != 7 || cfg.token != "runtime-token" {
		t.Fatalf("config = %#v", cfg)
	}

	values["CPAMP_RUNTIME_ADDR"] = ":28318"
	cfg, err = loadConfig(func(key string) string { return values[key] })
	if err != nil {
		t.Fatalf("loadConfig() custom address error = %v", err)
	}
	if cfg.addr != ":28318" {
		t.Fatalf("address = %q", cfg.addr)
	}
}

func TestLoadConfigRejectsInvalidRequiredValues(t *testing.T) {
	valid := map[string]string{
		"CPAMP_RUNTIME_IDENTITY":   "runtime-01",
		"CPAMP_RUNTIME_GENERATION": "1",
		"CPAMP_RUNTIME_TOKEN":      "runtime-token",
	}
	tests := []struct {
		name  string
		key   string
		value string
		want  string
	}{
		{name: "identity", key: "CPAMP_RUNTIME_IDENTITY", want: "CPAMP_RUNTIME_IDENTITY is required"},
		{name: "generation missing", key: "CPAMP_RUNTIME_GENERATION", want: "CPAMP_RUNTIME_GENERATION is required"},
		{name: "generation zero", key: "CPAMP_RUNTIME_GENERATION", value: "0", want: "CPAMP_RUNTIME_GENERATION must be a positive integer"},
		{name: "generation invalid", key: "CPAMP_RUNTIME_GENERATION", value: "next", want: "CPAMP_RUNTIME_GENERATION must be a positive integer"},
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
			_, err := loadConfig(func(key string) string { return values[key] })
			if err == nil || err.Error() != test.want {
				t.Fatalf("loadConfig() error = %v, want %q", err, test.want)
			}
		})
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
