package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/protocol"
	runtimeupdate "github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/update"
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
	handler, err := newRuntimeHandler(t.Context(), cfg)
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

func TestPrepareUpdateTransportBudgetIsDedicatedAndOrdered(t *testing.T) {
	server := newHTTPServer(http.NewServeMux())
	if server.WriteTimeout != 10*time.Second {
		t.Fatalf("ordinary Runtime WriteTimeout = %s, want 10s", server.WriteTimeout)
	}
	if prepareUpdateResponseTimeout <= lifecycle.PrepareUpdateExecutionTimeout+
		lifecycle.PrepareUpdateTerminalPersistenceTimeout ||
		prepareUpdateResponseTimeout <= runtimeupdate.ReleaseClientTimeout {
		t.Fatalf("prepare response budget = %s, execution=%s persistence=%s release=%s", prepareUpdateResponseTimeout,
			lifecycle.PrepareUpdateExecutionTimeout, lifecycle.PrepareUpdateTerminalPersistenceTimeout, runtimeupdate.ReleaseClientTimeout)
	}
	if activateUpdateResponseTimeout <= lifecycle.ActivationExecutionTimeout+
		lifecycle.ActivationTerminalPersistenceTimeout ||
		lifecycle.ActivationCandidateReadinessTimeout+lifecycle.ActivationRollbackReadinessTimeout >=
			lifecycle.ActivationExecutionTimeout {
		t.Fatalf("activate response budget=%s execution=%s persistence=%s candidate=%s rollback=%s",
			activateUpdateResponseTimeout, lifecycle.ActivationExecutionTimeout,
			lifecycle.ActivationTerminalPersistenceTimeout, lifecycle.ActivationCandidateReadinessTimeout,
			lifecycle.ActivationRollbackReadinessTimeout)
	}
}

func TestPrepareUpdateResponseDeadlineIsRouteSpecific(t *testing.T) {
	for _, test := range []struct {
		name      string
		path      string
		deadlines int
	}{
		{name: "ordinary lifecycle", path: "/v1/runtime/operations/start"},
		{name: "prepare update", path: prepareUpdateResponsePath, deadlines: 2},
		{name: "activate update", path: activateUpdateResponsePath, deadlines: 2},
	} {
		t.Run(test.name, func(t *testing.T) {
			writer := &deadlineResponseWriter{ResponseRecorder: httptest.NewRecorder()}
			handler := withPrepareUpdateResponseBudget(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNoContent)
			}))
			request := httptest.NewRequest(http.MethodPost, test.path, nil)
			handler.ServeHTTP(writer, request)
			if len(writer.deadlines) != test.deadlines {
				t.Fatalf("write deadlines = %d, want %d", len(writer.deadlines), test.deadlines)
			}
			if test.path == prepareUpdateResponsePath && !writer.deadlines[0].After(time.Now().Add(11*time.Minute)) {
				t.Fatalf("prepare deadline = %s, want route budget near %s", writer.deadlines[0], prepareUpdateResponseTimeout)
			}
			if test.path == activateUpdateResponsePath && !writer.deadlines[0].After(time.Now().Add(2*time.Minute)) {
				t.Fatalf("activate deadline = %s, want route budget near %s", writer.deadlines[0], activateUpdateResponseTimeout)
			}
		})
	}
}

type deadlineResponseWriter struct {
	*httptest.ResponseRecorder
	deadlines []time.Time
}

func (w *deadlineResponseWriter) SetWriteDeadline(deadline time.Time) error {
	w.deadlines = append(w.deadlines, deadline)
	return nil
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

func TestSupervisorShutdownClosesLifecycleAdmissionBeforeDraining(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAccepted := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(func() {
		cancel()
		releaseAccepted()
		_ = listener.Close()
	})

	journalPath := filepath.Join(t.TempDir(), "operations.sqlite")
	store, err := journal.Open(t.Context(), journalPath, journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	trackedJournal := &shutdownJournal{Store: store}
	child := &shutdownProcess{entered: make(chan struct{}), release: release}
	executor, err := lifecycle.NewExecutor(
		journal.Authority{RuntimeIdentity: "runtime-01", RuntimeGeneration: 41},
		trackedJournal,
		child,
		"supervisor-local-cpa",
	)
	if err != nil {
		t.Fatal(err)
	}
	queued := &queuedStartExecutor{
		next:        executor,
		operationID: "queued-start",
		waiting:     make(chan struct{}),
	}
	protocolHandler, err := protocol.NewHandler(protocol.Config{
		RuntimeIdentity:   "runtime-01",
		RuntimeGeneration: 41,
		Token:             "runtime-token",
		Start:             queued,
		Stop:              executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	runtime := &runtimeHandler{Handler: protocolHandler, executor: executor}
	shutdown := &shutdownAdmissionObserver{runtimeHandler: runtime, closed: make(chan struct{})}
	serveResult := make(chan error, 1)
	go func() {
		serveErr := serve(ctx, listener, shutdown)
		serveResult <- errors.Join(serveErr, runtime.Close())
	}()

	client := &http.Client{Timeout: 5 * time.Second}
	submit := func(operationID string) <-chan shutdownHTTPResult {
		result := make(chan shutdownHTTPResult, 1)
		go func() {
			req, err := http.NewRequest(
				http.MethodPost,
				"http://"+listener.Addr().String()+"/v1/runtime/operations/start",
				strings.NewReader(startBody(operationID, 41)),
			)
			if err != nil {
				result <- shutdownHTTPResult{err: err}
				return
			}
			req.Header.Set("Authorization", "Bearer runtime-token")
			response, err := client.Do(req)
			if err != nil {
				result <- shutdownHTTPResult{err: err}
				return
			}
			body, readErr := io.ReadAll(response.Body)
			result <- shutdownHTTPResult{
				status: response.StatusCode,
				body:   string(body),
				err:    errors.Join(readErr, response.Body.Close()),
			}
		}()
		return result
	}

	acceptedResult := submit("accepted-start")
	select {
	case <-child.entered:
	case <-time.After(time.Second):
		t.Fatal("accepted Start did not reach its process side effect")
	}
	queuedResult := submit("queued-start")
	select {
	case <-queued.waiting:
	case <-time.After(time.Second):
		t.Fatal("second Start did not enter the handler and wait on the lifecycle gate")
	}

	cancel()
	select {
	case <-shutdown.closed:
	case <-time.After(time.Second):
		t.Fatal("Supervisor shutdown did not close lifecycle admission")
	}
	if child.startCount() != 1 {
		t.Fatalf("process starts before drain = %d, want 1", child.startCount())
	}
	if trackedJournal.closeCount() != 0 {
		t.Fatalf("journal closed while accepted Start was still executing")
	}
	if resolves, begins := trackedJournal.submissionCounts(); resolves != 1 || begins != 1 {
		t.Fatalf("journal submissions before drain = Resolve %d, Begin %d; want 1, 1", resolves, begins)
	}
	select {
	case err := <-serveResult:
		t.Fatalf("server shutdown completed before accepted Start drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}

	reader, err := journal.Open(t.Context(), journalPath, journal.Options{})
	if err != nil {
		t.Fatalf("open journal during accepted execution: %v", err)
	}
	accepted, acceptedErr := reader.Get(t.Context(), "runtime-01", "accepted-start")
	_, queuedErr := reader.Get(t.Context(), "runtime-01", "queued-start")
	if closeErr := reader.Close(); closeErr != nil {
		t.Fatal(closeErr)
	}
	if acceptedErr != nil || accepted.State != journal.StateRunning {
		t.Fatalf("accepted durable execution = %+v, %v", accepted, acceptedErr)
	}
	if !errors.Is(queuedErr, journal.ErrOperationNotFound) {
		t.Fatalf("queued Start wrote durable intent before drain: %v", queuedErr)
	}

	releaseAccepted()
	acceptedHTTP := <-acceptedResult
	if acceptedHTTP.err != nil || acceptedHTTP.status != http.StatusOK || !strings.Contains(acceptedHTTP.body, `"state":"succeeded"`) {
		t.Fatalf("accepted Start response = %d, %s, %v", acceptedHTTP.status, acceptedHTTP.body, acceptedHTTP.err)
	}
	queuedHTTP := <-queuedResult
	if queuedHTTP.err != nil || queuedHTTP.status != http.StatusServiceUnavailable ||
		!strings.Contains(queuedHTTP.body, "operation_persistence_unavailable") {
		t.Fatalf("queued Start response = %d, %s, %v", queuedHTTP.status, queuedHTTP.body, queuedHTTP.err)
	}
	select {
	case err := <-serveResult:
		if err != nil {
			t.Fatalf("Supervisor shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("server shutdown did not finish after accepted Start drained")
	}
	if child.startCount() != 1 {
		t.Fatalf("shutdown admitted an additional process Start: %d", child.startCount())
	}
	if trackedJournal.closeCount() != 1 {
		t.Fatalf("journal close calls = %d, want 1", trackedJournal.closeCount())
	}
	if resolves, begins := trackedJournal.submissionCounts(); resolves != 1 || begins != 1 {
		t.Fatalf("queued Start reached journal = Resolve %d, Begin %d; want 1, 1", resolves, begins)
	}
	if err := runtime.Close(); err != nil || trackedJournal.closeCount() != 1 {
		t.Fatalf("repeated runtime Close = %v, journal close calls %d", err, trackedJournal.closeCount())
	}

	reopened, err := journal.Open(t.Context(), journalPath, journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	accepted, acceptedErr = reopened.Get(t.Context(), "runtime-01", "accepted-start")
	_, queuedErr = reopened.Get(t.Context(), "runtime-01", "queued-start")
	if acceptedErr != nil || accepted.State != journal.StateSucceeded {
		t.Fatalf("drained durable execution = %+v, %v", accepted, acceptedErr)
	}
	if !errors.Is(queuedErr, journal.ErrOperationNotFound) {
		t.Fatalf("queued Start wrote durable intent during shutdown: %v", queuedErr)
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
	ProtocolVersion       string                   `json:"protocolVersion"`
	RuntimeIdentity       string                   `json:"runtimeIdentity"`
	RuntimeGeneration     uint64                   `json:"runtimeGeneration"`
	State                 string                   `json:"state"`
	CPAObservedVersion    string                   `json:"cpaObservedVersion"`
	ActiveGatewayArtifact *runtimeArtifactResponse `json:"activeGatewayArtifact"`
	Capabilities          []string                 `json:"capabilities"`
	Recovery              *runtimeRecoveryResponse `json:"recovery"`
}

type runtimeArtifactResponse struct {
	Engine     string `json:"engine"`
	ArtifactID string `json:"artifactId"`
	Version    string `json:"version"`
}

type runtimeRecoveryResponse struct {
	State             string `json:"state"`
	AttemptsRemaining int    `json:"attemptsRemaining"`
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
	if path == "/v1/runtime/status" {
		req.Header.Set(protocol.ArtifactObservationHeader, protocol.ArtifactObservationFeature)
	}
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

type shutdownAdmissionObserver struct {
	*runtimeHandler
	once   sync.Once
	closed chan struct{}
}

func (h *shutdownAdmissionObserver) CloseAdmission() {
	h.runtimeHandler.CloseAdmission()
	h.once.Do(func() { close(h.closed) })
}

type queuedStartExecutor struct {
	next        protocol.StartExecutor
	operationID string
	waiting     chan struct{}
	once        sync.Once
}

func (e *queuedStartExecutor) Start(ctx context.Context, request lifecycle.StartRequest) (journal.Operation, error) {
	if request.OperationID != e.operationID {
		return e.next.Start(ctx, request)
	}
	type result struct {
		operation journal.Operation
		err       error
	}
	completed := make(chan result, 1)
	go func() {
		operation, err := e.next.Start(ctx, request)
		completed <- result{operation: operation, err: err}
	}()
	timer := time.NewTimer(20 * time.Millisecond)
	defer timer.Stop()
	select {
	case got := <-completed:
		return got.operation, got.err
	case <-timer.C:
		e.once.Do(func() { close(e.waiting) })
		got := <-completed
		return got.operation, got.err
	}
}

type shutdownJournal struct {
	*journal.Store
	mu           sync.Mutex
	resolveCalls int
	beginCalls   int
	closeCalls   int
}

func (j *shutdownJournal) Resolve(ctx context.Context, authority journal.Authority, intent journal.Intent) (journal.Operation, bool, error) {
	j.mu.Lock()
	j.resolveCalls++
	j.mu.Unlock()
	return j.Store.Resolve(ctx, authority, intent)
}

func (j *shutdownJournal) Begin(ctx context.Context, authority journal.Authority, intent journal.Intent) (journal.Operation, bool, error) {
	j.mu.Lock()
	j.beginCalls++
	j.mu.Unlock()
	return j.Store.Begin(ctx, authority, intent)
}

func (j *shutdownJournal) Close() error {
	j.mu.Lock()
	j.closeCalls++
	j.mu.Unlock()
	return j.Store.Close()
}

func (j *shutdownJournal) closeCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.closeCalls
}

func (j *shutdownJournal) submissionCounts() (int, int) {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.resolveCalls, j.beginCalls
}

type shutdownProcess struct {
	mu          sync.Mutex
	observation cpaprocess.Observation
	starts      int
	entered     chan struct{}
	release     <-chan struct{}
	once        sync.Once
}

func (p *shutdownProcess) Observe() cpaprocess.Observation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.observation.State == "" {
		return cpaprocess.Observation{State: cpaprocess.StateNotStarted}
	}
	return p.observation
}

func (p *shutdownProcess) Start(ctx context.Context, _ cpaprocess.StartSpec) (cpaprocess.Observation, error) {
	p.mu.Lock()
	p.starts++
	p.mu.Unlock()
	p.once.Do(func() { close(p.entered) })
	select {
	case <-p.release:
	case <-ctx.Done():
		return p.Observe(), ctx.Err()
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.observation = cpaprocess.Observation{State: cpaprocess.StateRunning, PID: 123}
	return p.observation, nil
}

func (p *shutdownProcess) PrepareStop() (cpaprocess.StopTarget, error) {
	return nil, cpaprocess.ErrStateConflict
}

func (p *shutdownProcess) ExitEvents() <-chan cpaprocess.ExitEvent { return nil }

func (p *shutdownProcess) startCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts
}

type shutdownHTTPResult struct {
	status int
	body   string
	err    error
}
