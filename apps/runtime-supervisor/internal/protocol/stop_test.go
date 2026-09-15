package protocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const validStopBody = `{"operationId":"op","expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":7}`

func TestStopAuthenticatesBeforeReadingBodyOrCallingExecutor(t *testing.T) {
	h := stopHandler(t, stopFunc(func(context.Context, lifecycle.StopRequest) (journal.Operation, error) {
		t.Fatal("unauthorized request reached Stop execution boundary")
		return journal.Operation{}, nil
	}))
	req := httptest.NewRequest(http.MethodPost, stopPath, nil)
	req.Body = unreadableBody{t}
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthorized response: %d, %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "unauthorized")
}

func TestStopStrictRequestValidationRejectsCallerTerminationAuthority(t *testing.T) {
	h := stopHandler(t, stopFunc(func(context.Context, lifecycle.StopRequest) (journal.Operation, error) {
		t.Fatal("invalid request reached journal/process executor")
		return journal.Operation{}, nil
	}))
	tests := map[string]string{
		"empty": "", "null": "null", "array": "[]", "string": `"request"`,
		"empty object": `{}`, "truncated": `{"operationId":`,
		"empty ID":           strings.Replace(validStopBody, `"op"`, `""`, 1),
		"long ID":            strings.Replace(validStopBody, `"op"`, `"`+strings.Repeat("x", 129)+`"`, 1),
		"UTF-8 byte limit":   strings.Replace(validStopBody, `"op"`, `"`+strings.Repeat("字", 43)+`"`, 1),
		"invalid UTF-8":      strings.Replace(validStopBody, "op\"", string([]byte{0xff})+"\"", 1),
		"missing identity":   `{"operationId":"op","expectedRuntimeGeneration":7}`,
		"blank identity":     strings.Replace(validStopBody, `"runtime-01"`, `"  "`, 1),
		"null identity":      strings.Replace(validStopBody, `"runtime-01"`, `null`, 1),
		"missing generation": `{"operationId":"op","expectedRuntimeIdentity":"runtime-01"}`,
		"trailing object":    validStopBody + `{}`, "trailing null": validStopBody + `null`,
		"trailing garbage": validStopBody + `x`,
		"oversized body":   strings.Repeat(" ", maxMutationBody) + validStopBody,
		"case alias":       strings.Replace(validStopBody, "operationId", "OperationId", 1),
		"duplicate ID":     strings.Replace(validStopBody, `"operationId":"op"`, `"operationId":"op","operationId":"other"`, 1),
	}
	for _, generation := range []string{"0", "-1", "1.5", "18446744073709551616", `"7"`, "null", "true"} {
		tests["generation "+generation] = strings.Replace(validStopBody, ":7", ":"+generation, 1)
	}
	for _, field := range []string{
		"pid", "signal", "force", "timeout", "grace", "executable", "argv", "args", "env", "cwd",
		"workingDirectory", "shell", "shellCommand", "action", "params",
	} {
		tests[field] = strings.TrimSuffix(validStopBody, "}") + `,"` + field + `":"not-allowed"}`
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			w := submitStop(t, h, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			assertErrorCode(t, w, "invalid_request")
		})
	}
}

func TestStopPreservesOpaqueIDAndFullWidthGeneration(t *testing.T) {
	id := strings.Repeat("字", 42) + " a"
	h := stopHandler(t, stopFunc(func(_ context.Context, request lifecycle.StopRequest) (journal.Operation, error) {
		if request.OperationID != id || request.ExpectedRuntimeGeneration != ^uint64(0) {
			t.Fatalf("typed request = %+v", request)
		}
		return journal.Operation{OperationID: id}, nil
	}))
	body := fmt.Sprintf(`{"operationId":%q,"expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":18446744073709551615}`, id)
	if w := submitStop(t, h, body); w.Code != http.StatusOK {
		t.Fatalf("valid opaque request = %d, %s", w.Code, w.Body.String())
	}
}

func TestStopReadOnlyModeIsUnsupported(t *testing.T) {
	h := newTestHandler(t)
	w := submitStop(t, h, validStopBody)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("read-only Stop = %d", w.Code)
	}
	assertErrorCode(t, w, "unsupported_operation")
	method := request(t, h, http.MethodGet, stopPath, testRuntimeToken)
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Stop method = %d, Allow %q", method.Code, method.Header().Get("Allow"))
	}
}

func TestLifecycleCapabilitiesAreDeterministicAndDoNotClaimReadiness(t *testing.T) {
	executor := lifecycleFunc{
		start: func(_ context.Context, request lifecycle.StartRequest) (journal.Operation, error) {
			return journal.Operation{OperationID: request.OperationID, OperationType: "start", RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, State: journal.StateSucceeded}, nil
		},
		stop: func(_ context.Context, request lifecycle.StopRequest) (journal.Operation, error) {
			return journal.Operation{OperationID: request.OperationID, OperationType: "stop", RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, State: journal.StateSucceeded}, nil
		},
	}
	h, err := NewHandler(Config{RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken, Start: executor, Stop: executor})
	if err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{handshakePath, statusPath} {
		var status statusResponse
		decodeResponse(t, request(t, h, http.MethodGet, path, testRuntimeToken), &status)
		if status.RuntimeGeneration != 7 || status.CPAObservedVersion != "" ||
			!reflect.DeepEqual(status.Capabilities, []string{"start", "stop"}) ||
			(path == statusPath && status.State != "unknown") {
			t.Fatalf("%s invented observation or lost lifecycle capability: %+v", path, status)
		}
	}
}

func TestStopResponseAndErrorMapping(t *testing.T) {
	for _, state := range []journal.State{journal.StateAccepted, journal.StateRunning, journal.StateSucceeded, journal.StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			h := stopHandler(t, stopFunc(func(_ context.Context, request lifecycle.StopRequest) (journal.Operation, error) {
				operation := journal.Operation{OperationID: request.OperationID, OperationType: "stop", RuntimeIdentity: "runtime-01", RuntimeGeneration: 6, State: state}
				if state == journal.StateFailed {
					operation.FailureCode = "process_stop_failed"
				}
				return operation, nil
			}))
			w := submitStop(t, h, validStopBody)
			if w.Code != http.StatusOK {
				t.Fatalf("Stop response = %d, %s", w.Code, w.Body.String())
			}
			var got operationResponse
			decodeResponse(t, w, &got)
			if got.OperationID != "op" || got.OperationType != "stop" || got.RuntimeIdentity != "runtime-01" ||
				got.RuntimeGeneration != 6 || got.State != state {
				t.Fatalf("operation response = %+v", got)
			}
			if state == journal.StateFailed && (got.Error == nil || got.Error.Code != "process_stop_failed") {
				t.Fatalf("replay lost Stop failure evidence: %+v", got)
			}
			if strings.Contains(w.Body.String(), "Fingerprint") || strings.Contains(w.Body.String(), "ready") {
				t.Fatalf("Stop response leaked journal fields or invented readiness: %s", w.Body.String())
			}
		})
	}

	tests := []struct {
		err    error
		status int
		code   string
	}{
		{lifecycle.ErrInvalidRequest, 400, "invalid_request"},
		{journal.ErrRuntimeIdentityMismatch, 409, "runtime_identity_mismatch"},
		{journal.ErrStaleRuntimeGeneration, 409, "stale_runtime_generation"},
		{journal.ErrOperationIDConflict, 409, "operation_id_conflict"},
		{journal.ErrOperationStateConflict, 409, "operation_state_conflict"},
		{lifecycle.ErrPersistenceUnavailable, 503, "operation_persistence_unavailable"},
		{errors.Join(lifecycle.ErrPersistenceUnavailable, journal.ErrOperationStateConflict), 503, "operation_persistence_unavailable"},
		{errors.Join(lifecycle.ErrExecutionFailed, journal.ErrOperationStateConflict), 500, "internal_error"},
		{errors.New("secret-pid-or-signal"), 500, "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.err.Error(), func(t *testing.T) {
			h := stopHandler(t, stopFunc(func(context.Context, lifecycle.StopRequest) (journal.Operation, error) {
				return journal.Operation{}, test.err
			}))
			w := submitStop(t, h, validStopBody)
			if w.Code != test.status {
				t.Fatalf("error response = %d, %s", w.Code, w.Body.String())
			}
			assertErrorCode(t, w, test.code)
			if strings.Contains(w.Body.String(), "secret-pid-or-signal") {
				t.Fatal("raw Stop execution error escaped to HTTP")
			}
		})
	}
}

type stopFunc func(context.Context, lifecycle.StopRequest) (journal.Operation, error)

func (f stopFunc) Stop(ctx context.Context, request lifecycle.StopRequest) (journal.Operation, error) {
	return f(ctx, request)
}

type lifecycleFunc struct {
	start startFunc
	stop  stopFunc
}

func (f lifecycleFunc) Start(ctx context.Context, request lifecycle.StartRequest) (journal.Operation, error) {
	return f.start(ctx, request)
}

func (f lifecycleFunc) Stop(ctx context.Context, request lifecycle.StopRequest) (journal.Operation, error) {
	return f.stop(ctx, request)
}

func stopHandler(t *testing.T, executor StopExecutor) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken, Stop: executor})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func submitStop(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, stopPath, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}
