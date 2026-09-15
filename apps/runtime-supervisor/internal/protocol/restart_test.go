package protocol

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const validRestartBody = `{"operationId":"op","expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":7}`

func TestRestartAuthenticatesBeforeReadingBodyOrCallingExecutor(t *testing.T) {
	h := restartHandler(t, restartFunc(func(context.Context, lifecycle.RestartRequest) (journal.Operation, error) {
		t.Fatal("unauthorized request reached Restart execution boundary")
		return journal.Operation{}, nil
	}))
	req := httptest.NewRequest(http.MethodPost, restartPath, nil)
	req.Body = unreadableBody{t}
	req.Header.Set("Authorization", "Bearer wrong-token")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || w.Header().Get("WWW-Authenticate") != "Bearer" {
		t.Fatalf("unauthorized response: %d, %s", w.Code, w.Body.String())
	}
	assertErrorCode(t, w, "unauthorized")
}

func TestRestartStrictRequestValidationRejectsCallerProcessAuthority(t *testing.T) {
	h := restartHandler(t, restartFunc(func(context.Context, lifecycle.RestartRequest) (journal.Operation, error) {
		t.Fatal("invalid request reached journal/process executor")
		return journal.Operation{}, nil
	}))
	tests := map[string]string{
		"empty": "", "null": "null", "array": "[]", "string": `"request"`,
		"empty object": `{}`, "truncated": `{"operationId":`,
		"empty ID":           strings.Replace(validRestartBody, `"op"`, `""`, 1),
		"long ID":            strings.Replace(validRestartBody, `"op"`, `"`+strings.Repeat("x", 129)+`"`, 1),
		"UTF-8 byte limit":   strings.Replace(validRestartBody, `"op"`, `"`+strings.Repeat("字", 43)+`"`, 1),
		"invalid UTF-8":      strings.Replace(validRestartBody, "op\"", string([]byte{0xff})+"\"", 1),
		"missing identity":   `{"operationId":"op","expectedRuntimeGeneration":7}`,
		"blank identity":     strings.Replace(validRestartBody, `"runtime-01"`, `"  "`, 1),
		"null identity":      strings.Replace(validRestartBody, `"runtime-01"`, `null`, 1),
		"missing generation": `{"operationId":"op","expectedRuntimeIdentity":"runtime-01"}`,
		"trailing object":    validRestartBody + `{}`, "trailing null": validRestartBody + `null`,
		"trailing garbage": validRestartBody + `x`,
		"oversized body":   strings.Repeat(" ", maxMutationBody) + validRestartBody,
		"case alias":       strings.Replace(validRestartBody, "operationId", "OperationId", 1),
		"duplicate ID":     strings.Replace(validRestartBody, `"operationId":"op"`, `"operationId":"op","operationId":"other"`, 1),
	}
	for _, generation := range []string{"0", "-1", "1.5", "18446744073709551616", `"7"`, "null", "true"} {
		tests["generation "+generation] = strings.Replace(validRestartBody, ":7", ":"+generation, 1)
	}
	for _, field := range []string{
		"pid", "signal", "force", "timeout", "grace", "executable", "argv", "args", "env", "cwd",
		"workingDirectory", "shell", "shellCommand", "action", "params", "stopOperationId", "startOperationId",
	} {
		tests[field] = strings.TrimSuffix(validRestartBody, "}") + `,"` + field + `":"not-allowed"}`
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			w := submitRestart(t, h, body)
			if w.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			assertErrorCode(t, w, "invalid_request")
		})
	}
}

func TestRestartPreservesOpaqueIDAndFullWidthGeneration(t *testing.T) {
	id := strings.Repeat("字", 42) + " a"
	h := restartHandler(t, restartFunc(func(_ context.Context, request lifecycle.RestartRequest) (journal.Operation, error) {
		if request.OperationID != id || request.ExpectedRuntimeGeneration != ^uint64(0) {
			t.Fatalf("typed request = %+v", request)
		}
		return journal.Operation{OperationID: id}, nil
	}))
	body := fmt.Sprintf(`{"operationId":%q,"expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":18446744073709551615}`, id)
	if w := submitRestart(t, h, body); w.Code != http.StatusOK {
		t.Fatalf("valid opaque request = %d, %s", w.Code, w.Body.String())
	}
}

func TestRestartReadOnlyModeIsUnsupported(t *testing.T) {
	h := newTestHandler(t)
	w := submitRestart(t, h, validRestartBody)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("read-only Restart = %d", w.Code)
	}
	assertErrorCode(t, w, "unsupported_operation")
	method := request(t, h, http.MethodGet, restartPath, testRuntimeToken)
	if method.Code != http.StatusMethodNotAllowed || method.Header().Get("Allow") != http.MethodPost {
		t.Fatalf("Restart method = %d, Allow %q", method.Code, method.Header().Get("Allow"))
	}
	if unknown := request(t, h, http.MethodPost, "/v1/runtime/operations/op", testRuntimeToken); unknown.Code != http.StatusNotFound {
		t.Fatalf("unknown operation endpoint = %d", unknown.Code)
	}
}

func TestRestartResponseAndErrorMapping(t *testing.T) {
	for _, state := range []journal.State{journal.StateAccepted, journal.StateRunning, journal.StateSucceeded, journal.StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			h := restartHandler(t, restartFunc(func(_ context.Context, request lifecycle.RestartRequest) (journal.Operation, error) {
				operation := journal.Operation{OperationID: request.OperationID, OperationType: "restart", RuntimeIdentity: "runtime-01", RuntimeGeneration: 6, State: state}
				if state == journal.StateFailed {
					operation.FailureCode = "process_restart_start_failed"
				}
				return operation, nil
			}))
			w := submitRestart(t, h, validRestartBody)
			if w.Code != http.StatusOK {
				t.Fatalf("Restart response = %d, %s", w.Code, w.Body.String())
			}
			var got operationResponse
			decodeResponse(t, w, &got)
			if got.OperationID != "op" || got.OperationType != "restart" || got.RuntimeIdentity != "runtime-01" ||
				got.RuntimeGeneration != 6 || got.State != state {
				t.Fatalf("operation response = %+v", got)
			}
			if state == journal.StateFailed && (got.Error == nil || got.Error.Code != "process_restart_start_failed") {
				t.Fatalf("replay lost Restart failure evidence: %+v", got)
			}
			if strings.Contains(w.Body.String(), "Fingerprint") || strings.Contains(w.Body.String(), "ready") {
				t.Fatalf("Restart response leaked journal fields or invented readiness: %s", w.Body.String())
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
		{errors.New("secret-process-detail"), 500, "internal_error"},
	}
	for _, test := range tests {
		t.Run(test.err.Error(), func(t *testing.T) {
			h := restartHandler(t, restartFunc(func(context.Context, lifecycle.RestartRequest) (journal.Operation, error) {
				return journal.Operation{}, test.err
			}))
			w := submitRestart(t, h, validRestartBody)
			if w.Code != test.status {
				t.Fatalf("error response = %d, %s", w.Code, w.Body.String())
			}
			assertErrorCode(t, w, test.code)
			if strings.Contains(w.Body.String(), "secret-process-detail") {
				t.Fatal("raw Restart execution error escaped to HTTP")
			}
		})
	}
}

type restartFunc func(context.Context, lifecycle.RestartRequest) (journal.Operation, error)

func (f restartFunc) Restart(ctx context.Context, request lifecycle.RestartRequest) (journal.Operation, error) {
	return f(ctx, request)
}

func restartHandler(t *testing.T, executor RestartExecutor) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken, Restart: executor})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func submitRestart(t *testing.T, handler http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, restartPath, strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}
