package protocol

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const validPrepareUpdateBody = `{"operationId":"op","expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":7,"expectedActiveArtifactId":"sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","targetVersion":"7.3.3"}`

type prepareUpdateFunc func(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error)

func (f prepareUpdateFunc) PrepareUpdate(ctx context.Context, request lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
	return f(ctx, request)
}

func TestPrepareUpdateAuthenticatesBeforeDecodeAndUsesTypedContract(t *testing.T) {
	called := false
	h := prepareUpdateHandler(t, prepareUpdateFunc(func(_ context.Context, request lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
		called = true
		if request.OperationID != "op" || request.ExpectedRuntimeIdentity != "runtime-01" || request.ExpectedRuntimeGeneration != 7 ||
			request.ExpectedActiveArtifactID != "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" ||
			request.TargetVersion != "7.3.3" {
			t.Fatalf("typed request = %+v", request)
		}
		return journal.Operation{
			OperationID: "op", OperationType: "prepare_update", RuntimeIdentity: "runtime-01",
			RuntimeGeneration: 7, State: journal.StateSucceeded,
		}, nil
	}))
	unauthorized := httptest.NewRequest(http.MethodPost, prepareUpdatePath, strings.NewReader(validPrepareUpdateBody))
	response := httptest.NewRecorder()
	h.ServeHTTP(response, unauthorized)
	if response.Code != http.StatusUnauthorized || called {
		t.Fatalf("unauthorized response=%d called=%v", response.Code, called)
	}
	response = submitPrepareUpdate(t, h, validPrepareUpdateBody)
	if response.Code != http.StatusOK || !called {
		t.Fatalf("prepare response=%d body=%s called=%v", response.Code, response.Body.String(), called)
	}
	var operation operationResponse
	decodeResponse(t, response, &operation)
	if operation.OperationType != "prepare_update" || operation.State != journal.StateSucceeded {
		t.Fatalf("operation = %+v", operation)
	}
}

func TestPrepareUpdateStrictValidationRejectsAliasesAndDownloadAuthority(t *testing.T) {
	h := prepareUpdateHandler(t, prepareUpdateFunc(func(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
		t.Fatal("invalid request reached executor")
		return journal.Operation{}, nil
	}))
	tests := map[string]string{
		"empty": "", "null": "null", "array": "[]", "missing target": strings.Replace(validPrepareUpdateBody, `,"targetVersion":"7.3.3"`, "", 1),
		"latest":            strings.Replace(validPrepareUpdateBody, `"7.3.3"`, `"latest"`, 1),
		"tag alias":         strings.Replace(validPrepareUpdateBody, `"7.3.3"`, `"v7.3.3"`, 1),
		"URL target":        strings.Replace(validPrepareUpdateBody, `"7.3.3"`, `"https://example.test/a"`, 1),
		"path target":       strings.Replace(validPrepareUpdateBody, `"7.3.3"`, `"../7.3.3"`, 1),
		"whitespace target": strings.Replace(validPrepareUpdateBody, `"7.3.3"`, `" 7.3.3"`, 1),
		"uppercase digest":  strings.Replace(validPrepareUpdateBody, strings.Repeat("a", 64), strings.Repeat("A", 64), 1),
		"duplicate target":  strings.Replace(validPrepareUpdateBody, `"targetVersion":"7.3.3"`, `"targetVersion":"7.3.3","targetVersion":"7.3.4"`, 1),
		"trailing":          validPrepareUpdateBody + `{}`,
	}
	for _, field := range []string{"url", "repository", "assetName", "archiveDigest", "executable", "path", "mirror", "action", "params"} {
		tests[field] = strings.TrimSuffix(validPrepareUpdateBody, "}") + `,"` + field + `":"forbidden"}`
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			response := submitPrepareUpdate(t, h, body)
			if response.Code != http.StatusBadRequest {
				t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
			}
			assertErrorCode(t, response, "invalid_request")
		})
	}
}

func TestPrepareUpdateCapabilityIsAdditive(t *testing.T) {
	h := prepareUpdateHandler(t, prepareUpdateFunc(func(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
		return journal.Operation{}, nil
	}))
	for _, path := range []string{handshakePath, statusPath} {
		var response statusResponse
		decodeResponse(t, request(t, h, http.MethodGet, path, testRuntimeToken), &response)
		if !reflect.DeepEqual(response.Capabilities, []string{CapabilityPrepareUpdate}) {
			t.Fatalf("%s capabilities = %v", path, response.Capabilities)
		}
	}
	readOnly := newTestHandler(t)
	response := submitPrepareUpdate(t, readOnly, validPrepareUpdateBody)
	if response.Code != http.StatusBadRequest {
		t.Fatalf("unsupported response = %d", response.Code)
	}
	assertErrorCode(t, response, "unsupported_operation")
}

func TestPrepareUpdateMapsStableFenceAndReleaseErrors(t *testing.T) {
	tests := []struct {
		err    error
		status int
		code   string
	}{
		{err: lifecycle.ErrActiveArtifactUnavailable, status: http.StatusConflict, code: "active_artifact_unavailable"},
		{err: lifecycle.ErrActiveArtifactMismatch, status: http.StatusConflict, code: "active_artifact_mismatch"},
		{err: lifecycle.ErrUnsupportedStaging, status: http.StatusBadRequest, code: "unsupported_staging_platform"},
		{err: lifecycle.ErrReleaseMetadataInvalid, status: http.StatusBadGateway, code: "release_metadata_invalid"},
	}
	for _, test := range tests {
		h := prepareUpdateHandler(t, prepareUpdateFunc(func(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
			return journal.Operation{}, test.err
		}))
		response := submitPrepareUpdate(t, h, validPrepareUpdateBody)
		if response.Code != test.status {
			t.Fatalf("error %v status=%d body=%s", test.err, response.Code, response.Body.String())
		}
		assertErrorCode(t, response, test.code)
	}
}

func TestPrepareUpdateFailedReplayUsesCommonOperationResponse(t *testing.T) {
	h := prepareUpdateHandler(t, prepareUpdateFunc(func(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error) {
		return journal.Operation{
			OperationID: "op", OperationType: "prepare_update", RuntimeIdentity: "runtime-01",
			RuntimeGeneration: 6, State: journal.StateFailed, FailureCode: "release_asset_invalid",
		}, nil
	}))
	response := submitPrepareUpdate(t, h, validPrepareUpdateBody)
	if response.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", response.Code, response.Body.String())
	}
	var got operationResponse
	decodeResponse(t, response, &got)
	if got.Error == nil || got.Error.Code != "release_asset_invalid" {
		t.Fatalf("operation response = %+v", got)
	}
}

func prepareUpdateHandler(t *testing.T, executor PrepareUpdateExecutor) http.Handler {
	t.Helper()
	h, err := NewHandler(Config{
		RuntimeIdentity: "runtime-01", RuntimeGeneration: 7, Token: testRuntimeToken, PrepareUpdate: executor,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func submitPrepareUpdate(t *testing.T, h http.Handler, body string) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequest(http.MethodPost, prepareUpdatePath, strings.NewReader(body))
	request.Header.Set("Authorization", "Bearer "+testRuntimeToken)
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	h.ServeHTTP(response, request)
	return response
}
