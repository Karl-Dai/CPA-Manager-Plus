package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const (
	// CapabilityStart means typed Start submission is supported, not that the
	// current child state permits it or that the Runtime is ready.
	CapabilityStart = "start"
	startPath       = "/v1/runtime/operations/start"
	maxStartBody    = 16 << 10
)

type StartExecutor interface {
	Start(context.Context, lifecycle.StartRequest) (journal.Operation, error)
}

type operationResponse struct {
	OperationID       string         `json:"operationId"`
	OperationType     string         `json:"operationType"`
	RuntimeIdentity   string         `json:"runtimeIdentity"`
	RuntimeGeneration uint64         `json:"runtimeGeneration"`
	State             journal.State  `json:"state"`
	Error             *protocolError `json:"error,omitempty"`
}

func (h *handler) submitStart(w http.ResponseWriter, r *http.Request) {
	request, err := decodeStartRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid Start request")
		return
	}
	if h.start == nil {
		writeError(w, http.StatusBadRequest, "unsupported_operation", "Start is not configured")
		return
	}
	operation, err := h.start.Start(r.Context(), request)
	if err != nil {
		writeStartError(w, err)
		return
	}
	response := operationResponse{
		OperationID:       operation.OperationID,
		OperationType:     operation.OperationType,
		RuntimeIdentity:   operation.RuntimeIdentity,
		RuntimeGeneration: operation.RuntimeGeneration,
		State:             operation.State,
	}
	if operation.FailureCode != "" {
		response.Error = &protocolError{Code: operation.FailureCode, Message: "Start execution failed"}
	}
	writeJSON(w, http.StatusOK, response)
}

func decodeStartRequest(w http.ResponseWriter, r *http.Request) (lifecycle.StartRequest, error) {
	var request lifecycle.StartRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxStartBody))
	if err != nil || !utf8.Valid(body) {
		return request, lifecycle.ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return request, lifecycle.ErrInvalidRequest
	}
	// Decode this one typed envelope explicitly so unknown, case-aliased and
	// duplicate fields cannot change the meaning of a privileged submission.
	seen := make(map[string]bool, 3)
	for decoder.More() {
		token, err := decoder.Token()
		field, ok := token.(string)
		if err != nil || !ok || seen[field] {
			return request, lifecycle.ErrInvalidRequest
		}
		seen[field] = true
		switch field {
		case "operationId":
			err = decoder.Decode(&request.OperationID)
		case "expectedRuntimeIdentity":
			err = decoder.Decode(&request.ExpectedRuntimeIdentity)
		case "expectedRuntimeGeneration":
			err = decoder.Decode(&request.ExpectedRuntimeGeneration)
		default:
			return request, lifecycle.ErrInvalidRequest
		}
		if err != nil {
			return request, lifecycle.ErrInvalidRequest
		}
	}
	closing, err := decoder.Token()
	if err != nil || closing != json.Delim('}') {
		return request, lifecycle.ErrInvalidRequest
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return request, lifecycle.ErrInvalidRequest
	}
	return request, request.Validate()
}

func writeStartError(w http.ResponseWriter, err error) {
	code, message, status := "internal_error", "Start execution failed", http.StatusInternalServerError
	switch {
	case errors.Is(err, lifecycle.ErrExecutionFailed):
		// Post-spawn/result failures remain internal errors even when their
		// cause is a journal state conflict; retained evidence owns replay.
	case errors.Is(err, lifecycle.ErrPersistenceUnavailable):
		code, message, status = "operation_persistence_unavailable", "operation persistence is unavailable", http.StatusServiceUnavailable
	case errors.Is(err, lifecycle.ErrInvalidRequest):
		code, message, status = "invalid_request", "invalid Start request", http.StatusBadRequest
	case errors.Is(err, journal.ErrRuntimeIdentityMismatch):
		code, message, status = "runtime_identity_mismatch", "runtime identity does not match", http.StatusConflict
	case errors.Is(err, journal.ErrStaleRuntimeGeneration):
		code, message, status = "stale_runtime_generation", "runtime generation is stale", http.StatusConflict
	case errors.Is(err, journal.ErrOperationIDConflict):
		code, message, status = "operation_id_conflict", "operation ID names a different request", http.StatusConflict
	case errors.Is(err, journal.ErrOperationStateConflict):
		code, message, status = "operation_state_conflict", "CPA process ownership does not permit Start", http.StatusConflict
	}
	writeError(w, status, code, message)
}
