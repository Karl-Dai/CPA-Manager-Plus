package protocol

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/artifact"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const (
	CapabilityPrepareUpdate = "prepare_update"
	prepareUpdatePath       = "/v1/runtime/operations/prepare-update"
)

type PrepareUpdateExecutor interface {
	PrepareUpdate(context.Context, lifecycle.PrepareUpdateRequest) (journal.Operation, error)
}

type prepareUpdateRequest struct {
	OperationID               string      `json:"operationId"`
	ExpectedRuntimeIdentity   string      `json:"expectedRuntimeIdentity"`
	ExpectedRuntimeGeneration uint64      `json:"expectedRuntimeGeneration"`
	ExpectedActiveArtifactID  artifact.ID `json:"expectedActiveArtifactId"`
	TargetVersion             string      `json:"targetVersion"`
}

func (h *handler) submitPrepareUpdate(w http.ResponseWriter, r *http.Request) {
	request, err := decodePrepareUpdateRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid prepare-update request")
		return
	}
	if h.prepareUpdate == nil {
		writeError(w, http.StatusBadRequest, "unsupported_operation", "prepare-update is not configured")
		return
	}
	operation, err := h.prepareUpdate.PrepareUpdate(r.Context(), lifecycle.PrepareUpdateRequest{
		OperationID:               request.OperationID,
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		ExpectedActiveArtifactID:  request.ExpectedActiveArtifactID,
		TargetVersion:             request.TargetVersion,
	})
	if err != nil {
		writeMutationError(w, err, "prepare-update")
		return
	}
	writeOperationResponse(w, operation, "prepare-update")
}

func decodePrepareUpdateRequest(w http.ResponseWriter, r *http.Request) (prepareUpdateRequest, error) {
	var request prepareUpdateRequest
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxMutationBody))
	if err != nil || !utf8.Valid(body) {
		return request, lifecycle.ErrInvalidRequest
	}
	decoder := json.NewDecoder(bytes.NewReader(body))
	opening, err := decoder.Token()
	if err != nil || opening != json.Delim('{') {
		return request, lifecycle.ErrInvalidRequest
	}
	seen := make(map[string]bool, 5)
	for decoder.More() {
		token, tokenErr := decoder.Token()
		field, ok := token.(string)
		if tokenErr != nil || !ok || seen[field] {
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
		case "expectedActiveArtifactId":
			err = decoder.Decode(&request.ExpectedActiveArtifactID)
		case "targetVersion":
			err = decoder.Decode(&request.TargetVersion)
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
	typed := lifecycle.PrepareUpdateRequest{
		OperationID:               request.OperationID,
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		ExpectedActiveArtifactID:  request.ExpectedActiveArtifactID,
		TargetVersion:             request.TargetVersion,
	}
	if err := typed.Validate(); err != nil {
		return request, err
	}
	return request, nil
}
