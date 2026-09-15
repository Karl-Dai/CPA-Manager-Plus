package protocol

import (
	"context"
	"net/http"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const (
	// CapabilityRestart means typed Restart submission is supported, not that a
	// running child is currently available or its replacement will be ready.
	CapabilityRestart = "restart"
	restartPath       = "/v1/runtime/operations/restart"
)

type RestartExecutor interface {
	Restart(context.Context, lifecycle.RestartRequest) (journal.Operation, error)
}

func (h *handler) submitRestart(w http.ResponseWriter, r *http.Request) {
	envelope, err := decodeMutationRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid Restart request")
		return
	}
	if h.restart == nil {
		writeError(w, http.StatusBadRequest, "unsupported_operation", "Restart is not configured")
		return
	}
	operation, err := h.restart.Restart(r.Context(), lifecycle.RestartRequest(envelope))
	if err != nil {
		writeMutationError(w, err, "Restart")
		return
	}
	writeOperationResponse(w, operation, "Restart")
}
