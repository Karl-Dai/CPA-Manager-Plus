package protocol

import (
	"context"
	"net/http"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
)

const (
	// CapabilityStop means typed Stop submission is supported, not that a
	// running child is currently available to terminate.
	CapabilityStop = "stop"
	stopPath       = "/v1/runtime/operations/stop"
)

type StopExecutor interface {
	Stop(context.Context, lifecycle.StopRequest) (journal.Operation, error)
}

func (h *handler) submitStop(w http.ResponseWriter, r *http.Request) {
	envelope, err := decodeMutationRequest(w, r)
	if err != nil {
		writeError(w, http.StatusBadRequest, "invalid_request", "invalid Stop request")
		return
	}
	if h.stop == nil {
		writeError(w, http.StatusBadRequest, "unsupported_operation", "Stop is not configured")
		return
	}
	operation, err := h.stop.Stop(r.Context(), lifecycle.StopRequest(envelope))
	if err != nil {
		writeMutationError(w, err, "Stop")
		return
	}
	writeOperationResponse(w, operation, "Stop")
}
