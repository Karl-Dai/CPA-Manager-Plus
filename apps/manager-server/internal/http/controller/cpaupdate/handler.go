package cpaupdate

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/app"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/http/middleware"
	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/http/response"
)

const routePrefix = "/usage-service/runtime/updates"

type Handler struct {
	App *app.Context
}

func (h *Handler) Handle(w http.ResponseWriter, r *http.Request) {
	if !middleware.AuthorizePanel(w, r, h.App.AdminAuthService) {
		return
	}
	w.Header().Set("Cache-Control", "no-store")

	suffix := strings.TrimPrefix(r.URL.Path, routePrefix)
	if !((suffix == "" && r.Method == http.MethodGet) || (suffix == "/check" && r.Method == http.MethodPost)) {
		response.MethodNotAllowed(w)
		return
	}
	if !emptyRequestBody(w, r) {
		response.Error(w, http.StatusBadRequest, errors.New("request body must be empty"))
		return
	}
	service := h.App.CPAUpdateService
	if service == nil {
		response.Error(w, http.StatusServiceUnavailable, errors.New("CPA update state unavailable"))
		return
	}

	var (
		payload any
		err     error
	)
	if suffix == "" {
		payload, err = service.Status(r.Context())
	} else {
		payload, err = service.Check(r.Context())
	}
	if err != nil {
		response.Error(w, http.StatusServiceUnavailable, errors.New("CPA update state unavailable"))
		return
	}
	response.JSON(w, http.StatusOK, payload)
}

func emptyRequestBody(w http.ResponseWriter, r *http.Request) bool {
	data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 0))
	return err == nil && len(data) == 0
}
