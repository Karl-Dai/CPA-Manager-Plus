// Package protocol implements the authenticated Runtime Protocol v1 surface.
package protocol

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/readiness"
)

const (
	Version       = "v1"
	handshakePath = "/v1/runtime/handshake"
	statusPath    = "/v1/runtime/status"
)

type Config struct {
	RuntimeIdentity   string
	RuntimeGeneration uint64
	Token             string
	Start             StartExecutor
	Stop              StopExecutor
	Restart           RestartExecutor
	Status            StatusObserver
}

// StatusObserver supplies read-only availability facts after authentication.
// Handshake and mutation submission never call it.
type StatusObserver interface {
	Observe(context.Context) readiness.State
}

type handler struct {
	runtimeIdentity   string
	runtimeGeneration uint64
	tokenDigest       [sha256.Size]byte
	start             StartExecutor
	stop              StopExecutor
	restart           RestartExecutor
	status            StatusObserver
}

type handshakeResponse struct {
	ProtocolVersion   string   `json:"protocolVersion"`
	RuntimeIdentity   string   `json:"runtimeIdentity"`
	RuntimeGeneration uint64   `json:"runtimeGeneration"`
	Capabilities      []string `json:"capabilities"`
}

type statusResponse struct {
	ProtocolVersion    string   `json:"protocolVersion"`
	RuntimeIdentity    string   `json:"runtimeIdentity"`
	RuntimeGeneration  uint64   `json:"runtimeGeneration"`
	State              string   `json:"state"`
	CPAObservedVersion string   `json:"cpaObservedVersion"`
	Capabilities       []string `json:"capabilities"`
}

type errorResponse struct {
	Error protocolError `json:"error"`
}

type protocolError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func NewHandler(config Config) (http.Handler, error) {
	identity := strings.TrimSpace(config.RuntimeIdentity)
	if identity == "" {
		return nil, errors.New("runtime identity is required")
	}
	if config.RuntimeGeneration == 0 {
		return nil, errors.New("runtime generation must be greater than zero")
	}
	if strings.TrimSpace(config.Token) == "" {
		return nil, errors.New("runtime token is required")
	}
	if strings.IndexFunc(config.Token, unicode.IsSpace) >= 0 {
		return nil, errors.New("runtime token must not contain whitespace")
	}
	return &handler{
		runtimeIdentity:   identity,
		runtimeGeneration: config.RuntimeGeneration,
		tokenDigest:       sha256.Sum256([]byte(config.Token)),
		start:             config.Start,
		stop:              config.Stop,
		restart:           config.Restart,
		status:            config.Status,
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	method := http.MethodGet
	switch r.URL.Path {
	case handshakePath, statusPath:
	case startPath, stopPath, restartPath:
		method = http.MethodPost
	default:
		writeError(w, http.StatusNotFound, "not_found", "runtime endpoint not found")
		return
	}
	if r.Method != method {
		w.Header().Set("Allow", method)
		writeError(w, http.StatusMethodNotAllowed, "method_not_allowed", "method not allowed")
		return
	}
	if !h.authorized(r.Header.Get("Authorization")) {
		w.Header().Set("WWW-Authenticate", "Bearer")
		writeError(w, http.StatusUnauthorized, "unauthorized", "request is not authorized")
		return
	}

	switch r.URL.Path {
	case handshakePath:
		writeJSON(w, http.StatusOK, handshakeResponse{
			ProtocolVersion:   Version,
			RuntimeIdentity:   h.runtimeIdentity,
			RuntimeGeneration: h.runtimeGeneration,
			Capabilities:      h.capabilities(),
		})
	case statusPath:
		state := readiness.Unknown
		if h.status != nil {
			state = h.status.Observe(r.Context())
		}
		writeJSON(w, http.StatusOK, statusResponse{
			ProtocolVersion:    Version,
			RuntimeIdentity:    h.runtimeIdentity,
			RuntimeGeneration:  h.runtimeGeneration,
			State:              string(state),
			CPAObservedVersion: "",
			Capabilities:       h.capabilities(),
		})
	case startPath:
		h.submitStart(w, r)
	case stopPath:
		h.submitStop(w, r)
	case restartPath:
		h.submitRestart(w, r)
	}
}

func (h *handler) capabilities() []string {
	capabilities := make([]string, 0, 3)
	if h.start != nil {
		capabilities = append(capabilities, CapabilityStart)
	}
	if h.stop != nil {
		capabilities = append(capabilities, CapabilityStop)
	}
	if h.restart != nil {
		capabilities = append(capabilities, CapabilityRestart)
	}
	return capabilities
}

func (h *handler) authorized(authorization string) bool {
	fields := strings.Fields(authorization)
	if len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return false
	}
	providedDigest := sha256.Sum256([]byte(fields[1]))
	return subtle.ConstantTimeCompare(providedDigest[:], h.tokenDigest[:]) == 1
}

func writeError(w http.ResponseWriter, status int, code string, message string) {
	writeJSON(w, status, errorResponse{Error: protocolError{Code: code, Message: message}})
}

func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
