// Package protocol implements the read-only Runtime Protocol v1 surface.
package protocol

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"unicode"
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
}

type handler struct {
	runtimeIdentity   string
	runtimeGeneration uint64
	tokenDigest       [sha256.Size]byte
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
	}, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case handshakePath, statusPath:
	default:
		writeError(w, http.StatusNotFound, "not_found", "runtime endpoint not found")
		return
	}
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
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
			Capabilities:      []string{},
		})
	case statusPath:
		writeJSON(w, http.StatusOK, statusResponse{
			ProtocolVersion:    Version,
			RuntimeIdentity:    h.runtimeIdentity,
			RuntimeGeneration:  h.runtimeGeneration,
			State:              "unknown",
			CPAObservedVersion: "",
			Capabilities:       []string{},
		})
	}
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
