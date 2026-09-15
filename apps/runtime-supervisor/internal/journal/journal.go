// Package journal owns the Runtime Supervisor's private durable operation
// records. It deliberately has no dependency on Manager storage or product
// configuration.
package journal

import (
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxOperationIDBytes = 128
	maxStableCodeBytes  = 128
)

var (
	ErrRuntimeIdentityMismatch = errors.New("runtime identity mismatch")
	ErrStaleRuntimeGeneration  = errors.New("stale runtime generation")
	ErrOperationIDConflict     = errors.New("operation ID conflict")
	ErrOperationNotFound       = errors.New("operation not found")
	ErrOperationStateConflict  = errors.New("operation state conflict")
)

// State is the frozen Runtime Protocol v1 operation state vocabulary.
type State string

const (
	StateAccepted  State = "accepted"
	StateRunning   State = "running"
	StateSucceeded State = "succeeded"
	StateFailed    State = "failed"
)

func (state State) terminal() bool {
	return state == StateSucceeded || state == StateFailed
}

// RequestFingerprint identifies an operation-specific typed payload. The
// operation type is stored and compared separately by Store.Begin.
type RequestFingerprint [sha256.Size]byte

// FingerprintPayload hashes an operation-specific canonical payload. Callers
// must build those bytes deterministically from the typed request; journal does
// not accept maps or raw request bodies and never persists canonicalPayload.
func FingerprintPayload(canonicalPayload []byte) RequestFingerprint {
	return sha256.Sum256(canonicalPayload)
}

// Authority is the current Supervisor process incarnation authority. It is
// supplied for each Begin call and is never restored from journal storage.
type Authority struct {
	RuntimeIdentity   string
	RuntimeGeneration uint64
}

// Intent contains only durable operation identity. Secret-bearing request
// material must be reduced to RequestFingerprint before reaching this type.
type Intent struct {
	OperationID               string
	OperationType             string
	ExpectedRuntimeIdentity   string
	ExpectedRuntimeGeneration uint64
	RequestFingerprint        RequestFingerprint
}

// Operation is one durable Supervisor execution record.
type Operation struct {
	OperationID        string
	OperationType      string
	RuntimeIdentity    string
	RuntimeGeneration  uint64
	RequestFingerprint RequestFingerprint
	State              State
	CreatedAt          time.Time
	UpdatedAt          time.Time
	CompletedAt        *time.Time
	FailureCode        string
	TombstonedAt       *time.Time
}

func validateAuthority(authority Authority) error {
	if strings.TrimSpace(authority.RuntimeIdentity) == "" {
		return errors.New("runtime identity is required")
	}
	if authority.RuntimeGeneration == 0 {
		return errors.New("runtime generation must be greater than zero")
	}
	return nil
}

func validateIntent(intent Intent) error {
	if intent.OperationID == "" {
		return errors.New("operation ID is required")
	}
	if !utf8.ValidString(intent.OperationID) {
		return errors.New("operation ID must be valid UTF-8")
	}
	if len([]byte(intent.OperationID)) > maxOperationIDBytes {
		return fmt.Errorf("operation ID exceeds %d UTF-8 bytes", maxOperationIDBytes)
	}
	if !validStableCode(intent.OperationType) {
		return errors.New("operation type must be a lower_snake_case stable code")
	}
	if strings.TrimSpace(intent.ExpectedRuntimeIdentity) == "" {
		return errors.New("expected runtime identity is required")
	}
	if intent.ExpectedRuntimeGeneration == 0 {
		return errors.New("expected runtime generation must be greater than zero")
	}
	if intent.RequestFingerprint == (RequestFingerprint{}) {
		return errors.New("request fingerprint is required")
	}
	return nil
}

func validateFailureCode(state State, failureCode string) error {
	switch state {
	case StateSucceeded:
		if failureCode != "" {
			return errors.New("succeeded operation must not have a failure code")
		}
	case StateFailed:
		if !validStableCode(failureCode) {
			return errors.New("failed operation requires a lower_snake_case failure code")
		}
	default:
		return fmt.Errorf("terminal state must be %q or %q", StateSucceeded, StateFailed)
	}
	return nil
}

func validStableCode(value string) bool {
	if value == "" || len(value) > maxStableCodeBytes {
		return false
	}
	for index, char := range []byte(value) {
		if char >= 'a' && char <= 'z' {
			continue
		}
		if index > 0 && (char == '_' || char >= '0' && char <= '9') {
			continue
		}
		return false
	}
	return true
}
