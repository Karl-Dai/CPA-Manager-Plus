// Package lifecycle orchestrates Supervisor-private, durable CPA mutations.
package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"unicode/utf8"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

var (
	ErrInvalidRequest         = errors.New("invalid Start request")
	ErrPersistenceUnavailable = errors.New("operation persistence unavailable")
	ErrExecutionFailed        = errors.New("Start execution failed")
)

// StartRequest has an empty typed payload. Execution inputs belong exclusively
// to Supervisor-local configuration, never to the HTTP caller.
type StartRequest struct {
	OperationID               string `json:"operationId"`
	ExpectedRuntimeIdentity   string `json:"expectedRuntimeIdentity"`
	ExpectedRuntimeGeneration uint64 `json:"expectedRuntimeGeneration"`
}

func (r StartRequest) Validate() error {
	if r.OperationID == "" || !utf8.ValidString(r.OperationID) || len(r.OperationID) > 128 ||
		strings.TrimSpace(r.ExpectedRuntimeIdentity) == "" || !utf8.ValidString(r.ExpectedRuntimeIdentity) ||
		r.ExpectedRuntimeGeneration == 0 {
		return ErrInvalidRequest
	}
	return nil
}

type operationJournal interface {
	Resolve(context.Context, journal.Authority, journal.Intent) (journal.Operation, bool, error)
	Begin(context.Context, journal.Authority, journal.Intent) (journal.Operation, bool, error)
	MarkRunning(context.Context, string, string) (journal.Operation, error)
	Complete(context.Context, string, string, journal.State, string) (journal.Operation, error)
	Close() error
}

type process interface {
	Observe() cpaprocess.Observation
	Start(context.Context, cpaprocess.StartSpec) (cpaprocess.Observation, error)
}

// Starter serializes submissions for one Supervisor incarnation. Keep one
// Starter and one child manager: the lock covers resolve, precondition and all
// execution evidence, while cpaprocess remains the final ownership fence.
type Starter struct {
	mu         sync.Mutex
	authority  journal.Authority
	journal    operationJournal
	process    process
	executable string
	closed     bool
}

func NewStarter(authority journal.Authority, store operationJournal, child process, executable string) (*Starter, error) {
	if strings.TrimSpace(authority.RuntimeIdentity) == "" || authority.RuntimeGeneration == 0 || store == nil || child == nil {
		return nil, errors.New("Start requires Supervisor authority, journal and child manager")
	}
	if strings.TrimSpace(executable) == "" || strings.ContainsRune(executable, '\x00') {
		return nil, errors.New("Start requires a local CPA executable without NUL")
	}
	return &Starter{authority: authority, journal: store, process: child, executable: executable}, nil
}

func (s *Starter) Start(ctx context.Context, request StartRequest) (journal.Operation, error) {
	if err := request.Validate(); err != nil {
		return journal.Operation{}, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return journal.Operation{}, err
	}
	if s.closed {
		return journal.Operation{}, ErrPersistenceUnavailable
	}
	intent := journal.Intent{
		OperationID:               request.OperationID,
		OperationType:             "start",
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		RequestFingerprint:        sha256.Sum256([]byte("runtime.start/v1:{}")),
	}
	operation, found, err := s.journal.Resolve(ctx, s.authority, intent)
	if err != nil {
		return journal.Operation{}, submissionError(err)
	}
	if found {
		// Retained accepted/running evidence is replayed, never resumed here.
		return operation, nil
	}
	switch s.process.Observe().State {
	case cpaprocess.StateNotStarted, cpaprocess.StateExited:
		// cpaprocess only publishes exited after a confirmed Wait/reap.
	default:
		return journal.Operation{}, journal.ErrOperationStateConflict
	}
	operation, created, err := s.journal.Begin(ctx, s.authority, intent)
	if err != nil {
		return journal.Operation{}, submissionError(err)
	}
	if !created {
		return operation, nil
	}

	// Durable acceptance transfers execution to the Supervisor. Request
	// cancellation/deadlines/values no longer control the operation or child.
	// This remains synchronous; each journal write has its own bounded retry.
	executionCtx := context.Background()
	operation, err = s.journal.MarkRunning(executionCtx, s.authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		return journal.Operation{}, fmt.Errorf("%w: %w", ErrPersistenceUnavailable, err)
	}
	_, spawnErr := s.process.Start(executionCtx, cpaprocess.StartSpec{Executable: s.executable})
	state, failureCode := journal.StateSucceeded, ""
	if spawnErr != nil {
		state, failureCode = journal.StateFailed, "process_start_failed"
	}
	result, err := s.journal.Complete(executionCtx, s.authority.RuntimeIdentity, intent.OperationID, state, failureCode)
	if err != nil {
		// Spawn may already have succeeded. Never retry it or kill the child
		// to compensate for missing terminal evidence.
		return operation, fmt.Errorf("%w: record result: %w", ErrExecutionFailed, err)
	}
	if spawnErr != nil {
		return result, fmt.Errorf("%w: %w", ErrExecutionFailed, spawnErr)
	}
	// Success means OS spawn and ownership publication, not Runtime readiness.
	return result, nil
}

func submissionError(err error) error {
	switch {
	case errors.Is(err, journal.ErrRuntimeIdentityMismatch),
		errors.Is(err, journal.ErrStaleRuntimeGeneration),
		errors.Is(err, journal.ErrOperationIDConflict):
		return err
	default:
		return fmt.Errorf("%w: %w", ErrPersistenceUnavailable, err)
	}
}

// Close drains any synchronous execution before closing the startup-owned
// journal, including after HTTP shutdown forcibly disconnects a caller. It
// rejects later submissions and does not stop the CPA child.
func (s *Starter) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	return s.journal.Close()
}
