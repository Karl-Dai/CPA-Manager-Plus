// Package lifecycle orchestrates Supervisor-private, durable CPA mutations.
package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

var (
	ErrInvalidRequest         = errors.New("invalid lifecycle request")
	ErrPersistenceUnavailable = errors.New("operation persistence unavailable")
	ErrExecutionFailed        = errors.New("lifecycle execution failed")
)

// StartRequest has an empty typed payload. Execution inputs belong exclusively
// to Supervisor-local configuration, never to the HTTP caller.
type StartRequest struct {
	OperationID               string `json:"operationId"`
	ExpectedRuntimeIdentity   string `json:"expectedRuntimeIdentity"`
	ExpectedRuntimeGeneration uint64 `json:"expectedRuntimeGeneration"`
}

func (r StartRequest) Validate() error {
	return validateRequest(r.OperationID, r.ExpectedRuntimeIdentity, r.ExpectedRuntimeGeneration)
}

// StopRequest has an empty typed payload. The owned child target and hard
// termination policy belong exclusively to the Supervisor.
type StopRequest struct {
	OperationID               string `json:"operationId"`
	ExpectedRuntimeIdentity   string `json:"expectedRuntimeIdentity"`
	ExpectedRuntimeGeneration uint64 `json:"expectedRuntimeGeneration"`
}

func (r StopRequest) Validate() error {
	return validateRequest(r.OperationID, r.ExpectedRuntimeIdentity, r.ExpectedRuntimeGeneration)
}

func validateRequest(operationID, expectedIdentity string, expectedGeneration uint64) error {
	if operationID == "" || !utf8.ValidString(operationID) || len(operationID) > 128 ||
		strings.TrimSpace(expectedIdentity) == "" || !utf8.ValidString(expectedIdentity) || expectedGeneration == 0 {
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
	PrepareStop() (cpaprocess.StopTarget, error)
}

// Executor owns the shared Start/Stop mutation serialization and resources for
// one Supervisor incarnation. The lock covers resolve, precondition and all
// execution evidence, while cpaprocess remains the final ownership fence.
type Executor struct {
	mu         sync.Mutex
	authority  journal.Authority
	journal    operationJournal
	process    process
	executable string
	closed     atomic.Bool
	closeOnce  sync.Once
	closeErr   error
}

func NewExecutor(authority journal.Authority, store operationJournal, child process, executable string) (*Executor, error) {
	if strings.TrimSpace(authority.RuntimeIdentity) == "" || authority.RuntimeGeneration == 0 || store == nil || child == nil {
		return nil, errors.New("lifecycle mutations require Supervisor authority, journal and child manager")
	}
	if strings.TrimSpace(executable) == "" || strings.ContainsRune(executable, '\x00') {
		return nil, errors.New("lifecycle mutations require a local CPA executable without NUL")
	}
	return &Executor{authority: authority, journal: store, process: child, executable: executable}, nil
}

func (e *Executor) Start(ctx context.Context, request StartRequest) (journal.Operation, error) {
	if err := request.Validate(); err != nil {
		return journal.Operation{}, err
	}
	if e.closed.Load() {
		return journal.Operation{}, ErrPersistenceUnavailable
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return journal.Operation{}, err
	}
	if e.closed.Load() {
		return journal.Operation{}, ErrPersistenceUnavailable
	}
	intent := journal.Intent{
		OperationID:               request.OperationID,
		OperationType:             "start",
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		RequestFingerprint:        sha256.Sum256([]byte("runtime.start/v1:{}")),
	}
	operation, found, err := e.journal.Resolve(ctx, e.authority, intent)
	if err != nil {
		return journal.Operation{}, submissionError(err)
	}
	if found {
		// Retained accepted/running evidence is replayed, never resumed here.
		return operation, nil
	}
	switch e.process.Observe().State {
	case cpaprocess.StateNotStarted, cpaprocess.StateExited:
		// cpaprocess only publishes exited after a confirmed Wait/reap.
	default:
		return journal.Operation{}, journal.ErrOperationStateConflict
	}
	operation, created, err := e.journal.Begin(ctx, e.authority, intent)
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
	operation, err = e.journal.MarkRunning(executionCtx, e.authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		return journal.Operation{}, fmt.Errorf("%w: %w", ErrPersistenceUnavailable, err)
	}
	_, spawnErr := e.process.Start(executionCtx, cpaprocess.StartSpec{Executable: e.executable})
	state, failureCode := journal.StateSucceeded, ""
	if spawnErr != nil {
		state, failureCode = journal.StateFailed, "process_start_failed"
	}
	result, err := e.journal.Complete(executionCtx, e.authority.RuntimeIdentity, intent.OperationID, state, failureCode)
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

// CloseAdmission prevents new lifecycle mutations from entering the shared
// execution gate. Submissions already queued on the gate recheck this state
// before resolving or recording durable intent.
func (e *Executor) CloseAdmission() {
	e.closed.Store(true)
}

// Close drains any synchronous execution before closing the startup-owned
// journal, including after HTTP shutdown forcibly disconnects a caller. It
// rejects later submissions and does not stop the CPA child.
func (e *Executor) Close() error {
	e.CloseAdmission()
	e.closeOnce.Do(func() {
		e.mu.Lock()
		defer e.mu.Unlock()
		e.closeErr = e.journal.Close()
	})
	return e.closeErr
}
