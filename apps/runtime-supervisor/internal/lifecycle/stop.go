package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

// Stop serializes with Start through the Executor gate. PrepareStop reserves
// the exact owned child before durable intent; no replacement child can cross
// that reservation while persistence is committed and termination completes.
func (e *Executor) Stop(ctx context.Context, request StopRequest) (journal.Operation, error) {
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
		OperationType:             "stop",
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		RequestFingerprint:        sha256.Sum256([]byte("runtime.stop/v1:{}")),
	}
	operation, found, err := e.journal.Resolve(ctx, e.authority, intent)
	if err != nil {
		// Resolve is read-only and cannot supersede existing recovery authority.
		return journal.Operation{}, submissionError(err)
	}
	if found {
		// Retained evidence owns replay. Never reserve or terminate a current
		// child for an accepted, running, succeeded or failed Stop retry.
		return operation, nil
	}
	target, err := e.process.PrepareStop()
	if err != nil {
		if errors.Is(err, cpaprocess.ErrStateConflict) {
			return journal.Operation{}, journal.ErrOperationStateConflict
		}
		return journal.Operation{}, fmt.Errorf("%w: prepare Stop: %w", ErrExecutionFailed, err)
	}
	defer target.Release()

	operation, created, err := e.journal.Begin(ctx, e.authority, intent)
	if err != nil {
		e.failRecoveryForAmbiguousBegin(err)
		return journal.Operation{}, submissionError(err)
	}
	if !created {
		return operation, nil
	}

	// Durable acceptance transfers execution to the Supervisor. The target is
	// already bound to one exact child; caller cancellation cannot replace it
	// or interrupt termination and confirmed Wait/reap.
	executionCtx := context.Background()
	operation, err = e.journal.MarkRunning(executionCtx, e.authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		e.disableRecovery(RecoveryStateManualIntervention)
		return journal.Operation{}, fmt.Errorf("%w: %w", ErrPersistenceUnavailable, err)
	}
	// Durable running evidence establishes this as an expected termination.
	// Disarm before touching the exact child so its Wait event cannot respawn.
	e.disableRecovery(RecoveryStateInactive)
	_, stopErr := target.Terminate(executionCtx)
	state, failureCode := journal.StateSucceeded, ""
	if stopErr != nil {
		state, failureCode = journal.StateFailed, "process_stop_failed"
	}
	result, err := e.journal.Complete(executionCtx, e.authority.RuntimeIdentity, intent.OperationID, state, failureCode)
	if err != nil {
		// The exact target may already be reaped. Retained running evidence
		// prevents replay from terminating a current or later child.
		e.disableRecovery(RecoveryStateManualIntervention)
		return operation, fmt.Errorf("%w: record result: %w", ErrExecutionFailed, err)
	}
	if stopErr != nil {
		return result, fmt.Errorf("%w: %w", ErrExecutionFailed, stopErr)
	}
	return result, nil
}
