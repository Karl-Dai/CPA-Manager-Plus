package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/artifact"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	runtimeupdate "github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/update"
)

var (
	ErrActiveArtifactUnavailable = errors.New("active artifact is unavailable")
	ErrActiveArtifactMismatch    = errors.New("active artifact does not match expected identity")
	ErrUnsupportedStaging        = errors.New("update staging is unsupported on this platform")
	ErrReleaseMetadataInvalid    = errors.New("official release metadata is unavailable or invalid")
)

type activeArtifactRefresher interface {
	Refresh() error
	Observation() *artifact.Observation
}

type updatePreparer interface {
	Resolve(context.Context, string) (runtimeupdate.Release, error)
	Stage(context.Context, runtimeupdate.Release) (runtimeupdate.Metadata, error)
}

// PrepareUpdateRequest contains only exact target policy and freshness fences.
// Release authority and all filesystem paths remain Supervisor-owned.
type PrepareUpdateRequest struct {
	OperationID               string
	ExpectedRuntimeIdentity   string
	ExpectedRuntimeGeneration uint64
	ExpectedActiveArtifactID  artifact.ID
	TargetVersion             string
}

func (r PrepareUpdateRequest) Validate() error {
	if err := validateRequest(r.OperationID, r.ExpectedRuntimeIdentity, r.ExpectedRuntimeGeneration); err != nil ||
		!r.ExpectedActiveArtifactID.IsValid() || runtimeupdate.ValidateVersion(r.TargetVersion) != nil {
		return ErrInvalidRequest
	}
	return nil
}

// EnablePrepareUpdate installs the Runtime16 dependencies before the executor
// is exposed. Unsupported builds leave this unset and omit the capability.
func (e *Executor) EnablePrepareUpdate(observer activeArtifactRefresher, preparer updatePreparer) error {
	if observer == nil || preparer == nil {
		return errors.New("prepare-update requires artifact observation and trusted staging")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.closed.Load() {
		return ErrPersistenceUnavailable
	}
	e.artifact = observer
	e.updates = preparer
	return nil
}

func (e *Executor) PrepareUpdate(ctx context.Context, request PrepareUpdateRequest) (journal.Operation, error) {
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
	if e.artifact == nil || e.updates == nil {
		return journal.Operation{}, ErrUnsupportedStaging
	}
	intent := journal.Intent{
		OperationID:               request.OperationID,
		OperationType:             "prepare_update",
		ExpectedRuntimeIdentity:   request.ExpectedRuntimeIdentity,
		ExpectedRuntimeGeneration: request.ExpectedRuntimeGeneration,
		RequestFingerprint: sha256.Sum256([]byte(
			"runtime.prepare_update/v1:{targetVersion:" + request.TargetVersion + "}",
		)),
	}
	operation, found, err := e.journal.Resolve(ctx, e.authority, intent)
	if err != nil {
		return journal.Operation{}, submissionError(err)
	}
	if found {
		return operation, nil
	}

	// Refresh the exact configured executable at the fence. Manifest/version
	// errors do not invalidate an otherwise exact digest observation.
	refreshErr := e.artifact.Refresh()
	active := e.artifact.Observation()
	if errors.Is(refreshErr, artifact.ErrExecutableUnavailable) || active == nil || !active.ArtifactID.IsValid() {
		return journal.Operation{}, ErrActiveArtifactUnavailable
	}
	if active.ArtifactID != request.ExpectedActiveArtifactID {
		return journal.Operation{}, ErrActiveArtifactMismatch
	}

	// Exact official release and supported-asset preconditions precede durable
	// intent. Archive download and all staging bytes happen only after running.
	release, err := e.updates.Resolve(ctx, request.TargetVersion)
	if err != nil {
		switch {
		case errors.Is(err, runtimeupdate.ErrUnsupportedPlatform):
			return journal.Operation{}, ErrUnsupportedStaging
		case errors.Is(err, runtimeupdate.ErrInvalidTargetVersion):
			return journal.Operation{}, ErrInvalidRequest
		default:
			return journal.Operation{}, fmt.Errorf("%w: %v", ErrReleaseMetadataInvalid, err)
		}
	}
	operation, created, err := e.journal.Begin(ctx, e.authority, intent)
	if err != nil {
		return journal.Operation{}, submissionError(err)
	}
	if !created {
		return operation, nil
	}

	executionCtx := context.Background()
	operation, err = e.journal.MarkRunning(executionCtx, e.authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		return journal.Operation{}, fmt.Errorf("%w: %w", ErrPersistenceUnavailable, err)
	}
	_, stageErr := e.updates.Stage(executionCtx, release)
	state, failureCode := journal.StateSucceeded, ""
	if stageErr != nil {
		state, failureCode = journal.StateFailed, "staging_failed"
		if errors.Is(stageErr, runtimeupdate.ErrArchiveInvalid) {
			failureCode = "release_asset_invalid"
		}
	}
	result, err := e.journal.Complete(executionCtx, e.authority.RuntimeIdentity, intent.OperationID, state, failureCode)
	if err != nil {
		return operation, fmt.Errorf("%w: record prepare-update result: %w", ErrExecutionFailed, err)
	}
	return result, nil
}
