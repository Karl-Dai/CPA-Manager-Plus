package journal

import (
	"context"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	modernsqlite "modernc.org/sqlite"
	sqlite3 "modernc.org/sqlite/lib"
)

const (
	schemaVersion      = 1
	defaultBusyTimeout = 5 * time.Second
	maximumBusyTimeout = 30 * time.Second
	busyRetryInterval  = 5 * time.Millisecond
)

const operationsSchema = `
create table if not exists operations (
	runtime_identity blob not null,
	operation_id blob not null,
	operation_type text not null,
	request_fingerprint blob not null
		check (typeof(request_fingerprint) = 'blob' and length(request_fingerprint) = 32),
	runtime_generation blob not null
		check (typeof(runtime_generation) = 'blob' and length(runtime_generation) = 8),
	state text not null
		check (state in ('accepted', 'running', 'succeeded', 'failed')),
	created_at_ms integer not null,
	updated_at_ms integer not null,
	completed_at_ms integer,
	failure_code text,
	tombstoned_at_ms integer,
	primary key (runtime_identity, operation_id),
	check (
		(state in ('accepted', 'running') and completed_at_ms is null and failure_code is null and tombstoned_at_ms is null)
		or (state = 'succeeded' and completed_at_ms is not null and failure_code is null)
		or (state = 'failed' and completed_at_ms is not null and failure_code is not null)
	),
	check (tombstoned_at_ms is null or state in ('succeeded', 'failed'))
) strict;
`

// Options controls bounded local SQLite behavior.
type Options struct {
	BusyTimeout time.Duration
}

// Store is a Supervisor-private SQLite operation journal.
type Store struct {
	db          *sql.DB
	busyTimeout time.Duration
}

// Open opens path and initializes the journal schema transactionally.
func Open(ctx context.Context, path string, options Options) (*Store, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(path) == "" {
		return nil, errors.New("journal path is required")
	}
	timeout := options.BusyTimeout
	if timeout == 0 {
		timeout = defaultBusyTimeout
	}
	if timeout < time.Millisecond || timeout > maximumBusyTimeout {
		return nil, fmt.Errorf("busy timeout must be between 1ms and %s", maximumBusyTimeout)
	}
	absolutePath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("resolve journal path: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(absolutePath), 0o700); err != nil {
		return nil, fmt.Errorf("create journal directory: %w", err)
	}
	db, err := sql.Open("sqlite", dataSourceName(absolutePath))
	if err != nil {
		return nil, fmt.Errorf("open journal: %w", err)
	}
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	store := &Store{db: db, busyTimeout: timeout}
	if err := store.initialize(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	if err := os.Chmod(absolutePath, 0o600); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("restrict journal permissions: %w", err)
	}
	return store, nil
}

func dataSourceName(path string) string {
	uriPath := filepath.ToSlash(path)
	if !strings.HasPrefix(uriPath, "/") {
		uriPath = "/" + uriPath
	}
	dsn := &url.URL{Scheme: "file", Path: uriPath}
	query := dsn.Query()
	query.Add("_txlock", "immediate")
	// A driver-level busy timeout blocks inside SQLite and cannot promptly
	// observe context cancellation. Store.beginTransaction implements the
	// bounded busy budget with cancellable retries instead.
	query.Add("_pragma", "busy_timeout(0)")
	query.Add("_pragma", "foreign_keys(1)")
	query.Add("_pragma", "synchronous(FULL)")
	dsn.RawQuery = query.Encode()
	return dsn.String()
}

func (store *Store) initialize(ctx context.Context) error {
	if err := store.db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping journal: %w", err)
	}
	var journalMode string
	if err := store.db.QueryRowContext(ctx, `pragma journal_mode = WAL`).Scan(&journalMode); err != nil {
		return fmt.Errorf("enable journal WAL mode: %w", err)
	}
	if journalMode != "wal" {
		return fmt.Errorf("enable journal WAL mode: received %q", journalMode)
	}
	tx, err := store.beginTransaction(ctx)
	if err != nil {
		return fmt.Errorf("begin journal schema transaction: %w", err)
	}
	defer tx.Rollback()
	var version int
	if err := tx.QueryRowContext(ctx, `pragma user_version`).Scan(&version); err != nil {
		return fmt.Errorf("read journal schema version: %w", err)
	}
	if version != 0 && version != schemaVersion {
		return fmt.Errorf("unsupported journal schema version %d", version)
	}
	if _, err := tx.ExecContext(ctx, operationsSchema); err != nil {
		return fmt.Errorf("initialize journal schema: %w", err)
	}
	if version == 0 {
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("pragma user_version = %d", schemaVersion)); err != nil {
			return fmt.Errorf("record journal schema version: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit journal schema: %w", err)
	}
	return nil
}

// Close closes the private journal.
func (store *Store) Close() error {
	return store.db.Close()
}

// Begin validates current process authority, durably commits a new accepted
// intent, or returns the existing equivalent operation. created is true only
// after the new intent transaction commits.
func (store *Store) Begin(ctx context.Context, authority Authority, intent Intent) (operation Operation, created bool, err error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, false, err
	}
	if err := validateAuthority(authority); err != nil {
		return Operation{}, false, err
	}
	if err := validateIntent(intent); err != nil {
		return Operation{}, false, err
	}
	if intent.ExpectedRuntimeIdentity != authority.RuntimeIdentity {
		return Operation{}, false, ErrRuntimeIdentityMismatch
	}
	if intent.ExpectedRuntimeGeneration != authority.RuntimeGeneration {
		return Operation{}, false, ErrStaleRuntimeGeneration
	}
	tx, err := store.beginTransaction(ctx)
	if err != nil {
		return Operation{}, false, fmt.Errorf("begin operation transaction: %w", err)
	}
	defer tx.Rollback()
	now := time.Now().UTC().UnixMilli()
	result, err := tx.ExecContext(ctx, `
		insert into operations (
			runtime_identity, operation_id, operation_type, request_fingerprint,
			runtime_generation, state, created_at_ms, updated_at_ms
		) values (?, ?, ?, ?, ?, ?, ?, ?)
		on conflict(runtime_identity, operation_id) do nothing
	`, []byte(authority.RuntimeIdentity), []byte(intent.OperationID), intent.OperationType,
		intent.RequestFingerprint[:], encodeGeneration(authority.RuntimeGeneration), StateAccepted, now, now)
	if err != nil {
		return Operation{}, false, fmt.Errorf("insert operation intent: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return Operation{}, false, fmt.Errorf("inspect operation insert: %w", err)
	}
	operation, err = getOperation(ctx, tx, authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		return Operation{}, false, err
	}
	created = rows == 1
	if !created && (operation.OperationType != intent.OperationType || operation.RequestFingerprint != intent.RequestFingerprint) {
		return Operation{}, false, ErrOperationIDConflict
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, false, fmt.Errorf("commit operation intent: %w", err)
	}
	return operation, created, nil
}

// Get loads one operation without changing current authority.
func (store *Store) Get(ctx context.Context, runtimeIdentity string, operationID string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	if strings.TrimSpace(runtimeIdentity) == "" || operationID == "" {
		return Operation{}, errors.New("runtime identity and operation ID are required")
	}
	return getOperation(ctx, store.db, runtimeIdentity, operationID)
}

// MarkRunning moves an accepted intent into running state.
func (store *Store) MarkRunning(ctx context.Context, runtimeIdentity string, operationID string) (Operation, error) {
	return store.transition(ctx, runtimeIdentity, operationID, StateRunning, "")
}

// Complete durably records a terminal state and an optional structured failure
// code. Failure messages and raw result bodies are intentionally not stored.
func (store *Store) Complete(ctx context.Context, runtimeIdentity string, operationID string, state State, failureCode string) (Operation, error) {
	if err := validateFailureCode(state, failureCode); err != nil {
		return Operation{}, err
	}
	return store.transition(ctx, runtimeIdentity, operationID, state, failureCode)
}

func (store *Store) transition(ctx context.Context, runtimeIdentity string, operationID string, target State, failureCode string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	tx, err := store.beginTransaction(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("begin operation state transaction: %w", err)
	}
	defer tx.Rollback()
	current, err := getOperation(ctx, tx, runtimeIdentity, operationID)
	if err != nil {
		return Operation{}, err
	}
	if !validTransition(current.State, target) {
		return Operation{}, fmt.Errorf("%w: %s to %s", ErrOperationStateConflict, current.State, target)
	}
	now := time.Now().UTC().UnixMilli()
	var completedAt any
	var storedFailure any
	if target.terminal() {
		completedAt = now
	}
	if failureCode != "" {
		storedFailure = failureCode
	}
	if _, err := tx.ExecContext(ctx, `
		update operations
		set state = ?, updated_at_ms = ?, completed_at_ms = ?, failure_code = ?
		where runtime_identity = ? and operation_id = ?
	`, target, now, completedAt, storedFailure, []byte(runtimeIdentity), []byte(operationID)); err != nil {
		return Operation{}, fmt.Errorf("update operation state: %w", err)
	}
	updated, err := getOperation(ctx, tx, runtimeIdentity, operationID)
	if err != nil {
		return Operation{}, err
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, fmt.Errorf("commit operation state: %w", err)
	}
	return updated, nil
}

func validTransition(current State, target State) bool {
	switch current {
	case StateAccepted:
		return target == StateRunning || target.terminal()
	case StateRunning:
		return target.terminal()
	default:
		return false
	}
}

// Tombstone marks terminal evidence as compacted without releasing the
// operation ID or discarding request identity and outcome metadata. It does not
// implement a retention policy.
func (store *Store) Tombstone(ctx context.Context, runtimeIdentity string, operationID string) (Operation, error) {
	if err := ctx.Err(); err != nil {
		return Operation{}, err
	}
	tx, err := store.beginTransaction(ctx)
	if err != nil {
		return Operation{}, fmt.Errorf("begin operation tombstone transaction: %w", err)
	}
	defer tx.Rollback()
	operation, err := getOperation(ctx, tx, runtimeIdentity, operationID)
	if err != nil {
		return Operation{}, err
	}
	if !operation.State.terminal() {
		return Operation{}, fmt.Errorf("%w: cannot tombstone %s operation", ErrOperationStateConflict, operation.State)
	}
	if operation.TombstonedAt == nil {
		now := time.Now().UTC().UnixMilli()
		if _, err := tx.ExecContext(ctx, `
			update operations set updated_at_ms = ?, tombstoned_at_ms = ?
			where runtime_identity = ? and operation_id = ?
		`, now, now, []byte(runtimeIdentity), []byte(operationID)); err != nil {
			return Operation{}, fmt.Errorf("tombstone operation: %w", err)
		}
		operation, err = getOperation(ctx, tx, runtimeIdentity, operationID)
		if err != nil {
			return Operation{}, err
		}
	}
	if err := tx.Commit(); err != nil {
		return Operation{}, fmt.Errorf("commit operation tombstone: %w", err)
	}
	return operation, nil
}

type rowScanner interface {
	Scan(dest ...any) error
}

type queryRower interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func getOperation(ctx context.Context, source queryRower, runtimeIdentity string, operationID string) (Operation, error) {
	row := source.QueryRowContext(ctx, `
		select operation_id, operation_type, runtime_identity, runtime_generation,
			request_fingerprint, state, created_at_ms, updated_at_ms,
			completed_at_ms, failure_code, tombstoned_at_ms
		from operations where runtime_identity = ? and operation_id = ?
	`, []byte(runtimeIdentity), []byte(operationID))
	operation, err := scanOperation(row)
	if errors.Is(err, sql.ErrNoRows) {
		return Operation{}, ErrOperationNotFound
	}
	if err != nil {
		return Operation{}, fmt.Errorf("read operation: %w", err)
	}
	return operation, nil
}

func scanOperation(row rowScanner) (Operation, error) {
	var operation Operation
	var operationID []byte
	var runtimeIdentity []byte
	var generation []byte
	var fingerprint []byte
	var createdAt int64
	var updatedAt int64
	var completedAt sql.NullInt64
	var failureCode sql.NullString
	var tombstonedAt sql.NullInt64
	if err := row.Scan(
		&operationID, &operation.OperationType, &runtimeIdentity, &generation,
		&fingerprint, &operation.State, &createdAt, &updatedAt,
		&completedAt, &failureCode, &tombstonedAt,
	); err != nil {
		return Operation{}, err
	}
	if len(generation) != 8 || len(fingerprint) != sha256Size {
		return Operation{}, errors.New("journal contains invalid binary operation identity")
	}
	operation.OperationID = string(operationID)
	operation.RuntimeIdentity = string(runtimeIdentity)
	operation.RuntimeGeneration = binary.BigEndian.Uint64(generation)
	copy(operation.RequestFingerprint[:], fingerprint)
	operation.CreatedAt = time.UnixMilli(createdAt).UTC()
	operation.UpdatedAt = time.UnixMilli(updatedAt).UTC()
	if completedAt.Valid {
		completed := time.UnixMilli(completedAt.Int64).UTC()
		operation.CompletedAt = &completed
	}
	if failureCode.Valid {
		operation.FailureCode = failureCode.String
	}
	if tombstonedAt.Valid {
		tombstoned := time.UnixMilli(tombstonedAt.Int64).UTC()
		operation.TombstonedAt = &tombstoned
	}
	return operation, nil
}

const sha256Size = 32

func encodeGeneration(generation uint64) []byte {
	encoded := make([]byte, 8)
	binary.BigEndian.PutUint64(encoded, generation)
	return encoded
}

func (store *Store) beginTransaction(ctx context.Context) (*sql.Tx, error) {
	deadline := time.Now().Add(store.busyTimeout)
	for {
		tx, err := store.db.BeginTx(ctx, nil)
		if err == nil {
			return tx, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, ctxErr
		}
		if !sqliteBusy(err) {
			return nil, err
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, err
		}
		delay := min(remaining, busyRetryInterval)
		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func sqliteBusy(err error) bool {
	var sqliteErr *modernsqlite.Error
	if !errors.As(err, &sqliteErr) {
		return false
	}
	code := sqliteErr.Code() & 0xff
	return code == sqlite3.SQLITE_BUSY || code == sqlite3.SQLITE_LOCKED
}
