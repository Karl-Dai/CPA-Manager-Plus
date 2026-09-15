package journal

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestOpenCreatesSchema(t *testing.T) {
	store, path := openTestStore(t, Options{})
	var tableName string
	if err := store.db.QueryRow(`select name from sqlite_master where type = 'table' and name = 'operations'`).Scan(&tableName); err != nil {
		t.Fatal(err)
	}
	if tableName != "operations" {
		t.Fatalf("table name = %q, want operations", tableName)
	}
	var version int
	if err := store.db.QueryRow(`pragma user_version`).Scan(&version); err != nil {
		t.Fatal(err)
	}
	if version != schemaVersion {
		t.Fatalf("schema version = %d, want %d", version, schemaVersion)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm()&0o077 != 0 {
		t.Fatalf("journal permissions = %o, want no group/other permissions", info.Mode().Perm())
	}
}

func TestOpenMakesDatabaseAndLiveWALPrivate(t *testing.T) {
	directory := filepath.Join(t.TempDir(), "permissive-runtime")
	if err := os.Mkdir(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(directory, 0o755); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, "operations.sqlite")
	store := openStore(t, path, Options{})
	if _, _, err := store.Begin(context.Background(), testAuthority(13), testIntent("op-private", 13, "target=v0")); err != nil {
		t.Fatal(err)
	}

	for _, journalPath := range []string{path, path + "-wal", path + "-shm"} {
		info, err := os.Stat(journalPath)
		if err != nil {
			t.Fatalf("stat live journal file %q: %v", filepath.Base(journalPath), err)
		}
		if info.Mode().Perm()&0o077 != 0 {
			t.Fatalf("live journal file %q permissions = %o, want no group/other permissions", filepath.Base(journalPath), info.Mode().Perm())
		}
	}
}

func TestOpenInitializesSchemaIdempotently(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "operations.sqlite")
	first := openStore(t, path, Options{})
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second := openStore(t, path, Options{})
	var count int
	if err := second.db.QueryRow(`select count(*) from sqlite_master where type = 'table' and name = 'operations'`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("operations table count = %d, want 1", count)
	}
}

func TestBeginAndGetOperation(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(17)
	intent := testIntent("op-1", 17, "target=v1")
	operation, created, err := store.Begin(context.Background(), authority, intent)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("new operation was not reported as created")
	}
	assertOperationIdentity(t, operation, authority, intent)
	if operation.State != StateAccepted || operation.CompletedAt != nil || operation.TombstonedAt != nil {
		t.Fatalf("new operation state = %#v, want accepted non-terminal record", operation)
	}

	loaded, err := store.Get(context.Background(), authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	assertOperationIdentity(t, loaded, authority, intent)
	if !loaded.CreatedAt.Equal(operation.CreatedAt) || !loaded.UpdatedAt.Equal(operation.UpdatedAt) {
		t.Fatalf("loaded timestamps = %v/%v, want %v/%v", loaded.CreatedAt, loaded.UpdatedAt, operation.CreatedAt, operation.UpdatedAt)
	}
}

func TestTerminalResultPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	store := openStore(t, path, Options{})
	authority := testAuthority(23)
	intent := testIntent("op-terminal", 23, "target=v2")
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), authority.RuntimeIdentity, intent.OperationID); err != nil {
		t.Fatal(err)
	}
	completed, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateFailed, "execution_failed")
	if err != nil {
		t.Fatal(err)
	}
	if completed.State != StateFailed || completed.FailureCode != "execution_failed" || completed.CompletedAt == nil {
		t.Fatalf("completed operation = %#v", completed)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, path, Options{})
	loaded, err := reopened.Get(context.Background(), authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.State != StateFailed || loaded.FailureCode != "execution_failed" || loaded.CompletedAt == nil {
		t.Fatalf("reopened operation = %#v", loaded)
	}
}

func TestAcceptedIntentSurvivesReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	store := openStore(t, path, Options{})
	authority := testAuthority(29)
	intent := testIntent("op-recoverable", 29, "target=v3")
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, path, Options{})
	operation, err := reopened.Get(context.Background(), authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.State != StateAccepted || operation.CompletedAt != nil {
		t.Fatalf("reopened intent = %#v, want recoverable accepted record", operation)
	}
}

func TestBeginSameOperationReturnsExistingWithoutDuplicate(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(31)
	intent := testIntent("op-same", 31, "target=v4")
	first, created, err := store.Begin(context.Background(), authority, intent)
	if err != nil || !created {
		t.Fatalf("first Begin() = %#v, %t, %v", first, created, err)
	}
	second, created, err := store.Begin(context.Background(), authority, intent)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("duplicate Begin reported a new operation")
	}
	if !second.CreatedAt.Equal(first.CreatedAt) || second.RuntimeGeneration != first.RuntimeGeneration {
		t.Fatalf("duplicate Begin returned %#v, want original %#v", second, first)
	}
	if count := operationCount(t, store); count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
}

func TestDifferentOperationIDsCreateDifferentRecords(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(37)
	for _, id := range []string{"op-a", "op-b"} {
		if _, created, err := store.Begin(context.Background(), authority, testIntent(id, 37, "same-payload")); err != nil || !created {
			t.Fatalf("Begin(%q) created = %t, err = %v", id, created, err)
		}
	}
	if count := operationCount(t, store); count != 2 {
		t.Fatalf("operation count = %d, want 2", count)
	}
}

func TestOperationIDNamespaceIncludesRuntimeIdentity(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	firstAuthority := testAuthority(39)
	firstIntent := testIntent("shared-id", 39, "first-runtime-request")
	if _, created, err := store.Begin(context.Background(), firstAuthority, firstIntent); err != nil || !created {
		t.Fatalf("first runtime Begin created = %t, err = %v", created, err)
	}
	secondAuthority := Authority{RuntimeIdentity: "runtime-02", RuntimeGeneration: 40}
	secondIntent := Intent{
		OperationID:               firstIntent.OperationID,
		OperationType:             firstIntent.OperationType,
		ExpectedRuntimeIdentity:   secondAuthority.RuntimeIdentity,
		ExpectedRuntimeGeneration: secondAuthority.RuntimeGeneration,
		RequestFingerprint:        testRequestFingerprint("second-runtime-request"),
	}
	if _, created, err := store.Begin(context.Background(), secondAuthority, secondIntent); err != nil || !created {
		t.Fatalf("second runtime Begin created = %t, err = %v", created, err)
	}
	if count := operationCount(t, store); count != 2 {
		t.Fatalf("operation count = %d, want two Runtime-scoped records", count)
	}
}

func TestBeginRejectsConflictingOperationID(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(41)
	intent := testIntent("op-conflict", 41, "target=v5")
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}

	t.Run("different fingerprint", func(t *testing.T) {
		conflict := intent
		conflict.RequestFingerprint = testRequestFingerprint("target=v6")
		if _, _, err := store.Begin(context.Background(), authority, conflict); !errors.Is(err, ErrOperationIDConflict) {
			t.Fatalf("Begin() error = %v, want operation ID conflict", err)
		}
	})
	t.Run("different operation type", func(t *testing.T) {
		conflict := intent
		conflict.OperationType = "other_operation"
		if _, _, err := store.Begin(context.Background(), authority, conflict); !errors.Is(err, ErrOperationIDConflict) {
			t.Fatalf("Begin() error = %v, want operation ID conflict", err)
		}
	})
	if count := operationCount(t, store); count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
}

func TestDatabaseUniqueConstraintRejectsDuplicateOperationID(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(43)
	intent := testIntent("op-unique", 43, "target=v7")
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	_, err := store.db.ExecContext(context.Background(), `
		insert into operations
		select * from operations where runtime_identity = ? and operation_id = ?
	`, []byte(authority.RuntimeIdentity), []byte(intent.OperationID))
	if err == nil || !strings.Contains(strings.ToLower(err.Error()), "unique") {
		t.Fatalf("direct duplicate insert error = %v, want database unique constraint", err)
	}
}

func TestRuntimeGenerationRoundTripsFullUint64(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(math.MaxUint64)
	intent := testIntent("op-generation", math.MaxUint64, "target=v8")
	operation, _, err := store.Begin(context.Background(), authority, intent)
	if err != nil {
		t.Fatal(err)
	}
	if operation.RuntimeGeneration != math.MaxUint64 {
		t.Fatalf("runtime generation = %d, want %d", operation.RuntimeGeneration, uint64(math.MaxUint64))
	}
}

func TestNewSupervisorGenerationDoesNotReplaceHistoricalAuthority(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	oldAuthority := testAuthority(47)
	intent := testIntent("op-old-generation", 47, "target=v9")
	store := openStore(t, path, Options{})
	if _, _, err := store.Begin(context.Background(), oldAuthority, intent); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, path, Options{})
	newAuthority := testAuthority(53)
	staleConflict := intent
	staleConflict.RequestFingerprint = testRequestFingerprint("different-request")
	if _, _, err := reopened.Begin(context.Background(), newAuthority, staleConflict); !errors.Is(err, ErrStaleRuntimeGeneration) {
		t.Fatalf("stale replay error = %v, want stale generation", err)
	}
	refreshed := intent
	refreshed.ExpectedRuntimeGeneration = newAuthority.RuntimeGeneration
	operation, created, err := reopened.Begin(context.Background(), newAuthority, refreshed)
	if err != nil {
		t.Fatal(err)
	}
	if created {
		t.Fatal("cross-generation replay created another operation")
	}
	if operation.RuntimeGeneration != oldAuthority.RuntimeGeneration {
		t.Fatalf("historical generation = %d, want creation generation %d", operation.RuntimeGeneration, oldAuthority.RuntimeGeneration)
	}
	if count := operationCount(t, reopened); count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
	refreshedConflict := staleConflict
	refreshedConflict.ExpectedRuntimeGeneration = newAuthority.RuntimeGeneration
	if _, _, err := reopened.Begin(context.Background(), newAuthority, refreshedConflict); !errors.Is(err, ErrOperationIDConflict) {
		t.Fatalf("refreshed conflicting replay error = %v, want operation ID conflict", err)
	}
}

func TestBeginChecksIdentityBeforeGenerationAndStorage(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(59)
	intent := testIntent("op-fenced", 59, "target=v10")
	intent.ExpectedRuntimeIdentity = "another-runtime"
	intent.ExpectedRuntimeGeneration = 60
	if _, _, err := store.Begin(context.Background(), authority, intent); !errors.Is(err, ErrRuntimeIdentityMismatch) {
		t.Fatalf("identity mismatch error = %v", err)
	}
	if count := operationCount(t, store); count != 0 {
		t.Fatalf("operation count = %d after fenced request, want 0", count)
	}
}

func TestJournalPersistsOnlySecretFreeRequestIdentity(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "operations.sqlite")
	store := openStore(t, path, Options{})
	type testMutationRequest struct {
		target        string
		managementKey string
	}
	request := testMutationRequest{
		target:        "v11",
		managementKey: "cpamp-test-secret-DO-NOT-PERSIST-7x4Q9m2K",
	}
	secretFreeIdentity := "target=" + request.target
	intent := testIntent("op-secret-safe", 61, secretFreeIdentity)
	if _, _, err := store.Begin(context.Background(), testAuthority(61), intent); err != nil {
		t.Fatal(err)
	}
	operation, err := store.Get(context.Background(), "runtime-01", intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if operation.RequestFingerprint != testRequestFingerprint(secretFreeIdentity) {
		t.Fatalf("stored fingerprint = %x, want secret-free request identity fingerprint", operation.RequestFingerprint)
	}

	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), filepath.Base(path)) {
			continue
		}
		content, err := os.ReadFile(filepath.Join(directory, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(content, []byte(request.managementKey)) {
			t.Fatalf("journal file %q contains supplied secret", entry.Name())
		}
	}
}

func TestContextCancellationIsPropagated(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(67)
	intent := testIntent("op-cancelled", 67, "target=v12")
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, _, err := store.Begin(cancelled, authority, intent); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Begin error = %v", err)
	}
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	checks := []struct {
		name string
		call func() error
	}{
		{name: "Get", call: func() error {
			_, err := store.Get(cancelled, authority.RuntimeIdentity, intent.OperationID)
			return err
		}},
		{name: "MarkRunning", call: func() error {
			_, err := store.MarkRunning(cancelled, authority.RuntimeIdentity, intent.OperationID)
			return err
		}},
		{name: "Complete", call: func() error {
			_, err := store.Complete(cancelled, authority.RuntimeIdentity, intent.OperationID, StateSucceeded, "")
			return err
		}},
		{name: "Tombstone", call: func() error {
			_, err := store.Tombstone(cancelled, authority.RuntimeIdentity, intent.OperationID)
			return err
		}},
	}
	for _, check := range checks {
		t.Run(check.name, func(t *testing.T) {
			if err := check.call(); !errors.Is(err, context.Canceled) {
				t.Fatalf("error = %v, want context cancellation", err)
			}
		})
	}
}

func TestMalformedStateTransitionsAreRejected(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority := testAuthority(71)
	intent := testIntent("op-state", 71, "target=v13")
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateRunning, ""); err == nil {
		t.Fatal("Complete accepted non-terminal state")
	}
	if _, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateFailed, "secret with spaces"); err == nil {
		t.Fatal("Complete accepted malformed failure code")
	}
	if _, err := store.Tombstone(context.Background(), authority.RuntimeIdentity, intent.OperationID); !errors.Is(err, ErrOperationStateConflict) {
		t.Fatalf("accepted tombstone error = %v", err)
	}
	if _, err := store.MarkRunning(context.Background(), authority.RuntimeIdentity, intent.OperationID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkRunning(context.Background(), authority.RuntimeIdentity, intent.OperationID); !errors.Is(err, ErrOperationStateConflict) {
		t.Fatalf("repeated running transition error = %v", err)
	}
	if _, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateSucceeded, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateFailed, "late_failure"); !errors.Is(err, ErrOperationStateConflict) {
		t.Fatalf("terminal transition error = %v", err)
	}
	if _, err := store.db.ExecContext(context.Background(), `update operations set state = 'queued'`); err == nil {
		t.Fatal("database accepted unsupported state")
	}
}

func TestConcurrentDuplicateBeginCreatesOneDurableOperation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	stores := []*Store{
		openStore(t, path, Options{}),
		openStore(t, path, Options{}),
	}
	authority := testAuthority(73)
	intent := testIntent("op-concurrent", 73, "target=v14")
	const goroutines = 24
	start := make(chan struct{})
	type result struct {
		created bool
		err     error
	}
	results := make(chan result, goroutines)
	var group sync.WaitGroup
	for index := 0; index < goroutines; index++ {
		group.Add(1)
		go func(store *Store) {
			defer group.Done()
			<-start
			_, created, err := store.Begin(context.Background(), authority, intent)
			results <- result{created: created, err: err}
		}(stores[index%len(stores)])
	}
	close(start)
	group.Wait()
	close(results)
	createdCount := 0
	for result := range results {
		if result.err != nil {
			t.Errorf("concurrent Begin error = %v", result.err)
		}
		if result.created {
			createdCount++
		}
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
	if count := operationCount(t, stores[0]); count != 1 {
		t.Fatalf("operation count = %d, want 1", count)
	}
}

func TestBusyTransactionErrorIsReturnedAndBounded(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	options := Options{BusyTimeout: 25 * time.Millisecond}
	locker := openStore(t, path, options)
	contender := openStore(t, path, options)
	tx, err := locker.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	started := time.Now()
	_, _, err = contender.Begin(context.Background(), testAuthority(79), testIntent("op-busy", 79, "target=v15"))
	elapsed := time.Since(started)
	if err == nil {
		t.Fatal("Begin unexpectedly swallowed busy transaction error")
	}
	if elapsed > time.Second {
		t.Fatalf("busy error returned after %s, want bounded timeout", elapsed)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "locked") && !strings.Contains(strings.ToLower(err.Error()), "busy") {
		t.Fatalf("Begin error = %v, want SQLite lock/busy evidence", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, created, err := contender.Begin(context.Background(), testAuthority(79), testIntent("op-busy", 79, "target=v15")); err != nil || !created {
		t.Fatalf("Begin after rollback created = %t, err = %v", created, err)
	}
}

func TestBusyBeginHonorsContextCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	locker := openStore(t, path, Options{})
	contender := openStore(t, path, Options{})
	tx, err := locker.db.BeginTx(context.Background(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	ctx, cancel := context.WithTimeout(context.Background(), 25*time.Millisecond)
	defer cancel()
	started := time.Now()
	_, _, err = contender.Begin(ctx, testAuthority(81), testIntent("op-context-busy", 81, "context-request"))
	elapsed := time.Since(started)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Begin error = %v, want context deadline", err)
	}
	if elapsed > time.Second {
		t.Fatalf("context cancellation returned after %s", elapsed)
	}
}

func TestTombstonePersistsAndPreservesIdempotency(t *testing.T) {
	path := filepath.Join(t.TempDir(), "operations.sqlite")
	authority := testAuthority(83)
	intent := testIntent("op-tombstone", 83, "target=v16")
	store := openStore(t, path, Options{})
	if _, _, err := store.Begin(context.Background(), authority, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Complete(context.Background(), authority.RuntimeIdentity, intent.OperationID, StateFailed, "execution_failed"); err != nil {
		t.Fatal(err)
	}
	tombstone, err := store.Tombstone(context.Background(), authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if tombstone.TombstonedAt == nil || tombstone.State != StateFailed || tombstone.FailureCode != "execution_failed" {
		t.Fatalf("tombstone = %#v", tombstone)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	reopened := openStore(t, path, Options{})
	loaded, err := reopened.Get(context.Background(), authority.RuntimeIdentity, intent.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.TombstonedAt == nil || loaded.RequestFingerprint != intent.RequestFingerprint || loaded.State != StateFailed {
		t.Fatalf("reopened tombstone = %#v", loaded)
	}
	replayed, created, err := reopened.Begin(context.Background(), authority, intent)
	if err != nil || created || replayed.TombstonedAt == nil {
		t.Fatalf("tombstone replay = %#v, created %t, error %v", replayed, created, err)
	}
	conflict := intent
	conflict.RequestFingerprint = testRequestFingerprint("target=v17")
	if _, _, err := reopened.Begin(context.Background(), authority, conflict); !errors.Is(err, ErrOperationIDConflict) {
		t.Fatalf("tombstone conflict error = %v", err)
	}
}

func openTestStore(t *testing.T, options Options) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "runtime", "operations.sqlite")
	return openStore(t, path, options), path
}

func openStore(t *testing.T, path string, options Options) *Store {
	t.Helper()
	store, err := Open(context.Background(), path, options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testAuthority(generation uint64) Authority {
	return Authority{RuntimeIdentity: "runtime-01", RuntimeGeneration: generation}
}

func testIntent(id string, generation uint64, secretFreeIdentity string) Intent {
	return Intent{
		OperationID:               id,
		OperationType:             "test_operation",
		ExpectedRuntimeIdentity:   "runtime-01",
		ExpectedRuntimeGeneration: generation,
		RequestFingerprint:        testRequestFingerprint(secretFreeIdentity),
	}
}

// testRequestFingerprint stands in for an operation-specific caller after it
// has removed all secret-bearing fields from the logical request identity.
func testRequestFingerprint(secretFreeIdentity string) RequestFingerprint {
	return sha256.Sum256([]byte(secretFreeIdentity))
}

func assertOperationIdentity(t *testing.T, operation Operation, authority Authority, intent Intent) {
	t.Helper()
	if operation.OperationID != intent.OperationID ||
		operation.OperationType != intent.OperationType ||
		operation.RuntimeIdentity != authority.RuntimeIdentity ||
		operation.RuntimeGeneration != authority.RuntimeGeneration ||
		operation.RequestFingerprint != intent.RequestFingerprint {
		t.Fatalf("operation identity = %#v, authority %#v, intent %#v", operation, authority, intent)
	}
}

func operationCount(t *testing.T, store *Store) int {
	t.Helper()
	var count int
	if err := store.db.QueryRow(`select count(*) from operations`).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}
