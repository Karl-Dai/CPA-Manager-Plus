package journal

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

func TestResolveIsReadOnlyEvenWhileWriterIsLocked(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	authority, intent := testAuthority(41), testIntent("existing", 41, "request")
	before, _, err := store.Begin(t.Context(), authority, intent)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := store.beginTransaction(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()

	got, found, err := store.Resolve(t.Context(), authority, intent)
	if err != nil || !found || !reflect.DeepEqual(got, before) {
		t.Fatalf("Resolve existing = %+v, %t, %v; want unchanged %+v", got, found, err, before)
	}
	intent.OperationID = "unseen"
	got, found, err = store.Resolve(t.Context(), authority, intent)
	if err != nil || found || !reflect.DeepEqual(got, Operation{}) {
		t.Fatalf("Resolve unseen = %+v, %t, %v", got, found, err)
	}
	if count := operationCount(t, store); count != 1 {
		t.Fatalf("Resolve wrote an intent: count = %d", count)
	}
}

func TestResolveValidatesAndFencesBeforeLookup(t *testing.T) {
	store, _ := openTestStore(t, Options{})
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		change func(*Authority, *Intent)
		want   string
	}{
		{"invalid authority", func(a *Authority, _ *Intent) { a.RuntimeGeneration = 0 }, "runtime generation must"},
		{"invalid intent", func(_ *Authority, i *Intent) { i.OperationID = "" }, "operation ID is required"},
		{"identity before generation", func(_ *Authority, i *Intent) {
			i.ExpectedRuntimeIdentity = "another-runtime"
			i.ExpectedRuntimeGeneration++
		}, ErrRuntimeIdentityMismatch.Error()},
		{"generation", func(_ *Authority, i *Intent) { i.ExpectedRuntimeGeneration++ }, ErrStaleRuntimeGeneration.Error()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority, intent := testAuthority(41), testIntent("op", 41, "request")
			test.change(&authority, &intent)
			_, found, err := store.Resolve(t.Context(), authority, intent)
			if found || err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Resolve with closed database = found %t, error %v; want %s before storage", found, err, test.want)
			}
		})
	}
	if _, found, err := store.Resolve(t.Context(), testAuthority(41), testIntent("op", 41, "request")); found || err == nil {
		t.Fatalf("unavailable lookup was treated as unseen: found %t, error %v", found, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if _, _, err := store.Resolve(ctx, testAuthority(41), testIntent("op", 41, "request")); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled Resolve error = %v", err)
	}
}

func TestResolveRetainsReplayAndConflictsAcrossGenerations(t *testing.T) {
	for _, state := range []State{StateAccepted, StateRunning, StateSucceeded, StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			store, path := openTestStore(t, Options{})
			authority, intent := testAuthority(41), testIntent("retained", 41, "request")
			original, _, err := store.Begin(t.Context(), authority, intent)
			if err != nil {
				t.Fatal(err)
			}
			if state == StateRunning {
				original, err = store.MarkRunning(t.Context(), authority.RuntimeIdentity, intent.OperationID)
			} else if state.terminal() {
				code := ""
				if state == StateFailed {
					code = "process_start_failed"
				}
				original, err = store.Complete(t.Context(), authority.RuntimeIdentity, intent.OperationID, state, code)
				if err == nil {
					original, err = store.Tombstone(t.Context(), authority.RuntimeIdentity, intent.OperationID)
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openStore(t, path, Options{})
			authority.RuntimeGeneration++
			if _, _, err := store.Resolve(t.Context(), authority, intent); !errors.Is(err, ErrStaleRuntimeGeneration) {
				t.Fatalf("stale replay = %v", err)
			}
			intent.ExpectedRuntimeGeneration = authority.RuntimeGeneration
			got, found, err := store.Resolve(t.Context(), authority, intent)
			if err != nil || !found || !reflect.DeepEqual(got, original) {
				t.Fatalf("replay = %+v, %t, %v; original %+v", got, found, err, original)
			}
			for _, change := range []func(*Intent){
				func(i *Intent) { i.OperationType = "different_operation" },
				func(i *Intent) { i.RequestFingerprint = testRequestFingerprint("different-request") },
			} {
				conflict := intent
				change(&conflict)
				if _, _, err := store.Resolve(t.Context(), authority, conflict); !errors.Is(err, ErrOperationIDConflict) {
					t.Fatalf("conflicting replay = %v", err)
				}
			}
			if count := operationCount(t, store); count != 1 {
				t.Fatalf("Resolve changed retained namespace: count %d", count)
			}
		})
	}
}
