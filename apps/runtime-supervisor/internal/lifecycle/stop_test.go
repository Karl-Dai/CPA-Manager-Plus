package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

func TestStopOrdersDurableEvidenceBeforeTerminationAndConfirmedReap(t *testing.T) {
	f := newStopFixture(t)
	f.child.onStop = func(ctx context.Context) error {
		reader, err := journal.Open(ctx, f.path, journal.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		operation, err := reader.Get(ctx, "runtime-01", "op")
		if err != nil || operation.State != journal.StateRunning || operation.RuntimeGeneration != 41 ||
			operation.RequestFingerprint != sha256.Sum256([]byte("runtime.stop/v1:{}")) {
			t.Fatalf("pre-termination durable evidence = %+v, %v", operation, err)
		}
		return nil
	}
	result, err := f.starter.Stop(t.Context(), stopRequest("op"))
	if err != nil || result.State != journal.StateSucceeded || f.child.observation.State != cpaprocess.StateExited {
		t.Fatalf("Stop = %+v, %v; process %+v", result, err, f.child.observation)
	}
	want := []string{"resolve", "prepare-stop", "begin", "running", "terminate", "complete:succeeded"}
	if !reflect.DeepEqual(f.events, want) {
		t.Fatalf("ordering = %v, want %v", f.events, want)
	}
	if f.starter.authority.RuntimeGeneration != 41 {
		t.Fatal("Stop changed Supervisor generation")
	}
}

func TestStopFailureWindowsNeverResumeOnReplay(t *testing.T) {
	tests := []struct {
		name      string
		failAt    string
		stopError bool
		wantState journal.State
		wantStops int
		wantError error
	}{
		{"lookup", "resolve", false, "", 0, ErrPersistenceUnavailable},
		{"intent", "begin", false, "", 0, ErrPersistenceUnavailable},
		{"running", "running", false, journal.StateAccepted, 0, ErrPersistenceUnavailable},
		{"termination", "", true, journal.StateFailed, 1, ErrExecutionFailed},
		{"success result", "complete", false, journal.StateRunning, 1, ErrExecutionFailed},
		{"failure result", "complete", true, journal.StateRunning, 1, ErrExecutionFailed},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newStopFixture(t)
			f.journal.failAt = test.failAt
			if test.stopError {
				f.child.onStop = func(context.Context) error { return cpaprocess.ErrStopFailed }
			}
			_, err := f.starter.Stop(t.Context(), stopRequest("op"))
			if !errors.Is(err, test.wantError) || f.child.stops != test.wantStops {
				t.Fatalf("Stop error %v, terminations %d", err, f.child.stops)
			}
			stored, err := f.journal.Store.Get(t.Context(), "runtime-01", "op")
			if test.wantState == "" {
				if !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("unexpected intent: %+v, %v", stored, err)
				}
				return
			}
			if err != nil || stored.State != test.wantState {
				t.Fatalf("retained state = %+v, %v", stored, err)
			}
			if test.wantState == journal.StateFailed && stored.FailureCode != "process_stop_failed" {
				t.Fatalf("failure evidence = %+v", stored)
			}
			f.journal.failAt = ""
			f.events = nil
			replay, err := f.starter.Stop(t.Context(), stopRequest("op"))
			if err != nil || !reflect.DeepEqual(replay, stored) || f.child.stops != test.wantStops ||
				!reflect.DeepEqual(f.events, []string{"resolve"}) {
				t.Fatalf("replay = %+v, %v; terminations %d; events %v", replay, err, f.child.stops, f.events)
			}
		})
	}
}

func TestStopPreconditionRejectsUnownedOrUnconfirmedChildWithoutIntent(t *testing.T) {
	for _, state := range []cpaprocess.State{cpaprocess.StateNotStarted, cpaprocess.StateExited, cpaprocess.StateUnknown, "unrecognized"} {
		t.Run(string(state), func(t *testing.T) {
			f := newStopFixture(t)
			f.child.observation = cpaprocess.Observation{State: state, PID: 123}
			_, err := f.starter.Stop(t.Context(), stopRequest("op"))
			if !errors.Is(err, journal.ErrOperationStateConflict) || f.child.stops != 0 ||
				!reflect.DeepEqual(f.events, []string{"resolve", "prepare-stop"}) {
				t.Fatalf("Stop error %v; terminations %d; events %v", err, f.child.stops, f.events)
			}
			if _, err := f.journal.Store.Get(t.Context(), "runtime-01", "op"); !errors.Is(err, journal.ErrOperationNotFound) {
				t.Fatalf("precondition failure wrote intent: %v", err)
			}
		})
	}
}

func TestStopFencingPrecedesProcessPrecondition(t *testing.T) {
	for _, identityMismatch := range []bool{true, false} {
		f := newStopFixture(t)
		f.child.observation.State = cpaprocess.StateUnknown
		request := stopRequest("op")
		request.ExpectedRuntimeGeneration++
		want := journal.ErrStaleRuntimeGeneration
		if identityMismatch {
			request.ExpectedRuntimeIdentity = "another-runtime"
			want = journal.ErrRuntimeIdentityMismatch
		}
		_, err := f.starter.Stop(t.Context(), request)
		if !errors.Is(err, want) || !reflect.DeepEqual(f.events, []string{"resolve"}) {
			t.Fatalf("fenced Stop error = %v, events %v", err, f.events)
		}
	}
}

func TestStopReplaysAllRetainedStatesAndRejectsDifferentType(t *testing.T) {
	for _, state := range []journal.State{journal.StateAccepted, journal.StateRunning, journal.StateSucceeded, journal.StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			f := newStopFixture(t)
			intent := stopIntent("op")
			if _, _, err := f.journal.Store.Begin(t.Context(), f.starter.authority, intent); err != nil {
				t.Fatal(err)
			}
			var err error
			if state == journal.StateRunning {
				_, err = f.journal.Store.MarkRunning(t.Context(), "runtime-01", "op")
			} else if state != journal.StateAccepted {
				code := ""
				if state == journal.StateFailed {
					code = "process_stop_failed"
				}
				_, err = f.journal.Store.Complete(t.Context(), "runtime-01", "op", state, code)
			}
			if err != nil {
				t.Fatal(err)
			}
			f.child.observation.State = cpaprocess.StateUnknown
			f.starter.authority.RuntimeGeneration = 42
			request := stopRequest("op")
			request.ExpectedRuntimeGeneration = 42
			got, err := f.starter.Stop(t.Context(), request)
			if err != nil || got.State != state || got.RuntimeGeneration != 41 || f.child.stops != 0 ||
				!reflect.DeepEqual(f.events, []string{"resolve"}) {
				t.Fatalf("retained replay = %+v, %v; events %v", got, err, f.events)
			}
		})
	}

	for _, test := range []struct {
		name   string
		intent journal.Intent
	}{
		{name: "type", intent: startIntent("shared-id")},
		{name: "fingerprint", intent: func() journal.Intent {
			intent := stopIntent("shared-id")
			intent.RequestFingerprint = sha256.Sum256([]byte("runtime.stop/v2:{}"))
			return intent
		}()},
	} {
		t.Run("conflicting "+test.name, func(t *testing.T) {
			f := newStopFixture(t)
			if _, _, err := f.journal.Store.Begin(t.Context(), f.starter.authority, test.intent); err != nil {
				t.Fatal(err)
			}
			if _, err := f.starter.Stop(t.Context(), stopRequest("shared-id")); !errors.Is(err, journal.ErrOperationIDConflict) ||
				f.child.stops != 0 || !reflect.DeepEqual(f.events, []string{"resolve"}) {
				t.Fatalf("conflicting Stop replay = %v; events %v", err, f.events)
			}
		})
	}
}

func TestStopCallerCancellationBeforeAndAfterDurableIntent(t *testing.T) {
	for _, when := range []string{"before submission", "before commit", "after commit"} {
		t.Run(when, func(t *testing.T) {
			f := newStopFixture(t)
			type requestKey struct{}
			ctx, cancel := context.WithCancel(context.WithValue(t.Context(), requestKey{}, "caller-value"))
			defer cancel()
			switch when {
			case "before submission":
				cancel()
			case "before commit":
				f.journal.beforeBegin = cancel
			case "after commit":
				f.journal.afterBegin = cancel
			}
			f.child.onStop = func(execution context.Context) error {
				if ctx.Err() == nil || execution.Err() != nil || execution.Value(requestKey{}) != nil {
					t.Fatal("accepted Stop execution still belongs to caller context")
				}
				return nil
			}
			got, err := f.starter.Stop(ctx, stopRequest("op"))
			if when == "after commit" {
				if err != nil || got.State != journal.StateSucceeded || f.child.stops != 1 {
					t.Fatalf("accepted Stop = %+v, %v", got, err)
				}
			} else {
				if !errors.Is(err, context.Canceled) || f.child.stops != 0 {
					t.Fatalf("cancelled Stop = %+v, %v; terminations %d", got, err, f.child.stops)
				}
				if _, err := f.journal.Store.Get(t.Context(), "runtime-01", "op"); !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("cancelled request left intent: %v", err)
				}
			}
		})
	}
}

func TestStopDoesNotSucceedUntilTargetReportsConfirmedReap(t *testing.T) {
	f := newStopFixture(t)
	entered, reaped := make(chan struct{}), make(chan struct{})
	f.child.onStop = func(context.Context) error {
		close(entered)
		<-reaped
		return nil
	}
	result := make(chan struct {
		operation journal.Operation
		err       error
	}, 1)
	go func() {
		operation, err := f.starter.Stop(t.Context(), stopRequest("op"))
		result <- struct {
			operation journal.Operation
			err       error
		}{operation, err}
	}()
	<-entered
	reader, err := journal.Open(t.Context(), f.path, journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	stored, err := reader.Get(t.Context(), "runtime-01", "op")
	if err != nil || stored.State != journal.StateRunning {
		t.Fatalf("Stop before reap = %+v, %v", stored, err)
	}
	select {
	case early := <-result:
		t.Fatalf("Stop completed before confirmed reap: %+v, %v", early.operation, early.err)
	case <-time.After(10 * time.Millisecond):
	}
	close(reaped)
	completed := <-result
	if completed.err != nil || completed.operation.State != journal.StateSucceeded {
		t.Fatalf("Stop after reap = %+v, %v", completed.operation, completed.err)
	}
}

func TestStopNaturalExitAfterExactTargetReservationSucceedsWithoutTermination(t *testing.T) {
	f := newStopFixture(t)
	f.journal.beforeBegin = func() {
		f.child.observation = cpaprocess.Observation{State: cpaprocess.StateExited}
	}
	got, err := f.starter.Stop(t.Context(), stopRequest("op"))
	if err != nil || got.State != journal.StateSucceeded || f.child.stops != 0 {
		t.Fatalf("natural-exit Stop = %+v, %v; terminations %d", got, err, f.child.stops)
	}
}

func TestStopTerminalPersistenceFailureReplayNeverTargetsReplacementChild(t *testing.T) {
	f := newStopFixture(t)
	f.journal.failAt = "complete"
	_, err := f.starter.Stop(t.Context(), stopRequest("old-stop"))
	if !errors.Is(err, ErrExecutionFailed) || f.child.stops != 1 || f.child.observation.State != cpaprocess.StateExited {
		t.Fatalf("Stop with terminal persistence failure = %v; terminations %d; state %+v", err, f.child.stops, f.child.observation)
	}
	retained, err := f.journal.Store.Get(t.Context(), "runtime-01", "old-stop")
	if err != nil || retained.State != journal.StateRunning {
		t.Fatalf("retained Stop evidence = %+v, %v", retained, err)
	}

	f.journal.failAt = ""
	started, err := f.starter.Start(t.Context(), startRequest("replacement-start"))
	if err != nil || started.State != journal.StateSucceeded || f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("replacement Start = %+v, %v", started, err)
	}
	replay, err := f.starter.Stop(t.Context(), stopRequest("old-stop"))
	if err != nil || !reflect.DeepEqual(replay, retained) || f.child.stops != 1 || f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("old Stop replay = %+v, %v; terminations %d; state %+v", replay, err, f.child.stops, f.child.observation)
	}
}

func TestConcurrentStopIDsTerminateExactlyOnce(t *testing.T) {
	f := newStopFixture(t)
	gate := make(chan struct{})
	results := make(chan error, 32)
	var callers sync.WaitGroup
	for i := range 32 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-gate
			_, err := f.starter.Stop(t.Context(), stopRequest(fmt.Sprintf("op-%d", i)))
			results <- err
		}()
	}
	close(gate)
	callers.Wait()
	close(results)
	var successes, conflicts int
	for err := range results {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, journal.ErrOperationStateConflict):
			conflicts++
		default:
			t.Fatalf("concurrent Stop = %v", err)
		}
	}
	if successes != 1 || conflicts != 31 || f.child.stops != 1 {
		t.Fatalf("successes %d, conflicts %d, terminations %d", successes, conflicts, f.child.stops)
	}
	var intents int
	for _, event := range f.events {
		if event == "begin" {
			intents++
		}
	}
	if intents != 1 {
		t.Fatalf("concurrent Stop submissions created %d intents", intents)
	}
}

func TestStartAndStopShareLifecycleSerialization(t *testing.T) {
	f := newStopFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.child.onStop = func(context.Context) error {
		close(entered)
		<-release
		return nil
	}
	stopResult, startResult := make(chan error, 1), make(chan error, 1)
	go func() { _, err := f.starter.Stop(t.Context(), stopRequest("stop")); stopResult <- err }()
	<-entered
	go func() { _, err := f.starter.Start(t.Context(), startRequest("replacement")); startResult <- err }()
	select {
	case err := <-startResult:
		t.Fatalf("Start crossed in-flight Stop serialization: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-stopResult; err != nil {
		t.Fatal(err)
	}
	if err := <-startResult; err != nil {
		t.Fatal(err)
	}
	if f.child.stops != 1 || f.child.starts != 1 || f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("serialized lifecycle = terminations %d, starts %d, state %+v", f.child.stops, f.child.starts, f.child.observation)
	}
	var completed, secondResolve int = -1, -1
	for index, event := range f.events {
		if event == "complete:succeeded" && completed == -1 {
			completed = index
		}
		if event == "resolve" && completed != -1 {
			secondResolve = index
			break
		}
	}
	if completed == -1 || secondResolve <= completed {
		t.Fatalf("Start/Stop operations interleaved: %v", f.events)
	}
}

func TestCloseDrainsStopAndClosesSharedJournalOnce(t *testing.T) {
	f := newStopFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	f.child.onStop = func(context.Context) error {
		close(entered)
		<-release
		return nil
	}
	stopped, closed := make(chan error, 1), make(chan error, 1)
	go func() { _, err := f.starter.Stop(t.Context(), stopRequest("op")); stopped <- err }()
	<-entered
	go func() { closed <- f.starter.Close() }()
	for !f.starter.closed.Load() {
		time.Sleep(time.Millisecond)
	}
	if _, err := f.starter.Start(t.Context(), startRequest("during-close-start")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Start admitted after shutdown began = %v", err)
	}
	if _, err := f.starter.Stop(t.Context(), stopRequest("during-close-stop")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Stop admitted after shutdown began = %v", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("journal closed during accepted Stop: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	close(release)
	if err := <-stopped; err != nil {
		t.Fatal(err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if err := f.starter.Close(); err != nil || f.journal.closeCalls != 1 {
		t.Fatalf("Close = %v, calls %d", err, f.journal.closeCalls)
	}
	if _, err := f.starter.Start(t.Context(), startRequest("later-start")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Start after Close = %v", err)
	}
	if _, err := f.starter.Stop(t.Context(), stopRequest("later-stop")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Stop after Close = %v", err)
	}
}

func newStopFixture(t *testing.T) *startFixture {
	t.Helper()
	f := newStartFixture(t)
	f.child.observation = cpaprocess.Observation{State: cpaprocess.StateRunning, PID: 123}
	return f
}

func stopRequest(id string) StopRequest {
	return StopRequest{OperationID: id, ExpectedRuntimeIdentity: "runtime-01", ExpectedRuntimeGeneration: 41}
}

func stopIntent(id string) journal.Intent {
	return journal.Intent{OperationID: id, OperationType: "stop", ExpectedRuntimeIdentity: "runtime-01",
		ExpectedRuntimeGeneration: 41, RequestFingerprint: sha256.Sum256([]byte("runtime.stop/v1:{}"))}
}
