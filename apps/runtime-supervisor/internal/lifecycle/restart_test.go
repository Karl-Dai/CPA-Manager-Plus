package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

func TestRestartOrdersOneDurableIntentBeforeExactTerminationAndReplacementSpawn(t *testing.T) {
	f := newStopFixture(t)
	f.child.onStop = func(ctx context.Context) error {
		reader, err := journal.Open(ctx, f.path, journal.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		operation, err := reader.Get(ctx, "runtime-01", "op")
		if err != nil || operation.State != journal.StateRunning || operation.RuntimeGeneration != 41 ||
			operation.RequestFingerprint != sha256.Sum256([]byte("runtime.restart/v1:{}")) {
			t.Fatalf("pre-termination durable evidence = %+v, %v", operation, err)
		}
		return nil
	}
	f.child.onStart = func(ctx context.Context, spec cpaprocess.StartSpec) error {
		if spec.Executable != "supervisor-local-cpa" || len(spec.Args) != 0 ||
			f.child.observation.State != cpaprocess.StateExited {
			t.Fatalf("replacement input/state = %+v / %+v", spec, f.child.observation)
		}
		reader, err := journal.Open(ctx, f.path, journal.Options{})
		if err != nil {
			t.Fatal(err)
		}
		defer reader.Close()
		operation, err := reader.Get(ctx, "runtime-01", "op")
		if err != nil || operation.State != journal.StateRunning {
			t.Fatalf("pre-spawn durable evidence = %+v, %v", operation, err)
		}
		return nil
	}

	result, err := f.starter.Restart(t.Context(), restartRequest("op"))
	if err != nil || result.State != journal.StateSucceeded || f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("Restart = %+v, %v; process %+v", result, err, f.child.observation)
	}
	want := []string{"resolve", "prepare-stop", "begin", "running", "terminate", "spawn", "complete:succeeded"}
	if !reflect.DeepEqual(f.events, want) {
		t.Fatalf("ordering = %v, want %v", f.events, want)
	}
	if f.child.stops != 1 || f.child.starts != 1 || f.starter.authority.RuntimeGeneration != 41 {
		t.Fatalf("side effects/generation = stops %d, starts %d, generation %d", f.child.stops, f.child.starts, f.starter.authority.RuntimeGeneration)
	}
}

func TestRestartFailureWindowsNeverResumeOnReplay(t *testing.T) {
	tests := []struct {
		name, failAt, sideEffectError string
		wantState                     journal.State
		wantFailure                   string
		wantStops, wantStarts         int
		wantProcess                   cpaprocess.State
		wantError                     error
	}{
		{name: "lookup", failAt: "resolve", wantError: ErrPersistenceUnavailable, wantProcess: cpaprocess.StateRunning},
		{name: "intent", failAt: "begin", wantError: ErrPersistenceUnavailable, wantProcess: cpaprocess.StateRunning},
		{name: "running", failAt: "running", wantState: journal.StateAccepted, wantError: ErrPersistenceUnavailable, wantProcess: cpaprocess.StateRunning},
		{name: "termination", sideEffectError: "stop", wantState: journal.StateFailed, wantFailure: "process_restart_stop_failed", wantStops: 1, wantError: ErrExecutionFailed, wantProcess: cpaprocess.StateRunning},
		{name: "replacement spawn", sideEffectError: "start", wantState: journal.StateFailed, wantFailure: "process_restart_start_failed", wantStops: 1, wantStarts: 1, wantError: ErrExecutionFailed, wantProcess: cpaprocess.StateExited},
		{name: "success result", failAt: "complete", wantState: journal.StateRunning, wantStops: 1, wantStarts: 1, wantError: ErrExecutionFailed, wantProcess: cpaprocess.StateRunning},
		{name: "stop failure result", failAt: "complete", sideEffectError: "stop", wantState: journal.StateRunning, wantStops: 1, wantError: ErrExecutionFailed, wantProcess: cpaprocess.StateRunning},
		{name: "start failure result", failAt: "complete", sideEffectError: "start", wantState: journal.StateRunning, wantStops: 1, wantStarts: 1, wantError: ErrExecutionFailed, wantProcess: cpaprocess.StateExited},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			f := newStopFixture(t)
			f.journal.failAt = test.failAt
			if test.sideEffectError == "stop" {
				f.child.onStop = func(context.Context) error { return cpaprocess.ErrStopFailed }
			}
			if test.sideEffectError == "start" {
				f.child.onStart = func(context.Context, cpaprocess.StartSpec) error { return cpaprocess.ErrSpawnFailed }
			}
			_, err := f.starter.Restart(t.Context(), restartRequest("op"))
			if !errors.Is(err, test.wantError) || f.child.stops != test.wantStops || f.child.starts != test.wantStarts ||
				f.child.observation.State != test.wantProcess {
				t.Fatalf("Restart error %v, stops %d, starts %d, process %+v", err, f.child.stops, f.child.starts, f.child.observation)
			}
			stored, err := f.journal.Store.Get(t.Context(), "runtime-01", "op")
			if test.wantState == "" {
				if !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("unexpected intent: %+v, %v", stored, err)
				}
				return
			}
			if err != nil || stored.State != test.wantState || stored.FailureCode != test.wantFailure {
				t.Fatalf("retained evidence = %+v, %v", stored, err)
			}
			f.journal.failAt = ""
			f.events = nil
			replay, err := f.starter.Restart(t.Context(), restartRequest("op"))
			if err != nil || !reflect.DeepEqual(replay, stored) || f.child.stops != test.wantStops ||
				f.child.starts != test.wantStarts || f.child.observation.State != test.wantProcess ||
				!reflect.DeepEqual(f.events, []string{"resolve"}) {
				t.Fatalf("replay = %+v, %v; stops %d, starts %d, state %+v, events %v", replay, err, f.child.stops, f.child.starts, f.child.observation, f.events)
			}
		})
	}
}

func TestRestartPreconditionRejectsUnownedOrUnconfirmedChildWithoutIntent(t *testing.T) {
	for _, state := range []cpaprocess.State{cpaprocess.StateNotStarted, cpaprocess.StateExited, cpaprocess.StateUnknown, "unrecognized"} {
		t.Run(string(state), func(t *testing.T) {
			f := newStopFixture(t)
			f.child.observation = cpaprocess.Observation{State: state, PID: 123}
			_, err := f.starter.Restart(t.Context(), restartRequest("op"))
			if !errors.Is(err, journal.ErrOperationStateConflict) || f.child.stops != 0 || f.child.starts != 0 ||
				!reflect.DeepEqual(f.events, []string{"resolve", "prepare-stop"}) {
				t.Fatalf("Restart error %v; stops %d, starts %d, events %v", err, f.child.stops, f.child.starts, f.events)
			}
			if _, err := f.journal.Store.Get(t.Context(), "runtime-01", "op"); !errors.Is(err, journal.ErrOperationNotFound) {
				t.Fatalf("precondition failure wrote intent: %v", err)
			}
		})
	}
}

func TestRestartFencingAndReplayPrecedeProcessPrecondition(t *testing.T) {
	for _, identityMismatch := range []bool{true, false} {
		f := newStopFixture(t)
		f.child.observation.State = cpaprocess.StateUnknown
		request := restartRequest("op")
		request.ExpectedRuntimeGeneration++
		want := journal.ErrStaleRuntimeGeneration
		if identityMismatch {
			request.ExpectedRuntimeIdentity = "another-runtime"
			want = journal.ErrRuntimeIdentityMismatch
		}
		_, err := f.starter.Restart(t.Context(), request)
		if !errors.Is(err, want) || !reflect.DeepEqual(f.events, []string{"resolve"}) {
			t.Fatalf("fenced Restart error = %v, events %v", err, f.events)
		}
	}

	for _, state := range []journal.State{journal.StateAccepted, journal.StateRunning, journal.StateSucceeded, journal.StateFailed} {
		t.Run(string(state), func(t *testing.T) {
			f := newStopFixture(t)
			intent := restartIntent("op")
			if _, _, err := f.journal.Store.Begin(t.Context(), f.starter.authority, intent); err != nil {
				t.Fatal(err)
			}
			var err error
			if state == journal.StateRunning {
				_, err = f.journal.Store.MarkRunning(t.Context(), "runtime-01", "op")
			} else if state != journal.StateAccepted {
				code := ""
				if state == journal.StateFailed {
					code = "process_restart_start_failed"
				}
				_, err = f.journal.Store.Complete(t.Context(), "runtime-01", "op", state, code)
			}
			if err != nil {
				t.Fatal(err)
			}
			f.child.observation.State = cpaprocess.StateUnknown
			f.starter.authority.RuntimeGeneration = 42
			request := restartRequest("op")
			request.ExpectedRuntimeGeneration = 42
			got, err := f.starter.Restart(t.Context(), request)
			if err != nil || got.State != state || got.RuntimeGeneration != 41 || f.child.stops != 0 || f.child.starts != 0 ||
				!reflect.DeepEqual(f.events, []string{"resolve"}) {
				t.Fatalf("retained replay = %+v, %v; events %v", got, err, f.events)
			}
		})
	}

	for _, intent := range []journal.Intent{startIntent("shared-id"), stopIntent("shared-id")} {
		f := newStopFixture(t)
		if _, _, err := f.journal.Store.Begin(t.Context(), f.starter.authority, intent); err != nil {
			t.Fatal(err)
		}
		if _, err := f.starter.Restart(t.Context(), restartRequest("shared-id")); !errors.Is(err, journal.ErrOperationIDConflict) ||
			f.child.stops != 0 || f.child.starts != 0 || !reflect.DeepEqual(f.events, []string{"resolve"}) {
			t.Fatalf("conflicting Restart replay = %v; events %v", err, f.events)
		}
	}
}

func TestRestartCallerCancellationBeforeAndAfterDurableIntent(t *testing.T) {
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
			assertSupervisorContext := func(execution context.Context) {
				if ctx.Err() == nil || execution.Err() != nil || execution.Value(requestKey{}) != nil {
					t.Fatal("accepted Restart execution still belongs to caller context")
				}
			}
			f.child.onStop = func(execution context.Context) error {
				assertSupervisorContext(execution)
				return nil
			}
			f.child.onStart = func(execution context.Context, _ cpaprocess.StartSpec) error {
				assertSupervisorContext(execution)
				return nil
			}
			got, err := f.starter.Restart(ctx, restartRequest("op"))
			if when == "after commit" {
				if err != nil || got.State != journal.StateSucceeded || f.child.stops != 1 || f.child.starts != 1 {
					t.Fatalf("accepted Restart = %+v, %v; stops %d, starts %d", got, err, f.child.stops, f.child.starts)
				}
			} else {
				if !errors.Is(err, context.Canceled) || f.child.stops != 0 || f.child.starts != 0 {
					t.Fatalf("cancelled Restart = %+v, %v; stops %d, starts %d", got, err, f.child.stops, f.child.starts)
				}
				if _, err := f.journal.Store.Get(t.Context(), "runtime-01", "op"); !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("cancelled request left intent: %v", err)
				}
			}
		})
	}
}

func TestRestartNaturalExitRaceStillStartsOneReplacement(t *testing.T) {
	f := newStopFixture(t)
	f.journal.beforeBegin = func() {
		f.child.observation = cpaprocess.Observation{State: cpaprocess.StateExited}
	}
	got, err := f.starter.Restart(t.Context(), restartRequest("op"))
	if err != nil || got.State != journal.StateSucceeded || f.child.stops != 0 || f.child.starts != 1 ||
		f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("natural-exit Restart = %+v, %v; stops %d, starts %d, state %+v", got, err, f.child.stops, f.child.starts, f.child.observation)
	}
}

func TestConcurrentSameIDRestartExecutesOnce(t *testing.T) {
	f := newStopFixture(t)
	gate := make(chan struct{})
	results := make(chan error, 32)
	var callers sync.WaitGroup
	for range 32 {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-gate
			_, err := f.starter.Restart(t.Context(), restartRequest("same-id"))
			results <- err
		}()
	}
	close(gate)
	callers.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent same-ID Restart = %v", err)
		}
	}
	if f.child.stops != 1 || f.child.starts != 1 {
		t.Fatalf("same-ID side effects = stops %d, starts %d", f.child.stops, f.child.starts)
	}
	var intents int
	for _, event := range f.events {
		if event == "begin" {
			intents++
		}
	}
	if intents != 1 {
		t.Fatalf("same-ID Restart submissions created %d intents", intents)
	}
}

func TestRestartSharesLifecycleSerializationWithStartAndStop(t *testing.T) {
	for _, test := range []struct {
		operation             string
		wantError             error
		wantStops, wantStarts int
		wantState             cpaprocess.State
	}{
		{"start", journal.ErrOperationStateConflict, 1, 1, cpaprocess.StateRunning},
		{"stop", nil, 2, 1, cpaprocess.StateExited},
		{"restart", nil, 2, 2, cpaprocess.StateRunning},
	} {
		t.Run(test.operation, func(t *testing.T) {
			f := newStopFixture(t)
			entered, release := make(chan struct{}), make(chan struct{})
			var blockOnce, releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			f.child.onStart = func(context.Context, cpaprocess.StartSpec) error {
				// Hold the gap after old-child reap, before replacement ownership
				// publication, where a separate Start must not acquire the child.
				blockOnce.Do(func() {
					close(entered)
					<-release
				})
				return nil
			}
			restarted, queued := make(chan error, 1), make(chan error, 1)
			go func() { _, err := f.starter.Restart(t.Context(), restartRequest("restart")); restarted <- err }()
			<-entered
			go func() {
				var err error
				switch test.operation {
				case "start":
					_, err = f.starter.Start(t.Context(), startRequest("queued"))
				case "stop":
					_, err = f.starter.Stop(t.Context(), stopRequest("queued"))
				case "restart":
					_, err = f.starter.Restart(t.Context(), restartRequest("queued"))
				}
				queued <- err
			}()
			select {
			case err := <-queued:
				t.Fatalf("%s crossed in-flight Restart serialization: %v", test.operation, err)
			case <-time.After(10 * time.Millisecond):
			}
			unblock()
			if err := <-restarted; err != nil {
				t.Fatal(err)
			}
			if err := <-queued; !errors.Is(err, test.wantError) {
				t.Fatalf("queued %s = %v, want %v", test.operation, err, test.wantError)
			}
			if f.child.stops != test.wantStops || f.child.starts != test.wantStarts || f.child.observation.State != test.wantState {
				t.Fatalf("serialized lifecycle = stops %d, starts %d, state %+v", f.child.stops, f.child.starts, f.child.observation)
			}
			if test.wantError != nil {
				if _, err := f.journal.Store.Get(t.Context(), "runtime-01", "queued"); !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("rejected queued request left intent: %v", err)
				}
			}
		})
	}
}

func TestRestartTerminalPersistenceFailureReplayLeavesReplacementUntouched(t *testing.T) {
	f := newStopFixture(t)
	f.journal.failAt = "complete"
	_, err := f.starter.Restart(t.Context(), restartRequest("restart"))
	if !errors.Is(err, ErrExecutionFailed) || f.child.stops != 1 || f.child.starts != 1 ||
		f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("Restart with terminal persistence failure = %v; stops %d, starts %d, state %+v", err, f.child.stops, f.child.starts, f.child.observation)
	}
	retained, err := f.journal.Store.Get(t.Context(), "runtime-01", "restart")
	if err != nil || retained.State != journal.StateRunning {
		t.Fatalf("retained Restart evidence = %+v, %v", retained, err)
	}
	f.journal.failAt = ""
	replay, err := f.starter.Restart(t.Context(), restartRequest("restart"))
	if err != nil || !reflect.DeepEqual(replay, retained) || f.child.stops != 1 || f.child.starts != 1 ||
		f.child.observation.State != cpaprocess.StateRunning {
		t.Fatalf("Restart replay = %+v, %v; stops %d, starts %d, state %+v", replay, err, f.child.stops, f.child.starts, f.child.observation)
	}
}

func TestCloseDrainsRestartAndRejectsAllLifecycleSubmissions(t *testing.T) {
	f := newStopFixture(t)
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	defer unblock()
	f.child.onStart = func(context.Context, cpaprocess.StartSpec) error {
		close(entered)
		<-release
		return nil
	}
	restarted, closed, queued := make(chan error, 1), make(chan error, 1), make(chan error, 1)
	go func() { _, err := f.starter.Restart(t.Context(), restartRequest("restart")); restarted <- err }()
	<-entered
	go func() { _, err := f.starter.Restart(t.Context(), restartRequest("queued")); queued <- err }()
	select {
	case err := <-queued:
		t.Fatalf("queued Restart crossed active execution: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
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
	if _, err := f.starter.Restart(t.Context(), restartRequest("during-close")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Restart admitted after shutdown began = %v", err)
	}
	select {
	case err := <-closed:
		t.Fatalf("journal closed during accepted Restart: %v", err)
	case <-time.After(10 * time.Millisecond):
	}
	unblock()
	if err := <-restarted; err != nil {
		t.Fatal(err)
	}
	if err := <-queued; !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("queued Restart admitted after shutdown began = %v", err)
	}
	if err := <-closed; err != nil {
		t.Fatal(err)
	}
	if f.child.stops != 1 || f.child.starts != 1 || f.child.observation.State != cpaprocess.StateRunning ||
		!reflect.DeepEqual(f.events[len(f.events)-2:], []string{"complete:succeeded", "close"}) {
		t.Fatalf("shutdown did not drain Restart before journal close: stops %d, starts %d, state %+v, events %v", f.child.stops, f.child.starts, f.child.observation, f.events)
	}
	reader, err := journal.Open(t.Context(), f.path, journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	for _, id := range []string{"queued", "during-close-start", "during-close-stop", "during-close"} {
		if _, err := reader.Get(t.Context(), "runtime-01", id); !errors.Is(err, journal.ErrOperationNotFound) {
			t.Fatalf("shutdown request %q left intent: %v", id, err)
		}
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
	if _, err := f.starter.Restart(t.Context(), restartRequest("later-restart")); !errors.Is(err, ErrPersistenceUnavailable) {
		t.Fatalf("Restart after Close = %v", err)
	}
}

func restartRequest(id string) RestartRequest {
	return RestartRequest{OperationID: id, ExpectedRuntimeIdentity: "runtime-01", ExpectedRuntimeGeneration: 41}
}

func restartIntent(id string) journal.Intent {
	return journal.Intent{OperationID: id, OperationType: "restart", ExpectedRuntimeIdentity: "runtime-01",
		ExpectedRuntimeGeneration: 41, RequestFingerprint: sha256.Sum256([]byte("runtime.restart/v1:{}"))}
}
