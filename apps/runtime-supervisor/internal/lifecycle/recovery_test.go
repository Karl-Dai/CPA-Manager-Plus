package lifecycle

import (
	"context"
	"crypto/sha256"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

func TestRecoveryLeaseStartsInactiveAndExplicitStartArmsOnce(t *testing.T) {
	f := newRecoveryFixture(t)
	assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
	if f.process.startCount() != 0 {
		t.Fatal("Supervisor startup auto-started CPA")
	}

	result, err := f.executor.Start(t.Context(), startRequest("explicit-start"))
	if err != nil || result.State != journal.StateSucceeded {
		t.Fatalf("Start() = %+v, %v", result, err)
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
	if f.process.startCount() != 1 || f.executor.authority.RuntimeGeneration != 41 {
		t.Fatalf("starts/generation = %d/%d", f.process.startCount(), f.executor.authority.RuntimeGeneration)
	}

	replay, err := f.executor.Start(t.Context(), startRequest("explicit-start"))
	if err != nil || !reflect.DeepEqual(replay, result) || f.process.startCount() != 1 {
		t.Fatalf("Start replay = %+v, %v; starts %d", replay, err, f.process.startCount())
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
}

func TestConfirmedUnexpectedExitUsesDurablePrivateOperationBeforeSpawn(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "explicit-start")
	crashed := f.process.exitCurrent(cpaprocess.StateExited, true)
	waitRecoveryCondition(t, func() bool {
		status := f.executor.RecoveryStatus()
		return f.process.startCount() == 2 && status.State == RecoveryStateArmed && status.AttemptsRemaining == 2
	})
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 2)

	ids := f.store.operationIDs("auto_recovery")
	if len(ids) != 1 || !strings.HasPrefix(ids[0], "auto-recovery-") || len(ids[0]) < 50 {
		t.Fatalf("internal recovery IDs = %q", ids)
	}
	operation, err := f.store.Store.Get(t.Context(), "runtime-01", ids[0])
	if err != nil || operation.State != journal.StateSucceeded || operation.RuntimeGeneration != 41 ||
		operation.RequestFingerprint != sha256.Sum256([]byte("runtime.auto-recovery/v1:{}")) {
		t.Fatalf("recovery evidence = %+v, %v", operation, err)
	}
	if !f.process.durableEvidenceBeforeSecondStart.Load() {
		t.Fatal("automatic spawn ran before durable running evidence")
	}

	// A stale event for A cannot affect B, even when the fake reuses its PID.
	f.process.publishExit(crashed)
	time.Sleep(20 * time.Millisecond)
	if f.process.startCount() != 2 {
		t.Fatalf("stale exact-child exit spawned another child: %d", f.process.startCount())
	}
}

func TestRecoveryUsesFixedDelayAndStaleTimerCannotCrossExplicitStart(t *testing.T) {
	if automaticRecoveryDelay != time.Second {
		t.Fatalf("automatic recovery delay = %s", automaticRecoveryDelay)
	}
	f := newRecoveryFixture(t)
	waiting := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.executor.waitForRecoveryDelay = func(ctx context.Context) bool {
		once.Do(func() { close(waiting) })
		select {
		case <-ctx.Done():
			return false
		case <-release:
			return true
		}
	}
	startRecoveryEpoch(t, f, "start-a")
	f.process.exitCurrent(cpaprocess.StateExited, true)
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("recovery timer was not scheduled")
	}
	if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatal("recovery spawned or wrote intent before the delay completed")
	}

	result, err := f.executor.Start(t.Context(), startRequest("start-b"))
	if err != nil || result.State != journal.StateSucceeded {
		t.Fatalf("manual replacement Start = %+v, %v", result, err)
	}
	close(release)
	time.Sleep(20 * time.Millisecond)
	if f.process.startCount() != 2 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatalf("stale timer crossed new epoch: starts=%d ids=%v", f.process.startCount(), f.store.operationIDs("auto_recovery"))
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
}

func TestCanceledExplicitLifecycleBeforeDurableCommitPreservesRecoveryLease(t *testing.T) {
	for _, operationType := range []string{"start", "stop", "restart"} {
		for _, phase := range []string{"resolve", "begin"} {
			t.Run(operationType+"/"+phase, func(t *testing.T) {
				f := newRecoveryFixture(t)
				startRecoveryEpoch(t, f, "initial-start")
				operationID := operationType + "-cancelled-" + phase
				before := f.executor.RecoveryStatus()

				var releaseTimer chan struct{}
				if operationType == "start" {
					timerWaiting := make(chan struct{})
					releaseTimer = make(chan struct{})
					var once sync.Once
					f.executor.waitForRecoveryDelay = func(ctx context.Context) bool {
						once.Do(func() { close(timerWaiting) })
						select {
						case <-ctx.Done():
							return false
						case <-releaseTimer:
							return true
						}
					}
					f.process.exitCurrent(cpaprocess.StateExited, true)
					select {
					case <-timerWaiting:
					case <-time.After(time.Second):
						t.Fatal("existing recovery timer was not pending")
					}
					before = RecoveryStatus{State: RecoveryStateRecovering, AttemptsRemaining: 3}
					assertRecoveryStatus(t, f.executor, before.State, before.AttemptsRemaining)
				}

				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				f.store.setBefore(phase, operationType, cancel)
				var err error
				switch operationType {
				case "start":
					_, err = f.executor.Start(ctx, startRequest(operationID))
				case "stop":
					_, err = f.executor.Stop(ctx, stopRequest(operationID))
				case "restart":
					_, err = f.executor.Restart(ctx, restartRequest(operationID))
				}
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled %s at %s error = %v", operationType, phase, err)
				}
				if got := f.executor.RecoveryStatus(); got != before {
					t.Fatalf("cancelled %s at %s changed lease: got %+v, want %+v", operationType, phase, got, before)
				}
				if _, err := f.store.Store.Get(t.Context(), "runtime-01", operationID); !errors.Is(err, journal.ErrOperationNotFound) {
					t.Fatalf("cancelled %s at %s left durable intent: %v", operationType, phase, err)
				}

				if operationType == "start" {
					if f.process.startCount() != 1 {
						t.Fatal("cancelled Start spawned before the pending recovery timer")
					}
					close(releaseTimer)
					waitRecoveryCondition(t, func() bool {
						status := f.executor.RecoveryStatus()
						return f.process.startCount() == 2 && status == (RecoveryStatus{State: RecoveryStateArmed, AttemptsRemaining: 2})
					})
				} else if f.process.startCount() != 1 || f.process.stopCount() != 0 ||
					f.process.current().State != cpaprocess.StateRunning {
					t.Fatalf("cancelled %s changed the current child", operationType)
				}
			})
		}
	}
}

func TestStopDisarmsBeforeExpectedExitAndRestartIgnoresOldExit(t *testing.T) {
	t.Run("Stop", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		result, err := f.executor.Stop(t.Context(), stopRequest("stop"))
		if err != nil || result.State != journal.StateSucceeded {
			t.Fatalf("Stop = %+v, %v", result, err)
		}
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 1 || f.process.stopCount() != 1 {
			t.Fatalf("Stop caused recovery: starts/stops = %d/%d", f.process.startCount(), f.process.stopCount())
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
	})

	t.Run("Restart", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		oldInstance := f.process.current().InstanceID
		result, err := f.executor.Restart(t.Context(), restartRequest("restart"))
		if err != nil || result.State != journal.StateSucceeded {
			t.Fatalf("Restart = %+v, %v", result, err)
		}
		f.process.publishExit(oldInstance)
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 2 || f.process.stopCount() != 1 {
			t.Fatalf("old Restart exit caused recovery: starts/stops = %d/%d", f.process.startCount(), f.process.stopCount())
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
	})
}

func TestRecoveryBudgetIsBoundedAndExplicitLifecycleResetsIt(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "start")
	for attempt := 1; attempt <= 3; attempt++ {
		f.process.exitCurrent(cpaprocess.StateExited, true)
		wantStarts := attempt + 1
		waitRecoveryCondition(t, func() bool {
			status := f.executor.RecoveryStatus()
			if attempt == 3 {
				return f.process.startCount() == wantStarts && status.State == RecoveryStateManualIntervention
			}
			return f.process.startCount() == wantStarts && status.State == RecoveryStateArmed && status.AttemptsRemaining == 3-attempt
		})
		if attempt < 3 {
			assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3-attempt)
		}
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
	if ids := f.store.operationIDs("auto_recovery"); len(ids) != 3 || ids[0] == ids[1] || ids[1] == ids[2] || ids[0] == ids[2] {
		t.Fatalf("bounded unique recovery operations = %q", ids)
	}

	// Exhaustion disables privilege even though the third replacement is alive.
	f.process.exitCurrent(cpaprocess.StateExited, true)
	time.Sleep(20 * time.Millisecond)
	if f.process.startCount() != 4 {
		t.Fatalf("budget exhaustion allowed another spawn: %d", f.process.startCount())
	}

	result, err := f.executor.Start(t.Context(), startRequest("fresh-start"))
	if err != nil || result.State != journal.StateSucceeded || f.process.startCount() != 5 {
		t.Fatalf("explicit Start after exhaustion = %+v, %v; starts %d", result, err, f.process.startCount())
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
}

func TestAutomaticSpawnFailuresConsumeExactlyThreeAttempts(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "start")
	f.process.setStartFailures(cpaprocess.ErrSpawnFailed, cpaprocess.ErrSpawnFailed, cpaprocess.ErrSpawnFailed)
	f.process.exitCurrent(cpaprocess.StateExited, true)
	waitRecoveryCondition(t, func() bool {
		return f.process.startCount() == 4 && f.executor.RecoveryStatus().State == RecoveryStateManualIntervention
	})
	assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
	ids := f.store.operationIDs("auto_recovery")
	if len(ids) != 3 {
		t.Fatalf("automatic attempts = %d, want 3", len(ids))
	}
	for _, id := range ids {
		operation, err := f.store.Store.Get(t.Context(), "runtime-01", id)
		if err != nil || operation.State != journal.StateFailed || operation.FailureCode != "process_recovery_start_failed" {
			t.Fatalf("spawn failure evidence = %+v, %v", operation, err)
		}
	}
}

func TestExplicitSpawnFailureDoesNotGrantAutomaticCompensation(t *testing.T) {
	t.Run("Start", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.process.setStartFailures(cpaprocess.ErrSpawnFailed)
		if _, err := f.executor.Start(t.Context(), startRequest("start")); !errors.Is(err, ErrExecutionFailed) {
			t.Fatalf("Start error = %v", err)
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
		if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
			t.Fatal("failed explicit Start triggered automatic compensation")
		}
	})

	t.Run("Restart replacement", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		f.process.setStartFailures(cpaprocess.ErrSpawnFailed)
		if _, err := f.executor.Restart(t.Context(), restartRequest("restart")); !errors.Is(err, ErrExecutionFailed) {
			t.Fatalf("Restart error = %v", err)
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 2 || f.process.stopCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
			t.Fatal("failed Restart replacement triggered automatic compensation")
		}
	})
}

func TestExplicitRestartResetsExhaustedBudgetWhileChildRuns(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "start")
	for attempt := 1; attempt <= 3; attempt++ {
		f.process.exitCurrent(cpaprocess.StateExited, true)
		wantStarts := attempt + 1
		waitRecoveryCondition(t, func() bool {
			status := f.executor.RecoveryStatus()
			if attempt == 3 {
				return f.process.startCount() == wantStarts && status.State == RecoveryStateManualIntervention
			}
			return f.process.startCount() == wantStarts && status.State == RecoveryStateArmed && status.AttemptsRemaining == 3-attempt
		})
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
	if f.process.current().State != cpaprocess.StateRunning {
		t.Fatal("third automatic replacement is not running")
	}
	result, err := f.executor.Restart(t.Context(), restartRequest("restart"))
	if err != nil || result.State != journal.StateSucceeded {
		t.Fatalf("Restart = %+v, %v", result, err)
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
	if f.process.startCount() != 5 || f.process.stopCount() != 1 {
		t.Fatalf("Restart side effects = starts/stops %d/%d", f.process.startCount(), f.process.stopCount())
	}
}

func TestRecoveryPersistenceFailuresFailClosed(t *testing.T) {
	for _, phase := range []string{"begin", "running", "complete"} {
		t.Run(phase, func(t *testing.T) {
			f := newRecoveryFixture(t)
			startRecoveryEpoch(t, f, "start")
			f.store.setFailure(phase, "auto_recovery")
			f.process.exitCurrent(cpaprocess.StateExited, true)
			waitRecoveryCondition(t, func() bool {
				return f.executor.RecoveryStatus().State == RecoveryStateManualIntervention
			})
			assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
			wantStarts := 1
			if phase == "complete" {
				wantStarts = 2
				if f.process.current().State != cpaprocess.StateRunning {
					t.Fatal("terminal persistence failure killed the replacement")
				}
			}
			if f.process.startCount() != wantStarts {
				t.Fatalf("%s persistence failure starts = %d, want %d", phase, f.process.startCount(), wantStarts)
			}
			time.Sleep(20 * time.Millisecond)
			if f.process.startCount() != wantStarts {
				t.Fatal("persistence ambiguity retried an automatic attempt")
			}
		})
	}
}

func TestUnconfirmedWaitAndReadinessOnlyFailureNeverRecover(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "start")
	f.process.exitCurrent(cpaprocess.StateUnknown, false)
	time.Sleep(20 * time.Millisecond)
	if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatal("StateUnknown triggered recovery")
	}
	// Runtime 10 readiness failures emit no process exit event. Leaving the
	// exact child running (whether /healthz is refused, times out, or returns a
	// Home-like 503) therefore cannot reach this recovery path.
	f.process.setObservation(cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 1, PID: 777})
	time.Sleep(20 * time.Millisecond)
	if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatal("readiness-only state triggered recovery")
	}
}

func TestFastExitsAreReconciledWithoutDoubleRecovery(t *testing.T) {
	t.Run("explicit Start", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.process.exitAfterStart(1)
		result, err := f.executor.Start(t.Context(), startRequest("start"))
		if err != nil || result.State != journal.StateSucceeded {
			t.Fatalf("Start = %+v, %v", result, err)
		}
		waitRecoveryCondition(t, func() bool {
			status := f.executor.RecoveryStatus()
			return f.process.startCount() == 2 && status.State == RecoveryStateArmed && status.AttemptsRemaining == 2
		})
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 2 || len(f.store.operationIDs("auto_recovery")) != 1 {
			t.Fatalf("fast explicit exit recovery = starts %d, ops %v", f.process.startCount(), f.store.operationIDs("auto_recovery"))
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 2)
	})

	t.Run("Restart replacement", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		f.process.exitAfterStart(2)
		result, err := f.executor.Restart(t.Context(), restartRequest("restart"))
		if err != nil || result.State != journal.StateSucceeded {
			t.Fatalf("Restart = %+v, %v", result, err)
		}
		waitRecoveryCondition(t, func() bool {
			status := f.executor.RecoveryStatus()
			return f.process.startCount() == 3 && status.State == RecoveryStateArmed && status.AttemptsRemaining == 2
		})
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 3 || len(f.store.operationIDs("auto_recovery")) != 1 {
			t.Fatal("fast Restart exit was lost or double recovered")
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 2)
	})

	t.Run("automatic replacement", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		f.process.exitAfterStart(2)
		f.process.exitCurrent(cpaprocess.StateExited, true)
		waitRecoveryCondition(t, func() bool {
			status := f.executor.RecoveryStatus()
			return f.process.startCount() == 3 && status.State == RecoveryStateArmed && status.AttemptsRemaining == 1
		})
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 3 || len(f.store.operationIDs("auto_recovery")) != 2 {
			t.Fatal("fast automatic exit was lost or double recovered")
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 1)
	})
}

func TestExplicitTerminalPersistenceAmbiguityNeverArmsOrCompensates(t *testing.T) {
	t.Run("Start", func(t *testing.T) {
		f := newRecoveryFixture(t)
		f.store.setFailure("complete", "start")
		if _, err := f.executor.Start(t.Context(), startRequest("start")); !errors.Is(err, ErrExecutionFailed) {
			t.Fatalf("Start error = %v", err)
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
		f.process.exitCurrent(cpaprocess.StateExited, true)
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 1 {
			t.Fatal("ambiguous Start terminal result auto-compensated")
		}
	})

	t.Run("Restart", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		f.store.setFailure("complete", "restart")
		if _, err := f.executor.Restart(t.Context(), restartRequest("restart")); !errors.Is(err, ErrExecutionFailed) {
			t.Fatalf("Restart error = %v", err)
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
		f.process.exitCurrent(cpaprocess.StateExited, true)
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 2 {
			t.Fatal("ambiguous Restart terminal result auto-compensated")
		}
	})

	t.Run("Stop", func(t *testing.T) {
		f := newRecoveryFixture(t)
		startRecoveryEpoch(t, f, "start")
		f.store.setFailure("complete", "stop")
		if _, err := f.executor.Stop(t.Context(), stopRequest("stop")); !errors.Is(err, ErrExecutionFailed) {
			t.Fatalf("Stop error = %v", err)
		}
		assertRecoveryStatus(t, f.executor, RecoveryStateManualIntervention, 0)
		time.Sleep(20 * time.Millisecond)
		if f.process.startCount() != 1 || f.process.current().State != cpaprocess.StateExited {
			t.Fatal("ambiguous Stop terminal result re-armed recovery")
		}
	})
}

func TestShutdownCancelsPendingRecoveryBeforeDurableIntent(t *testing.T) {
	f := newRecoveryFixture(t)
	waiting := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	f.executor.waitForRecoveryDelay = func(ctx context.Context) bool {
		once.Do(func() { close(waiting) })
		select {
		case <-ctx.Done():
			return false
		case <-release:
			return true
		}
	}
	startRecoveryEpoch(t, f, "start")
	f.process.exitCurrent(cpaprocess.StateExited, true)
	select {
	case <-waiting:
	case <-time.After(time.Second):
		t.Fatal("pending recovery timer did not start")
	}
	f.executor.CloseAdmission()
	close(release)
	if err := f.executor.Close(); err != nil {
		t.Fatal(err)
	}
	if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatal("shutdown admitted a pending automatic recovery")
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
}

func TestShutdownAdmissionLinearizesAfterAutomaticRunningEvidence(t *testing.T) {
	f := newRecoveryFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	f.executor.newRecoveryOperationID = func() (string, error) {
		close(entered)
		<-release
		return "auto-recovery-shutdown-boundary", nil
	}
	startRecoveryEpoch(t, f, "start")
	f.process.exitCurrent(cpaprocess.StateExited, true)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("automatic recovery did not enter durable admission")
	}

	admissionClosed := make(chan struct{})
	go func() {
		f.executor.CloseAdmission()
		close(admissionClosed)
	}()
	select {
	case <-admissionClosed:
		t.Fatal("shutdown crossed an automatic durable admission in progress")
	case <-time.After(20 * time.Millisecond):
	}
	if f.executor.closed.Load() {
		t.Fatal("shutdown published closed before the automatic admission boundary")
	}

	close(release)
	select {
	case <-admissionClosed:
	case <-time.After(time.Second):
		t.Fatal("CloseAdmission did not finish after automatic running evidence")
	}
	if !f.store.hasPhase("auto-recovery-shutdown-boundary", "running") {
		t.Fatal("shutdown became closed before automatic running evidence")
	}
	if err := f.executor.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestShutdownFencesRecoveryQueuedAtSharedLifecycleGate(t *testing.T) {
	f := newRecoveryFixture(t)
	startRecoveryEpoch(t, f, "start")
	f.executor.mu.Lock()
	f.process.exitCurrent(cpaprocess.StateExited, true)
	waitRecoveryCondition(t, func() bool {
		return f.executor.RecoveryStatus().State == RecoveryStateRecovering
	})
	f.executor.CloseAdmission()
	f.executor.mu.Unlock()
	if err := f.executor.Close(); err != nil {
		t.Fatal(err)
	}
	if f.process.startCount() != 1 || len(f.store.operationIDs("auto_recovery")) != 0 {
		t.Fatal("recovery queued at the shared gate crossed shutdown admission")
	}
}

func TestShutdownDrainsAlreadyAcceptedAutomaticRecoveryBeforeJournalClose(t *testing.T) {
	f := newRecoveryFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	f.process.blockStart(2, entered, release)
	startRecoveryEpoch(t, f, "start")
	f.process.exitCurrent(cpaprocess.StateExited, true)
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("automatic spawn did not reach its side effect")
	}
	ids := f.store.operationIDs("auto_recovery")
	if len(ids) != 1 {
		t.Fatalf("accepted recovery IDs = %v", ids)
	}
	operation, err := f.store.Store.Get(t.Context(), "runtime-01", ids[0])
	if err != nil || operation.State != journal.StateRunning {
		t.Fatalf("pre-shutdown recovery evidence = %+v, %v", operation, err)
	}

	f.executor.CloseAdmission()
	closed := make(chan error, 1)
	go func() { closed <- f.executor.Close() }()
	select {
	case err := <-closed:
		t.Fatalf("journal closed before accepted recovery drained: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	if f.store.closeCount() != 0 {
		t.Fatal("journal closed while accepted recovery was running")
	}
	close(release)
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Close did not drain accepted recovery")
	}
	operation, err = f.store.Store.Get(t.Context(), "runtime-01", ids[0])
	if err == nil {
		// The Store is closed now; successful access would be unexpected. The
		// pre-close event sequence below proves the terminal write instead.
		t.Fatalf("closed journal remained readable: %+v", operation)
	}
	if f.store.closeCount() != 1 || !f.store.hasCompletion(ids[0], journal.StateSucceeded, "") {
		t.Fatalf("shutdown close/evidence = %d/%v", f.store.closeCount(), f.store.events)
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateInactive, 0)
}

func startRecoveryEpoch(t *testing.T, f *recoveryFixture, operationID string) {
	t.Helper()
	result, err := f.executor.Start(t.Context(), startRequest(operationID))
	if err != nil || result.State != journal.StateSucceeded {
		t.Fatalf("Start = %+v, %v", result, err)
	}
	assertRecoveryStatus(t, f.executor, RecoveryStateArmed, 3)
}

func assertRecoveryStatus(t *testing.T, executor *Executor, state RecoveryState, attempts int) {
	t.Helper()
	if got := executor.RecoveryStatus(); got != (RecoveryStatus{State: state, AttemptsRemaining: attempts}) {
		t.Fatalf("recovery status = %+v, want %s/%d", got, state, attempts)
	}
}

func waitRecoveryCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for recovery state")
		}
		time.Sleep(time.Millisecond)
	}
}

type recoveryFixture struct {
	executor *Executor
	store    *recoveryJournal
	process  *recoveryProcess
}

func newRecoveryFixture(t *testing.T) *recoveryFixture {
	t.Helper()
	store, err := journal.Open(t.Context(), t.TempDir()+"/operations.sqlite", journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	fixture := &recoveryFixture{
		store:   newRecoveryJournal(store),
		process: newRecoveryProcess(),
	}
	fixture.executor, err = NewExecutor(
		journal.Authority{RuntimeIdentity: "runtime-01", RuntimeGeneration: 41},
		fixture.store,
		fixture.process,
		"supervisor-local-cpa",
	)
	if err != nil {
		t.Fatal(err)
	}
	fixture.executor.waitForRecoveryDelay = func(ctx context.Context) bool { return ctx.Err() == nil }
	fixture.process.beforeStart = func(call int) {
		if call != 2 {
			return
		}
		ids := fixture.store.operationIDs("auto_recovery")
		if len(ids) == 0 {
			return
		}
		operation, err := fixture.store.Store.Get(context.Background(), "runtime-01", ids[len(ids)-1])
		if err == nil && operation.State == journal.StateRunning {
			fixture.process.durableEvidenceBeforeSecondStart.Store(true)
		}
	}
	t.Cleanup(func() { _ = fixture.executor.Close() })
	return fixture
}

type recoveryJournalEvent struct {
	phase         string
	operationID   string
	operationType string
	state         journal.State
	failureCode   string
}

type recoveryJournal struct {
	*journal.Store
	mu          sync.Mutex
	events      []recoveryJournalEvent
	types       map[string]string
	failPhase   string
	failType    string
	beforePhase string
	beforeType  string
	beforeHook  func()
	closeCalls  int
}

func newRecoveryJournal(store *journal.Store) *recoveryJournal {
	return &recoveryJournal{Store: store, types: make(map[string]string)}
}

func (j *recoveryJournal) setFailure(phase string, operationType string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.failPhase = phase
	j.failType = operationType
}

func (j *recoveryJournal) setBefore(phase string, operationType string, hook func()) {
	j.mu.Lock()
	defer j.mu.Unlock()
	j.beforePhase = phase
	j.beforeType = operationType
	j.beforeHook = hook
}

func (j *recoveryJournal) takeBeforeLocked(phase string, operationType string) func() {
	if j.beforePhase != phase || j.beforeType != operationType {
		return nil
	}
	hook := j.beforeHook
	j.beforeHook = nil
	return hook
}

func (j *recoveryJournal) shouldFailLocked(phase string, operationType string) bool {
	return j.failPhase == phase && j.failType == operationType
}

func (j *recoveryJournal) Resolve(ctx context.Context, authority journal.Authority, intent journal.Intent) (journal.Operation, bool, error) {
	j.mu.Lock()
	j.events = append(j.events, recoveryJournalEvent{phase: "resolve", operationID: intent.OperationID, operationType: intent.OperationType})
	fail := j.shouldFailLocked("resolve", intent.OperationType)
	before := j.takeBeforeLocked("resolve", intent.OperationType)
	j.mu.Unlock()
	if before != nil {
		before()
	}
	if fail {
		return journal.Operation{}, false, injectedFailure
	}
	return j.Store.Resolve(ctx, authority, intent)
}

func (j *recoveryJournal) Begin(ctx context.Context, authority journal.Authority, intent journal.Intent) (journal.Operation, bool, error) {
	j.mu.Lock()
	j.events = append(j.events, recoveryJournalEvent{phase: "begin", operationID: intent.OperationID, operationType: intent.OperationType})
	j.types[intent.OperationID] = intent.OperationType
	fail := j.shouldFailLocked("begin", intent.OperationType)
	before := j.takeBeforeLocked("begin", intent.OperationType)
	j.mu.Unlock()
	if before != nil {
		before()
	}
	if fail {
		return journal.Operation{}, false, injectedFailure
	}
	return j.Store.Begin(ctx, authority, intent)
}

func (j *recoveryJournal) MarkRunning(ctx context.Context, identity string, operationID string) (journal.Operation, error) {
	j.mu.Lock()
	operationType := j.types[operationID]
	j.events = append(j.events, recoveryJournalEvent{phase: "running", operationID: operationID, operationType: operationType})
	fail := j.shouldFailLocked("running", operationType)
	j.mu.Unlock()
	if fail {
		return journal.Operation{}, injectedFailure
	}
	return j.Store.MarkRunning(ctx, identity, operationID)
}

func (j *recoveryJournal) Complete(ctx context.Context, identity string, operationID string, state journal.State, failureCode string) (journal.Operation, error) {
	j.mu.Lock()
	operationType := j.types[operationID]
	j.events = append(j.events, recoveryJournalEvent{
		phase: "complete", operationID: operationID, operationType: operationType, state: state, failureCode: failureCode,
	})
	fail := j.shouldFailLocked("complete", operationType)
	j.mu.Unlock()
	if fail {
		return journal.Operation{}, injectedFailure
	}
	return j.Store.Complete(ctx, identity, operationID, state, failureCode)
}

func (j *recoveryJournal) operationIDs(operationType string) []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	var ids []string
	for _, event := range j.events {
		if event.phase == "begin" && event.operationType == operationType {
			ids = append(ids, event.operationID)
		}
	}
	return ids
}

func (j *recoveryJournal) hasCompletion(operationID string, state journal.State, failureCode string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, event := range j.events {
		if event.phase == "complete" && event.operationID == operationID && event.state == state && event.failureCode == failureCode {
			return true
		}
	}
	return false
}

func (j *recoveryJournal) hasPhase(operationID string, phase string) bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	for _, event := range j.events {
		if event.operationID == operationID && event.phase == phase {
			return true
		}
	}
	return false
}

func (j *recoveryJournal) Close() error {
	j.mu.Lock()
	j.closeCalls++
	j.mu.Unlock()
	return j.Store.Close()
}

func (j *recoveryJournal) closeCount() int {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.closeCalls
}

type recoveryProcess struct {
	mu       sync.Mutex
	observed cpaprocess.Observation
	nextID   uint64
	starts   int
	stops    int
	reserved uint64
	events   chan cpaprocess.ExitEvent

	startFailures []error
	exitOnStart   map[int]bool
	beforeStart   func(int)
	blockStartAt  int
	startEntered  chan struct{}
	startRelease  <-chan struct{}
	blockOnce     sync.Once

	durableEvidenceBeforeSecondStart atomic.Bool
}

func newRecoveryProcess() *recoveryProcess {
	return &recoveryProcess{events: make(chan cpaprocess.ExitEvent, 64), exitOnStart: make(map[int]bool)}
}

func (p *recoveryProcess) Observe() cpaprocess.Observation { return p.current() }

func (p *recoveryProcess) current() cpaprocess.Observation {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.observed.State == "" {
		return cpaprocess.Observation{State: cpaprocess.StateNotStarted}
	}
	return p.observed
}

func (p *recoveryProcess) setObservation(observation cpaprocess.Observation) {
	p.mu.Lock()
	p.observed = observation
	p.mu.Unlock()
}

func (p *recoveryProcess) Start(ctx context.Context, _ cpaprocess.StartSpec) (cpaprocess.Observation, error) {
	if err := ctx.Err(); err != nil {
		return p.current(), err
	}
	p.mu.Lock()
	p.starts++
	call := p.starts
	var startErr error
	if len(p.startFailures) > 0 {
		startErr = p.startFailures[0]
		p.startFailures = p.startFailures[1:]
	}
	beforeStart := p.beforeStart
	blockStartAt := p.blockStartAt
	startEntered := p.startEntered
	startRelease := p.startRelease
	p.mu.Unlock()
	if beforeStart != nil {
		beforeStart(call)
	}
	if call == blockStartAt {
		p.blockOnce.Do(func() { close(startEntered) })
		select {
		case <-ctx.Done():
			return p.current(), ctx.Err()
		case <-startRelease:
		}
	}
	if startErr != nil {
		return p.current(), startErr
	}

	p.mu.Lock()
	if p.reserved != 0 || p.observed.State == cpaprocess.StateRunning {
		observation := p.observed
		p.mu.Unlock()
		return observation, cpaprocess.ErrStateConflict
	}
	p.nextID++
	started := cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: p.nextID, PID: 777}
	p.observed = started
	exitImmediately := p.exitOnStart[call]
	if exitImmediately {
		p.observed = cpaprocess.Observation{State: cpaprocess.StateExited, InstanceID: started.InstanceID}
	}
	p.mu.Unlock()
	if exitImmediately {
		p.publishExit(started.InstanceID)
	}
	return started, nil
}

func (p *recoveryProcess) PrepareStop() (cpaprocess.StopTarget, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.observed.State != cpaprocess.StateRunning || p.observed.InstanceID == 0 || p.reserved != 0 {
		return nil, cpaprocess.ErrStateConflict
	}
	p.reserved = p.observed.InstanceID
	return &recoveryStopTarget{process: p, instanceID: p.reserved}, nil
}

func (p *recoveryProcess) ExitEvents() <-chan cpaprocess.ExitEvent { return p.events }

func (p *recoveryProcess) exitCurrent(state cpaprocess.State, publish bool) uint64 {
	p.mu.Lock()
	instanceID := p.observed.InstanceID
	pid := p.observed.PID
	p.observed = cpaprocess.Observation{State: state, InstanceID: instanceID, PID: pid}
	p.mu.Unlock()
	if publish {
		p.publishExit(instanceID)
	}
	return instanceID
}

func (p *recoveryProcess) publishExit(instanceID uint64) {
	select {
	case p.events <- cpaprocess.ExitEvent{InstanceID: instanceID}:
	default:
	}
}

func (p *recoveryProcess) setStartFailures(failures ...error) {
	p.mu.Lock()
	p.startFailures = append([]error(nil), failures...)
	p.mu.Unlock()
}

func (p *recoveryProcess) exitAfterStart(call int) {
	p.mu.Lock()
	p.exitOnStart[call] = true
	p.mu.Unlock()
}

func (p *recoveryProcess) blockStart(call int, entered chan struct{}, release <-chan struct{}) {
	p.mu.Lock()
	p.blockStartAt = call
	p.startEntered = entered
	p.startRelease = release
	p.mu.Unlock()
}

func (p *recoveryProcess) startCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.starts
}

func (p *recoveryProcess) stopCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.stops
}

type recoveryStopTarget struct {
	process    *recoveryProcess
	instanceID uint64
	once       sync.Once
}

func (target *recoveryStopTarget) Terminate(context.Context) (cpaprocess.Observation, error) {
	target.process.mu.Lock()
	if target.process.reserved != target.instanceID || target.process.observed.InstanceID != target.instanceID {
		observation := target.process.observed
		target.process.mu.Unlock()
		return observation, cpaprocess.ErrStateConflict
	}
	target.process.stops++
	target.process.observed = cpaprocess.Observation{State: cpaprocess.StateExited, InstanceID: target.instanceID}
	target.process.reserved = 0
	observation := target.process.observed
	target.process.mu.Unlock()
	target.process.publishExit(target.instanceID)
	return observation, nil
}

func (target *recoveryStopTarget) Release() {
	target.once.Do(func() {
		target.process.mu.Lock()
		if target.process.reserved == target.instanceID {
			target.process.reserved = 0
		}
		target.process.mu.Unlock()
	})
}
