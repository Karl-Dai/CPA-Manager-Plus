package cpaprocess

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestConfirmedExitEventCarriesExactReapedInstance(t *testing.T) {
	t.Parallel()
	var manager Manager
	events := manager.ExitEvents()
	child := newHelper(t, &manager, 0)
	started := startHelper(t, &manager, child)
	child.release(t)
	exited := waitForExit(t, &manager)
	select {
	case event := <-events:
		if event.InstanceID != started.InstanceID || event.InstanceID != exited.InstanceID {
			t.Fatalf("exit event = %+v, started/exited = %+v / %+v", event, started, exited)
		}
	case <-time.After(time.Second):
		t.Fatal("confirmed Wait/reap did not publish an exit event")
	}
}

func TestChildInstanceCorrelatesSpawnAndReapWithoutReuse(t *testing.T) {
	t.Parallel()
	var manager Manager
	first := newHelper(t, &manager, 0)
	a := startHelper(t, &manager, first)
	if a.InstanceID == 0 || manager.Observe().InstanceID != a.InstanceID {
		t.Fatalf("successful spawn has no stable instance: %+v", a)
	}
	first.release(t)
	if exited := waitForExit(t, &manager); exited.InstanceID != a.InstanceID {
		t.Fatalf("reap lost child instance: %+v", exited)
	}
	failed, err := manager.Start(t.Context(), StartSpec{Executable: filepath.Join(t.TempDir(), "missing-cpa")})
	if !errors.Is(err, ErrSpawnFailed) || failed.InstanceID != a.InstanceID {
		t.Fatalf("failed spawn changed instance: %+v, %v", failed, err)
	}
	second := newHelper(t, &manager, 0)
	b := startHelper(t, &manager, second)
	if b.InstanceID != a.InstanceID+1 {
		t.Fatalf("replacement instance = %d, old = %d", b.InstanceID, a.InstanceID)
	}
	second.release(t)
	if exited := waitForExit(t, &manager); exited.InstanceID != b.InstanceID {
		t.Fatalf("replacement reap lost child instance: %+v", exited)
	}
}

func TestChildInstanceExhaustionFailsBeforeSpawn(t *testing.T) {
	t.Parallel()
	manager := Manager{nextInstance: ^uint64(0) - 1}
	child := newHelper(t, &manager, 0)
	started := startHelper(t, &manager, child)
	if started.InstanceID != ^uint64(0) {
		t.Fatal("unexpected last instance")
	}
	child.release(t)
	exited := waitForExit(t, &manager)
	got, err := manager.Start(t.Context(), child.spec)
	if !errors.Is(err, ErrStateConflict) || got != exited {
		t.Fatalf("instance exhaustion allowed reuse: %+v, %v", got, err)
	}
	if len(child.starts(t)) != 1 {
		t.Fatal("instance exhaustion spawned another child")
	}
}

func TestChildInstanceIsNotJSONAuthority(t *testing.T) {
	observation := Observation{State: StateRunning, InstanceID: 123, PID: 456}
	encoded, err := json.Marshal(observation)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "instance") {
		t.Fatal("in-memory instance identity leaked into serialized observation")
	}
	var supplied Observation
	if err := json.Unmarshal([]byte(`{"State":"running","InstanceID":123}`), &supplied); err != nil {
		t.Fatal(err)
	}
	if supplied.InstanceID != 0 {
		t.Fatal("serialized input supplied child identity")
	}
}
