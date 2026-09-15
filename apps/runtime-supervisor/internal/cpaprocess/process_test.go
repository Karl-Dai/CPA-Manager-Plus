package cpaprocess

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestObserveBeforeStart(t *testing.T) {
	var manager Manager
	if got := manager.Observe(); got != (Observation{State: StateNotStarted}) {
		t.Fatalf("Observe() = %+v", got)
	}
}

func TestStartObserveAndReap(t *testing.T) {
	for _, code := range []int{0, 23} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			t.Parallel()
			var manager Manager
			child := newHelper(t, &manager, code)
			started := startHelper(t, &manager, child)
			waitFor(t, func() bool { return len(child.starts(t)) == 1 })
			manager.mu.Lock()
			cmd := manager.cmd
			manager.mu.Unlock()

			got, err := manager.Start(t.Context(), child.spec)
			if !errors.Is(err, ErrStateConflict) || got != started {
				t.Fatalf("duplicate Start() = %+v, %v", got, err)
			}
			child.release(t)
			assertExit(t, waitForExit(t, &manager), code)
			if cmd.ProcessState == nil || !cmd.ProcessState.Exited() {
				t.Fatal("exit was published before Cmd.Wait reaped the child")
			}
		})
	}
}

func TestSpawnFailureAllowsRetry(t *testing.T) {
	t.Parallel()
	var manager Manager
	missing := StartSpec{Executable: filepath.Join(t.TempDir(), "missing-cpa")}
	got, err := manager.Start(t.Context(), missing)
	if !errors.Is(err, ErrSpawnFailed) || !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing executable Start() error = %v", err)
	}
	if got != (Observation{State: StateNotStarted}) || manager.Observe() != got {
		t.Fatalf("failed spawn left child state: %+v", got)
	}
	child := newHelper(t, &manager, 0)
	startHelper(t, &manager, child)
	child.release(t)
	exited := waitForExit(t, &manager)
	assertExit(t, exited, 0)
	got, err = manager.Start(t.Context(), missing)
	if !errors.Is(err, ErrSpawnFailed) || got != exited || manager.Observe() != exited {
		t.Fatalf("failed retry changed the last exit: %+v, %v", got, err)
	}
}

func TestInvalidStartSpec(t *testing.T) {
	for _, spec := range []StartSpec{
		{},
		{Executable: " \t"},
		{Executable: "cpa\x00"},
		{Executable: "cpa", Args: []string{"secret\x00argument"}},
	} {
		var manager Manager
		got, err := manager.Start(t.Context(), spec)
		if !errors.Is(err, ErrInvalidSpec) || got != (Observation{State: StateNotStarted}) {
			t.Fatalf("invalid Start() = %+v, %v", got, err)
		}
	}
}

func TestConcurrentStartSpawnsExactlyOneChild(t *testing.T) {
	t.Parallel()
	var manager Manager
	child := newHelper(t, &manager, 0)
	const attempts = 32
	gate := make(chan struct{})
	results := make(chan error, attempts)
	var callers sync.WaitGroup
	for range attempts {
		callers.Add(1)
		go func() {
			defer callers.Done()
			<-gate
			_, err := manager.Start(t.Context(), child.spec)
			results <- err
			_ = manager.Observe()
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
		case errors.Is(err, ErrStateConflict):
			conflicts++
		default:
			t.Fatalf("concurrent Start() error = %v", err)
		}
	}
	if successes != 1 || conflicts != attempts-1 {
		t.Fatalf("successes = %d, conflicts = %d", successes, conflicts)
	}
	waitFor(t, func() bool { return len(child.starts(t)) > 0 })
	entries := child.starts(t)
	if len(entries) != 1 || entries[0].Name() != strconv.Itoa(manager.Observe().PID) {
		t.Fatalf("spawned child markers = %v, observation = %+v", entries, manager.Observe())
	}
	child.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
	if entries := child.starts(t); len(entries) != 1 {
		t.Fatalf("spawned %d children", len(entries))
	}
}

func TestCanceledContextDoesNotSpawn(t *testing.T) {
	t.Parallel()
	var manager Manager
	child := newHelper(t, &manager, 0)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	got, err := manager.Start(ctx, child.spec)
	if !errors.Is(err, context.Canceled) || got != (Observation{State: StateNotStarted}) {
		t.Fatalf("canceled Start() = %+v, %v", got, err)
	}
	if manager.Observe() != got || len(child.starts(t)) != 0 {
		t.Fatal("canceled Start created a child")
	}
	startHelper(t, &manager, child)
	child.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
}

func TestCancelAfterStartDoesNotKillChild(t *testing.T) {
	t.Parallel()
	var manager Manager
	child := newHelper(t, &manager, 0)
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	started, err := manager.Start(ctx, child.spec)
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, func() bool { return len(child.starts(t)) == 1 })
	cancel()
	for deadline := time.Now().Add(100 * time.Millisecond); time.Now().Before(deadline); {
		if got := manager.Observe(); got != started {
			t.Fatalf("caller cancellation changed the running child: %+v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	child.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
}

func TestFastExitAllowsAnotherStart(t *testing.T) {
	t.Parallel()
	var manager Manager
	first := newHelper(t, &manager, 23)
	first.release(t) // The helper can exit as soon as the OS schedules it.
	startHelper(t, &manager, first)
	assertExit(t, waitForExit(t, &manager), 23)
	second := newHelper(t, &manager, 0)
	startHelper(t, &manager, second)
	second.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
}

func TestArgumentsRemainLiteral(t *testing.T) {
	t.Parallel()
	var manager Manager
	child := newHelper(t, &manager, 0)
	want := []string{"value with spaces", "$(not-a-command)", ";", "*", ""}
	child.spec.Args = append(child.spec.Args, want...)
	started := startHelper(t, &manager, child)
	waitFor(t, func() bool { return len(child.starts(t)) == 1 })
	child.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
	if got := child.report(t, started.PID).Args; !reflect.DeepEqual(got, want) {
		t.Fatalf("child arguments = %q, want %q", got, want)
	}
}

func TestStartDoesNotInheritSupervisorEnvironment(t *testing.T) {
	var manager Manager
	child := newHelper(t, &manager, 0)
	private := []string{
		"CPAMP_RUNTIME_TOKEN", "CPAMP_RUNTIME_JOURNAL_PATH", "CPAMP_RUNTIME_IDENTITY",
		"CPAMP_RUNTIME_ADDR", "CPAMP_RUNTIME_GENERATION", "CPAMP_CPA_EXECUTABLE",
		"CPAMP_START_TEST_HELPER", "UNRELATED_PARENT_CREDENTIAL", "HTTP_PROXY",
	}
	for _, key := range private {
		t.Setenv(key, "test-only-private-value")
	}
	allowed := []string{"PATH", "TMP", "TEMP"}
	if runtime.GOOS == "windows" {
		allowed = append(allowed, "USERPROFILE")
	} else {
		allowed = append(allowed, "HOME", "TMPDIR")
	}
	for _, key := range allowed {
		t.Setenv(key, child.dir)
	}
	started := startHelper(t, &manager, child)
	waitFor(t, func() bool { return len(child.starts(t)) == 1 })
	child.release(t)
	assertExit(t, waitForExit(t, &manager), 0)
	report := child.report(t, started.PID)
	for _, key := range private {
		if _, found := report.Environment[key]; found {
			t.Errorf("CPA child inherited private environment variable %s", key)
		}
	}
	for _, key := range allowed {
		if !report.Environment[key] {
			t.Errorf("CPA child did not preserve allowed environment variable %s", key)
		}
	}
}

func TestChildEnvironmentAllowlist(t *testing.T) {
	tests := []struct {
		name    string
		windows bool
		parent  []string
		want    []string
	}{
		{
			name: "Unix names are case sensitive",
			parent: []string{
				"PATH=/bin", "HOME=/home/cpa", "TMPDIR=/tmp/cpa", "TMP=", "TEMP=/tmp/with=equals",
				"Path=/other", "home=/other", "SystemRoot=/other", "USERPROFILE=/other",
				"CPAMP_RUNTIME_TOKEN=secret", "cpamp_runtime_token=secret", "HOME_TOKEN=secret",
				"FUTURE_PRIVATE_SETTING=secret", "HTTP_PROXY=secret", "LD_PRELOAD=secret", "malformed",
			},
			want: []string{"PATH=/bin", "HOME=/home/cpa", "TMPDIR=/tmp/cpa", "TMP=", "TEMP=/tmp/with=equals"},
		},
		{
			name:    "Windows names are case insensitive",
			windows: true,
			parent: []string{
				`Path=C:\bin`, `SystemRoot=C:\Windows`, `wInDiR=C:\Windows`, `UserProfile=C:\Users\cpa`,
				`HomeDrive=C:`, `HomePath=\Users\cpa`, `tMp=C:\tmp`, `TeMp=C:\temp`,
				"HOME=other", "TMPDIR=other", "CpAmP_RuNtImE_ToKeN=secret", "Path_TOKEN=secret",
				"FUTURE_PRIVATE_SETTING=secret", "HTTP_PROXY=secret", `=C:=C:\private`,
			},
			want: []string{
				`Path=C:\bin`, `SystemRoot=C:\Windows`, `wInDiR=C:\Windows`, `UserProfile=C:\Users\cpa`,
				`HomeDrive=C:`, `HomePath=\Users\cpa`, `tMp=C:\tmp`, `TeMp=C:\temp`,
			},
		},
		{name: "empty Unix environment", want: []string{}},
		{name: "empty Windows environment", windows: true, want: []string{}},
		{name: "only private Unix variables", parent: []string{"CPAMP_RUNTIME_TOKEN=secret"}, want: []string{}},
		{name: "only private Windows variables", windows: true, parent: []string{"cpamp_runtime_token=secret"}, want: []string{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := childEnvironment(test.parent, test.windows)
			if got == nil {
				t.Fatal("nil child environment would restore full Supervisor inheritance")
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("child environment = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSignalExitHasNoInventedExitCode(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Windows Kill reports an exit code rather than Unix signal termination")
	}
	t.Parallel()
	var manager Manager
	child := newHelper(t, &manager, 0)
	startHelper(t, &manager, child)
	waitFor(t, func() bool { return len(child.starts(t)) == 1 })
	manager.mu.Lock()
	process := manager.cmd.Process
	manager.mu.Unlock()
	// Only the test owns this cleanup action; the product has no Kill/Stop API.
	if err := process.Kill(); err != nil {
		t.Fatal(err)
	}
	if got := waitForExit(t, &manager); got != (Observation{State: StateExited}) {
		t.Fatalf("signal exit = %+v, want unknown exit code", got)
	}
}

func TestWaitFailureRetainsOwnership(t *testing.T) {
	// Fault-inject a Wait error with no ProcessState using an unstarted Cmd.
	// No real child is created or abandoned by this error-path test.
	cmd := exec.Command("unused-test-command")
	manager := Manager{cmd: cmd, observation: Observation{State: StateRunning, PID: 123}}
	manager.wait(cmd)
	got := manager.Observe()
	if got.State != StateUnknown || got.PID != 123 || got.WaitError == nil || got.ExitCodeKnown {
		t.Fatalf("unconfirmed wait = %+v", got)
	}
	if manager.cmd != cmd {
		t.Fatal("unconfirmed wait released child ownership")
	}
	if _, err := manager.Start(t.Context(), StartSpec{Executable: "unused-test-command"}); !errors.Is(err, ErrStateConflict) {
		t.Fatalf("Start after unconfirmed wait error = %v", err)
	}
}

type helperChild struct {
	spec StartSpec
	dir  string
}

type helperReport struct {
	Args []string
	// Record names and whether the value equals the fixture directory, never
	// arbitrary environment values that could contain Supervisor credentials.
	Environment map[string]bool
}

func newHelper(t *testing.T, manager *Manager, code int) helperChild {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, "starts"), 0o700); err != nil {
		t.Fatal(err)
	}
	child := helperChild{
		dir: dir,
		spec: StartSpec{Executable: executable, Args: []string{
			"-test.run=^TestCPAProcessHelper$", "--", "cpamp-cpa-test-child", dir, strconv.Itoa(code),
		}},
	}
	t.Cleanup(func() {
		child.release(t)
		waitFor(t, func() bool { return manager.Observe().State != StateRunning })
	})
	return child
}

func startHelper(t *testing.T, manager *Manager, child helperChild) Observation {
	t.Helper()
	got, err := manager.Start(t.Context(), child.spec)
	if err != nil || got.State != StateRunning || got.PID <= 0 || got.ExitCodeKnown || got.WaitError != nil {
		t.Fatalf("Start() = %+v, %v", got, err)
	}
	return got
}

func (child helperChild) starts(t *testing.T) []os.DirEntry {
	t.Helper()
	entries, err := os.ReadDir(filepath.Join(child.dir, "starts"))
	if err != nil {
		t.Fatal(err)
	}
	return entries
}

func (child helperChild) report(t *testing.T, pid int) helperReport {
	t.Helper()
	encoded, err := os.ReadFile(filepath.Join(child.dir, "starts", strconv.Itoa(pid)))
	if err != nil {
		t.Fatal(err)
	}
	var report helperReport
	if err := json.Unmarshal(encoded, &report); err != nil {
		t.Fatal(err)
	}
	return report
}

func (child helperChild) release(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(child.dir, "exit"), nil, 0o600); err != nil {
		t.Error(err)
	}
}

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("timed out waiting for helper process observation")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func waitForExit(t *testing.T, manager *Manager) Observation {
	t.Helper()
	waitFor(t, func() bool { return manager.Observe().State == StateExited })
	return manager.Observe()
}

func assertExit(t *testing.T, got Observation, code int) {
	t.Helper()
	if got != (Observation{State: StateExited, ExitCode: code, ExitCodeKnown: true}) {
		t.Fatalf("exit = %+v, want reaped child with code %d", got, code)
	}
}

func TestCPAProcessHelper(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 || len(os.Args) < separator+4 || os.Args[separator+1] != "cpamp-cpa-test-child" {
		return
	}
	dir := os.Args[separator+2]
	code, err := strconv.Atoi(os.Args[separator+3])
	if err != nil {
		os.Exit(96)
	}
	report := helperReport{Args: os.Args[separator+4:], Environment: make(map[string]bool)}
	for _, entry := range os.Environ() {
		key, value, _ := strings.Cut(entry, "=")
		if runtime.GOOS == "windows" {
			key = strings.ToUpper(key)
		}
		report.Environment[key] = value == dir
	}
	encoded, err := json.Marshal(report)
	if err != nil {
		os.Exit(96)
	}
	if err := os.WriteFile(filepath.Join(dir, "starts", strconv.Itoa(os.Getpid())), encoded, 0o600); err != nil {
		os.Exit(96)
	}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(filepath.Join(dir, "exit")); err == nil {
			os.Exit(code)
		}
		time.Sleep(5 * time.Millisecond)
	}
	os.Exit(97)
}
