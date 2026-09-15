package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
)

const startHelperName = "cpamp-start-test-child.exe"

// A private copy of the test executable selects the helper by filename. The
// configured executable needs neither arguments nor inherited environment.
func TestMain(m *testing.M) {
	if executable, err := os.Executable(); err == nil && filepath.Base(executable) == startHelperName {
		directory := filepath.Dir(executable)
		var names []string
		for _, entry := range os.Environ() {
			name, _, _ := strings.Cut(entry, "=")
			names = append(names, name)
		}
		// Only names are evidence; never serialize inherited credential values.
		if err := os.WriteFile(filepath.Join(directory, "environment-names"), []byte(strings.Join(names, "\n")), 0o600); err != nil {
			os.Exit(96)
		}
		file, err := os.OpenFile(filepath.Join(directory, "spawns"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			os.Exit(96)
		}
		_, _ = fmt.Fprintln(file, os.Getpid())
		_ = file.Close()
		for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
			if _, err := os.Stat(filepath.Join(directory, "exit")); err == nil {
				os.Exit(0)
			}
			time.Sleep(5 * time.Millisecond)
		}
		os.Exit(97)
	}
	os.Exit(m.Run())
}

func TestStartConfigurationIsAllOrNothing(t *testing.T) {
	tests := []struct {
		name, journal, executable string
		wantError                 bool
	}{
		{"read-only", "", "", false},
		{"journal only", "operations.sqlite", "", true},
		{"executable only", "", "cpa", true},
		{"enabled", " operations.sqlite ", " cpa ", false},
		{"invalid executable", "operations.sqlite", "cpa\x00", true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			values := validConfigValues()
			values["CPAMP_RUNTIME_JOURNAL_PATH"] = test.journal
			values["CPAMP_CPA_EXECUTABLE"] = test.executable
			cfg, err := loadConfig(func(key string) string { return values[key] }, fixedGeneration(41))
			if (err != nil) != test.wantError {
				t.Fatalf("loadConfig = %+v, %v", cfg, err)
			}
			if err == nil && (cfg.journalPath != strings.TrimSpace(test.journal) || cfg.cpaExecutable != strings.TrimSpace(test.executable)) {
				t.Fatalf("Start configuration = %+v", cfg)
			}
		})
	}
}

func TestRuntimeStartupFailsIfJournalCannotOpen(t *testing.T) {
	cfg := loadTestConfig(t, fixedGeneration(41))
	cfg.cpaExecutable = "cpa"
	cfg.journalPath = t.TempDir() // A directory cannot be opened as SQLite.
	handler, err := newRuntimeHandler(t.Context(), cfg)
	if err == nil || handler != nil {
		t.Fatalf("startup with unavailable journal = %+v, %v", handler, err)
	}
}

func TestRuntimeStartEndToEnd(t *testing.T) {
	directory := t.TempDir()
	values := validConfigValues()
	values["CPAMP_RUNTIME_ADDR"] = "127.0.0.1:18318"
	values["CPAMP_RUNTIME_JOURNAL_PATH"] = filepath.Join(directory, "runtime", "operations.sqlite")
	values["CPAMP_CPA_EXECUTABLE"] = copyStartHelper(t, directory)
	values["CPAMP_START_TEST_HELPER"] = "must-not-be-inherited"
	values["UNRELATED_PARENT_CREDENTIAL"] = "test-only-private-value"
	for key, value := range values {
		t.Setenv(key, value)
	}
	cfg, err := loadConfig(os.Getenv, fixedGeneration(41))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := newRuntimeHandler(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = handler.Close() })
	exitFile := filepath.Join(directory, "exit")
	t.Cleanup(func() { _ = os.WriteFile(exitFile, nil, 0o600) })
	assertRuntime := func(h http.Handler, generation uint64) {
		t.Helper()
		for _, path := range []string{"/v1/runtime/handshake", "/v1/runtime/status"} {
			got := requestRuntime(t, h, path)
			if got.RuntimeGeneration != generation || !reflect.DeepEqual(got.Capabilities, []string{"start"}) ||
				got.CPAObservedVersion != "" || (path == "/v1/runtime/status" && got.State != "unknown") {
				t.Fatalf("Start changed authority or invented readiness: %+v", got)
			}
		}
	}
	assertRuntime(handler, 41)

	// An unauthorized request cannot create intent or start the local executable.
	req := httptest.NewRequest(http.MethodPost, "/v1/runtime/operations/start", strings.NewReader(startBody("unauthorized", 41)))
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized Start = %d", w.Code)
	}
	if _, err := os.Stat(filepath.Join(directory, "spawns")); !os.IsNotExist(err) {
		t.Fatal("unauthorized Start spawned a child")
	}

	ctx, cancel := context.WithCancel(t.Context())
	w = runtimeStart(handler, ctx, "first", 41)
	if w.Code != http.StatusOK {
		t.Fatalf("Start = %d, %s", w.Code, w.Body.String())
	}
	cancel()
	awaitSpawnCount(t, directory, 1)
	assertRuntime(handler, 41)
	if replay := runtimeStart(handler, t.Context(), "first", 41); replay.Code != http.StatusOK || replay.Body.String() != w.Body.String() {
		t.Fatalf("same-ID replay = %d, %s", replay.Code, replay.Body.String())
	}
	conflict := runtimeStart(handler, t.Context(), "while-running", 41)
	if conflict.Code != http.StatusConflict || !strings.Contains(conflict.Body.String(), "operation_state_conflict") {
		t.Fatalf("new ID while running = %d, %s", conflict.Code, conflict.Body.String())
	}

	reader, err := journal.Open(t.Context(), cfg.journalPath, journal.Options{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reader.Close() })
	for _, id := range []string{"unauthorized", "while-running"} {
		if _, err := reader.Get(t.Context(), cfg.runtimeIdentity, id); !errors.Is(err, journal.ErrOperationNotFound) {
			t.Fatalf("rejected request %q wrote intent: %v", id, err)
		}
	}
	stored, err := reader.Get(t.Context(), cfg.runtimeIdentity, "first")
	if err != nil || stored.State != journal.StateSucceeded || stored.RuntimeGeneration != 41 {
		t.Fatalf("durable Start result = %+v, %v", stored, err)
	}

	// Only a new ID after confirmed reap can spawn again. An early retry must
	// remain a precondition conflict and must not consume that new ID.
	if err := os.WriteFile(exitFile, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	for deadline := time.Now().Add(10 * time.Second); ; {
		next := runtimeStart(handler, t.Context(), "after-exit", 41)
		if next.Code == http.StatusOK {
			break
		}
		if next.Code != http.StatusConflict || time.Now().After(deadline) {
			t.Fatalf("Start after exit = %d, %s", next.Code, next.Body.String())
		}
		time.Sleep(5 * time.Millisecond)
	}
	awaitSpawnCount(t, directory, 2)
	assertRuntime(handler, 41)
	if err := handler.Close(); err != nil {
		t.Fatal(err)
	}
	if closed := runtimeStart(handler, t.Context(), "closed", 41); closed.Code != http.StatusServiceUnavailable {
		t.Fatalf("request after journal shutdown = %d", closed.Code)
	}

	// A fresh Supervisor generation reopens the same durable namespace.
	cfg.runtimeGeneration = 42
	restarted, err := newRuntimeHandler(t.Context(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = restarted.Close() })
	assertRuntime(restarted, 42)
	if stale := runtimeStart(restarted, t.Context(), "first", 41); stale.Code != http.StatusConflict || !strings.Contains(stale.Body.String(), "stale_runtime_generation") {
		t.Fatalf("stale replay = %d, %s", stale.Code, stale.Body.String())
	}
	replay := runtimeStart(restarted, t.Context(), "first", 42)
	var result struct {
		RuntimeGeneration uint64        `json:"runtimeGeneration"`
		State             journal.State `json:"state"`
	}
	if err := json.Unmarshal(replay.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if replay.Code != http.StatusOK || result.RuntimeGeneration != 41 || result.State != journal.StateSucceeded {
		t.Fatalf("cross-generation replay = %d, %s", replay.Code, replay.Body.String())
	}
	awaitSpawnCount(t, directory, 2)
}

func copyStartHelper(t *testing.T, directory string) string {
	t.Helper()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	source, err := os.Open(executable)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	name := filepath.Join(directory, startHelperName)
	destination, err := os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o700)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(destination, source)
	if err := errors.Join(copyErr, destination.Close()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = os.WriteFile(filepath.Join(directory, "exit"), nil, 0o600)
		// Windows retains an executable file until the child actually exits.
		for deadline := time.Now().Add(10 * time.Second); ; {
			err := os.Remove(name)
			if err == nil || os.IsNotExist(err) {
				return
			}
			if time.Now().After(deadline) {
				t.Errorf("remove helper executable: %v", err)
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	})
	return name
}

func startBody(id string, generation uint64) string {
	return fmt.Sprintf(`{"operationId":%q,"expectedRuntimeIdentity":"runtime-01","expectedRuntimeGeneration":%d}`, id, generation)
}

func runtimeStart(handler http.Handler, ctx context.Context, id string, generation uint64) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/v1/runtime/operations/start", strings.NewReader(startBody(id, generation))).WithContext(ctx)
	req.Header.Set("Authorization", "Bearer runtime-token")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func awaitSpawnCount(t *testing.T, directory string, count int) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		data, err := os.ReadFile(filepath.Join(directory, "spawns"))
		if err == nil {
			got := len(strings.Fields(string(data)))
			if got > count {
				t.Fatalf("spawn count = %d, want %d", got, count)
			}
			if got == count {
				names, err := os.ReadFile(filepath.Join(directory, "environment-names"))
				if err != nil {
					t.Fatal(err)
				}
				for _, name := range strings.Fields(string(names)) {
					name = strings.ToUpper(name)
					if strings.HasPrefix(name, "CPAMP_") || name == "UNRELATED_PARENT_CREDENTIAL" {
						t.Errorf("typed Start passed private environment variable %s to CPA", name)
					}
				}
				return
			}
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("did not observe %d child spawns", count)
}
