package main

import (
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
)

func TestCPAStartAndExitPreserveSupervisorGeneration(t *testing.T) {
	var generationCalls int
	cfg := loadTestConfig(t, func() (uint64, error) {
		generationCalls++
		return 41, nil
	})
	handler, err := newRuntimeHandler(cfg)
	if err != nil {
		t.Fatal(err)
	}
	assertAuthority := func() {
		t.Helper()
		if cfg.runtimeGeneration != 41 || generationCalls != 1 {
			t.Fatalf("generation = %d, samples = %d", cfg.runtimeGeneration, generationCalls)
		}
		for _, path := range []string{"/v1/runtime/handshake", "/v1/runtime/status"} {
			response := requestRuntime(t, handler, path)
			if response.RuntimeGeneration != 41 || response.RuntimeIdentity != cfg.runtimeIdentity ||
				len(response.Capabilities) != 0 || response.CPAObservedVersion != "" {
				t.Fatalf("%s metadata changed with CPA lifecycle: %+v", path, response)
			}
			if path == "/v1/runtime/status" && response.State != "unknown" {
				t.Fatalf("private process observation became Runtime readiness: %+v", response)
			}
		}
	}
	assertAuthority()
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	var child cpaprocess.Manager
	for range 2 {
		exitFile := filepath.Join(t.TempDir(), "exit")
		t.Cleanup(func() {
			if err := os.WriteFile(exitFile, nil, 0o600); err != nil {
				t.Error(err)
			}
			if child.Observe().State == cpaprocess.StateRunning {
				waitForCPAExit(t, &child)
			}
		})
		got, err := child.Start(t.Context(), cpaprocess.StartSpec{
			Executable: executable,
			Args:       []string{"-test.run=^TestRuntimeGenerationChild$", "--", "cpamp-generation-test-child", exitFile},
		})
		if err != nil || got.State != cpaprocess.StateRunning || got.PID <= 0 {
			t.Fatalf("Start() = %+v, %v", got, err)
		}
		assertAuthority()
		if err := os.WriteFile(exitFile, nil, 0o600); err != nil {
			t.Fatal(err)
		}
		waitForCPAExit(t, &child)
		assertAuthority()
	}
}

func waitForCPAExit(t *testing.T, child *cpaprocess.Manager) {
	t.Helper()
	for deadline := time.Now().Add(10 * time.Second); time.Now().Before(deadline); {
		got := child.Observe()
		if got.State == cpaprocess.StateExited {
			if got.PID != 0 || !got.ExitCodeKnown || got.ExitCode != 0 || got.WaitError != nil {
				t.Fatalf("child exit = %+v", got)
			}
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for CPA child exit: %+v", child.Observe())
}

func TestRuntimeGenerationChild(t *testing.T) {
	separator := slices.Index(os.Args, "--")
	if separator < 0 || len(os.Args) != separator+3 || os.Args[separator+1] != "cpamp-generation-test-child" {
		return
	}
	for deadline := time.Now().Add(15 * time.Second); time.Now().Before(deadline); {
		if _, err := os.Stat(os.Args[separator+2]); err == nil {
			os.Exit(0)
		}
		time.Sleep(5 * time.Millisecond)
	}
	os.Exit(97)
}
