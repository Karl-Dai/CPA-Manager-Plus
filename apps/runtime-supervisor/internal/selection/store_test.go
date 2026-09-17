package selection

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/artifact"
	runtimeupdate "github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/update"
)

func TestSelectionAbsenceUsesBundledAndCommittedStageSurvivesReopen(t *testing.T) {
	root := t.TempDir()
	stages := newStageStore(t, filepath.Join(root, "artifacts"))
	selectionRoot := filepath.Join(root, "active")
	store, err := NewStore(selectionRoot, stages)
	if err != nil {
		t.Fatal(err)
	}
	bundled := Bundled("/usr/local/bin/cli-proxy-api", "/usr/local/share/cpamp/cpa-artifact.json")
	selected, err := store.Load(bundled)
	if err != nil || selected != bundled {
		t.Fatalf("Load(absent) = %+v, %v", selected, err)
	}

	candidate := seedFinalizedStage(t, filepath.Join(root, "artifacts"), "7.3.4", []byte("candidate-b"))
	if err := store.Commit(candidate); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStore(selectionRoot, stages)
	if err != nil {
		t.Fatal(err)
	}
	selected, err = reopened.Load(bundled)
	if err != nil || selected != candidate {
		t.Fatalf("Load(committed) = %+v, %v, want %+v", selected, err, candidate)
	}
	encoded, err := os.ReadFile(filepath.Join(selectionRoot, selectionName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), "path") || strings.Contains(string(encoded), candidate.ExecutablePath) {
		t.Fatalf("selection leaked path authority: %s", encoded)
	}
}

func TestSelectionPresenceFailsClosedForStrictOrInvalidStageState(t *testing.T) {
	for name, mutate := range map[string]func(*testing.T, string, string){
		"unknown field": func(t *testing.T, selectionRoot, _ string) {
			writeSelection(t, selectionRoot, `{"schemaVersion":1,"engine":"cpa","version":"7.3.4","artifactId":"sha256:`+strings.Repeat("a", 64)+`","path":"/tmp/evil"}`)
		},
		"duplicate field": func(t *testing.T, selectionRoot, _ string) {
			writeSelection(t, selectionRoot, `{"schemaVersion":1,"engine":"cpa","version":"7.3.4","version":"7.3.5","artifactId":"sha256:`+strings.Repeat("a", 64)+`"}`)
		},
		"trailing value": func(t *testing.T, selectionRoot, _ string) {
			writeSelection(t, selectionRoot, `{"schemaVersion":1,"engine":"cpa","version":"7.3.4","artifactId":"sha256:`+strings.Repeat("a", 64)+`"}{}`)
		},
		"oversized": func(t *testing.T, selectionRoot, _ string) {
			writeSelection(t, selectionRoot, strings.Repeat(" ", selectionLimit+1))
		},
		"missing stage": func(t *testing.T, selectionRoot, _ string) {
			writeSelection(t, selectionRoot, `{"schemaVersion":1,"engine":"cpa","version":"7.3.4","artifactId":"sha256:`+strings.Repeat("a", 64)+`"}`)
		},
		"digest mismatch": func(t *testing.T, selectionRoot, stageRoot string) {
			candidate := seedFinalizedStage(t, stageRoot, "7.3.4", []byte("candidate-b"))
			writeSelection(t, selectionRoot, `{"schemaVersion":1,"engine":"cpa","version":"7.3.4","artifactId":"sha256:`+strings.Repeat("a", 64)+`"}`)
			if candidate.ArtifactID == artifact.ID("sha256:"+strings.Repeat("a", 64)) {
				t.Fatal("fixture digest collision")
			}
		},
		"stage tamper": func(t *testing.T, selectionRoot, stageRoot string) {
			candidate := seedFinalizedStage(t, stageRoot, "7.3.4", []byte("candidate-b"))
			if err := os.WriteFile(candidate.ExecutablePath, []byte("tampered"), 0o755); err != nil {
				t.Fatal(err)
			}
			writeSelection(t, selectionRoot, selectionJSON(candidate))
		},
	} {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			stageRoot := filepath.Join(root, "artifacts")
			stages := newStageStore(t, stageRoot)
			selectionRoot := filepath.Join(root, "active")
			if err := os.MkdirAll(selectionRoot, 0o700); err != nil {
				t.Fatal(err)
			}
			mutate(t, selectionRoot, stageRoot)
			store, err := NewStore(selectionRoot, stages)
			if err != nil {
				t.Fatal(err)
			}
			if selected, err := store.Load(Bundled("/bundled", "")); !errors.Is(err, ErrSelectionInvalid) || selected.ExecutablePath != "" {
				t.Fatalf("Load() = %+v, %v", selected, err)
			}
		})
	}
}

func TestSelectionCommitClassifiesPrePublishAndPostPublishAmbiguity(t *testing.T) {
	root := t.TempDir()
	stageRoot := filepath.Join(root, "artifacts")
	stages := newStageStore(t, stageRoot)
	candidate := seedFinalizedStage(t, stageRoot, "7.3.4", []byte("candidate-b"))

	preRoot := filepath.Join(root, "pre")
	pre, err := newStore(preRoot, stages, func(string, string) error { return errors.New("rename denied") }, syncDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if err := pre.Commit(candidate); !errors.Is(err, ErrSelectionNotPublished) {
		t.Fatalf("Commit(pre-publish) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(preRoot, selectionName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-publish selection exists: %v", err)
	}

	ambiguousRoot := filepath.Join(root, "ambiguous")
	ambiguous, err := newStore(ambiguousRoot, stages, os.Rename, func(string) error { return errors.New("sync denied") })
	if err != nil {
		t.Fatal(err)
	}
	if err := ambiguous.Commit(candidate); !errors.Is(err, ErrSelectionPublicationAmbiguous) {
		t.Fatalf("Commit(ambiguous) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(ambiguousRoot, selectionName)); err != nil {
		t.Fatalf("ambiguous publication should exist: %v", err)
	}
}

func newStageStore(t *testing.T, root string) *runtimeupdate.Store {
	t.Helper()
	store, err := runtimeupdate.NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	return store
}

func seedFinalizedStage(t *testing.T, root, version string, executable []byte) Descriptor {
	t.Helper()
	directory := filepath.Join(root, version)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(executable)
	artifactID := artifact.ID("sha256:" + hex.EncodeToString(digest[:]))
	executablePath := filepath.Join(directory, "cli-proxy-api")
	metadataPath := filepath.Join(directory, "artifact.json")
	if err := os.WriteFile(executablePath, executable, 0o755); err != nil {
		t.Fatal(err)
	}
	metadata := `{"schemaVersion":1,"engine":"cpa","version":"` + version + `","artifactId":"` + string(artifactID) + `","sourceArchiveDigest":"sha256:` + strings.Repeat("b", 64) + `"}`
	if err := os.WriteFile(metadataPath, []byte(metadata), 0o444); err != nil {
		t.Fatal(err)
	}
	return Descriptor{
		Source: SourceFinalized, Version: version, ArtifactID: artifactID,
		ExecutablePath: executablePath, MetadataPath: metadataPath,
	}
}

func writeSelection(t *testing.T, root, encoded string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(root, selectionName), []byte(encoded), 0o600); err != nil {
		t.Fatal(err)
	}
}

func selectionJSON(candidate Descriptor) string {
	return `{"schemaVersion":1,"engine":"cpa","version":"` + candidate.Version + `","artifactId":"` + string(candidate.ArtifactID) + `"}`
}
