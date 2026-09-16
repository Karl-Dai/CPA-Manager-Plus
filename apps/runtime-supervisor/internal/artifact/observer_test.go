package artifact

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestArtifactIDValidation(t *testing.T) {
	valid := ID("sha256:" + strings.Repeat("a", 64))
	if !valid.IsValid() {
		t.Fatalf("canonical artifact ID rejected: %q", valid)
	}
	for _, invalid := range []ID{
		"", ID("sha256:" + strings.Repeat("a", 63)), ID("sha256:" + strings.Repeat("a", 65)),
		ID("SHA256:" + strings.Repeat("a", 64)), ID("sha256:" + strings.Repeat("A", 64)),
		ID("sha256:" + strings.Repeat("g", 64)), ID(strings.Repeat("a", 64)),
	} {
		if invalid.IsValid() {
			t.Fatalf("noncanonical artifact ID accepted: %q", invalid)
		}
	}
}

func TestObserverBindsTrustedVersionToExactExecutableDigest(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "gateway")
	manifestPath := filepath.Join(directory, "artifact.json")
	bytes := []byte("exact-gateway-bytes")
	writeExecutable(t, executable, bytes)
	const id = ID("sha256:6332b6a477ee0720b295d407ce045a3d62aff1dc0bd26b5c8eb7250b5bd9d06b")
	if calculated := artifactID(bytes); calculated != id {
		t.Fatalf("known SHA-256 = %q, want %q", calculated, id)
	}
	writeManifest(t, manifestPath, "7.3.3", id)

	observer := NewObserver(executable, manifestPath)
	if err := observer.Refresh(); err != nil {
		t.Fatal(err)
	}
	observed := observer.Observation()
	if observed == nil || observed.Engine != EngineCPA || observed.ArtifactID != id || observed.Version != "7.3.3" {
		t.Fatalf("observation = %#v", observed)
	}
}

func TestObserverWithholdsUntrustedVersionButPreservesExactDigest(t *testing.T) {
	directory := t.TempDir()
	executable := filepath.Join(directory, "gateway")
	bytes := []byte("exact-gateway-bytes")
	writeExecutable(t, executable, bytes)
	wantID := artifactID(bytes)

	tests := []struct {
		name      string
		manifest  func(string)
		wantError error
	}{
		{name: "missing", wantError: ErrManifestUnavailable},
		{name: "malformed", manifest: func(path string) { writeFile(t, path, []byte(`{"schemaVersion":`)) }, wantError: ErrManifestInvalid},
		{name: "unknown field", manifest: func(path string) {
			writeFile(t, path, []byte(`{"schemaVersion":1,"engine":"cpa","version":"7.3.3","artifactId":"`+wantID+`","extra":true}`))
		}, wantError: ErrManifestInvalid},
		{name: "same version wrong digest", manifest: func(path string) { writeManifest(t, path, "7.3.3", ID("sha256:"+strings.Repeat("0", 64))) }, wantError: ErrManifestMismatch},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			manifestPath := filepath.Join(t.TempDir(), "artifact.json")
			if test.manifest != nil {
				test.manifest(manifestPath)
			}
			observer := NewObserver(executable, manifestPath)
			err := observer.Refresh()
			if !errors.Is(err, test.wantError) || !errors.Is(observer.LastError(), test.wantError) {
				t.Fatalf("Refresh() error = %v, last = %v", err, observer.LastError())
			}
			observed := observer.Observation()
			if observed == nil || observed.ArtifactID != wantID || observed.Version != "" {
				t.Fatalf("untrusted observation = %#v", observed)
			}
		})
	}
}

func TestObserverDoesNotInventIdentityForUnavailableExecutable(t *testing.T) {
	manifestPath := filepath.Join(t.TempDir(), "artifact.json")
	writeManifest(t, manifestPath, "7.3.3", ID("sha256:"+strings.Repeat("0", 64)))
	observer := NewObserver(filepath.Join(t.TempDir(), "missing"), manifestPath)
	if err := observer.Refresh(); !errors.Is(err, ErrExecutableUnavailable) || observer.Observation() != nil {
		t.Fatalf("missing executable observation = %#v, error = %v", observer.Observation(), err)
	}

	unreadable := newObserver("gateway", manifestPath, func(string) (io.ReadCloser, error) {
		return nil, os.ErrPermission
	}, os.ReadFile)
	if err := unreadable.Refresh(); !errors.Is(err, ErrExecutableUnavailable) || unreadable.Observation() != nil {
		t.Fatalf("unreadable executable observation = %#v, error = %v", unreadable.Observation(), err)
	}
}

func TestObserverFollowsConfiguredSymlinkToExecutedBytes(t *testing.T) {
	directory := t.TempDir()
	target := filepath.Join(directory, "gateway-v1")
	symlink := filepath.Join(directory, "gateway-active")
	manifestPath := filepath.Join(directory, "artifact.json")
	bytes := []byte("symlink-target-bytes")
	writeExecutable(t, target, bytes)
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	writeManifest(t, manifestPath, "7.3.3", artifactID(bytes))
	observer := NewObserver(symlink, manifestPath)
	if err := observer.Refresh(); err != nil {
		t.Fatal(err)
	}
	if got := observer.Observation(); got == nil || got.ArtifactID != artifactID(bytes) || got.Version != "7.3.3" {
		t.Fatalf("symlink observation = %#v", got)
	}
}

func TestStatusReadsCachedObservationWithoutRehashing(t *testing.T) {
	bytes := []byte("cached-executable")
	id := artifactID(bytes)
	manifest := fmt.Sprintf(`{"schemaVersion":1,"engine":"cpa","version":"7.3.3","artifactId":%q}`, id)
	var executableReads, manifestReads int
	observer := newObserver("gateway", "manifest", func(string) (io.ReadCloser, error) {
		executableReads++
		return io.NopCloser(strings.NewReader(string(bytes))), nil
	}, func(string) ([]byte, error) {
		manifestReads++
		return []byte(manifest), nil
	})
	if err := observer.Refresh(); err != nil {
		t.Fatal(err)
	}
	for range 20 {
		if got := observer.Observation(); got == nil || got.ArtifactID != id {
			t.Fatalf("cached observation = %#v", got)
		}
	}
	if executableReads != 1 || manifestReads != 1 {
		t.Fatalf("status cache performed %d executable and %d manifest reads", executableReads, manifestReads)
	}
}

func artifactID(bytes []byte) ID {
	digest := sha256.Sum256(bytes)
	return ID("sha256:" + hex.EncodeToString(digest[:]))
}

func writeExecutable(t *testing.T, path string, bytes []byte) {
	t.Helper()
	if err := os.WriteFile(path, bytes, 0o700); err != nil {
		t.Fatal(err)
	}
}

func writeManifest(t *testing.T, path, version string, id ID) {
	t.Helper()
	writeFile(t, path, []byte(fmt.Sprintf(`{"schemaVersion":1,"engine":"cpa","version":%q,"artifactId":%q}`, version, id)))
}

func writeFile(t *testing.T, path string, bytes []byte) {
	t.Helper()
	if err := os.WriteFile(path, bytes, 0o600); err != nil {
		t.Fatal(err)
	}
}
