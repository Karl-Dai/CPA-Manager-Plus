package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type archiveEntry struct {
	name         string
	typeFlag     byte
	data         []byte
	declaredSize int64
	linkName     string
}

func TestStoreStagesOnlyExactExecutableWithExactIdentity(t *testing.T) {
	binary := []byte("exact staged executable bytes")
	archive := tarGzip(t,
		archiveEntry{name: "README.md", typeFlag: tar.TypeReg, data: []byte("ignored")},
		archiveEntry{name: stagedExecutableName, typeFlag: tar.TypeReg, data: binary},
		archiveEntry{name: "config.example.yaml", typeFlag: tar.TypeReg, data: []byte("ignored")},
	)
	release := releaseForArchive("7.3.3", archive)
	store, err := NewStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var downloads int
	metadata, err := store.Stage(t.Context(), release, func(_ context.Context, got Release, destination io.Writer) error {
		downloads++
		if got != release {
			t.Fatalf("download release = %#v", got)
		}
		_, err := destination.Write(archive)
		return err
	})
	if err != nil {
		t.Fatalf("Stage() error = %v", err)
	}
	executableDigest := sha256.Sum256(binary)
	wantArtifactID := "sha256:" + hex.EncodeToString(executableDigest[:])
	if downloads != 1 || string(metadata.ArtifactID) != wantArtifactID ||
		metadata.SourceArchiveDigest != release.ArchiveDigest || metadata.Version != release.Version {
		t.Fatalf("Stage() downloads=%d metadata=%#v", downloads, metadata)
	}
	final := filepath.Join(store.root, release.Version)
	entries, err := os.ReadDir(final)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 2 {
		t.Fatalf("final entries = %v", entries)
	}
	gotBinary, err := os.ReadFile(filepath.Join(final, stagedExecutableName))
	if err != nil || !bytes.Equal(gotBinary, binary) {
		t.Fatalf("staged executable = %q, %v", gotBinary, err)
	}
	assertMode(t, filepath.Join(final, stagedExecutableName), 0o755)
	assertMode(t, filepath.Join(final, stagedMetadataName), 0o444)
	stored, err := readMetadata(filepath.Join(final, stagedMetadataName))
	if err != nil || stored != metadata {
		t.Fatalf("stored metadata = %#v, %v", stored, err)
	}
}

func TestStoreRejectsUnsafeOrInvalidArchivesAndCleansTemporaryState(t *testing.T) {
	oversize := archiveEntry{name: stagedExecutableName, typeFlag: tar.TypeReg, declaredSize: maxExtractedBinaryBytes + 1}
	tests := map[string][]archiveEntry{
		"missing executable": {{name: "README.md", typeFlag: tar.TypeReg, data: []byte("x")}},
		"duplicate executable": {
			{name: stagedExecutableName, typeFlag: tar.TypeReg, data: []byte("one")},
			{name: stagedExecutableName, typeFlag: tar.TypeReg, data: []byte("two")},
		},
		"traversal":              {{name: "../outside", typeFlag: tar.TypeReg, data: []byte("x")}},
		"absolute":               {{name: "/tmp/outside", typeFlag: tar.TypeReg, data: []byte("x")}},
		"path alias":             {{name: "./cli-proxy-api", typeFlag: tar.TypeReg, data: []byte("x")}},
		"backslash path":         {{name: `dir\cli-proxy-api`, typeFlag: tar.TypeReg, data: []byte("x")}},
		"symlink target":         {{name: stagedExecutableName, typeFlag: tar.TypeSymlink, linkName: "elsewhere"}},
		"hardlink target":        {{name: stagedExecutableName, typeFlag: tar.TypeLink, linkName: "elsewhere"}},
		"fifo target":            {{name: stagedExecutableName, typeFlag: tar.TypeFifo}},
		"device target":          {{name: stagedExecutableName, typeFlag: tar.TypeChar}},
		"empty executable":       {{name: stagedExecutableName, typeFlag: tar.TypeReg}},
		"oversize binary":        {oversize},
		"oversize ignored entry": {{name: "README.md", typeFlag: tar.TypeReg, declaredSize: maxExpandedArchiveBytes + 1}},
	}
	for name, entries := range tests {
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			store, err := NewStore(root)
			if err != nil {
				t.Fatal(err)
			}
			archive := tarGzip(t, entries...)
			release := releaseForArchive("7.3.3", archive)
			_, err = store.Stage(t.Context(), release, func(_ context.Context, _ Release, destination io.Writer) error {
				_, writeErr := destination.Write(archive)
				return writeErr
			})
			if !errors.Is(err, ErrStaging) {
				t.Fatalf("Stage() error = %v", err)
			}
			if _, err := os.Stat(filepath.Join(root, release.Version)); !errors.Is(err, os.ErrNotExist) {
				t.Fatalf("invalid archive published final stage: %v", err)
			}
			entries, err := os.ReadDir(root)
			if err != nil || len(entries) != 0 {
				t.Fatalf("temporary state remains: %v, %v", entries, err)
			}
		})
	}
}

func TestStoreReusesOnlyVerifiedImmutableFinalStageAcrossReopen(t *testing.T) {
	root := t.TempDir()
	archive := tarGzip(t, archiveEntry{name: stagedExecutableName, typeFlag: tar.TypeReg, data: []byte("binary")})
	release := releaseForArchive("7.3.3", archive)
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	downloads := 0
	download := func(_ context.Context, _ Release, destination io.Writer) error {
		downloads++
		_, err := destination.Write(archive)
		return err
	}
	want, err := store.Stage(t.Context(), release, download)
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	got, err := reopened.Stage(t.Context(), release, download)
	if err != nil || got != want || downloads != 1 {
		t.Fatalf("reopened Stage() = %#v downloads=%d error=%v", got, downloads, err)
	}

	if err := os.WriteFile(filepath.Join(root, release.Version, stagedExecutableName), []byte("tampered"), 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := reopened.Stage(t.Context(), release, download); !errors.Is(err, ErrStageConflict) || downloads != 1 {
		t.Fatalf("corrupt finalized stage error=%v downloads=%d", err, downloads)
	}
}

func TestStoreRejectsChangedOfficialSourceWithoutOverwrite(t *testing.T) {
	root := t.TempDir()
	archive := tarGzip(t, archiveEntry{name: stagedExecutableName, typeFlag: tar.TypeReg, data: []byte("binary")})
	release := releaseForArchive("7.3.3", archive)
	store, err := NewStore(root)
	if err != nil {
		t.Fatal(err)
	}
	download := func(_ context.Context, _ Release, destination io.Writer) error {
		_, err := destination.Write(archive)
		return err
	}
	if _, err := store.Stage(t.Context(), release, download); err != nil {
		t.Fatal(err)
	}
	original, err := os.ReadFile(filepath.Join(root, release.Version, stagedExecutableName))
	if err != nil {
		t.Fatal(err)
	}
	changed := release
	changed.ArchiveDigest = "sha256:" + strings.Repeat("f", 64)
	called := false
	if _, err := store.Stage(t.Context(), changed, func(context.Context, Release, io.Writer) error {
		called = true
		return nil
	}); !errors.Is(err, ErrStageConflict) || called {
		t.Fatalf("changed source Stage() error=%v downloadCalled=%v", err, called)
	}
	after, err := os.ReadFile(filepath.Join(root, release.Version, stagedExecutableName))
	if err != nil || !bytes.Equal(after, original) {
		t.Fatalf("finalized stage was overwritten: %q, %v", after, err)
	}
}

func TestNewStoreCleansOnlyIncompleteTemporaryState(t *testing.T) {
	root := t.TempDir()
	if err := os.Mkdir(filepath.Join(root, temporaryPrefix+"crash"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(root, "7.3.3"), 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := NewStore(root); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(root, temporaryPrefix+"crash")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("temporary crash state remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(root, "7.3.3")); err != nil {
		t.Fatalf("finalized directory was removed: %v", err)
	}
}

func releaseForArchive(version string, archive []byte) Release {
	digest := sha256.Sum256(archive)
	return Release{
		Version:       version,
		AssetName:     "CLIProxyAPI_" + version + "_linux_amd64.tar.gz",
		AssetURL:      "https://github.com/router-for-me/CLIProxyAPI/releases/download/v" + version + "/CLIProxyAPI_" + version + "_linux_amd64.tar.gz",
		ArchiveDigest: "sha256:" + hex.EncodeToString(digest[:]),
		ArchiveSize:   int64(len(archive)),
	}
}

func tarGzip(t *testing.T, entries ...archiveEntry) []byte {
	t.Helper()
	var encoded bytes.Buffer
	gzipWriter := gzip.NewWriter(&encoded)
	tarWriter := tar.NewWriter(gzipWriter)
	for _, entry := range entries {
		typeFlag := entry.typeFlag
		if typeFlag == 0 {
			typeFlag = tar.TypeReg
		}
		size := int64(len(entry.data))
		if entry.declaredSize != 0 {
			size = entry.declaredSize
		}
		header := &tar.Header{Name: entry.name, Typeflag: typeFlag, Mode: 0o755, Size: size, Linkname: entry.linkName}
		if err := tarWriter.WriteHeader(header); err != nil {
			t.Fatalf("write tar header: %v", err)
		}
		if len(entry.data) > 0 {
			if _, err := tarWriter.Write(entry.data); err != nil {
				t.Fatalf("write tar data: %v", err)
			}
		}
	}
	_ = tarWriter.Close()
	if err := gzipWriter.Close(); err != nil {
		t.Fatalf("close gzip: %v", err)
	}
	return encoded.Bytes()
}

func assertMode(t *testing.T, name string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Fatalf("%s mode = %o, want %o", name, info.Mode().Perm(), want)
	}
}
