//go:build linux

package update

import (
	"archive/tar"
	"context"
	"io"
	"os"
	"path/filepath"
	"syscall"
	"testing"
)

func TestExecutionGroupCorridorIsIdempotentAndLeavesInvalidStagePrivate(t *testing.T) {
	root := t.TempDir()
	private := filepath.Join(root, "7.3.2")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	archive := tarGzip(t, archiveEntry{name: stagedExecutableName, typeFlag: tar.TypeReg, data: []byte("binary")})
	release := releaseForArchive("7.3.3", archive)
	final := filepath.Join(root, release.Version)
	t.Cleanup(func() { _ = os.Chmod(final, 0o700) })
	store, err := NewStoreWithExecutionGroup(root, uint32(os.Getgid()))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := store.Stage(t.Context(), release, func(_ context.Context, _ Release, destination io.Writer) error {
		_, writeErr := destination.Write(archive)
		return writeErr
	}); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		store, err = NewStoreWithExecutionGroup(root, uint32(os.Getgid()))
		if err != nil {
			t.Fatal(err)
		}
		if _, err := store.ResolveFinalized(release.Version); err != nil {
			t.Fatal(err)
		}
	}
	assertMode(t, root, 0o710)
	assertMode(t, final, 0o510)
	assertMode(t, private, 0o700)
	info, err := os.Stat(final)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Sys().(*syscall.Stat_t).Gid; got != uint32(os.Getgid()) {
		t.Fatalf("finalized execution GID = %d, want %d", got, os.Getgid())
	}
}
