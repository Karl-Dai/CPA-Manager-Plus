//go:build linux

package cpaprocess

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func TestConfiguredLinuxIdentityAppliesToActualChild(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("actual Linux credential switching requires a privileged Supervisor test process")
	}
	uid := uint32(65534)
	gid := uint32(65534)
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o777); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(directory, "identity")
	manager, err := NewManagerWithIdentity(nil, ChildIdentity{UID: uid, GID: gid})
	if err != nil {
		t.Fatal(err)
	}
	command := fmt.Sprintf("id -u > %q && id -g >> %q", output, output)
	started, err := manager.Start(t.Context(), StartSpec{Executable: "/bin/sh", Args: []string{"-c", command}})
	if err != nil || started.State != StateRunning {
		t.Fatalf("Start() = %+v, %v", started, err)
	}
	waitFor(t, func() bool { return manager.Observe().State == StateExited })
	encoded, err := os.ReadFile(output)
	if err != nil {
		t.Fatal(err)
	}
	want := strconv.FormatUint(uint64(uid), 10) + "\n" + strconv.FormatUint(uint64(gid), 10)
	if strings.TrimSpace(string(encoded)) != want {
		t.Fatalf("child identity = %q, want %q", encoded, want)
	}
}
