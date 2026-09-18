package filetrust

import (
	"errors"
	"io/fs"
	"os"
	"testing"
	"time"
)

type ownerTestInfo struct {
	uid uint32
}

func (info ownerTestInfo) Name() string       { return "persisted" }
func (info ownerTestInfo) Size() int64        { return 0 }
func (info ownerTestInfo) Mode() fs.FileMode  { return 0o600 }
func (info ownerTestInfo) ModTime() time.Time { return time.Time{} }
func (info ownerTestInfo) IsDir() bool        { return false }
func (info ownerTestInfo) Sys() any           { return &struct{ Uid uint32 }{Uid: info.uid} }

func TestRequireCurrentProcessOwner(t *testing.T) {
	effectiveUID := os.Geteuid()
	if effectiveUID < 0 {
		t.Skip("numeric ownership is unavailable")
	}
	if err := RequireCurrentProcessOwner(ownerTestInfo{uid: uint32(effectiveUID)}); err != nil {
		t.Fatalf("current owner rejected: %v", err)
	}
	otherUID := uint32(effectiveUID) + 1
	if otherUID == uint32(effectiveUID) {
		otherUID--
	}
	if err := RequireCurrentProcessOwner(ownerTestInfo{uid: otherUID}); !errors.Is(err, ErrUntrustedOwner) {
		t.Fatalf("different owner error = %v, want %v", err, ErrUntrustedOwner)
	}
}

func TestRequireCurrentProcessOwnerFailsWhenOwnerIsUnavailable(t *testing.T) {
	if os.Geteuid() < 0 {
		t.Skip("numeric ownership is unavailable")
	}
	info := ownerTestInfo{}
	infoWithMissingUID := missingOwnerInfo{ownerTestInfo: info}
	if err := RequireCurrentProcessOwner(infoWithMissingUID); !errors.Is(err, ErrUntrustedOwner) {
		t.Fatalf("missing owner error = %v, want %v", err, ErrUntrustedOwner)
	}
}

func TestRequirePrivateDirectoryPermissions(t *testing.T) {
	if os.Geteuid() < 0 {
		if err := RequirePrivateDirectoryPermissions(permissionTestInfo{mode: 0o777}); err != nil {
			t.Fatalf("platform-native writable directory rejected: %v", err)
		}
		return
	}

	for name, test := range map[string]struct {
		mode    fs.FileMode
		wantErr bool
	}{
		"private":        {mode: 0o700},
		"group writable": {mode: 0o720, wantErr: true},
		"other writable": {mode: 0o702, wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			err := RequirePrivateDirectoryPermissions(permissionTestInfo{mode: test.mode})
			if test.wantErr && !errors.Is(err, ErrUntrustedPermissions) {
				t.Fatalf("error = %v, want %v", err, ErrUntrustedPermissions)
			}
			if !test.wantErr && err != nil {
				t.Fatalf("private directory rejected: %v", err)
			}
		})
	}
}

type missingOwnerInfo struct {
	ownerTestInfo
}

func (missingOwnerInfo) Sys() any { return &struct{ Gid uint32 }{} }

type permissionTestInfo struct {
	ownerTestInfo
	mode fs.FileMode
}

func (info permissionTestInfo) Mode() fs.FileMode { return info.mode }
