//go:build linux

package cpaprocess

import (
	"os/exec"
	"syscall"
)

func applyChildIdentity(cmd *exec.Cmd, identity ChildIdentity) error {
	if err := identity.validate(); err != nil {
		return err
	}
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Credential: &syscall.Credential{
			Uid:    identity.UID,
			Gid:    identity.GID,
			Groups: []uint32{identity.GID},
		},
	}
	return nil
}
