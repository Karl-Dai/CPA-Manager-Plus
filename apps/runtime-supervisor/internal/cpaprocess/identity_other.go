//go:build !linux

package cpaprocess

import (
	"fmt"
	"os/exec"
)

func applyChildIdentity(_ *exec.Cmd, identity ChildIdentity) error {
	if err := identity.validate(); err != nil {
		return err
	}
	return fmt.Errorf("%w: credential switching is unsupported on this platform", ErrInvalidIdentity)
}
