// Package cpaprocess owns the Supervisor's in-memory CPA child process.
// It exposes no product entry point. Future lifecycle callers must durably
// record intent before calling Start; this primitive does not own that policy.
package cpaprocess

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"strings"
	"sync"
)

var (
	ErrStateConflict = errors.New("CPA child is already owned")
	ErrInvalidSpec   = errors.New("invalid CPA start spec")
	ErrSpawnFailed   = errors.New("CPA child spawn failed")
)

// StartSpec supplies an executable and literal arguments, never a shell command.
// The child inherits the Supervisor's environment and working directory. Standard
// input/output/error use os/exec's null-device defaults; no logs are collected.
type StartSpec struct {
	Executable string
	Args       []string
}

type State string

const (
	StateNotStarted State = "not_started"
	StateRunning    State = "running"
	StateExited     State = "exited"
	StateUnknown    State = "unknown"
)

// Observation contains process facts, never CPA readiness. PID is cleared after
// a confirmed exit; it is only an in-memory observation, not restart authority.
// ExitCode is meaningful only when ExitCodeKnown is true. WaitError reports an
// unexpected wait failure, not an ordinary non-zero exit or signal termination.
// If Wait cannot confirm termination, StateUnknown retains ownership and the
// last observed PID, and subsequent Start calls fail closed.
type Observation struct {
	State         State
	PID           int
	ExitCode      int
	ExitCodeKnown bool
	WaitError     error
}

// Manager owns at most one CPA child. Use one Manager per Supervisor incarnation.
// Its zero value is ready to use; it must not be copied after first use.
type Manager struct {
	mu          sync.Mutex
	cmd         *exec.Cmd
	observation Observation
}

// Start serializes validation, spawn and ownership publication. Context is
// checked before spawn, including after executable lookup. It does not bound
// the OS spawn call or the child's lifetime: once spawn succeeds, cancellation
// does not kill the child or turn the successful Start into an error.
// On failure the previous observation is unchanged. A new Start is allowed
// only after the previous child has been waited/reaped, never automatically.
func (m *Manager) Start(ctx context.Context, spec StartSpec) (Observation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if err := ctx.Err(); err != nil {
		return m.observeLocked(), err
	}
	if m.cmd != nil {
		return m.observeLocked(), ErrStateConflict
	}
	if strings.TrimSpace(spec.Executable) == "" || strings.ContainsRune(spec.Executable, '\x00') {
		return m.observeLocked(), fmt.Errorf("%w: executable is required and must not contain NUL", ErrInvalidSpec)
	}
	for _, arg := range spec.Args {
		if strings.ContainsRune(arg, '\x00') {
			return m.observeLocked(), fmt.Errorf("%w: arguments must not contain NUL", ErrInvalidSpec)
		}
	}

	cmd := exec.Command(spec.Executable, spec.Args...)
	if err := ctx.Err(); err != nil {
		return m.observeLocked(), err
	}
	if err := cmd.Start(); err != nil {
		return m.observeLocked(), fmt.Errorf("%w: %w", ErrSpawnFailed, err)
	}
	m.cmd = cmd
	m.observation = Observation{State: StateRunning, PID: cmd.Process.Pid}
	go m.wait(cmd)
	return m.observation, nil
}

// Observe returns a snapshot. Running means spawn succeeded and exit has not
// yet been observed by Wait; a fast exit can precede its observation.
func (m *Manager) Observe() Observation {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.observeLocked()
}

func (m *Manager) observeLocked() Observation {
	if m.observation.State == "" {
		return Observation{State: StateNotStarted}
	}
	return m.observation
}

func (m *Manager) wait(cmd *exec.Cmd) {
	err := cmd.Wait()
	m.mu.Lock()
	defer m.mu.Unlock()

	if cmd.ProcessState == nil {
		// An OS wait failure is not proof the child exited. Retain the handle
		// reservation so an unconfirmed exit cannot permit a second child.
		m.observation.State = StateUnknown
		m.observation.WaitError = err
		return
	}
	m.cmd = nil
	m.observation = Observation{State: StateExited}
	if code := cmd.ProcessState.ExitCode(); code >= 0 {
		m.observation.ExitCode = code
		m.observation.ExitCodeKnown = true
	}
	var exitErr *exec.ExitError
	if err != nil && !errors.As(err, &exitErr) {
		m.observation.WaitError = err
	}
}
