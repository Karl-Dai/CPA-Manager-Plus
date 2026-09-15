package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/journal"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/lifecycle"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/protocol"
	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/readiness"
)

type runtimeHandler struct {
	http.Handler
	executor *lifecycle.Executor
}

func validateLifecycleConfig(journalPath, executable string) error {
	if (journalPath == "") != (executable == "") {
		return errors.New("CPAMP_RUNTIME_JOURNAL_PATH and CPAMP_CPA_EXECUTABLE must be configured together")
	}
	if strings.ContainsRune(executable, '\x00') {
		return errors.New("CPAMP_CPA_EXECUTABLE must not contain NUL")
	}
	return nil
}

// newRuntimeHandler opens the private journal once at Supervisor startup. HTTP
// submissions share this resource and the same child ownership/serialization.
func newRuntimeHandler(ctx context.Context, cfg config) (*runtimeHandler, error) {
	if err := validateLifecycleConfig(cfg.journalPath, cfg.cpaExecutable); err != nil {
		return nil, err
	}
	if err := readiness.ValidateAddress(cfg.cpaAddr); err != nil {
		return nil, fmt.Errorf("CPAMP_RUNTIME_CPA_ADDR: %w", err)
	}
	settings := protocol.Config{
		RuntimeIdentity:   cfg.runtimeIdentity,
		RuntimeGeneration: cfg.runtimeGeneration,
		Token:             cfg.token,
	}
	runtime := &runtimeHandler{}
	if cfg.journalPath != "" {
		child := &cpaprocess.Manager{}
		observer, err := readiness.New(child, cfg.cpaAddr)
		if err != nil {
			return nil, err
		}
		store, err := journal.Open(ctx, cfg.journalPath, journal.Options{})
		if err != nil {
			return nil, fmt.Errorf("open operation journal: %w", err)
		}
		runtime.executor, err = lifecycle.NewExecutor(journal.Authority{
			RuntimeIdentity:   strings.TrimSpace(cfg.runtimeIdentity),
			RuntimeGeneration: cfg.runtimeGeneration,
		}, store, child, cfg.cpaExecutable)
		if err != nil {
			return nil, errors.Join(err, store.Close())
		}
		settings.Start = runtime.executor
		settings.Stop = runtime.executor
		settings.Restart = runtime.executor
		settings.Status = observer
		settings.Recovery = runtime.executor
	}
	handler, err := protocol.NewHandler(settings)
	if err != nil {
		return nil, errors.Join(fmt.Errorf("configure Runtime Protocol: %w", err), runtime.Close())
	}
	runtime.Handler = handler
	return runtime, nil
}

func (r *runtimeHandler) Close() error {
	if r.executor == nil {
		return nil
	}
	return r.executor.Close()
}

func (r *runtimeHandler) CloseAdmission() {
	if r.executor != nil {
		r.executor.CloseAdmission()
	}
}
