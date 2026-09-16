package main

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"
	"unicode"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/readiness"
)

const (
	defaultRuntimeAddr = "127.0.0.1:18318"
	defaultCPAAddr     = "127.0.0.1:8317"
	shutdownTimeout    = 10 * time.Second
)

type config struct {
	addr                string
	runtimeIdentity     string
	runtimeGeneration   uint64
	token               string
	journalPath         string
	cpaExecutable       string
	cpaArtifactManifest string
	cpaAddr             string
}

type generationSource func() (uint64, error)

func main() {
	cfg, err := loadConfig(os.Getenv, randomRuntimeGeneration)
	if err != nil {
		log.Fatalf("configure runtime supervisor: %v", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx, cfg); err != nil {
		log.Fatalf("runtime supervisor: %v", err)
	}
}

func loadConfig(getenv func(string) string, nextGeneration generationSource) (config, error) {
	addr := strings.TrimSpace(getenv("CPAMP_RUNTIME_ADDR"))
	if addr == "" {
		addr = defaultRuntimeAddr
	}
	identity := strings.TrimSpace(getenv("CPAMP_RUNTIME_IDENTITY"))
	if identity == "" {
		return config{}, errors.New("CPAMP_RUNTIME_IDENTITY is required")
	}
	token := getenv("CPAMP_RUNTIME_TOKEN")
	if strings.TrimSpace(token) == "" {
		return config{}, errors.New("CPAMP_RUNTIME_TOKEN is required")
	}
	if strings.IndexFunc(token, unicode.IsSpace) >= 0 {
		return config{}, errors.New("CPAMP_RUNTIME_TOKEN must not contain whitespace")
	}
	journalPath := strings.TrimSpace(getenv("CPAMP_RUNTIME_JOURNAL_PATH"))
	executable := strings.TrimSpace(getenv("CPAMP_CPA_EXECUTABLE"))
	artifactManifest := strings.TrimSpace(getenv("CPAMP_CPA_ARTIFACT_MANIFEST"))
	if err := validateLifecycleConfig(journalPath, executable, artifactManifest); err != nil {
		return config{}, err
	}
	cpaAddr := strings.TrimSpace(getenv("CPAMP_RUNTIME_CPA_ADDR"))
	if cpaAddr == "" {
		cpaAddr = defaultCPAAddr
	}
	if err := readiness.ValidateAddress(cpaAddr); err != nil {
		return config{}, fmt.Errorf("CPAMP_RUNTIME_CPA_ADDR: %w", err)
	}
	var generation uint64
	for generation == 0 {
		var err error
		generation, err = nextGeneration()
		if err != nil {
			return config{}, fmt.Errorf("generate runtime generation: %w", err)
		}
	}
	return config{
		addr:                addr,
		runtimeIdentity:     identity,
		runtimeGeneration:   generation,
		token:               token,
		journalPath:         journalPath,
		cpaExecutable:       executable,
		cpaArtifactManifest: artifactManifest,
		cpaAddr:             cpaAddr,
	}, nil
}

func randomRuntimeGeneration() (uint64, error) {
	var encoded [8]byte
	if _, err := rand.Read(encoded[:]); err != nil {
		return 0, fmt.Errorf("read secure random bytes: %w", err)
	}
	return binary.BigEndian.Uint64(encoded[:]), nil
}

func run(ctx context.Context, cfg config) (err error) {
	handler, err := newRuntimeHandler(ctx, cfg)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, handler.Close()) }()
	listener, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.addr, err)
	}
	log.Printf("cpamp-runtime-supervisor listening on %s", listener.Addr())
	return serve(ctx, listener, handler)
}

func serve(ctx context.Context, listener net.Listener, handler http.Handler) error {
	return serveWithShutdownTimeout(ctx, listener, handler, shutdownTimeout)
}

type lifecycleAdmissionCloser interface {
	CloseAdmission()
}

func serveWithShutdownTimeout(ctx context.Context, listener net.Listener, handler http.Handler, timeout time.Duration) error {
	server := newHTTPServer(handler)
	serveResult := make(chan error, 1)
	go func() {
		err := server.Serve(listener)
		if errors.Is(err, http.ErrServerClosed) {
			err = nil
		}
		serveResult <- err
	}()

	select {
	case err := <-serveResult:
		return err
	case <-ctx.Done():
	}

	if lifecycle, ok := handler.(lifecycleAdmissionCloser); ok {
		lifecycle.CloseAdmission()
	}
	shutdownCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	shutdownErr := server.Shutdown(shutdownCtx)
	var closeErr error
	if shutdownErr != nil {
		closeErr = server.Close()
		if errors.Is(closeErr, http.ErrServerClosed) {
			closeErr = nil
		}
	}
	serveErr := <-serveResult
	if shutdownErr != nil || closeErr != nil || serveErr != nil {
		return errors.Join(shutdownErr, closeErr, serveErr)
	}
	return nil
}

func newHTTPServer(handler http.Handler) *http.Server {
	return &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
		MaxHeaderBytes:    16 << 10,
	}
}
