package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/ingress/internal/ingress"
)

const (
	defaultAddr       = "0.0.0.0:18317"
	defaultManagerURL = "http://cpamp-manager:18317"
	defaultGatewayURL = "http://cpamp-runtime:8317"
)

type config struct {
	addr       string
	managerURL *url.URL
	gatewayURL *url.URL
}

func main() {
	cfg, err := loadConfig(os.Getenv)
	if err != nil {
		log.Fatalf("configure CPAMP Ingress: %v", err)
	}
	logger := log.New(os.Stderr, "cpamp-ingress: ", log.LstdFlags)
	handler := ingress.NewHandler(ingress.NewTransitionalClassifier(), cfg.managerURL, cfg.gatewayURL, logger)
	server := &http.Server{
		Addr:              cfg.addr,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    1 << 20,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	result := make(chan error, 1)
	go func() {
		logger.Printf("listening on %s", cfg.addr)
		result <- server.ListenAndServe()
	}()

	select {
	case err := <-result:
		if !errors.Is(err, http.ErrServerClosed) {
			logger.Fatalf("serve: %v", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Fatalf("shutdown: %v", err)
		}
	}
}

func loadConfig(getenv func(string) string) (config, error) {
	addr := strings.TrimSpace(getenv("CPAMP_INGRESS_ADDR"))
	if addr == "" {
		addr = defaultAddr
	}
	managerURL, err := parseUpstream("CPAMP_MANAGER_URL", getenv("CPAMP_MANAGER_URL"), defaultManagerURL)
	if err != nil {
		return config{}, err
	}
	gatewayURL, err := parseUpstream("CPAMP_GATEWAY_URL", getenv("CPAMP_GATEWAY_URL"), defaultGatewayURL)
	if err != nil {
		return config{}, err
	}
	return config{addr: addr, managerURL: managerURL, gatewayURL: gatewayURL}, nil
}

func parseUpstream(name, value, fallback string) (*url.URL, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		value = fallback
	}
	parsed, err := url.Parse(value)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	if parsed.Scheme != "http" || parsed.Host == "" || parsed.User != nil ||
		(parsed.Path != "" && parsed.Path != "/") || parsed.RawQuery != "" || parsed.Fragment != "" {
		return nil, fmt.Errorf("%s must be an http origin without credentials, path, query, or fragment", name)
	}
	parsed.Path = ""
	return parsed, nil
}
