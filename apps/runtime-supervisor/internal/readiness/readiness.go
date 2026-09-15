// Package readiness observes the owned CPA child and its expected local HTTP
// listener. It has no lifecycle, journal, product configuration or secret authority.
package readiness

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
)

type State string

const (
	Unknown  State = "unknown"
	Offline  State = "offline"
	Starting State = "starting"
	Ready    State = "ready"

	probeTimeout = time.Second
)

type processObserver interface {
	Observe() cpaprocess.Observation
}

// Observer computes one observation per call; it does not poll or cache results.
type Observer struct {
	process processObserver
	target  string
	client  *http.Client
}

// ValidateAddress accepts only a loopback literal or localhost and a numeric
// port in 1..65535. URLs, credentials, paths, queries and remote hosts are invalid.
func ValidateAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return errors.New("CPA address must be a loopback host:port")
	}
	ip := net.ParseIP(host)
	if !strings.EqualFold(host, "localhost") && (ip == nil || !ip.IsLoopback()) {
		return errors.New("CPA address must use a loopback literal or localhost")
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return errors.New("CPA address port must be numeric and in 1..65535")
		}
	}
	if number, err := strconv.ParseUint(port, 10, 16); err != nil || number == 0 {
		return errors.New("CPA address port must be numeric and in 1..65535")
	}
	return nil
}

func New(process processObserver, address string) (*Observer, error) {
	if process == nil {
		return nil, errors.New("readiness requires a CPA process observer")
	}
	if err := ValidateAddress(address); err != nil {
		return nil, err
	}
	target := url.URL{Scheme: "http", Host: address, Path: "/healthz"}
	return &Observer{
		process: process,
		target:  target.String(),
		client: &http.Client{
			Timeout: probeTimeout,
			Transport: &http.Transport{
				Proxy:                  nil,
				DialContext:            (&net.Dialer{Timeout: probeTimeout}).DialContext,
				DisableKeepAlives:      true,
				MaxResponseHeaderBytes: 8 << 10,
			},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		},
	}, nil
}

func (o *Observer) Observe(ctx context.Context) State {
	before := o.process.Observe()
	if state := processState(before); state != Starting {
		return state
	}
	healthy := o.healthy(ctx)
	after := o.process.Observe()
	if state := processState(after); state != Starting {
		return state
	}
	// A successful probe of A cannot make replacement B ready, even if the OS
	// reuses a PID. Only a later probe correlated to B can establish its readiness.
	if healthy && before.InstanceID == after.InstanceID {
		return Ready
	}
	return Starting
}

func processState(observation cpaprocess.Observation) State {
	switch observation.State {
	case cpaprocess.StateNotStarted, cpaprocess.StateExited:
		return Offline
	case cpaprocess.StateRunning:
		if observation.InstanceID != 0 {
			return Starting
		}
	}
	return Unknown
}

func (o *Observer) healthy(ctx context.Context) bool {
	request, err := http.NewRequestWithContext(ctx, http.MethodHead, o.target, nil)
	if err != nil {
		return false
	}
	response, err := o.client.Do(request)
	if err != nil {
		return false
	}
	defer response.Body.Close()
	return response.StatusCode == http.StatusOK
}
