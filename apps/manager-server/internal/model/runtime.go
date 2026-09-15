package model

import (
	"errors"
	"fmt"
	"strings"
)

// RuntimeMode is the Manager-owned choice of how CPA is operated.
type RuntimeMode string

const (
	RuntimeModeEmbedded RuntimeMode = "embedded"
	RuntimeModeExternal RuntimeMode = "external"
)

func (m RuntimeMode) IsValid() bool {
	switch m {
	case RuntimeModeEmbedded, RuntimeModeExternal:
		return true
	default:
		return false
	}
}

type RuntimeIdentity string

type RuntimeGeneration uint64

// RuntimeProtocolVersion is empty when an adapter does not observe a versioned
// Runtime Protocol endpoint, as is allowed for an External runtime.
type RuntimeProtocolVersion string

// RuntimeState describes the currently observed availability of CPA.
type RuntimeState string

const (
	RuntimeStateUnknown  RuntimeState = "unknown"
	RuntimeStateOffline  RuntimeState = "offline"
	RuntimeStateStarting RuntimeState = "starting"
	RuntimeStateReady    RuntimeState = "ready"
)

func (s RuntimeState) IsValid() bool {
	switch s {
	case RuntimeStateUnknown, RuntimeStateOffline, RuntimeStateStarting, RuntimeStateReady:
		return true
	default:
		return false
	}
}

// CPAObservedVersion is an optional observed fact. Readiness alone does not
// imply a safe version source or an expected-version operation precondition.
type CPAObservedVersion string

type RuntimeCapability string

type RuntimeCapabilities []RuntimeCapability

func (c RuntimeCapabilities) Supports(capability RuntimeCapability) bool {
	if strings.TrimSpace(string(capability)) == "" {
		return false
	}
	for _, candidate := range c {
		if candidate == capability {
			return true
		}
	}
	return false
}

// RuntimeObservedStatus contains only state reported by the Runtime. Manager-
// owned desired configuration, including RuntimeMode, intentionally lives
// outside this model.
type RuntimeObservedStatus struct {
	Identity           RuntimeIdentity
	Generation         RuntimeGeneration
	ProtocolVersion    RuntimeProtocolVersion
	State              RuntimeState
	CPAObservedVersion CPAObservedVersion
	Capabilities       RuntimeCapabilities
}

func (s RuntimeObservedStatus) Validate() error {
	hasProtocol := strings.TrimSpace(string(s.ProtocolVersion)) != ""
	hasIdentity := strings.TrimSpace(string(s.Identity)) != ""
	if hasProtocol {
		if !hasIdentity {
			return errors.New("runtime identity is required when Runtime Protocol is present")
		}
		if s.Generation == 0 {
			return errors.New("runtime generation is required when Runtime Protocol is present")
		}
	} else if hasIdentity || s.Generation != 0 {
		return errors.New("runtime identity and generation require Runtime Protocol")
	}
	if !s.State.IsValid() {
		return fmt.Errorf("invalid runtime state %q", s.State)
	}
	for _, capability := range s.Capabilities {
		if strings.TrimSpace(string(capability)) == "" {
			return errors.New("runtime capability must not be empty")
		}
	}
	return nil
}
