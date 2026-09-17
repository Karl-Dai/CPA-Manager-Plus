package model

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

// EmbeddedRuntimeDesiredLifecycle is the Manager-owned desired lifecycle for
// an Embedded Runtime. Absence of the persisted setting means unresolved; it
// is deliberately not represented by another enum value.
type EmbeddedRuntimeDesiredLifecycle string

const (
	EmbeddedRuntimeDesiredRunning EmbeddedRuntimeDesiredLifecycle = "running"
	EmbeddedRuntimeDesiredStopped EmbeddedRuntimeDesiredLifecycle = "stopped"
)

func (l EmbeddedRuntimeDesiredLifecycle) IsValid() bool {
	switch l {
	case EmbeddedRuntimeDesiredRunning, EmbeddedRuntimeDesiredStopped:
		return true
	default:
		return false
	}
}

// EmbeddedRuntimeDesiredState is Manager product state. Revision changes only
// when DesiredLifecycle changes and is unrelated to RuntimeGeneration.
type EmbeddedRuntimeDesiredState struct {
	DesiredLifecycle EmbeddedRuntimeDesiredLifecycle `json:"desiredLifecycle"`
	Revision         uint64                          `json:"revision"`
	UpdatedAtMS      int64                           `json:"updatedAtMs"`
}

func (s EmbeddedRuntimeDesiredState) Validate() error {
	if !s.DesiredLifecycle.IsValid() {
		return fmt.Errorf("invalid embedded Runtime desired lifecycle %q", s.DesiredLifecycle)
	}
	if s.Revision == 0 {
		return errors.New("embedded Runtime desired revision must be positive")
	}
	if s.UpdatedAtMS <= 0 {
		return errors.New("embedded Runtime desired updatedAtMs must be positive")
	}
	return nil
}

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

// CPAObservedVersion is optional display metadata, never update authority.
// Embedded populates it only as a projection of ActiveGatewayArtifact.Version;
// External may retain its authenticated adapter-specific version observation.
type CPAObservedVersion string

type RuntimeArtifactID string

func (id RuntimeArtifactID) IsValid() bool {
	value := string(id)
	const prefix = "sha256:"
	if len(value) != len(prefix)+64 || !strings.HasPrefix(value, prefix) {
		return false
	}
	for _, character := range value[len(prefix):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// ActiveGatewayArtifact is the exact executable identity observed by the
// Embedded Supervisor. Version is trusted display metadata only; ArtifactID is
// the future update-fencing authority.
type ActiveGatewayArtifact struct {
	Engine     string
	ArtifactID RuntimeArtifactID
	Version    string
}

func (a ActiveGatewayArtifact) Validate() error {
	if a.Engine != "cpa" {
		return fmt.Errorf("unsupported active Gateway artifact engine %q", a.Engine)
	}
	if !a.ArtifactID.IsValid() {
		return fmt.Errorf("invalid active Gateway artifact ID %q", a.ArtifactID)
	}
	if a.Version != strings.TrimSpace(a.Version) {
		return errors.New("active Gateway artifact version must not contain surrounding whitespace")
	}
	return nil
}

// RuntimeRecoveryState observes the Embedded Supervisor's process-local
// bounded recovery policy. It does not express Manager desired state.
type RuntimeRecoveryState string

const (
	RuntimeRecoveryStateInactive           RuntimeRecoveryState = "inactive"
	RuntimeRecoveryStateArmed              RuntimeRecoveryState = "armed"
	RuntimeRecoveryStateRecovering         RuntimeRecoveryState = "recovering"
	RuntimeRecoveryStateManualIntervention RuntimeRecoveryState = "manual_intervention"
)

func (s RuntimeRecoveryState) IsValid() bool {
	switch s {
	case RuntimeRecoveryStateInactive,
		RuntimeRecoveryStateArmed,
		RuntimeRecoveryStateRecovering,
		RuntimeRecoveryStateManualIntervention:
		return true
	default:
		return false
	}
}

type RuntimeRecoveryObservation struct {
	State             RuntimeRecoveryState
	AttemptsRemaining int
}

type RuntimeCapability string

const (
	RuntimeCapabilityStart         RuntimeCapability = "start"
	RuntimeCapabilityStop          RuntimeCapability = "stop"
	RuntimeCapabilityRestart       RuntimeCapability = "restart"
	RuntimeCapabilityPrepareUpdate RuntimeCapability = "prepare_update"
)

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
	Identity              RuntimeIdentity
	Generation            RuntimeGeneration
	ProtocolVersion       RuntimeProtocolVersion
	State                 RuntimeState
	CPAObservedVersion    CPAObservedVersion
	ActiveGatewayArtifact *ActiveGatewayArtifact
	Capabilities          RuntimeCapabilities
	Recovery              *RuntimeRecoveryObservation
}

type RuntimeOperationType string

const (
	RuntimeOperationStart         RuntimeOperationType = "start"
	RuntimeOperationStop          RuntimeOperationType = "stop"
	RuntimeOperationRestart       RuntimeOperationType = "restart"
	RuntimeOperationPrepareUpdate RuntimeOperationType = "prepare_update"
)

func (t RuntimeOperationType) IsValid() bool {
	switch t {
	case RuntimeOperationStart, RuntimeOperationStop, RuntimeOperationRestart, RuntimeOperationPrepareUpdate:
		return true
	default:
		return false
	}
}

type RuntimeOperationState string

const (
	RuntimeOperationAccepted  RuntimeOperationState = "accepted"
	RuntimeOperationRunning   RuntimeOperationState = "running"
	RuntimeOperationSucceeded RuntimeOperationState = "succeeded"
	RuntimeOperationFailed    RuntimeOperationState = "failed"
)

func (s RuntimeOperationState) IsValid() bool {
	switch s {
	case RuntimeOperationAccepted,
		RuntimeOperationRunning,
		RuntimeOperationSucceeded,
		RuntimeOperationFailed:
		return true
	default:
		return false
	}
}

type RuntimeMutationRequest struct {
	OperationID               string
	ExpectedRuntimeIdentity   RuntimeIdentity
	ExpectedRuntimeGeneration RuntimeGeneration
}

func (r RuntimeMutationRequest) Validate() error {
	if strings.TrimSpace(r.OperationID) == "" {
		return errors.New("runtime operation ID is required")
	}
	if !utf8.ValidString(r.OperationID) {
		return errors.New("runtime operation ID must be valid UTF-8")
	}
	if len([]byte(r.OperationID)) > 128 {
		return errors.New("runtime operation ID exceeds 128 UTF-8 bytes")
	}
	if strings.TrimSpace(string(r.ExpectedRuntimeIdentity)) == "" {
		return errors.New("expected runtime identity is required")
	}
	if r.ExpectedRuntimeGeneration == 0 {
		return errors.New("expected runtime generation must be positive")
	}
	return nil
}

type RuntimePrepareUpdateRequest struct {
	RuntimeMutationRequest
	ExpectedActiveArtifactID RuntimeArtifactID
	TargetVersion            string
}

func (r RuntimePrepareUpdateRequest) Validate() error {
	if err := r.RuntimeMutationRequest.Validate(); err != nil {
		return err
	}
	if !r.ExpectedActiveArtifactID.IsValid() {
		return errors.New("expected active artifact ID must be canonical SHA-256")
	}
	if !validRuntimeTargetVersion(r.TargetVersion) {
		return errors.New("target version must be an exact canonical release version")
	}
	return nil
}

func validRuntimeTargetVersion(version string) bool {
	if version == "" || version != strings.TrimSpace(version) || len(version) > 96 ||
		strings.ContainsAny(version, "/\\:\x00") {
		return false
	}
	coreAndPrerelease, build, ok := splitRuntimeVersion(version, "+")
	if !ok || (build != "" && !validRuntimeVersionIdentifiers(build, false)) {
		return false
	}
	core, prerelease, ok := splitRuntimeVersion(coreAndPrerelease, "-")
	if !ok || (prerelease != "" && !validRuntimeVersionIdentifiers(prerelease, true)) {
		return false
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" || (len(part) > 1 && part[0] == '0') {
			return false
		}
		for _, character := range part {
			if character < '0' || character > '9' {
				return false
			}
		}
	}
	return true
}

func splitRuntimeVersion(value, separator string) (string, string, bool) {
	left, right, found := strings.Cut(value, separator)
	if found && (left == "" || right == "") {
		return "", "", false
	}
	return left, right, true
}

func validRuntimeVersionIdentifiers(value string, rejectLeadingZeroNumeric bool) bool {
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		numeric := true
		for _, character := range identifier {
			if character < '0' || character > '9' {
				numeric = false
			}
			if (character < '0' || character > '9') &&
				(character < 'A' || character > 'Z') &&
				(character < 'a' || character > 'z') && character != '-' {
				return false
			}
		}
		if rejectLeadingZeroNumeric && numeric && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

type RuntimeOperationFailure struct {
	Code    string
	Message string
}

type RuntimeOperationResult struct {
	OperationID       string
	OperationType     RuntimeOperationType
	RuntimeIdentity   RuntimeIdentity
	RuntimeGeneration RuntimeGeneration
	State             RuntimeOperationState
	Failure           *RuntimeOperationFailure
}

func (r RuntimeOperationResult) Validate() error {
	if strings.TrimSpace(r.OperationID) == "" {
		return errors.New("runtime operation result ID is required")
	}
	if !r.OperationType.IsValid() {
		return fmt.Errorf("invalid runtime operation type %q", r.OperationType)
	}
	if strings.TrimSpace(string(r.RuntimeIdentity)) == "" {
		return errors.New("runtime operation identity is required")
	}
	if r.RuntimeGeneration == 0 {
		return errors.New("runtime operation generation must be positive")
	}
	if !r.State.IsValid() {
		return fmt.Errorf("invalid runtime operation state %q", r.State)
	}
	if r.Failure != nil && strings.TrimSpace(r.Failure.Code) == "" {
		return errors.New("runtime operation failure code is required")
	}
	return nil
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
	if s.ActiveGatewayArtifact != nil {
		if !hasProtocol {
			return errors.New("active Gateway artifact observation requires Runtime Protocol")
		}
		if err := s.ActiveGatewayArtifact.Validate(); err != nil {
			return err
		}
		if string(s.CPAObservedVersion) != s.ActiveGatewayArtifact.Version {
			return errors.New("CPA observed version must project trusted active Gateway artifact version")
		}
	} else if hasProtocol && s.CPAObservedVersion != "" {
		return errors.New("Embedded CPA observed version requires trusted active Gateway artifact observation")
	}
	for _, capability := range s.Capabilities {
		if strings.TrimSpace(string(capability)) == "" {
			return errors.New("runtime capability must not be empty")
		}
	}
	if s.Recovery != nil {
		if !s.Recovery.State.IsValid() {
			return fmt.Errorf("invalid runtime recovery state %q", s.Recovery.State)
		}
		if s.Recovery.AttemptsRemaining < 0 || s.Recovery.AttemptsRemaining > 3 {
			return fmt.Errorf("runtime recovery attempts remaining must be between 0 and 3")
		}
	}
	return nil
}
