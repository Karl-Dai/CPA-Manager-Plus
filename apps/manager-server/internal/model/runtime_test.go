package model

import "testing"

func TestEmbeddedRuntimeDesiredStateValidate(t *testing.T) {
	valid := EmbeddedRuntimeDesiredState{
		DesiredLifecycle: EmbeddedRuntimeDesiredRunning,
		Revision:         1,
		UpdatedAtMS:      1,
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid desired state: %v", err)
	}
	for name, state := range map[string]EmbeddedRuntimeDesiredState{
		"invalid lifecycle": {DesiredLifecycle: "unknown", Revision: 1, UpdatedAtMS: 1},
		"zero revision":     {DesiredLifecycle: EmbeddedRuntimeDesiredRunning, UpdatedAtMS: 1},
		"zero timestamp":    {DesiredLifecycle: EmbeddedRuntimeDesiredStopped, Revision: 1},
	} {
		t.Run(name, func(t *testing.T) {
			if err := state.Validate(); err == nil {
				t.Fatal("Validate() error = nil")
			}
		})
	}
}

func TestRuntimeModeIsValid(t *testing.T) {
	for _, mode := range []RuntimeMode{RuntimeModeEmbedded, RuntimeModeExternal} {
		if !mode.IsValid() {
			t.Fatalf("mode %q is invalid", mode)
		}
	}
	for _, mode := range []RuntimeMode{"", "managed", "EMBEDDED"} {
		if mode.IsValid() {
			t.Fatalf("mode %q is valid", mode)
		}
	}
}

func TestRuntimeCapabilitiesSupportsExactCapability(t *testing.T) {
	capabilities := RuntimeCapabilities{"status", "health"}
	if !capabilities.Supports("status") {
		t.Fatal("status capability is not supported")
	}
	for _, capability := range []RuntimeCapability{"", "stat", "STATUS"} {
		if capabilities.Supports(capability) {
			t.Fatalf("capability %q is supported", capability)
		}
	}
	if RuntimeCapabilities(nil).Supports("status") {
		t.Fatal("nil capabilities support status")
	}
}

func TestRuntimeObservedStatusValidate(t *testing.T) {
	activeArtifact := &ActiveGatewayArtifact{
		Engine:     "cpa",
		ArtifactID: "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Version:    "v7.1.18",
	}
	valid := RuntimeObservedStatus{
		Identity:              "runtime-01",
		Generation:            1,
		ProtocolVersion:       "v1",
		State:                 RuntimeStateReady,
		CPAObservedVersion:    "v7.1.18",
		ActiveGatewayArtifact: activeArtifact,
		Capabilities:          RuntimeCapabilities{"status"},
		Recovery: &RuntimeRecoveryObservation{
			State:             RuntimeRecoveryStateArmed,
			AttemptsRemaining: 3,
		},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid status: %v", err)
	}
	withoutVersion := valid
	withoutVersion.CPAObservedVersion = ""
	withoutVersion.ActiveGatewayArtifact = nil
	if err := withoutVersion.Validate(); err != nil {
		t.Fatalf("ready status without a safe observed version: %v", err)
	}

	offline := withoutVersion
	offline.State = RuntimeStateOffline
	if err := offline.Validate(); err != nil {
		t.Fatalf("offline status without CPA version: %v", err)
	}
	starting := withoutVersion
	starting.State = RuntimeStateStarting
	if err := starting.Validate(); err != nil {
		t.Fatalf("starting status without CPA version: %v", err)
	}
	external := withoutVersion
	external.ProtocolVersion = ""
	external.Identity = ""
	external.Generation = 0
	external.CPAObservedVersion = "v7.1.18"
	if err := external.Validate(); err != nil {
		t.Fatalf("external status without Runtime Protocol metadata: %v", err)
	}

	tests := map[string]func(*RuntimeObservedStatus){
		"missing identity": func(status *RuntimeObservedStatus) {
			status.Identity = " "
		},
		"missing generation": func(status *RuntimeObservedStatus) {
			status.Generation = 0
		},
		"invalid state": func(status *RuntimeObservedStatus) {
			status.State = "stopping"
		},
		"identity without protocol": func(status *RuntimeObservedStatus) {
			status.ProtocolVersion = ""
			status.Generation = 0
		},
		"generation without protocol": func(status *RuntimeObservedStatus) {
			status.ProtocolVersion = ""
			status.Identity = ""
		},
		"empty capability": func(status *RuntimeObservedStatus) {
			status.Capabilities = RuntimeCapabilities{"status", " "}
		},
		"invalid recovery state": func(status *RuntimeObservedStatus) {
			status.Recovery = &RuntimeRecoveryObservation{State: "retrying", AttemptsRemaining: 1}
		},
		"negative recovery attempts": func(status *RuntimeObservedStatus) {
			status.Recovery = &RuntimeRecoveryObservation{State: RuntimeRecoveryStateRecovering, AttemptsRemaining: -1}
		},
		"excess recovery attempts": func(status *RuntimeObservedStatus) {
			status.Recovery = &RuntimeRecoveryObservation{State: RuntimeRecoveryStateArmed, AttemptsRemaining: 4}
		},
		"noncanonical artifact ID": func(status *RuntimeObservedStatus) {
			status.ActiveGatewayArtifact = &ActiveGatewayArtifact{Engine: "cpa", ArtifactID: "sha256:ABC", Version: "v7.1.18"}
		},
		"unsupported artifact engine": func(status *RuntimeObservedStatus) {
			status.ActiveGatewayArtifact = &ActiveGatewayArtifact{Engine: "gateway", ArtifactID: activeArtifact.ArtifactID, Version: "v7.1.18"}
		},
		"version differs from trusted artifact": func(status *RuntimeObservedStatus) {
			status.CPAObservedVersion = "v7.1.19"
		},
		"embedded version without artifact": func(status *RuntimeObservedStatus) {
			status.ActiveGatewayArtifact = nil
		},
		"artifact without Runtime Protocol": func(status *RuntimeObservedStatus) {
			status.ProtocolVersion = ""
			status.Identity = ""
			status.Generation = 0
		},
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			status := valid
			mutate(&status)
			if err := status.Validate(); err == nil {
				t.Fatal("validate error = nil")
			}
		})
	}
}
