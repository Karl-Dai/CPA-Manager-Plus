package model

import "testing"

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
	valid := RuntimeObservedStatus{
		Identity:           "runtime-01",
		Generation:         1,
		ProtocolVersion:    "v1",
		State:              RuntimeStateReady,
		CPAObservedVersion: "v7.1.18",
		Capabilities:       RuntimeCapabilities{"status"},
	}
	if err := valid.Validate(); err != nil {
		t.Fatalf("valid status: %v", err)
	}

	offline := valid
	offline.State = RuntimeStateOffline
	offline.CPAObservedVersion = ""
	if err := offline.Validate(); err != nil {
		t.Fatalf("offline status without CPA version: %v", err)
	}
	starting := valid
	starting.State = RuntimeStateStarting
	starting.CPAObservedVersion = ""
	if err := starting.Validate(); err != nil {
		t.Fatalf("starting status without CPA version: %v", err)
	}
	external := valid
	external.ProtocolVersion = ""
	external.Identity = ""
	external.Generation = 0
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
		"ready without CPA version": func(status *RuntimeObservedStatus) {
			status.CPAObservedVersion = ""
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
