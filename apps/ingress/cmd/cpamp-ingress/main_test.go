package main

import "testing"

func TestLoadConfigDefaults(t *testing.T) {
	t.Parallel()
	cfg, err := loadConfig(func(string) string { return "" })
	if err != nil {
		t.Fatal(err)
	}
	if cfg.addr != defaultAddr || cfg.managerURL.String() != defaultManagerURL || cfg.gatewayURL.String() != defaultGatewayURL {
		t.Fatalf("unexpected defaults: %+v", cfg)
	}
}

func TestParseUpstreamRejectsAuthorityAndPathAmbiguity(t *testing.T) {
	t.Parallel()
	for _, value := range []string{
		"https://manager:18317",
		"http://user:pass@manager:18317",
		"http://manager:18317/control",
		"http://manager:18317?route=control",
	} {
		if _, err := parseUpstream("TEST_URL", value, ""); err == nil {
			t.Fatalf("parseUpstream(%q) succeeded", value)
		}
	}
}
