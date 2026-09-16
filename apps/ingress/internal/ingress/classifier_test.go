package ingress

import "testing"

func TestTransitionalClassifier(t *testing.T) {
	t.Parallel()
	classifier := NewTransitionalClassifier()
	tests := []struct {
		path string
		want Route
	}{
		{path: "/", want: RouteManager},
		{path: "/management.html", want: RouteManager},
		{path: "/health", want: RouteManager},
		{path: "/status", want: RouteManager},
		{path: "/setup", want: RouteManager},
		{path: "/usage-service", want: RouteManager},
		{path: "/usage-service/info", want: RouteManager},
		{path: "/v0/management", want: RouteManager},
		{path: "/v0/management/config", want: RouteManager},
		{path: "/v1/models", want: RouteGateway},
		{path: "/v1/chat/completions", want: RouteGateway},
		{path: "/v1/responses", want: RouteGateway},
		{path: "/v1/messages", want: RouteGateway},
		{path: "/future-gateway-protocol", want: RouteGateway},
		{path: "/usage-services", want: RouteGateway},
		{path: "/v0/management-other", want: RouteGateway},
	}
	for _, test := range tests {
		test := test
		t.Run(test.path, func(t *testing.T) {
			t.Parallel()
			if got := classifier.Classify(test.path); got != test.want {
				t.Fatalf("Classify(%q) = %v, want %v", test.path, got, test.want)
			}
		})
	}
}

func TestClassifierControlMatcherIsReplaceable(t *testing.T) {
	t.Parallel()
	classifier := NewClassifier(ControlMatcherFunc(func(path string) bool {
		return path == "/future-control" || len(path) > len("/future-control/") && path[:len("/future-control/")] == "/future-control/"
	}))
	if got := classifier.Classify("/future-control/settings"); got != RouteManager {
		t.Fatalf("future control path classified as %v", got)
	}
	if got := classifier.Classify("/health"); got != RouteGateway {
		t.Fatalf("transitional route leaked into replacement matcher: %v", got)
	}
}
