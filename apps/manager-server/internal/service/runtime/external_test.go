package runtime

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/seakee/cpa-manager-plus/apps/manager-server/internal/model"
)

func TestExternalClientStatus(t *testing.T) {
	var gotAuthorization string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthorization = r.Header.Get("Authorization")
		w.Header().Set("X-CPA-Version", "v7.2.130")
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	status, err := NewExternalClient(server.URL, "management-key").Status(t.Context())
	if err != nil {
		t.Fatalf("Status() error = %v", err)
	}
	if err := status.Validate(); err != nil {
		t.Fatalf("Status() returned invalid observation: %v", err)
	}
	if status.State != model.RuntimeStateReady || status.CPAObservedVersion != "v7.2.130" {
		t.Fatalf("Status() = %#v", status)
	}
	if status.ProtocolVersion != "" || status.Identity != "" || status.Generation != 0 {
		t.Fatalf("Status() invented Runtime Protocol metadata: %#v", status)
	}
	if status.Capabilities != nil {
		t.Fatalf("Status() capabilities = %#v, want nil", status.Capabilities)
	}
	if gotAuthorization != "Bearer management-key" {
		t.Fatalf("Authorization = %q", gotAuthorization)
	}
}

func TestExternalClientStatusRequiresReliableObservation(t *testing.T) {
	tests := []struct {
		name    string
		handler http.HandlerFunc
		want    string
	}{
		{
			name: "missing CPA version",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusOK)
			},
			want: "CPA version header is missing",
		},
		{
			name: "authentication failure",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
			},
			want: "401 Unauthorized",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(test.handler)
			defer server.Close()

			status, err := NewExternalClient(server.URL, "management-key").Status(t.Context())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Status() error = %v, want %q", err, test.want)
			}
			if !reflect.DeepEqual(status, model.RuntimeObservedStatus{}) {
				t.Fatalf("Status() = %#v, want zero value", status)
			}
		})
	}
}

func TestExternalClientStatusUsesCallerContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	status, err := NewExternalClient("http://127.0.0.1:1", "management-key").Status(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Status() error = %v, want context canceled", err)
	}
	if !reflect.DeepEqual(status, model.RuntimeObservedStatus{}) {
		t.Fatalf("Status() = %#v, want zero value", status)
	}
}
