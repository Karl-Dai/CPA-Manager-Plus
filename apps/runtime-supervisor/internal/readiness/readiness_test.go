package readiness

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/seakee/cpa-manager-plus/apps/runtime-supervisor/internal/cpaprocess"
)

func TestValidateAddress(t *testing.T) {
	for _, address := range []string{
		"127.0.0.1:8317", "127.10.20.30:1", "localhost:65535", "LOCALHOST:8317",
		"[::1]:8317", "[::ffff:127.0.0.1]:8317",
	} {
		t.Run(address, func(t *testing.T) {
			if err := ValidateAddress(address); err != nil {
				t.Fatalf("valid local address rejected: %v", err)
			}
		})
	}
	for _, address := range []string{
		"", "localhost", "localhost:", ":8317", "127.0.0.1:0", "localhost:65536",
		"localhost:-1", "localhost:+80", "localhost:http", "localhost:80/healthz",
		"localhost:80?target=x", "localhost:80#fragment", "localhost:80 ",
		"http://127.0.0.1:8317", "https://localhost:8317", "secret@localhost:8317",
		"localhost.evil.invalid:8317", "localhost.:8317", "example.invalid:8317",
		"0.0.0.0:8317", "192.168.1.1:8317", "8.8.8.8:8317", "127.1:8317",
		"[::]:8317", "[2001:db8::1]:8317", "[::1%zone]:8317", "::1:8317",
	} {
		t.Run("reject "+address, func(t *testing.T) {
			if err := ValidateAddress(address); err == nil {
				t.Fatal("unsafe or malformed address accepted")
			}
			if observer, err := New(&processSource{}, address); err == nil || observer != nil {
				t.Fatal("observer accepted invalid target")
			}
		})
	}
	if observer, err := New(nil, "localhost:8317"); err == nil || observer != nil {
		t.Fatal("observer accepted missing process source")
	}
}

func TestUnreadyProcessDoesNotProbe(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	for _, test := range []struct {
		name    string
		process cpaprocess.Observation
		want    State
	}{
		{"not started", cpaprocess.Observation{State: cpaprocess.StateNotStarted}, Offline},
		{"confirmed exited", cpaprocess.Observation{State: cpaprocess.StateExited, InstanceID: 1}, Offline},
		{"unconfirmed ownership", cpaprocess.Observation{State: cpaprocess.StateUnknown, InstanceID: 1, PID: 123}, Unknown},
		{"missing instance", cpaprocess.Observation{State: cpaprocess.StateRunning, PID: 123}, Unknown},
		{"unrecognized state", cpaprocess.Observation{State: "unexpected"}, Unknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			observer := newObserver(t, &processSource{observation: test.process}, server.Listener.Addr().String())
			if got := observer.Observe(t.Context()); got != test.want {
				t.Fatalf("Observe() = %s, want %s", got, test.want)
			}
		})
	}
	if probes.Load() != 0 {
		t.Fatal("process without confirmed running ownership caused network access")
	}
}

func TestProbeUsesOnlyHEADHealthzAndAcceptsOnly200(t *testing.T) {
	for _, code := range []int{200, 201, 204, 301, 302, 307, 308, 401, 403, 404, 500, 503} {
		t.Run(strconv.Itoa(code), func(t *testing.T) {
			var probes, redirects atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path != "/healthz" {
					redirects.Add(1)
					w.WriteHeader(http.StatusOK)
					return
				}
				probes.Add(1)
				if r.Method != http.MethodHead || r.URL.RawQuery != "" || r.ContentLength > 0 {
					t.Errorf("unexpected probe: %s %s, length %d", r.Method, r.URL, r.ContentLength)
				}
				if r.Header.Get("Authorization") != "" || r.Header.Get("X-Management-Key") != "" {
					t.Error("health probe carried credentials")
				}
				w.Header().Set("Location", "/redirected")
				w.WriteHeader(code)
			}))
			defer server.Close()
			observer := newObserver(t, runningSource(), server.Listener.Addr().String())
			want := Starting
			if code == http.StatusOK {
				want = Ready
			}
			if got := observer.Observe(t.Context()); got != want {
				t.Fatalf("Observe() = %s, want %s", got, want)
			}
			if probes.Load() != 1 || redirects.Load() != 0 {
				t.Fatalf("probes = %d, redirects followed = %d", probes.Load(), redirects.Load())
			}
		})
	}
}

func TestRefusedConnectionIsStarting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	address := server.Listener.Addr().String()
	server.Close()
	if got := newObserver(t, runningSource(), address).Observe(t.Context()); got != Starting {
		t.Fatalf("refused listener = %s, want starting", got)
	}
}

func TestProbeTimeoutIsBoundedAndStarting(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		probes.Add(1)
		<-r.Context().Done()
	}))
	defer server.Close()
	observer := newObserver(t, runningSource(), server.Listener.Addr().String())
	start := time.Now()
	if got := observer.Observe(t.Context()); got != Starting {
		t.Fatalf("timed out listener = %s, want starting", got)
	}
	if elapsed := time.Since(start); elapsed > 5*probeTimeout || elapsed < probeTimeout/2 {
		t.Fatalf("probe duration = %s, expected fixed bounded timeout %s", elapsed, probeTimeout)
	}
	if probes.Load() != 1 {
		t.Fatalf("timeout retried probe: %d", probes.Load())
	}
}

func TestCanceledObservationDoesNotProbe(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if got := newObserver(t, runningSource(), server.Listener.Addr().String()).Observe(ctx); got != Starting {
		t.Fatalf("canceled observation = %s", got)
	}
	if probes.Load() != 0 {
		t.Fatal("canceled observation accessed listener")
	}
}

func TestMalformedHTTPIsStarting(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		connection, writer, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer connection.Close()
		_, _ = writer.WriteString("invalid HTTP response\r\n\r\n")
		_ = writer.Flush()
	}))
	defer server.Close()
	if got := newObserver(t, runningSource(), server.Listener.Addr().String()).Observe(t.Context()); got != Starting {
		t.Fatalf("malformed listener response = %s", got)
	}
}

func TestProbeIgnoresEnvironmentProxy(t *testing.T) {
	var proxied atomic.Int32
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		proxied.Add(1)
		w.WriteHeader(http.StatusBadGateway)
	}))
	defer proxy.Close()
	for _, key := range []string{"HTTP_PROXY", "HTTPS_PROXY", "http_proxy", "https_proxy"} {
		t.Setenv(key, proxy.URL)
	}
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	_, port, err := net.SplitHostPort(server.Listener.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	for _, address := range []string{server.Listener.Addr().String(), "localhost:" + port} {
		observer := newObserver(t, runningSource(), address)
		transport, ok := observer.client.Transport.(*http.Transport)
		if !ok || transport.Proxy != nil || transport == http.DefaultTransport {
			t.Fatal("health probe must use its own direct transport with Proxy=nil")
		}
		if !transport.DisableKeepAlives || observer.client.Timeout != probeTimeout {
			t.Fatal("health probe must use a fresh connection and fixed timeout")
		}
		if got := observer.Observe(t.Context()); got != Ready {
			t.Fatalf("direct %s probe = %s", address, got)
		}
	}
	if proxied.Load() != 0 {
		t.Fatal("probe used environment proxy")
	}
}

func TestProbeFencesChildExitAndReplacementIncludingPIDReuse(t *testing.T) {
	for _, test := range []struct {
		name  string
		after cpaprocess.Observation
		want  State
	}{
		{"exited", cpaprocess.Observation{State: cpaprocess.StateExited, InstanceID: 1}, Offline},
		{"unknown", cpaprocess.Observation{State: cpaprocess.StateUnknown, InstanceID: 1, PID: 123}, Unknown},
		{"replacement", cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 2, PID: 456}, Starting},
		{"replacement reuses PID", cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 2, PID: 123}, Starting},
	} {
		t.Run(test.name, func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				once.Do(func() { close(entered) })
				<-release
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()
			var releaseOnce sync.Once
			unblock := func() { releaseOnce.Do(func() { close(release) }) }
			defer unblock()
			source := runningSource()
			observer := newObserver(t, source, server.Listener.Addr().String())
			result := make(chan State, 1)
			go func() { result <- observer.Observe(t.Context()) }()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("probe did not reach listener")
			}
			source.set(test.after)
			unblock()
			if got := <-result; got != test.want {
				t.Fatalf("stale successful probe = %s, want %s", got, test.want)
			}
			if test.after.State == cpaprocess.StateRunning {
				if got := observer.Observe(t.Context()); got != Ready {
					t.Fatalf("replacement's own subsequent probe = %s, want ready", got)
				}
			}
		})
	}
}

func TestFailedProbeStillReobservesProcess(t *testing.T) {
	source := runningSource()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		source.set(cpaprocess.Observation{State: cpaprocess.StateExited, InstanceID: 1})
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	if got := newObserver(t, source, server.Listener.Addr().String()).Observe(t.Context()); got != Offline {
		t.Fatalf("child exited during failed probe = %s, want offline", got)
	}
}

func TestConcurrentObservationsDoNotCacheResults(t *testing.T) {
	var probes atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		probes.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	observer := newObserver(t, runningSource(), server.Listener.Addr().String())
	const count = 16
	results := make(chan State, count)
	for range count {
		go func() { results <- observer.Observe(t.Context()) }()
	}
	for range count {
		if got := <-results; got != Ready {
			t.Errorf("concurrent observation = %s", got)
		}
	}
	if probes.Load() != count {
		t.Fatalf("probes = %d, want %d", probes.Load(), count)
	}
}

type processSource struct {
	mu          sync.Mutex
	observation cpaprocess.Observation
}

func runningSource() *processSource {
	return &processSource{observation: cpaprocess.Observation{State: cpaprocess.StateRunning, InstanceID: 1, PID: 123}}
}

func (s *processSource) Observe() cpaprocess.Observation {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.observation
}

func (s *processSource) set(observation cpaprocess.Observation) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.observation = observation
}

func newObserver(t *testing.T, process processObserver, address string) *Observer {
	t.Helper()
	observer, err := New(process, address)
	if err != nil {
		t.Fatal(err)
	}
	if observer.target != fmt.Sprintf("http://%s/healthz", address) || strings.Contains(observer.target, "management") {
		t.Fatalf("unexpected probe target: %s", observer.target)
	}
	return observer
}
