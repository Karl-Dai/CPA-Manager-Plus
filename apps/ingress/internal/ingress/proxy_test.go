package ingress

import (
	"bufio"
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"
)

func TestHandlerPreservesStreamingResponse(t *testing.T) {
	firstFlushed := make(chan struct{})
	releaseSecond := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = io.WriteString(w, "data: first\n\n")
		w.(http.Flusher).Flush()
		close(firstFlushed)
		<-releaseSecond
		_, _ = io.WriteString(w, "data: second\n\n")
	}))
	defer gateway.Close()
	proxy := newTestProxy(t, gateway.URL)
	defer proxy.Close()

	response, err := proxy.Client().Get(proxy.URL + "/v1/responses")
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	select {
	case <-firstFlushed:
	case <-time.After(time.Second):
		t.Fatal("upstream did not flush first event")
	}
	first, err := bufio.NewReader(response.Body).ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if first != "data: first\n" {
		t.Fatalf("first streamed line = %q", first)
	}
	close(releaseSecond)
}

func TestHandlerStreamsRequestBodyWithoutFullBuffering(t *testing.T) {
	readFirst := make(chan struct{})
	releaseRead := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		first := make([]byte, 5)
		if _, err := io.ReadFull(r.Body, first); err != nil {
			t.Errorf("read first request chunk: %v", err)
			return
		}
		if string(first) != "first" {
			t.Errorf("first request chunk = %q", first)
			return
		}
		close(readFirst)
		<-releaseRead
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read remaining request: %v", err)
			return
		}
		if string(body) != "second" {
			t.Errorf("remaining request body = %q", body)
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer gateway.Close()
	proxy := newTestProxy(t, gateway.URL)
	defer proxy.Close()

	reader, writer := io.Pipe()
	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/v1/chat/completions", reader)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		response, err := proxy.Client().Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()
	if _, err := io.WriteString(writer, "first"); err != nil {
		t.Fatal(err)
	}
	select {
	case <-readFirst:
	case <-time.After(time.Second):
		t.Fatal("proxy buffered the full request before forwarding")
	}
	close(releaseRead)
	if _, err := io.WriteString(writer, "second"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestHandlerProxiesWebSocketUpgrade(t *testing.T) {
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !strings.EqualFold(r.Header.Get("Connection"), "upgrade") || !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") {
			http.Error(w, "upgrade required", http.StatusBadRequest)
			return
		}
		connection, buffered, err := w.(http.Hijacker).Hijack()
		if err != nil {
			t.Errorf("hijack: %v", err)
			return
		}
		defer connection.Close()
		_, _ = buffered.WriteString("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
		_ = buffered.Flush()
		message := make([]byte, 4)
		if _, err := io.ReadFull(buffered, message); err != nil {
			t.Errorf("read upgraded payload: %v", err)
			return
		}
		_, _ = connection.Write(message)
	}))
	defer gateway.Close()
	proxy := newTestProxy(t, gateway.URL)
	defer proxy.Close()

	address := strings.TrimPrefix(proxy.URL, "http://")
	connection, err := net.DialTimeout("tcp", address, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	_ = connection.SetDeadline(time.Now().Add(2 * time.Second))
	_, _ = io.WriteString(connection, "GET /v1/websocket HTTP/1.1\r\nHost: public.example:18317\r\nConnection: Upgrade\r\nUpgrade: websocket\r\n\r\n")
	reader := bufio.NewReader(connection)
	status, err := reader.ReadString('\n')
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(status, "101 Switching Protocols") {
		t.Fatalf("upgrade status = %q", status)
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			t.Fatal(err)
		}
		if line == "\r\n" {
			break
		}
	}
	if _, err := connection.Write([]byte("ping")); err != nil {
		t.Fatal(err)
	}
	message := make([]byte, 4)
	if _, err := io.ReadFull(reader, message); err != nil {
		t.Fatal(err)
	}
	if string(message) != "ping" {
		t.Fatalf("upgraded payload = %q", message)
	}
}

func TestHandlerPropagatesClientCancellation(t *testing.T) {
	started := make(chan struct{})
	cancelled := make(chan struct{})
	gateway := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(cancelled)
	}))
	defer gateway.Close()
	proxy := newTestProxy(t, gateway.URL)
	defer proxy.Close()

	ctx, cancel := context.WithCancel(context.Background())
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, proxy.URL+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() {
		response, err := proxy.Client().Do(request)
		if response != nil {
			_ = response.Body.Close()
		}
		result <- err
	}()
	<-started
	cancel()
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream context was not cancelled")
	}
	if err := <-result; err == nil {
		t.Fatal("cancelled client request unexpectedly succeeded")
	}
}

func TestHandlerRebuildsForwardingHeadersAndPreservesRequest(t *testing.T) {
	type observation struct {
		host, authorization, userAgent, body string
		forwarded, forwardedFor              string
		forwardedHost, forwardedProto        string
		realIP                               string
	}
	observed := make(chan observation, 1)
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		observed <- observation{
			host: r.Host, authorization: r.Header.Get("Authorization"), userAgent: r.UserAgent(), body: string(body),
			forwarded: r.Header.Get("Forwarded"), forwardedFor: r.Header.Get("X-Forwarded-For"),
			forwardedHost: r.Header.Get("X-Forwarded-Host"), forwardedProto: r.Header.Get("X-Forwarded-Proto"),
			realIP: r.Header.Get("X-Real-IP"),
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer gateway.Close()
	proxy := newTestProxy(t, gateway.URL)
	defer proxy.Close()

	request, err := http.NewRequest(http.MethodPost, proxy.URL+"/v1/responses", strings.NewReader("payload"))
	if err != nil {
		t.Fatal(err)
	}
	request.Host = "public.example:18317"
	request.Header.Set("Authorization", "Bearer provider-secret")
	request.Header.Set("User-Agent", "cpamp-client/1")
	request.Header.Set("Forwarded", "for=spoofed;proto=https")
	request.Header.Set("X-Forwarded-For", "203.0.113.9")
	request.Header.Set("X-Forwarded-Host", "spoofed.example")
	request.Header.Set("X-Forwarded-Proto", "https")
	request.Header.Set("X-Real-IP", "203.0.113.10")
	response, err := proxy.Client().Do(request)
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	got := <-observed
	if got.host != "public.example:18317" || got.authorization != "Bearer provider-secret" || got.userAgent != "cpamp-client/1" || got.body != "payload" {
		t.Fatalf("request semantics changed: %+v", got)
	}
	if got.forwarded != "" || got.realIP != "" || strings.Contains(got.forwardedFor, "203.0.113.9") {
		t.Fatalf("spoofed forwarding metadata survived: %+v", got)
	}
	if net.ParseIP(strings.TrimSpace(got.forwardedFor)) == nil {
		t.Fatalf("rebuilt X-Forwarded-For = %q", got.forwardedFor)
	}
	if got.forwardedHost != "public.example:18317" || got.forwardedProto != "http" {
		t.Fatalf("rebuilt forwarding metadata = %+v", got)
	}
}

func TestHandlerClassifiesDecodedControlPath(t *testing.T) {
	managerHit := make(chan string, 1)
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		managerHit <- r.URL.Path
		w.WriteHeader(http.StatusNoContent)
	}))
	defer manager.Close()
	gateway := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "encoded control path reached Gateway", http.StatusBadGateway)
	}))
	defer gateway.Close()
	managerTarget, err := url.Parse(manager.URL)
	if err != nil {
		t.Fatal(err)
	}
	gatewayTarget, err := url.Parse(gateway.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httptest.NewServer(NewHandler(
		NewTransitionalClassifier(),
		managerTarget,
		gatewayTarget,
		log.New(io.Discard, "", 0),
	))
	defer proxy.Close()

	response, err := proxy.Client().Get(proxy.URL + "/%76%30/management/config")
	if err != nil {
		t.Fatal(err)
	}
	_ = response.Body.Close()
	if response.StatusCode != http.StatusNoContent {
		t.Fatalf("encoded control path status = %d", response.StatusCode)
	}
	if got := <-managerHit; got != "/v0/management/config" {
		t.Fatalf("decoded Manager path = %q", got)
	}
}

func newTestProxy(t *testing.T, gatewayURL string) *httptest.Server {
	t.Helper()
	manager := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "unexpected Manager route", http.StatusTeapot)
	}))
	t.Cleanup(manager.Close)
	managerTarget, err := url.Parse(manager.URL)
	if err != nil {
		t.Fatal(err)
	}
	gatewayTarget, err := url.Parse(gatewayURL)
	if err != nil {
		t.Fatal(err)
	}
	logger := log.New(io.Discard, "", 0)
	return httptest.NewServer(NewHandler(NewTransitionalClassifier(), managerTarget, gatewayTarget, logger))
}
