package ingress

import (
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"time"
)

const proxyBufferSize = 32 << 10

type Handler struct {
	classifier Classifier
	manager    http.Handler
	gateway    http.Handler
}

func NewHandler(classifier Classifier, managerURL, gatewayURL *url.URL, logger *log.Logger) *Handler {
	return &Handler{
		classifier: classifier,
		manager:    newReverseProxy(managerURL, logger),
		gateway:    newReverseProxy(gatewayURL, logger),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path
	if path == "" {
		path = "/"
	}
	if h.classifier.Classify(path) == RouteManager {
		h.manager.ServeHTTP(w, r)
		return
	}
	h.gateway.ServeHTTP(w, r)
}

func newReverseProxy(target *url.URL, logger *log.Logger) *httputil.ReverseProxy {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{
		Timeout:   10 * time.Second,
		KeepAlive: 30 * time.Second,
	}).DialContext
	transport.ForceAttemptHTTP2 = true
	transport.MaxIdleConns = 100
	transport.IdleConnTimeout = 90 * time.Second
	transport.ExpectContinueTimeout = time.Second

	return &httputil.ReverseProxy{
		Rewrite: func(request *httputil.ProxyRequest) {
			clearForwardingHeaders(request.Out.Header)
			request.SetURL(target)
			request.Out.Host = request.In.Host
			request.SetXForwarded()
		},
		Transport:     transport,
		FlushInterval: -1,
		BufferPool:    &bufferPool{},
		ErrorLog:      logger,
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, err error) {
			logger.Printf("upstream request failed: %v", err)
			http.Error(w, "upstream unavailable", http.StatusBadGateway)
		},
	}
}

func clearForwardingHeaders(header http.Header) {
	for name := range header {
		if strings.HasPrefix(strings.ToLower(name), "x-forwarded-") {
			header.Del(name)
		}
	}
	header.Del("Forwarded")
	header.Del("X-Real-IP")
}

type bufferPool struct {
	pool sync.Pool
}

func (p *bufferPool) Get() []byte {
	if value := p.pool.Get(); value != nil {
		return value.([]byte)
	}
	return make([]byte, proxyBufferSize)
}

func (p *bufferPool) Put(buffer []byte) {
	if cap(buffer) < proxyBufferSize {
		return
	}
	p.pool.Put(buffer[:proxyBufferSize])
}
