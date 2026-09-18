package cpaupdate

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func sourceWithResponse(t *testing.T, status int, body []byte, requests *atomic.Int32) *officialReleaseSource {
	t.Helper()
	return &officialReleaseSource{client: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		requests.Add(1)
		if request.URL.String() != OfficialLatestReleaseURL || request.URL.Scheme != "https" {
			t.Fatalf("release request URL = %q", request.URL.String())
		}
		return &http.Response{
			StatusCode: status,
			Header:     make(http.Header),
			Body:       io.NopCloser(bytes.NewReader(body)),
			Request:    request,
		}, nil
	})}}
}

func TestOfficialReleaseSourceAcceptsOnlyCanonicalStableRelease(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "draft", body: `{"tag_name":"v7.3.4","draft":true,"prerelease":false}`},
		{name: "prerelease flag", body: `{"tag_name":"v7.3.4","draft":false,"prerelease":true}`},
		{name: "missing draft flag", body: `{"tag_name":"v7.3.4","prerelease":false}`},
		{name: "missing prerelease flag", body: `{"tag_name":"v7.3.4","draft":false}`},
		{name: "tag without v", body: `{"tag_name":"7.3.4","draft":false,"prerelease":false}`},
		{name: "short tag", body: `{"tag_name":"v7.3","draft":false,"prerelease":false}`},
		{name: "prerelease tag", body: `{"tag_name":"v7.3.4-rc.1","draft":false,"prerelease":false}`},
		{name: "leading zero", body: `{"tag_name":"v7.03.4","draft":false,"prerelease":false}`},
		{name: "trailing JSON", body: `{"tag_name":"v7.3.4","draft":false,"prerelease":false} {}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var requests atomic.Int32
			_, err := sourceWithResponse(t, http.StatusOK, []byte(test.body), &requests).LatestStable(t.Context())
			if err == nil {
				t.Fatal("LatestStable() accepted invalid release")
			}
			if requests.Load() != 1 {
				t.Fatalf("requests = %d, want 1", requests.Load())
			}
		})
	}
}

func TestOfficialReleaseSourceNormalizesTagAndBoundsResponse(t *testing.T) {
	var requests atomic.Int32
	source := sourceWithResponse(
		t,
		http.StatusOK,
		[]byte(`{"tag_name":"v7.3.4","draft":false,"prerelease":false,"assets":[{"browser_download_url":"https://untrusted.invalid/ignored"}]}`),
		&requests,
	)
	version, err := source.LatestStable(t.Context())
	if err != nil || version != "7.3.4" {
		t.Fatalf("LatestStable() = %q, %v", version, err)
	}

	oversized := []byte(strings.Repeat("x", officialReleaseMaxBodyBytes+1))
	_, err = sourceWithResponse(t, http.StatusOK, oversized, &requests).LatestStable(context.Background())
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized response error = %v", err)
	}
	if requests.Load() != 2 {
		t.Fatalf("requests = %d, want 2", requests.Load())
	}
}
