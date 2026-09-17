// Package update owns trusted CPA release resolution and inactive artifact staging.
package update

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"strings"
	"time"
)

const (
	officialRepositoryOwner = "router-for-me"
	officialRepositoryName  = "CLIProxyAPI"
	githubAPIHost           = "api.github.com"
	githubHost              = "github.com"
	// ReleaseClientTimeout bounds each official metadata or asset request. The
	// prepare-update transport has a larger, route-specific budget because a
	// complete operation also includes both requests and local staging I/O.
	ReleaseClientTimeout    = 5 * time.Minute
	maxReleaseMetadataBytes = 1 << 20
	maxReleaseArchiveBytes  = 256 << 20
	metadataAccept          = "application/vnd.github+json"
	assetAccept             = "application/octet-stream"
	userAgent               = "cpamp-runtime-supervisor/prepare-update-v1"
)

var (
	ErrUnsupportedPlatform = errors.New("unsupported update staging platform")
	ErrReleaseMetadata     = errors.New("official release metadata is unavailable or invalid")
	ErrArchiveInvalid      = errors.New("official release asset is invalid")
)

// Release is an exact, verified selection from the official GitHub Release
// metadata. URL and filenames are Supervisor-derived and never caller input.
type Release struct {
	Version       string
	AssetName     string
	AssetURL      string
	ArchiveDigest string
	ArchiveSize   int64
}

type githubRelease struct {
	TagName string        `json:"tag_name"`
	Draft   bool          `json:"draft"`
	Assets  []githubAsset `json:"assets"`
}

type githubAsset struct {
	Name               string `json:"name"`
	Digest             string `json:"digest"`
	Size               int64  `json:"size"`
	BrowserDownloadURL string `json:"browser_download_url"`
}

type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// Source is the fixed official router-for-me/CLIProxyAPI release source.
type Source struct {
	client httpDoer
	asset  string
}

func NewSource() (*Source, error) {
	return newSource(runtime.GOOS, runtime.GOARCH, newHTTPClient())
}

func newSource(goos, goarch string, client httpDoer) (*Source, error) {
	assetSuffix, err := platformAssetSuffix(goos, goarch)
	if err != nil {
		return nil, err
	}
	if client == nil {
		return nil, errors.New("release HTTP client is required")
	}
	return &Source{client: client, asset: assetSuffix}, nil
}

func SupportedPlatform(goos, goarch string) bool {
	_, err := platformAssetSuffix(goos, goarch)
	return err == nil
}

func platformAssetSuffix(goos, goarch string) (string, error) {
	if goos != "linux" {
		return "", fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, goos, goarch)
	}
	switch goarch {
	case "amd64":
		return "linux_amd64.tar.gz", nil
	case "arm64":
		return "linux_aarch64.tar.gz", nil
	default:
		return "", fmt.Errorf("%w: %s/%s", ErrUnsupportedPlatform, goos, goarch)
	}
}

func (s *Source) Resolve(ctx context.Context, version string) (Release, error) {
	if err := ValidateVersion(version); err != nil {
		return Release{}, err
	}
	tag := "v" + version
	assetName := "CLIProxyAPI_" + version + "_" + s.asset
	metadataURL := (&url.URL{
		Scheme: "https",
		Host:   githubAPIHost,
		Path:   "/repos/" + officialRepositoryOwner + "/" + officialRepositoryName + "/releases/tags/" + tag,
	}).String()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, metadataURL, nil)
	if err != nil {
		return Release{}, fmt.Errorf("%w: create request: %v", ErrReleaseMetadata, err)
	}
	req.Header.Set("Accept", metadataAccept)
	req.Header.Set("User-Agent", userAgent)
	resp, err := s.client.Do(req)
	if err != nil {
		return Release{}, fmt.Errorf("%w: request exact tag: %v", ErrReleaseMetadata, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Request == nil || resp.Request.URL.String() != metadataURL {
		return Release{}, fmt.Errorf("%w: exact tag returned HTTP %d", ErrReleaseMetadata, resp.StatusCode)
	}
	if resp.ContentLength > maxReleaseMetadataBytes {
		return Release{}, fmt.Errorf("%w: response exceeds size limit", ErrReleaseMetadata)
	}
	encoded, err := io.ReadAll(io.LimitReader(resp.Body, maxReleaseMetadataBytes+1))
	if err != nil || len(encoded) > maxReleaseMetadataBytes {
		return Release{}, fmt.Errorf("%w: bounded response read failed", ErrReleaseMetadata)
	}
	var metadata githubRelease
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	if err := decoder.Decode(&metadata); err != nil {
		return Release{}, fmt.Errorf("%w: decode response: %v", ErrReleaseMetadata, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return Release{}, fmt.Errorf("%w: response must contain one JSON value", ErrReleaseMetadata)
	}
	if metadata.Draft || metadata.TagName != tag {
		return Release{}, fmt.Errorf("%w: release tag or draft state is invalid", ErrReleaseMetadata)
	}
	expectedURL := (&url.URL{
		Scheme: "https",
		Host:   githubHost,
		Path:   "/" + officialRepositoryOwner + "/" + officialRepositoryName + "/releases/download/" + tag + "/" + assetName,
	}).String()
	var selected *githubAsset
	for index := range metadata.Assets {
		if metadata.Assets[index].Name != assetName {
			continue
		}
		if selected != nil {
			return Release{}, fmt.Errorf("%w: duplicate expected asset", ErrReleaseMetadata)
		}
		selected = &metadata.Assets[index]
	}
	if selected == nil || selected.BrowserDownloadURL != expectedURL ||
		!canonicalDigest(selected.Digest) || selected.Size <= 0 || selected.Size > maxReleaseArchiveBytes {
		return Release{}, fmt.Errorf("%w: expected asset metadata is invalid", ErrReleaseMetadata)
	}
	return Release{
		Version:       version,
		AssetName:     assetName,
		AssetURL:      expectedURL,
		ArchiveDigest: selected.Digest,
		ArchiveSize:   selected.Size,
	}, nil
}

// Download writes and verifies the exact selected archive. It accepts no URL,
// digest, repository, or asset override from the caller.
func (s *Source) Download(ctx context.Context, release Release, destination io.Writer) error {
	if destination == nil || ValidateVersion(release.Version) != nil ||
		release.AssetName != "CLIProxyAPI_"+release.Version+"_"+s.asset ||
		!canonicalDigest(release.ArchiveDigest) || release.ArchiveSize <= 0 ||
		release.ArchiveSize > maxReleaseArchiveBytes {
		return fmt.Errorf("%w: noncanonical selected asset", ErrArchiveInvalid)
	}
	expectedURL := (&url.URL{
		Scheme: "https",
		Host:   githubHost,
		Path:   "/" + officialRepositoryOwner + "/" + officialRepositoryName + "/releases/download/v" + release.Version + "/" + release.AssetName,
	}).String()
	if release.AssetURL != expectedURL {
		return fmt.Errorf("%w: asset URL differs from official source", ErrArchiveInvalid)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, expectedURL, nil)
	if err != nil {
		return fmt.Errorf("%w: create request: %v", ErrArchiveInvalid, err)
	}
	req.Header.Set("Accept", assetAccept)
	req.Header.Set("User-Agent", userAgent)
	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("%w: download request: %v", ErrArchiveInvalid, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK || resp.Request == nil || resp.Request.URL.Scheme != "https" ||
		!allowedDownloadHost(resp.Request.URL.Hostname()) {
		return fmt.Errorf("%w: download returned HTTP %d", ErrArchiveInvalid, resp.StatusCode)
	}
	if resp.ContentLength >= 0 && resp.ContentLength != release.ArchiveSize {
		return fmt.Errorf("%w: download size differs from release metadata", ErrArchiveInvalid)
	}
	hasher := sha256.New()
	written, err := io.Copy(io.MultiWriter(destination, hasher), io.LimitReader(resp.Body, maxReleaseArchiveBytes+1))
	if err != nil || written != release.ArchiveSize || written > maxReleaseArchiveBytes {
		return fmt.Errorf("%w: bounded download size verification failed", ErrArchiveInvalid)
	}
	actual := "sha256:" + hex.EncodeToString(hasher.Sum(nil))
	if actual != release.ArchiveDigest {
		return fmt.Errorf("%w: archive digest mismatch", ErrArchiveInvalid)
	}
	return nil
}

func canonicalDigest(value string) bool {
	if len(value) != len("sha256:")+sha256.Size*2 || !strings.HasPrefix(value, "sha256:") {
		return false
	}
	for _, character := range value[len("sha256:"):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

func newHTTPClient() *http.Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = nil
	transport.DialContext = (&net.Dialer{Timeout: 10 * time.Second, KeepAlive: 30 * time.Second}).DialContext
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.ResponseHeaderTimeout = 30 * time.Second
	transport.ExpectContinueTimeout = time.Second
	return &http.Client{
		Transport: transport,
		Timeout:   ReleaseClientTimeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 || req.URL.Scheme != "https" || !allowedDownloadHost(req.URL.Hostname()) {
				return errors.New("unsafe GitHub release redirect")
			}
			if len(via) > 0 && via[0].URL.Hostname() == githubAPIHost {
				return errors.New("GitHub release metadata redirect is not allowed")
			}
			return nil
		},
	}
}

func allowedDownloadHost(host string) bool {
	switch strings.ToLower(host) {
	case githubHost, "release-assets.githubusercontent.com", "objects.githubusercontent.com", "github-releases.githubusercontent.com":
		return true
	default:
		return false
	}
}
