// Package artifact observes the exact executable selected by the Runtime
// Supervisor without making human-readable version metadata authoritative.
package artifact

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
)

const (
	EngineCPA             = "cpa"
	ManifestSchemaVersion = 1
	artifactIDPrefix      = "sha256:"
	manifestLimit         = 16 << 10
)

var (
	ErrExecutableUnavailable = errors.New("active executable is unavailable")
	ErrManifestUnavailable   = errors.New("trusted artifact manifest is unavailable")
	ErrManifestInvalid       = errors.New("trusted artifact manifest is invalid")
	ErrManifestMismatch      = errors.New("trusted artifact manifest does not match the active executable")
)

// ID is the content-derived identity of exact executable bytes.
type ID string

func (id ID) IsValid() bool {
	value := string(id)
	if len(value) != len(artifactIDPrefix)+sha256.Size*2 || !strings.HasPrefix(value, artifactIDPrefix) {
		return false
	}
	for _, character := range value[len(artifactIDPrefix):] {
		if (character < '0' || character > '9') && (character < 'a' || character > 'f') {
			return false
		}
	}
	return true
}

// Observation is safe to expose on the private Runtime status contract.
// Version is trusted display metadata only and is never an update fence.
type Observation struct {
	Engine     string `json:"engine"`
	ArtifactID ID     `json:"artifactId"`
	Version    string `json:"version,omitempty"`
}

func (o Observation) Validate() error {
	if o.Engine != EngineCPA {
		return fmt.Errorf("unsupported artifact engine %q", o.Engine)
	}
	if !o.ArtifactID.IsValid() {
		return fmt.Errorf("invalid artifact ID %q", o.ArtifactID)
	}
	if o.Version != strings.TrimSpace(o.Version) {
		return errors.New("artifact version must not contain surrounding whitespace")
	}
	return nil
}

type manifest struct {
	SchemaVersion int    `json:"schemaVersion"`
	Engine        string `json:"engine"`
	Version       string `json:"version"`
	ArtifactID    ID     `json:"artifactId"`
}

type fileOpener func(string) (io.ReadCloser, error)
type fileReader func(string) ([]byte, error)

// Observer caches an observation produced at an owning lifecycle boundary.
// Status polling only reads this cache; a future owning switch path can call
// Refresh after it has atomically selected a new executable.
type Observer struct {
	executablePath string
	manifestPath   string
	openFile       fileOpener
	readFile       fileReader

	mu          sync.RWMutex
	observation *Observation
	lastError   error
}

func NewObserver(executablePath, manifestPath string) *Observer {
	return newObserver(executablePath, manifestPath, func(path string) (io.ReadCloser, error) {
		return os.Open(path)
	}, readManifestFile)
}

func readManifestFile(path string) ([]byte, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	encoded, readErr := io.ReadAll(io.LimitReader(file, manifestLimit+1))
	return encoded, errors.Join(readErr, file.Close())
}

func newObserver(executablePath, manifestPath string, openFile fileOpener, readFile fileReader) *Observer {
	return &Observer{
		executablePath: executablePath,
		manifestPath:   manifestPath,
		openFile:       openFile,
		readFile:       readFile,
	}
}

// Refresh recomputes identity from the exact configured executable bytes.
// Manifest failures preserve the exact digest while withholding Version.
func (o *Observer) Refresh() error {
	observed, err := o.observe()
	o.mu.Lock()
	defer o.mu.Unlock()
	o.lastError = err
	if observed.ArtifactID == "" {
		o.observation = nil
		return err
	}
	o.observation = &observed
	return err
}

// Observation returns a defensive copy of the cached observation and performs
// no filesystem access or hashing.
func (o *Observer) Observation() *Observation {
	o.mu.RLock()
	defer o.mu.RUnlock()
	if o.observation == nil {
		return nil
	}
	observed := *o.observation
	return &observed
}

func (o *Observer) LastError() error {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return o.lastError
}

func (o *Observer) observe() (Observation, error) {
	file, err := o.openFile(o.executablePath)
	if err != nil {
		return Observation{}, fmt.Errorf("%w: open executable: %v", ErrExecutableUnavailable, err)
	}
	hasher := sha256.New()
	_, copyErr := io.Copy(hasher, file)
	closeErr := file.Close()
	if copyErr != nil || closeErr != nil {
		return Observation{}, fmt.Errorf("%w: read executable: %v", ErrExecutableUnavailable, errors.Join(copyErr, closeErr))
	}
	observed := Observation{
		Engine:     EngineCPA,
		ArtifactID: ID(artifactIDPrefix + hex.EncodeToString(hasher.Sum(nil))),
	}

	trusted, err := o.loadManifest()
	if err != nil {
		return observed, err
	}
	if trusted.ArtifactID != observed.ArtifactID {
		return observed, fmt.Errorf("%w: manifest artifact ID differs from executable digest", ErrManifestMismatch)
	}
	observed.Version = trusted.Version
	return observed, nil
}

func (o *Observer) loadManifest() (manifest, error) {
	if strings.TrimSpace(o.manifestPath) == "" {
		return manifest{}, ErrManifestUnavailable
	}
	encoded, err := o.readFile(o.manifestPath)
	if err != nil {
		return manifest{}, fmt.Errorf("%w: read manifest: %v", ErrManifestUnavailable, err)
	}
	if len(encoded) > manifestLimit {
		return manifest{}, fmt.Errorf("%w: manifest exceeds %d bytes", ErrManifestInvalid, manifestLimit)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	var trusted manifest
	if err := decoder.Decode(&trusted); err != nil {
		return manifest{}, fmt.Errorf("%w: decode manifest: %v", ErrManifestInvalid, err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return manifest{}, fmt.Errorf("%w: manifest must contain one JSON value", ErrManifestInvalid)
	}
	if trusted.SchemaVersion != ManifestSchemaVersion || trusted.Engine != EngineCPA ||
		trusted.Version == "" || trusted.Version != strings.TrimSpace(trusted.Version) || !trusted.ArtifactID.IsValid() {
		return manifest{}, fmt.Errorf("%w: unsupported schema or noncanonical fields", ErrManifestInvalid)
	}
	return trusted, nil
}
