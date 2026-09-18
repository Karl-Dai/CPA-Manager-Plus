package cpaupdate

import (
	"errors"
	"regexp"
	"strings"
)

const maxStableVersionLength = 96

var canonicalStableVersionPattern = regexp.MustCompile(`^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

type stableVersion struct {
	parts [3]string
}

func parseStableVersion(value string) (stableVersion, error) {
	if len(value) == 0 || len(value) > maxStableVersionLength {
		return stableVersion{}, errors.New("stable version has invalid length")
	}
	matches := canonicalStableVersionPattern.FindStringSubmatch(value)
	if matches == nil {
		return stableVersion{}, errors.New("stable version must be canonical MAJOR.MINOR.PATCH")
	}
	return stableVersion{parts: [3]string{matches[1], matches[2], matches[3]}}, nil
}

func parseStableReleaseTag(tag string) (string, error) {
	if !strings.HasPrefix(tag, "v") {
		return "", errors.New("stable release tag must use the vMAJOR.MINOR.PATCH form")
	}
	version := strings.TrimPrefix(tag, "v")
	if _, err := parseStableVersion(version); err != nil {
		return "", err
	}
	return version, nil
}

func compareStableVersions(left, right stableVersion) int {
	for index := range left.parts {
		if len(left.parts[index]) < len(right.parts[index]) {
			return -1
		}
		if len(left.parts[index]) > len(right.parts[index]) {
			return 1
		}
		if compared := strings.Compare(left.parts[index], right.parts[index]); compared != 0 {
			return compared
		}
	}
	return 0
}
