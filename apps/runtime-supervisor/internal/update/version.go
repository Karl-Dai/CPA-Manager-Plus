package update

import (
	"errors"
	"strings"
)

const maxVersionBytes = 96

var ErrInvalidTargetVersion = errors.New("target version is not an exact canonical release version")

// ValidateVersion accepts canonical SemVer text without the tag's leading v.
// Prerelease and build identifiers remain exact caller-selected release tags;
// floating aliases, paths, URLs, whitespace aliases, and leading-zero numeric
// identifiers fail closed.
func ValidateVersion(version string) error {
	if version == "" || version != strings.TrimSpace(version) || len(version) > maxVersionBytes ||
		strings.ContainsAny(version, "/\\:\x00") {
		return ErrInvalidTargetVersion
	}
	coreAndPrerelease, build, ok := splitOnce(version, "+")
	if !ok || (build != "" && !validIdentifiers(build, false)) {
		return ErrInvalidTargetVersion
	}
	core, prerelease, ok := splitOnce(coreAndPrerelease, "-")
	if !ok || (prerelease != "" && !validIdentifiers(prerelease, true)) {
		return ErrInvalidTargetVersion
	}
	parts := strings.Split(core, ".")
	if len(parts) != 3 {
		return ErrInvalidTargetVersion
	}
	for _, part := range parts {
		if !validNumericIdentifier(part) {
			return ErrInvalidTargetVersion
		}
	}
	return nil
}

func splitOnce(value, separator string) (string, string, bool) {
	if strings.Count(value, separator) > 1 {
		return "", "", false
	}
	left, right, found := strings.Cut(value, separator)
	if found && (left == "" || right == "") {
		return "", "", false
	}
	return left, right, true
}

func validIdentifiers(value string, rejectLeadingZeroNumeric bool) bool {
	for _, identifier := range strings.Split(value, ".") {
		if identifier == "" {
			return false
		}
		numeric := true
		for _, character := range identifier {
			if character < '0' || character > '9' {
				numeric = false
			}
			if (character < '0' || character > '9') &&
				(character < 'A' || character > 'Z') &&
				(character < 'a' || character > 'z') && character != '-' {
				return false
			}
		}
		if rejectLeadingZeroNumeric && numeric && len(identifier) > 1 && identifier[0] == '0' {
			return false
		}
	}
	return true
}

func validNumericIdentifier(value string) bool {
	if value == "" || (len(value) > 1 && value[0] == '0') {
		return false
	}
	for _, character := range value {
		if character < '0' || character > '9' {
			return false
		}
	}
	return true
}
