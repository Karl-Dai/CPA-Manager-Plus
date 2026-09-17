// Package filetrust validates persisted filesystem authority before callers
// repair permissions or consume persisted state.
package filetrust

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"reflect"
)

var ErrUntrustedOwner = errors.New("persisted state owner is not the Supervisor")

// RequireCurrentProcessOwner rejects persisted state that is not owned by the
// effective Supervisor identity. Windows has no numeric effective UID and does
// not support Runtime18's Linux child-identity boundary.
func RequireCurrentProcessOwner(info fs.FileInfo) error {
	effectiveUID := os.Geteuid()
	if effectiveUID < 0 {
		return nil
	}
	ownerUID, ok := numericOwnerUID(info)
	if !ok {
		return fmt.Errorf("%w: numeric owner is unavailable", ErrUntrustedOwner)
	}
	if ownerUID != uint64(effectiveUID) {
		return fmt.Errorf(
			"%w: owner UID %d differs from Supervisor UID %d",
			ErrUntrustedOwner,
			ownerUID,
			effectiveUID,
		)
	}
	return nil
}

func numericOwnerUID(info fs.FileInfo) (uint64, bool) {
	if info == nil || info.Sys() == nil {
		return 0, false
	}
	value := reflect.ValueOf(info.Sys())
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			return 0, false
		}
		value = value.Elem()
	}
	if value.Kind() != reflect.Struct {
		return 0, false
	}
	uid := value.FieldByName("Uid")
	if !uid.IsValid() {
		return 0, false
	}
	switch uid.Kind() {
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		return uid.Uint(), true
	default:
		return 0, false
	}
}
