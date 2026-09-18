package setting

import (
	"bytes"
	"path/filepath"
	"testing"

	sqliterepo "github.com/seakee/cpa-manager-plus/apps/manager-server/internal/repository/sqlite"
)

func TestCPAUpdateCheckRoundTripIsIndependentFromManagerUpdateCheck(t *testing.T) {
	database, err := sqliterepo.Open(filepath.Join(t.TempDir(), "manager.sqlite"))
	if err != nil {
		t.Fatalf("open database: %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })

	if initial, err := LoadCPAUpdateCheck(t.Context(), database); err != nil || initial != nil {
		t.Fatalf("initial CPA update state = %q, %v", initial, err)
	}
	cpaState := []byte(`{"schema_version":1,"target_version":"7.3.4"}`)
	managerState := []byte(`{"schema_version":1,"channel_preference":"stable"}`)
	if err := SaveCPAUpdateCheck(t.Context(), database, cpaState); err != nil {
		t.Fatalf("save CPA update state: %v", err)
	}
	if err := SaveUpdateCheck(t.Context(), database, managerState); err != nil {
		t.Fatalf("save Manager update state: %v", err)
	}

	loadedCPA, err := LoadCPAUpdateCheck(t.Context(), database)
	if err != nil || !bytes.Equal(loadedCPA, cpaState) {
		t.Fatalf("loaded CPA update state = %q, %v", loadedCPA, err)
	}
	loadedManager, err := LoadUpdateCheck(t.Context(), database)
	if err != nil || !bytes.Equal(loadedManager, managerState) {
		t.Fatalf("loaded Manager update state = %q, %v", loadedManager, err)
	}
}
