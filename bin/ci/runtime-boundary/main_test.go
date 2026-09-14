package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const supervisorModule = "github.com/seakee/cpa-manager-plus/apps/runtime-supervisor"

func fixture(t *testing.T, files map[string]string) string {
	t.Helper()
	t.Setenv("GOPROXY", "off")
	t.Setenv("GOSUMDB", "off")
	root := t.TempDir()
	defaults := map[string]string{
		"apps/runtime-supervisor/go.mod":     "module " + supervisorModule + "\n\ngo 1.24.0\n",
		"apps/runtime-supervisor/fixture.go": "package fixture\n",
	}
	for name, content := range files {
		defaults[name] = content
	}
	for name, content := range defaults {
		target := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(target), 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return filepath.Join(root, "apps/runtime-supervisor")
}

func TestBoundaryAllowsPrivateSQLiteJournalAndRuntimeToken(t *testing.T) {
	root := fixture(t, map[string]string{
		"apps/runtime-supervisor/go.mod": "module " + supervisorModule + `

go 1.24.0

require modernc.org/sqlite v0.0.0
replace modernc.org/sqlite => ../../sqlite
`,
		// A local stand-in proves that SQLite module names are allowed without
		// fetching or implementing any journal in the Supervisor itself.
		"sqlite/go.mod":    "module modernc.org/sqlite\n\ngo 1.24.0\n",
		"sqlite/sqlite.go": "package sqlite\n",
		"apps/runtime-supervisor/fixture.go": `package fixture
import (
	_ "database/sql"
	_ "modernc.org/sqlite"
)
// Manager usage.sqlite and data.key must never be opened by Supervisor.
const journal = "/runtime/operations.sqlite"
const tokenEnv = "CPAMP_RUNTIME_TOKEN"
`,
	})
	if err := checkBoundary(root); err != nil {
		t.Fatal(err)
	}
}

func TestBoundaryRejectsManagerModule(t *testing.T) {
	for _, source := range []string{
		"package fixture\n", // An unused module requirement is still forbidden.
		"package fixture\nimport _ \"" + managerModule + "/productconfig\"\n",
	} {
		t.Run(source, func(t *testing.T) {
			root := fixture(t, map[string]string{
				"apps/runtime-supervisor/go.mod":     "module " + supervisorModule + "\n\ngo 1.24.0\n\nrequire " + managerModule + " v0.0.0\nreplace " + managerModule + " => ../manager-server\n",
				"apps/runtime-supervisor/fixture.go": source,
				"apps/manager-server/go.mod":         "module " + managerModule + "\n\ngo 1.24.0\n",
				// This public package is legal under Go's internal visibility
				// rules, so rejecting it demonstrates the additional boundary.
				"apps/manager-server/productconfig/config.go": "package productconfig\n",
			})
			if err := checkBoundary(root); err == nil || !strings.Contains(err.Error(), "Manager-owned module dependency") {
				t.Fatalf("checkBoundary() = %v, want Manager module rejection", err)
			}
		})
	}
}

func TestBoundaryRejectsTransitiveManagerModule(t *testing.T) {
	root := fixture(t, map[string]string{
		"apps/runtime-supervisor/go.mod": "module " + supervisorModule + `

go 1.24.0

require example.com/adapter v0.0.0
require ` + managerModule + ` v0.0.0 // indirect
replace example.com/adapter => ../../adapter
replace ` + managerModule + ` => ../manager-server
`,
		"adapter/go.mod":                              "module example.com/adapter\n\ngo 1.24.0\nrequire " + managerModule + " v0.0.0\n",
		"adapter/adapter.go":                          "package adapter\nimport _ \"" + managerModule + "/productconfig\"\n",
		"apps/manager-server/go.mod":                  "module " + managerModule + "\n\ngo 1.24.0\n",
		"apps/manager-server/productconfig/config.go": "package productconfig\n",
		"apps/runtime-supervisor/fixture.go":          "package fixture\nimport _ \"example.com/adapter\"\n",
	})
	if err := checkBoundary(root); err == nil || !strings.Contains(err.Error(), "Manager-owned module dependency") {
		t.Fatalf("checkBoundary() = %v, want transitive Manager module rejection", err)
	}
}

func TestBoundaryRejectsReplacementIntoManagerDirectory(t *testing.T) {
	root := fixture(t, map[string]string{
		"apps/runtime-supervisor/go.mod": "module " + supervisorModule + `

go 1.24.0

require example.com/productconfig v0.0.0
replace example.com/productconfig => ../manager-server
`,
		"apps/manager-server/go.mod":         "module example.com/productconfig\n\ngo 1.24.0\n",
		"apps/manager-server/config.go":      "package productconfig\n",
		"apps/runtime-supervisor/fixture.go": "package fixture\nimport _ \"example.com/productconfig\"\n",
	})
	if err := checkBoundary(root); err == nil || !strings.Contains(err.Error(), "Manager-owned module directory") {
		t.Fatalf("checkBoundary() = %v, want Manager directory rejection", err)
	}
}

func TestBoundaryRejectsManagerStorageAndConfigReferences(t *testing.T) {
	for _, literal := range []string{
		`"usage.sqlite"`,
		`"file:/data/usage.sqlite?mode=ro"`,
		`"/data/usage.sqlite-wal"`,
		`"data.key"`,
		"`C:\\manager\\data.key`",
		`"\x75sage.sqlite"`,
		`"USAGE_DB_PATH"`,
		`"USAGE_DATA_DIR"`,
		`"USAGE_COLLECTOR_MODE"`,
		`"CPA_MANAGER_CONFIG"`,
		`"CPA_MANAGER_ADMIN_KEY"`,
		`"CPA_MANAGER_ADMIN_KEY_FILE"`,
		`"CPA_MANAGER_DATA_KEY"`,
		`"CPA_MANAGER_DATA_KEY_FILE"`,
		`"CPA_MANAGER_DATA_KEY_PATH"`,
		`"CPA_MANAGEMENT_KEY"`,
		`"CPA_MANAGEMENT_KEY_FILE"`,
		`"CPA_UPSTREAM_URL"`,
		`"/run/secrets/cpa_management_key"`,
		`"/run/secrets/cpa_admin_key"`,
		`"/run/secrets/cpa_data_key"`,
	} {
		t.Run(literal, func(t *testing.T) {
			root := fixture(t, map[string]string{
				"apps/runtime-supervisor/reference.go": "package fixture\nconst reference = " + literal + "\n",
			})
			if err := checkBoundary(root); err == nil || !strings.Contains(err.Error(), "reference.go:2:") || !strings.Contains(err.Error(), "Manager-owned storage/config reference") {
				t.Fatalf("checkBoundary() = %v, want Manager-owned reference rejection with source position", err)
			}
		})
	}
}

func TestBoundaryChecksTestsAndOtherPlatforms(t *testing.T) {
	for _, name := range []string{"storage_test.go", "storage_windows.go"} {
		t.Run(name, func(t *testing.T) {
			root := fixture(t, map[string]string{
				"apps/runtime-supervisor/" + name: "package fixture\nconst storage = \"usage.sqlite\"\n",
			})
			if err := checkBoundary(root); err == nil || !strings.Contains(err.Error(), "Manager-owned storage/config reference") {
				t.Fatalf("checkBoundary() = %v, want Manager storage rejection", err)
			}
		})
	}
}
