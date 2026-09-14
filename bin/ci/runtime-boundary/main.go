// runtime-boundary complements Go's internal package visibility checks with
// Manager ownership rules. SQLite itself is allowed for a private Supervisor
// journal. Dynamic file access and model traffic remain ADR/review invariants.
package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/scanner"
	"go/token"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
)

const managerModule = "github.com/seakee/cpa-manager-plus/apps/manager-server"

var managerStoragePath = regexp.MustCompile(`(?:^|[/\\:])(?:usage\.sqlite(?:-wal|-shm)?|data\.key)(?:$|[?#])`)

type module struct {
	Path    string
	Dir     string
	Replace *module
}

func main() {
	root := flag.String("module", "apps/runtime-supervisor", "Supervisor module directory")
	flag.Parse()
	if err := checkBoundary(*root); err != nil {
		fmt.Fprintln(os.Stderr, "Runtime Supervisor boundary failed:", err)
		os.Exit(1)
	}
	fmt.Println("Runtime Supervisor boundary passed: no Manager-owned storage/config dependencies")
}

func checkBoundary(root string) error {
	// Use Go's module graph instead of maintaining an import/dependency parser.
	// Test/build already enforce internal visibility; this also rejects unused
	// requirements, transitive modules, and replacements into Manager code.
	cmd := exec.Command("go", "list", "-mod=readonly", "-m", "-json", "all")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return fmt.Errorf("list Supervisor modules: %w: %s", err, exitErr.Stderr)
		}
		return fmt.Errorf("list Supervisor modules: %w", err)
	}
	decoder := json.NewDecoder(strings.NewReader(string(output)))
	for {
		var dependency module
		if err := decoder.Decode(&dependency); errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("decode Supervisor modules: %w", err)
		}
		for current := &dependency; current != nil; current = current.Replace {
			if current.Path == managerModule || strings.HasPrefix(current.Path, managerModule+"/") {
				return fmt.Errorf("Manager-owned module dependency %q", current.Path)
			}
			if current.Dir != "" {
				dir, err := filepath.EvalSymlinks(current.Dir)
				if err != nil {
					return fmt.Errorf("resolve module directory: %w", err)
				}
				if strings.Contains(filepath.ToSlash(dir)+"/", "/apps/manager-server/") {
					return fmt.Errorf("Manager-owned module directory %q", dir)
				}
			}
		}
	}

	// Check literal references to the Manager's concrete storage assets,
	// configuration keys, and secret paths, including tests and files for
	// other build platforms.
	// Go's scanner skips comments and decodes escaped/raw strings without AST
	// analysis. Third-party vendored source is covered by the module check.
	return filepath.WalkDir(root, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			if entry.Name() == "vendor" || entry.Name() == ".git" {
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(name) != ".go" {
			return nil
		}
		source, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		files := token.NewFileSet()
		var lexer scanner.Scanner
		var scanErr error
		lexer.Init(files.AddFile(name, -1, len(source)), source, func(pos token.Position, message string) {
			scanErr = fmt.Errorf("%s: %s", pos, message)
		}, 0)
		for {
			pos, kind, literal := lexer.Scan()
			if kind == token.EOF {
				return scanErr
			}
			if kind != token.STRING {
				continue
			}
			value, err := strconv.Unquote(literal)
			if err != nil {
				return fmt.Errorf("%s: %w", files.Position(pos), err)
			}
			if isManagerOwnedReference(value) {
				return fmt.Errorf("%s: Manager-owned storage/config reference %q", files.Position(pos), value)
			}
		}
	})
}

func isManagerOwnedReference(value string) bool {
	if managerStoragePath.MatchString(value) {
		return true
	}
	if strings.HasPrefix(value, "CPA_MANAGER_") || strings.HasPrefix(value, "USAGE_") {
		return true
	}
	switch value {
	case "CPA_MANAGEMENT_KEY", "CPA_MANAGEMENT_KEY_FILE", "CPA_UPSTREAM_URL",
		"/run/secrets/cpa_management_key", "/run/secrets/cpa_admin_key", "/run/secrets/cpa_data_key":
		return true
	default:
		return false
	}
}
