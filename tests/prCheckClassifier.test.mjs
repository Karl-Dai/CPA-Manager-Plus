import { execFileSync } from 'node:child_process';
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { describe, expect, it } from 'vitest';
import {
  classifyChangedFiles,
  findForbiddenUnicode,
  parseChangedFilesInput,
  scanChangedTextFiles,
} from '../bin/ci/classify-pr-checks.mjs';

const noChecks = {
  frontend: false,
  manager_server: false,
  runtime_supervisor: false,
  ingress: false,
  windows_sqlite: false,
  native_control: false,
  docker: false,
  demo_docs: false,
  release_content: false,
};

describe('PR check classifier', () => {
  it('fails closed when no changed files are available', () => {
    expect(classifyChangedFiles([])).toEqual({
      frontend: true,
      manager_server: true,
      runtime_supervisor: true,
      ingress: true,
      windows_sqlite: true,
      native_control: true,
      docker: true,
      demo_docs: true,
      release_content: true,
    });
  });

  it('skips application checks for ordinary non-site docs changes', () => {
    expect(classifyChangedFiles(['docs/release.md'])).toEqual(noChecks);
  });

  it('runs frontend and Docker checks for web changes', () => {
    expect(classifyChangedFiles(['apps/web/src/features/login/LoginPage.tsx'])).toEqual({
      ...noChecks,
      frontend: true,
      docker: true,
      demo_docs: true,
    });
  });

  it('runs frontend checks for docs-site and README changes', () => {
    expect(classifyChangedFiles(['apps/docs/index.md', 'README.md'])).toEqual({
      ...noChecks,
      frontend: true,
      demo_docs: true,
    });
  });

  it('runs Demo and Docs checks for web and docs-site changes', () => {
    expect(classifyChangedFiles(['apps/docs/index.md', 'apps/web/src/App.tsx'])).toEqual({
      ...noChecks,
      frontend: true,
      docker: true,
      demo_docs: true,
    });
  });

  it('runs release validation for release content and its validator', () => {
    expect(classifyChangedFiles(['docs/release-notes/v1.2.3-zh.md'])).toEqual({
      ...noChecks,
      release_content: true,
    });
    expect(classifyChangedFiles(['docs/release-posts/v1.2.3-telegram.html'])).toEqual({
      ...noChecks,
      release_content: true,
    });
    expect(classifyChangedFiles(['bin/release/validate-release.mjs'])).toEqual({
      ...noChecks,
      frontend: true,
      release_content: true,
    });
  });

  it('runs Node tests for every release automation script', () => {
    for (const filePath of [
      'bin/release/send-telegram-release.sh',
      'bin/release/verify-published-release.mjs',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        frontend: true,
      });
    }
  });

  it('runs workflow integrity tests for Dependabot configuration changes', () => {
    expect(classifyChangedFiles(['.github/dependabot.yml'])).toEqual({
      ...noChecks,
      frontend: true,
    });
  });

  it('runs Linux, Windows SQLite, and Docker checks for manager-server changes', () => {
    expect(
      classifyChangedFiles(['apps/manager-server/internal/repository/sqlite/database.go'])
    ).toEqual({
      ...noChecks,
      manager_server: true,
      windows_sqlite: true,
      docker: true,
    });
  });

  it('runs native checks only for native control changes', () => {
    expect(classifyChangedFiles(['bin/native/cpa-manager-plusctl.ps1'])).toEqual({
      ...noChecks,
      native_control: true,
    });
  });

  it('runs Supervisor and Docker checks for Supervisor module changes', () => {
    for (const filePath of [
      'apps/runtime-supervisor/go.mod',
      'apps/runtime-supervisor/internal/protocol/handler.go',
      './apps\\runtime-supervisor\\cmd\\cpamp-runtime-supervisor\\main_test.go',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        runtime_supervisor: true,
        docker: true,
      });
    }
  });

  it('runs both necessary check sets for mixed Manager and Supervisor changes', () => {
    expect(
      classifyChangedFiles([
        'apps/manager-server/internal/service/runtime/embedded.go',
        'apps/runtime-supervisor/internal/protocol/handler.go',
      ])
    ).toEqual({
      ...noChecks,
      manager_server: true,
      runtime_supervisor: true,
      docker: true,
      windows_sqlite: true,
    });
  });

  it('runs Ingress and Docker checks for Ingress changes', () => {
    for (const filePath of ['apps/ingress/internal/ingress/proxy.go', 'Dockerfile.ingress']) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        ingress: true,
        docker: true,
      });
    }
  });

  it('runs Supervisor and Docker checks for Runtime packaging changes', () => {
    for (const filePath of [
      'Dockerfile.runtime',
      'docker/runtime/entrypoint.sh',
      'docker/runtime/config.seed.yaml',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        runtime_supervisor: true,
        docker: true,
      });
    }
  });

  it('runs Supervisor checks when its architecture gate or fixtures change', () => {
    for (const filePath of [
      'bin/ci/runtime-boundary/main.go',
      'bin/ci/runtime-boundary/main_test.go',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        runtime_supervisor: true,
      });
    }
    expect(classifyChangedFiles(['apps/runtime-supervisor-extra/main.go'])).toEqual(noChecks);
  });

  it('runs build coverage for native packaging changes', () => {
    expect(classifyChangedFiles(['bin/release/package-native.sh'])).toEqual({
      ...noChecks,
      frontend: true,
      manager_server: true,
      windows_sqlite: true,
      native_control: true,
    });
  });

  it('runs Docker validation for Compose changes', () => {
    for (const filePath of [
      'docker-compose.yml',
      'docker-compose.manager.yml',
      'bin/ci/runtime12-docker-smoke.sh',
      'bin/ci/runtime14-failure-e2e.sh',
      'bin/ci/runtime17-activation-e2e.sh',
      'bin/ci/runtime18-secret-state-e2e.sh',
      'bin/ci/runtime19-phase1-exit-e2e.sh',
      'bin/ci/validate-runtime12-compose.mjs',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        ...noChecks,
        docker: true,
      });
    }
  });

  it('runs Docker failure validation for every combined Runtime lifecycle family', () => {
    for (const filePath of [
      'apps/ingress/internal/ingress/proxy.go',
      'apps/manager-server/internal/service/runtime/reconciler.go',
      'apps/manager-server/internal/repository/setting/repository.go',
      'apps/manager-server/internal/service/bootstrap/service.go',
      'apps/runtime-supervisor/internal/protocol/handler.go',
      'apps/runtime-supervisor/internal/lifecycle/recovery.go',
      'apps/runtime-supervisor/internal/readiness/readiness.go',
      'apps/runtime-supervisor/internal/journal/store.go',
      'apps/runtime-supervisor/internal/cpaprocess/process.go',
      'docker/runtime/entrypoint.sh',
      'Dockerfile.manager-server',
      'Dockerfile.ingress',
      'Dockerfile.runtime',
      'docker-compose.yml',
      'bin/ci/runtime12-docker-smoke.sh',
      'bin/ci/runtime14-failure-e2e.sh',
      'bin/ci/runtime17-activation-e2e.sh',
      'bin/ci/runtime18-secret-state-e2e.sh',
      'bin/ci/runtime19-phase1-exit-e2e.sh',
    ]) {
      expect(classifyChangedFiles([filePath]).docker, filePath).toBe(true);
    }
  });

  it('runs Node and Docker checks for root dependency changes', () => {
    expect(classifyChangedFiles(['package-lock.json'])).toEqual({
      ...noChecks,
      frontend: true,
      native_control: true,
      docker: true,
      demo_docs: true,
    });
  });

  it('runs all checks when the classifier or any workflow changes', () => {
    for (const filePath of [
      '.github/workflows/pr-check.yml',
      '.github/workflows/release.yml',
      'bin/ci/classify-pr-checks.mjs',
      'tests/prCheckClassifier.test.mjs',
    ]) {
      expect(classifyChangedFiles([filePath])).toEqual({
        frontend: true,
        manager_server: true,
        runtime_supervisor: true,
        ingress: true,
        windows_sqlite: true,
        native_control: true,
        docker: true,
        demo_docs: true,
        release_content: true,
      });
    }
  });

  it('normalizes CRLF input paths, Windows separators, blanks, and duplicates', () => {
    expect(
      classifyChangedFiles([
        'apps\\web\\src\\App.tsx\r',
        '',
        './apps/web/src/App.tsx',
        'apps/manager-server/cmd/cpa-manager-plus/main.go',
      ])
    ).toEqual({
      ...noChecks,
      frontend: true,
      manager_server: true,
      windows_sqlite: true,
      docker: true,
      demo_docs: true,
    });
  });

  it('detects forbidden invisible Unicode code points', () => {
    expect(findForbiddenUnicode('safe\u200Btext\u202E')).toEqual(['U+200B', 'U+202E']);
    expect(findForbiddenUnicode('plain text')).toEqual([]);
  });

  it('preserves Git NUL-delimited Unicode paths for classification and scanning', () => {
    const repository = mkdtempSync(path.join(tmpdir(), 'cpamp-classifier-'));
    const relativePath = 'apps/web/安全\u200B检查.ts';

    try {
      execFileSync('git', ['init', '--quiet'], { cwd: repository });
      mkdirSync(path.dirname(path.join(repository, relativePath)), { recursive: true });
      writeFileSync(path.join(repository, relativePath), 'safe\u202Etext\n');
      execFileSync('git', ['add', relativePath], { cwd: repository });
      const changedFiles = parseChangedFilesInput(
        execFileSync('git', ['diff', '--cached', '--name-only', '--no-renames', '-z'], {
          cwd: repository,
          encoding: 'utf8',
        }),
        { nullDelimited: true }
      );

      expect(changedFiles).toEqual([relativePath]);
      expect(classifyChangedFiles(changedFiles)).toMatchObject({ frontend: true, docker: true });
      expect(scanChangedTextFiles(changedFiles, { root: repository })).toEqual([
        `${relativePath} contains U+202E`,
      ]);
    } finally {
      rmSync(repository, { recursive: true, force: true });
    }
  });

  it('rejects changed paths that resolve outside the repository', () => {
    expect(scanChangedTextFiles(['../../outside.txt'])).toEqual([
      '../../outside.txt resolves outside the repository',
    ]);
  });
});
