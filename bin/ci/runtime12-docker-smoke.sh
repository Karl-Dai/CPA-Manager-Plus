#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
project="${CPAMP_SMOKE_PROJECT:-cpamp-runtime12-${RANDOM}-$$}"
public_port="${CPAMP_SMOKE_PORT:-28317}"
skip_build="${CPAMP_SMOKE_SKIP_BUILD:-false}"
compose=(docker compose --project-directory "${repo_root}" -p "${project}")

cleanup() {
  "${compose[@]}" down --volumes --remove-orphans >/dev/null 2>&1 || true
}
trap cleanup EXIT HUP INT TERM

retry() {
  local attempts="$1"
  shift
  local count=0
  until "$@"; do
    count=$((count + 1))
    if [ "$count" -ge "$attempts" ]; then
      return 1
    fi
    sleep 1
  done
}

runtime_get() {
  local request_path="$1"
  "${compose[@]}" exec -T cpamp-manager sh -ec '
    token="$(cat /run/cpamp/runtime-secret/token)"
    wget -qO- --header="Authorization: Bearer ${token}" "http://cpamp-runtime:9081${1}"
  ' sh "$request_path"
}

runtime_post() {
  local request_path="$1"
  local payload="$2"
  "${compose[@]}" exec -T cpamp-manager sh -ec '
    token="$(cat /run/cpamp/runtime-secret/token)"
    wget -qO- \
      --header="Authorization: Bearer ${token}" \
      --header="Content-Type: application/json" \
      --post-data="${2}" \
      "http://cpamp-runtime:9081${1}"
  ' sh "$request_path" "$payload"
}

json_field() {
  local field="$1"
  node -e '
    const value = JSON.parse(require("node:fs").readFileSync(0, "utf8"));
    const fields = process.argv[1].split(".");
    let current = value;
    for (const field of fields) current = current?.[field];
    if (current === undefined || current === null) process.exit(2);
    process.stdout.write(String(current));
  ' "$field"
}

json_uint_field() {
  local field="$1"
  node -e '
    const input = require("node:fs").readFileSync(0, "utf8");
    const match = input.match(new RegExp(`"${process.argv[1]}"\\s*:\\s*(\\d+)`));
    if (!match) process.exit(2);
    process.stdout.write(match[1]);
  ' "$field"
}

runtime_operation_payload() {
  local operation_id="$1"
  local status="$2"
  local identity_json generation
  identity_json="$(printf '%s' "$status" | node -e '
    const status = JSON.parse(require("node:fs").readFileSync(0, "utf8"));
    process.stdout.write(JSON.stringify(status.runtimeIdentity));
  ')"
  generation="$(printf '%s' "$status" | json_uint_field runtimeGeneration)"
  printf '{"operationId":"%s","expectedRuntimeIdentity":%s,"expectedRuntimeGeneration":%s}' \
    "$operation_id" "$identity_json" "$generation"
}

wait_runtime_state() {
  local expected="$1"
  local attempts="${2:-60}"
  local count=0
  while [ "$count" -lt "$attempts" ]; do
    local status
    if status="$(runtime_get /v1/runtime/status 2>/dev/null)" && [ "$(printf '%s' "$status" | json_field state)" = "$expected" ]; then
      printf '%s' "$status"
      return 0
    fi
    count=$((count + 1))
    sleep 1
  done
  return 1
}

cd "$repo_root"
CPAMP_PUBLIC_PORT=18317 "${compose[@]}" config --format json | node bin/ci/validate-runtime12-compose.mjs
docker compose -f docker-compose.manager.yml config >/dev/null
export CPAMP_PUBLIC_PORT="${public_port}"
if [ "$skip_build" != "true" ]; then
  "${compose[@]}" build
fi
docker run --rm \
  --env CPAMP_RUNTIME_TOKEN=runtime12-ci-test-only-transport-secret \
  seakee/cpamp-runtime:latest \
  sh -ec 'test "$(cat /run/cpamp/runtime-secret/token)" = "$CPAMP_RUNTIME_TOKEN"'
"${compose[@]}" up -d

public_origin="http://127.0.0.1:${public_port}"
retry 90 curl --fail --silent "${public_origin}/health" >/dev/null

"${compose[@]}" exec -T cpamp-runtime sh -ec '
  test "$(tr "\000" "\n" </proc/1/cmdline | head -n1)" = "/usr/local/bin/cpamp-runtime-supervisor"
  test "$(stat -c %a /run/cpamp/runtime-secret/token)" = "600"
  test "$(wc -c </run/cpamp/runtime-secret/token | tr -d " ")" -ge 32
  test -f /runtime/gateway/config.yaml
  test "$(stat -c %a /runtime/gateway/config.yaml)" = "600"
  test -d /runtime/supervisor
  test -d /runtime/gateway/auth
  ! grep -Eq "secret-key: +[^\" ]|api-keys: +\[[^]]" /runtime/gateway/config.yaml
'

initial_status="$(wait_runtime_state offline)"
if [ "$(printf '%s' "$initial_status" | json_field recovery.state)" != "inactive" ]; then
  echo "fresh Runtime recovery must be inactive" >&2
  exit 1
fi
if curl --silent --output /dev/null --fail "${public_origin}/v1/models"; then
  echo "Gateway unexpectedly auto-started with the Compose stack" >&2
  exit 1
fi

secret_before="$("${compose[@]}" exec -T cpamp-manager sha256sum /run/cpamp/runtime-secret/token | awk '{print $1}')"
config_before="$("${compose[@]}" exec -T cpamp-runtime sha256sum /runtime/gateway/config.yaml | awk '{print $1}')"
"${compose[@]}" restart cpamp-runtime >/dev/null
restarted_status="$(wait_runtime_state offline)"
if [ "$(printf '%s' "$restarted_status" | json_field recovery.state)" != "inactive" ]; then
  echo "Runtime restart restored a recovery lease" >&2
  exit 1
fi
secret_after="$("${compose[@]}" exec -T cpamp-manager sha256sum /run/cpamp/runtime-secret/token | awk '{print $1}')"
config_after="$("${compose[@]}" exec -T cpamp-runtime sha256sum /runtime/gateway/config.yaml | awk '{print $1}')"
if [ "$secret_before" != "$secret_after" ]; then
  echo "Runtime transport secret changed across restart" >&2
  exit 1
fi
if [ "$config_before" != "$config_after" ]; then
  echo "Gateway configuration seed was overwritten across restart" >&2
  exit 1
fi

"${compose[@]}" exec -T cpamp-runtime sh -ec '
  printf "\n# runtime12-smoke-existing-config-marker\n" >> /runtime/gateway/config.yaml
'
modified_config_before_recreate="$("${compose[@]}" exec -T cpamp-runtime sha256sum /runtime/gateway/config.yaml | awk '{print $1}')"
"${compose[@]}" up -d --force-recreate cpamp-runtime >/dev/null
recreated_status="$(wait_runtime_state offline)"
if [ "$(printf '%s' "$recreated_status" | json_field recovery.state)" != "inactive" ]; then
  echo "Runtime recreate restored a recovery lease" >&2
  exit 1
fi
secret_after_recreate="$("${compose[@]}" exec -T cpamp-manager sha256sum /run/cpamp/runtime-secret/token | awk '{print $1}')"
config_after_recreate="$("${compose[@]}" exec -T cpamp-runtime sha256sum /runtime/gateway/config.yaml | awk '{print $1}')"
if [ "$secret_before" != "$secret_after_recreate" ]; then
  echo "Runtime transport secret changed across recreate" >&2
  exit 1
fi
if [ "$modified_config_before_recreate" != "$config_after_recreate" ]; then
  echo "Existing Gateway configuration was overwritten across recreate" >&2
  exit 1
fi

generation_before_start="$(printf '%s' "$recreated_status" | json_uint_field runtimeGeneration)"
start_payload="$(runtime_operation_payload runtime12-smoke-start "$recreated_status")"
start_result="$(runtime_post /v1/runtime/operations/start "$start_payload")"
if [ "$(printf '%s' "$start_result" | json_field state)" != "succeeded" ]; then
  echo "Runtime Start did not succeed: ${start_result}" >&2
  exit 1
fi
ready_status="$(wait_runtime_state ready 90)"
if [ "$(printf '%s' "$ready_status" | json_field recovery.state)" != "armed" ]; then
  echo "successful explicit Start did not arm bounded recovery" >&2
  exit 1
fi
generation_after_start="$(printf '%s' "$ready_status" | json_uint_field runtimeGeneration)"
if [ "$generation_before_start" != "$generation_after_start" ]; then
  echo "Runtime generation changed across typed Start" >&2
  exit 1
fi
"${compose[@]}" exec -T cpamp-runtime sh -ec '
  cpa_pid=""
  for process_dir in /proc/[0-9]*; do
    if [ -r "${process_dir}/comm" ] && [ "$(cat "${process_dir}/comm")" = "cli-proxy-api" ]; then
      cpa_pid="${process_dir##*/}"
      break
    fi
  done
  test -n "$cpa_pid"
  test "$(readlink "/proc/${cpa_pid}/cwd")" = "/runtime/gateway"
'

curl --fail --silent --show-error "${public_origin}/v1/models" >/dev/null

"${compose[@]}" stop cpamp-manager >/dev/null
curl --fail --silent --show-error "${public_origin}/v1/models" >/dev/null
"${compose[@]}" start cpamp-manager >/dev/null
retry 90 curl --fail --silent "${public_origin}/health" >/dev/null

stop_status="$(runtime_get /v1/runtime/status)"
stop_payload="$(runtime_operation_payload runtime12-smoke-stop "$stop_status")"
stop_result="$(runtime_post /v1/runtime/operations/stop "$stop_payload")"
if [ "$(printf '%s' "$stop_result" | json_field state)" != "succeeded" ]; then
  echo "Runtime Stop did not succeed: ${stop_result}" >&2
  exit 1
fi
stopped_status="$(wait_runtime_state offline)"
if [ "$(printf '%s' "$stopped_status" | json_field recovery.state)" != "inactive" ]; then
  echo "successful typed Stop did not disarm bounded recovery" >&2
  exit 1
fi
generation_after_stop="$(printf '%s' "$stopped_status" | json_uint_field runtimeGeneration)"
if [ "$generation_before_start" != "$generation_after_stop" ]; then
  echo "Runtime generation changed across typed Start/Stop" >&2
  exit 1
fi

"${compose[@]}" stop cpamp-runtime >/dev/null
curl --fail --silent --show-error "${public_origin}/health" >/dev/null

echo "Runtime 12 Docker smoke passed: restart/recreate persistence, stable Start/Stop generation, recovery disarm, single public ingress, and failure isolation"
