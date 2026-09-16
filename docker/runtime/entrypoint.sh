#!/bin/sh
set -eu

runtime_root="/runtime"
supervisor_dir="${runtime_root}/supervisor"
gateway_dir="${runtime_root}/gateway"
token_file="${CPAMP_RUNTIME_TOKEN_FILE:-/run/cpamp/runtime-secret/token}"
seed_file="/usr/local/share/cpamp/runtime/config.seed.yaml"
gateway_config="${gateway_dir}/config.yaml"

mkdir -p "$supervisor_dir" "$gateway_dir/auth" "$gateway_dir/logs" "$gateway_dir/plugins" "$(dirname "$token_file")"
chmod 0700 "$supervisor_dir" "$gateway_dir" "$(dirname "$token_file")"

validate_runtime_token() {
  candidate="$1"
  candidate_bytes="$(printf '%s' "$candidate" | wc -c | tr -d ' ')"
  case "$candidate" in
    *[![:graph:]]*) echo "Runtime transport secret must not contain whitespace" >&2; return 1 ;;
  esac
  if [ "$candidate_bytes" -lt 32 ]; then
    echo "Runtime transport secret must contain at least 32 bytes" >&2
    return 1
  fi
}

explicit_token="${CPAMP_RUNTIME_TOKEN:-}"
if [ -n "$explicit_token" ]; then
  validate_runtime_token "$explicit_token"
fi

if [ ! -f "$token_file" ]; then
  token_tmp="${token_file}.tmp.$$"
  trap 'rm -f "$token_tmp"' EXIT HUP INT TERM
  umask 077
  if [ -n "$explicit_token" ]; then
    printf '%s' "$explicit_token" > "$token_tmp"
  else
    dd if=/dev/urandom bs=32 count=1 2>/dev/null | od -An -tx1 | tr -d ' \n' > "$token_tmp"
  fi
  chmod 0600 "$token_tmp"
  if ! ln "$token_tmp" "$token_file" 2>/dev/null && [ ! -f "$token_file" ]; then
    echo "failed to initialize Runtime transport secret" >&2
    exit 1
  fi
  rm -f "$token_tmp"
  trap - EXIT HUP INT TERM
fi

runtime_token="$(cat "$token_file")"
validate_runtime_token "$runtime_token"
if [ -n "$explicit_token" ] && [ "$runtime_token" != "$explicit_token" ]; then
  echo "explicit Runtime transport secret does not match existing secret source" >&2
  exit 1
fi
chmod 0600 "$token_file"

if [ ! -e "$gateway_config" ]; then
  config_tmp="${gateway_config}.tmp.$$"
  trap 'rm -f "$config_tmp"' EXIT HUP INT TERM
  umask 077
  cp "$seed_file" "$config_tmp"
  chmod 0600 "$config_tmp"
  if ! ln "$config_tmp" "$gateway_config" 2>/dev/null && [ ! -f "$gateway_config" ]; then
    echo "failed to initialize Gateway configuration" >&2
    exit 1
  fi
  rm -f "$config_tmp"
  trap - EXIT HUP INT TERM
fi

export CPAMP_RUNTIME_TOKEN="$runtime_token"
cd "$gateway_dir"
exec "$@"
