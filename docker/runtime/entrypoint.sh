#!/bin/sh
set -eu

runtime_root="/runtime"
supervisor_dir="${runtime_root}/supervisor"
gateway_dir="${runtime_root}/gateway"
token_file="${CPAMP_RUNTIME_TOKEN_FILE:-/run/cpamp/runtime-secret/token}"
cpa_uid="${CPAMP_RUNTIME_CPA_UID:-}"
cpa_gid="${CPAMP_RUNTIME_CPA_GID:-}"
seed_file="/usr/local/share/cpamp/runtime/config.seed.yaml"
gateway_config="${gateway_dir}/config.yaml"

case "${cpa_uid}:${cpa_gid}" in
  *[!0-9:]*|:*|*:) echo "Runtime CPA UID/GID must be positive decimal integers" >&2; exit 1 ;;
esac
if [ "$cpa_uid" -eq 0 ] || [ "$cpa_gid" -eq 0 ] ||
  [ "$(id -u cpamp-cpa)" != "$cpa_uid" ] || [ "$(id -g cpamp-cpa)" != "$cpa_gid" ]; then
  echo "Runtime CPA UID/GID does not match the image-owned cpamp-cpa identity" >&2
  exit 1
fi

token_dir="$(dirname "$token_file")"
artifact_root="${supervisor_dir}/artifacts"
artifact_cpa_root="${artifact_root}/cpa"

require_root_owned_directory() {
  trusted_directory="$1"
  if [ ! -d "$trusted_directory" ] || [ -L "$trusted_directory" ]; then
    echo "Supervisor persisted directory is not a trusted directory: ${trusted_directory}" >&2
    exit 1
  fi
  trusted_owner="$(stat -c '%u' "$trusted_directory")"
  if [ "$trusted_owner" != 0 ]; then
    echo "Supervisor persisted directory owner is not root: ${trusted_directory} (uid ${trusted_owner})" >&2
    exit 1
  fi
}

ensure_root_owned_directory() {
  trusted_directory="$1"
  if [ -e "$trusted_directory" ] || [ -L "$trusted_directory" ]; then
    require_root_owned_directory "$trusted_directory"
    return
  fi
  mkdir "$trusted_directory"
  require_root_owned_directory "$trusted_directory"
}

require_root_owned_directory "$runtime_root"
ensure_root_owned_directory "$supervisor_dir"
ensure_root_owned_directory "$artifact_root"
ensure_root_owned_directory "$artifact_cpa_root"
mkdir -p "$gateway_dir/auth" "$gateway_dir/logs" "$gateway_dir/plugins" "$token_dir"
chown root:root "$runtime_root" "$token_dir"
chmod 0711 "$runtime_root"
chmod 0700 "$token_dir"
chown root:"$cpa_gid" "$supervisor_dir" "$artifact_root" "$artifact_cpa_root"
chmod 0710 "$supervisor_dir" "$artifact_root" "$artifact_cpa_root"

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
chown root:root "$token_file"

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

chown -R --no-dereference "$cpa_uid:$cpa_gid" "$gateway_dir"
chmod 0700 "$gateway_dir" "$gateway_dir/auth" "$gateway_dir/logs" "$gateway_dir/plugins"

export CPAMP_RUNTIME_TOKEN="$runtime_token"
cd "$gateway_dir"
exec "$@"
