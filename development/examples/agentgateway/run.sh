#!/usr/bin/env bash
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# agentgateway in front of an Agent Mesh. Brings compose.yaml up in order,
# then runs each case of README.md and checks its outcome.
#
#   ./run.sh        bring everything up and run every case
#   ./run.sh down   stop everything and delete the generated tokens

set -euo pipefail
# The components run as a non-root user and read the files written below.
umask 022
# Git Bash on Windows rewrites arguments that look like paths; the container
# paths below are meant literally.
export MSYS_NO_PATHCONV=1

cd "$(dirname "${BASH_SOURCE[0]}")"
RUN_DIR=.run
COMPOSE=(docker compose --progress=quiet)

if [[ "${1:-}" == "down" ]]; then
  "${COMPOSE[@]}" --profile cases down --volumes --remove-orphans
  rm -rf "${RUN_DIR}"
  exit 0
fi

failures=0

# expect NAME WANT GOT prints one result line and counts a mismatch.
expect() {
  if [[ "$3" == "$2" ]]; then
    printf '  PASS  %-66s %s\n' "$1" "$3"
  else
    printf '  FAIL  %-66s got %s, want %s\n' "$1" "$3" "$2"
    failures=$((failures + 1))
  fi
}

# http runs curl inside the compose network, where keycloak, control-plane
# and agentgateway resolve as the components themselves see them.
http() {
  "${COMPOSE[@]}" run --rm --no-deps -T tools -s "$@"
}

# admin calls the control plane's admin API with the generated admin token.
admin() {
  local method=$1 path=$2
  shift 2
  http -X "${method}" -H "@/example/${RUN_DIR}/admin.header" "$@" "http://control-plane:8080${path}"
}

random_token() {
  head -c 24 /dev/urandom | od -An -tx1 | tr -d ' \n'
}

# mint ROLE NAME writes a single-use bootstrap token for ROLE to .run/NAME.token.
mint() {
  admin POST /admin/bootstrap-tokens -H 'Content-Type: application/json' \
    -d "{\"role\":\"$1\",\"ttl_hours\":1,\"max_usages\":1,\"description\":\"agentgateway example $2\"}" |
    sed -n 's/.*"token": *"\([^"]*\)".*/\1/p' >"${RUN_DIR}/$2.token"
  [[ -s "${RUN_DIR}/$2.token" ]] || { echo "could not mint a bootstrap token for $2" >&2; exit 1; }
}

# access_token USER CLIENT REALM logs USER in with the password grant and
# writes "Authorization: Bearer <access token>" to .run/USER.CLIENT.REALM.header.
# The password grant only lets this script run unattended: an agent or MCP
# client gets its token from its own login flow.
access_token() {
  local base="${RUN_DIR}/$1.$2.$3" token
  token=$(http -d grant_type=password -d "client_id=$2" -d "username=$1" -d "password=$1-test-only" -d scope=openid \
    "http://keycloak:8080/realms/$3/protocol/openid-connect/token" |
    sed -n 's/.*"access_token":"\([^"]*\)".*/\1/p')
  [[ -n "${token}" ]] || { echo "no access token for $1 from realm $3, client $2" >&2; exit 1; }
  printf 'Authorization: Bearer %s\n' "${token}" >"${base}.header"
}

# wait_log SERVICE TEXT waits for TEXT in the logs of SERVICE.
wait_log() {
  for _ in $(seq 1 90); do
    if "${COMPOSE[@]}" logs "$1" 2>/dev/null | grep -qF "$2"; then
      return 0
    fi
    sleep 1
  done
  echo "timed out waiting for \"$2\" from $1" >&2
  "${COMPOSE[@]}" logs --tail 20 "$1" >&2
  exit 1
}

# wait_until EPOCH sleeps until the given Unix time, if it is still ahead.
wait_until() {
  local left=$(($1 - $(date +%s)))
  if ((left > 0)); then
    echo "   waiting ${left}s"
    sleep "${left}"
  fi
}

INIT='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"agentgateway-example","version":"0"}}}'

# call_calc [HEADER] sends an MCP initialize to mcp://calc through
# agentgateway, with the header file .run/HEADER if given, and prints the
# HTTP status.
call_calc() {
  local auth=()
  if [[ -n "${1:-}" ]]; then
    auth=(-H "@/example/${RUN_DIR}/$1")
  fi
  http -o /dev/null -w '%{http_code}' -X POST "${auth[@]}" \
    -H 'Content-Type: application/json' -H 'Accept: application/json, text/event-stream' \
    --data "${INIT}" "http://agentgateway:3000/mesh/${PROVIDER}/mcp/calc"
}

echo "== Generating test-only tokens in ${RUN_DIR}/"
mkdir -p "${RUN_DIR}"
random_token >"${RUN_DIR}/admin.token"
printf 'Authorization: Bearer %s\n' "$(cat "${RUN_DIR}/admin.token")" >"${RUN_DIR}/admin.header"
for name in provider-api gateway-api; do
  random_token >"${RUN_DIR}/${name}.token"
done
printf 'Authorization: Bearer not-a-token\n' >"${RUN_DIR}/not-a-token.header"

echo "== Starting Keycloak, the control plane, the calc MCP server and the header echo"
"${COMPOSE[@]}" up -d --build keycloak control-plane calc echo
control_plane_ready=false
for _ in $(seq 1 60); do
  if [[ "$(http -o /dev/null -w '%{http_code}' http://control-plane:8080/info || true)" == 200 ]]; then
    control_plane_ready=true
    break
  fi
  sleep 2
done
if [[ "${control_plane_ready}" == false ]]; then
  echo "the control plane did not answer /info within 120s" >&2
  "${COMPOSE[@]}" logs --tail 20 control-plane >&2
  exit 1
fi

echo "== Loading the mesh policy (calc-user bound to group:devs)"
expect "load policy.json" 200 "$(admin POST /policies -H 'Content-Type: application/json' \
  --data-binary @/example/policy.json -o /dev/null -w '%{http_code}')"

echo "== Starting the router, the calc provider, the shared node and agentgateway"
mint mesh:role:router router
mint mesh:role:node provider
mint mesh:role:node gateway
"${COMPOSE[@]}" up -d router provider gateway
wait_log provider 'Registered and announced SERVICE_TYPE_MCP/calc'
wait_log gateway 'Agent Mesh Node Online'
"${COMPOSE[@]}" up -d agentgateway
wait_log agentgateway 'started bind'
PROVIDER=$("${COMPOSE[@]}" logs provider | sed -n 's/.*PeerID: \(12D3KooW[1-9A-HJ-NP-Za-km-z]*\).*/\1/p' | head -n 1)
[[ -n "${PROVIDER}" ]] || { echo "could not read the provider's peer ID" >&2; exit 1; }

echo "== Tokens from Keycloak"
access_token alice agentmesh mesh
access_token bob agentmesh mesh
access_token alice other-app mesh
access_token alice agentmesh elsewhere

echo
echo "== Calling mcp://calc through agentgateway"
expect "alice (calc-user through group:devs)" 200 "$(call_calc alice.agentmesh.mesh.header)"
expect "bob (no calc-user), refused by the provider" 403 "$(call_calc bob.agentmesh.mesh.header)"

echo
echo "== Refused at agentgateway"
expect "no token" 401 "$(call_calc)"
expect "not a token" 401 "$(call_calc not-a-token.header)"
expect "alice, token for client other-app (audience)" 401 "$(call_calc alice.other-app.mesh.header)"
expect "alice, token from realm elsewhere (issuer)" 401 "$(call_calc alice.agentmesh.elsewhere.header)"

echo
echo "== What a backend behind agentgateway receives"
received=$(http -X POST -H "@/example/${RUN_DIR}/alice.agentmesh.mesh.header" --data '{}' http://agentgateway:3000/echo/headers)
echo "   ${received}"
expect "the exchanged credential in x-mesh-authentication" present \
  "$(grep -q '"x-mesh-authentication": "Bearer ' <<<"${received}" && echo present || echo absent)"
expect "the caller's token in authorization" absent \
  "$(grep -q '"authorization"' <<<"${received}" && echo present || echo absent)"

echo
echo "== A token through its expiry (60s lifetime in this realm)"
access_token alice agentmesh mesh
issued_at=$(date +%s)
cp "${RUN_DIR}/alice.agentmesh.mesh.header" "${RUN_DIR}/alice-expiring.header"
expect "alice, before exp" 200 "$(call_calc alice-expiring.header)"
# agentgateway accepts a token for 60 seconds after its exp (the default
# leeway of the jsonwebtoken library, with no setting in v1.6.0); the control
# plane refuses it at the exchange, and agentgateway answers 400.
wait_until $((issued_at + 80))
expect "alice, 20s after exp: accepted by agentgateway, refused at the exchange" 400 "$(call_calc alice-expiring.header)"
wait_until $((issued_at + 140))
expect "alice, 80s after exp: refused by agentgateway" 401 "$(call_calc alice-expiring.header)"
access_token alice agentmesh mesh
expect "alice, a new token" 200 "$(call_calc alice.agentmesh.mesh.header)"

echo
echo "== Where each refusal is logged"
echo "-- agentgateway: tokens it refused"
"${COMPOSE[@]}" logs --no-log-prefix agentgateway |
  sed -n 's/.*error="\(authentication failure: .*\)" reason=.*/\1/p' | sort | uniq -c || true
echo "-- control plane: exchanges it refused"
"${COMPOSE[@]}" logs --no-log-prefix control-plane | grep -F 'Border Crossing' | grep -F '"decision": "deny"' | tail -n 2 || true
echo "-- provider: calls the caller's credential does not grant"
"${COMPOSE[@]}" logs --no-log-prefix provider | grep -F 'AuthZ Denied' | tail -n 2 || true

echo
if [[ "${failures}" -eq 0 ]]; then
  echo "All cases passed. Everything is still running; ./run.sh down removes it."
else
  echo "${failures} case(s) failed. Everything is still running for inspection; ./run.sh down removes it."
  exit 1
fi
