#!/usr/bin/env bash
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

set -eExv -o functrace

SCRIPT_DIR=$(readlink -f "$(dirname "${BASH_SOURCE[0]}")")

GENERATE_NODE_CONFIG="${GENERATE_NODE_CONFIG:-0}"
PRESTO_HOME="${PRESTO_HOME:-"/opt/presto-server"}"
HTTP_SERVER_PORT="${HTTP_SERVER_PORT:-"8080"}"
DISCOVERY_URI="${DISCOVERY_URI:-"http://127.0.0.1:${HTTP_SERVER_PORT}"}"
NODE_UUID="${NODE_UUID:-$(uuid)}"
NODE_IP="${NODE_IP:-$(hostname -I)}"

source "${SCRIPT_DIR}/common.sh"

trap 'exit 2' SIGINT SIGTERM SIGQUIT
trap 'failure "LINENO" "BASH_LINENO" "${BASH_COMMAND}" "${?}"; [ -z "${DEBUG}" ] && exit 1 || sleep 3600' ERR

if [[ "${DEBUG}" == "0" || "${DEBUG}" == "false" || "${DEBUG}" == "False" ]]; then DEBUG=""; fi

while getopts ':-:' optchar; do
  case "$optchar" in
    -)
      case "$OPTARG" in
        discovery-uri=*)    DISCOVERY_URI="${OPTARG#*=}" ;;
        http-server-port=*) HTTP_SERVER_PORT="${OPTARG#*=}" ;;
        node-uuid=*)        NODE_UUID="${OPTARG#*=}" ;;
        node-ip=*)          NODE_IP="${OPTARG#*=}" ;;
        node-memory-gb=*)   NODE_MEMORY_GB="${OPTARG#*=}" ;;
        use-env-params)     GENERATE_NODE_CONFIG=1 ;;
        *) presto_args+=("--${OPTARG}") ;;
      esac
      ;;
    *) presto_args+=("-${OPTARG}") ;;
  esac
done

NODE_MEMORY_GB="${NODE_MEMORY_GB:-$(memory_gb_preflight_check 32)}"
mkdir -p "${PRESTO_HOME}/catalog" || true

function generate_node_config()
{
  cat > "${PRESTO_HOME}/config.properties" <<- EOF
  	presto.version=0.280
  	http-server.http.port=${HTTP_SERVER_PORT}
  	discovery.uri=${DISCOVERY_URI}
	EOF

  cat > "${PRESTO_HOME}/node.properties" <<- EOF
  	node.environment=intel-poland
  	node.location=torun-cluster
  	node.id=${NODE_UUID}
  	node.ip=${NODE_IP}
  	node.memory_gb=${NODE_MEMORY_GB}
	EOF
}

function template_provided_config()
{
  render_node_configuration_files

  if [[ -z "$(grep -E '^ *node\.id=' "${PRESTO_HOME}/node.properties" | cut -d'=' -f2)" ]]; then
    printf "node.id=${NODE_UUID}\n" >> "${PRESTO_HOME}/node.properties"
  fi
  printf "node.ip=$(hostname -I)\n" >> "${PRESTO_HOME}/node.properties"

  if [[ -z "$(grep -E '^ *node\.memory_gb=' "${PRESTO_HOME}/node.properties")" ]]; then
    printf "node.memory_gb=${NODE_MEMORY_GB}\n" >> "${PRESTO_HOME}/node.properties"
  fi
}

if [ "${GENERATE_NODE_CONFIG}" == "1" ]; then
  generate_node_config
else
  template_provided_config
fi

cd "${PRESTO_HOME}"
"${PRESTO_HOME}/presto_server" --logtostderr=1 --v=2 "${presto_args[@]}"
