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
PRESTO_HOME="${PRESTO_HOME:-"/opt/presto"}"
USE_ENV_PARAMS=${USE_ENV_PARAMS:-0}

source "${SCRIPT_DIR}/common.sh"

trap 'exit 2' SIGSTOP SIGINT SIGTERM SIGQUIT
trap 'failure "LINENO" "BASH_LINENO" "${BASH_COMMAND}" "${?}"; [ -z "${DEBUG}" ] && exit 1 || sleep 3600' ERR

if [[ "${DEBUG}" == "0" || "${DEBUG}" == "false" || "${DEBUG}" == "False" ]]; then DEBUG=""; fi

HTTP_SERVER_PORT="${HTTP_SERVER_PORT:-"8080"}"
DISCOVERY_URI="${HTTP_SERVER_PORT:-"http://127.0.0.1:${HTTP_SERVER_PORT}"}"

while getopts ':-:' optchar; do
  case "$optchar" in
    -)
      case "$OPTARG" in
        discovery-uri=*) DISCOVERY_URI="${OPTARG#*=}" ;;
        http-server-port=*) HTTP_SERVER_PORT="${OPTARG#*=}" ;;
        node-memory-gb=*) NODE_MEMORY_GB="${OPTARG#*=}" ;;
        use-env-params) USE_ENV_PARAMS=1 ;;
        *)
          presto_args+=($optchar)
          ;;
      esac
      ;;
    *)
      presto_args+=($optchar)
      ;;
  esac
done


function node_command_line_config()
{
  printf "presto.version=0.273.3\n"                    >  "${PRESTO_HOME}/config.properties"
  printf "discovery.uri=${DISCOVERY_URI}\n"            >> "${PRESTO_HOME}/config.properties"
  printf "http-server.http.port=${HTTP_SERVER_PORT}\n" >> "${PRESTO_HOME}/config.properties"

  printf "node.environment=intel-poland\n"    >  "${PRESTO_HOME}/node.properties"
  printf "node.location=torun-cluster\n"      >> "${PRESTO_HOME}/node.properties"
  printf "node.id=${NODE_UUID}\n"             >> "${PRESTO_HOME}/node.properties"
  printf "node.ip=$(hostname -I)\n"           >> "${PRESTO_HOME}/node.properties"
  printf "node.memory_gb=${NODE_MEMORY_GB}\n" >> "${PRESTO_HOME}/node.properties"
}

function node_configuration()
{
  render_node_configuration_files

  [ -z "$NODE_UUID" ] && NODE_UUID=$(uuid) || return -2

  if [[ -z "$(grep -E '^ *node\.id=' "${PRESTO_HOME}/node.properties" | cut -d'=' -f2)" ]]; then
    printf "node.id=${NODE_UUID}\n" >> "${PRESTO_HOME}/node.properties"
  fi
  printf "node.ip=$(hostname -I)\n" >> "${PRESTO_HOME}/node.properties"

  if [[ -z "$(grep -E '^ *node\.memory_gb=' "${PRESTO_HOME}/node.properties")" ]]; then
    printf "node.memory_gb=${NODE_MEMORY_GB}\n" >> "${PRESTO_HOME}/node.properties"
  fi
}

NODE_MEMORY_GB="$(memory_gb_preflight_check ${NODE_MEMORY_GB})"

[ $USE_ENV_PARAMS == "1" ] && node_command_line_config || node_configuration

cd "${PRESTO_HOME}"
"${PRESTO_HOME}/presto_server" --logtostderr=1 --v=1 "${presto_args[@]}"
