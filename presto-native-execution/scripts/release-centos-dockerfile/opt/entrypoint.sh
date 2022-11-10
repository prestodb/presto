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

http_server_port=8080
discovery_uri="http://127.0.0.1:${http_server_port}"

while getopts ':-:' optchar; do
  case "$optchar" in
    -)
      case "$OPTARG" in
        discovery-uri=*) discovery_uri="${OPTARG#*=}" ;;
        http-server-port=*) http_server_port="${OPTARG#*=}" ;;
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
  printf "discovery.uri=${discovery_uri}\n"            >> "${PRESTO_HOME}/config.properties"
  printf "http-server.http.port=${http_server_port}\n" >> "${PRESTO_HOME}/config.properties"

  printf "node.environment=intel-poland\n" >  "${PRESTO_HOME}/node.properties"
  printf "node.location=torun-cluster\n"  >> "${PRESTO_HOME}/node.properties"
  printf "node.id=${NODE_UUID}\n"        >> "${PRESTO_HOME}/node.properties"
  printf "node.ip=$(hostname -I)\n"      >> "${PRESTO_HOME}/node.properties"
}

function node_configuration()
{
  if [ -f "${PRESTO_HOME}/config.properties.template" ]
  then
    prompt "Using user provided config.properties.template"
    cat "${PRESTO_HOME}/config.properties.template" > "${PRESTO_HOME}/config.properties"
  else
    prompt "Using default config.properties.template. No user config found."
    cat "${PRESTO_HOME}/etc/config.properties.template" > "${PRESTO_HOME}/config.properties"
  fi

  if [ -f "${PRESTO_HOME}/node.properties.template" ]
  then
    prompt "Using user provided node.properties.template"
    cat "${PRESTO_HOME}/node.properties.template" > "${PRESTO_HOME}/node.properties"
  else
    prompt "Using default node.properties.template. No user config found."
    cat "${PRESTO_HOME}/etc/node.properties.template" > "${PRESTO_HOME}/node.properties"
  fi

  [ -z "$NODE_UUID" ] && NODE_UUID=$(uuid) || return -2

  if [[ -z "$(grep -E '^ *node\.id=' "${PRESTO_HOME}/node.properties" | cut -d'=' -f2)" ]]
  then
    printf "node.id=${NODE_UUID}\n" >> "${PRESTO_HOME}/node.properties"
  fi
  printf "node.ip=$(hostname -I)\n" >> "${PRESTO_HOME}/node.properties"
}

[ $USE_ENV_PARAMS == "1" ] && node_command_line_config || node_configuration

cd "${PRESTO_HOME}"
"${PRESTO_HOME}/presto_server" --logtostderr=1 --v=1 "${presto_args[@]}"
