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

function message() {
    local type=$1
    shift
    echo -e "$type: $@" >&2
}

function prompt() {
    local PomptHBlue='\e[38;05;33m';
    local PomptTBlue='\e[38;05;45m';
    message "${PomptHBlue}INFO${PomptTBlue}" "$@\e[m"
}

function error() {
    local ErrorHRed='\e[38;05;1m';
    local PomptTBlue='\e[38;05;45m';
    message "${ErrorHRed}ERROR${PomptTBlue}" "$@\e[m"
}

function warning() {
    local WarningHPurple='\e[38;05;61m';
    local PomptTBlue='\e[38;05;45m';
    message "${WarningHPurple}WARN${PomptTBlue}" "$@\e[m"
}

function txt_green()  { echo "\e[38;05;2m$@\e[m";  }
function txt_yellow() { echo "\e[38;05;11m$@\e[m"; }
function txt_red()    { echo "\e[38;05;9m$@\e[m";  }

function get_filename() {
    local path=$1
    echo ${path##*/}
}

function get_dirname() {
    local path=$1
    echo "${path%/*}/"
}

function get_extension() {
    local filename=$(get_filename $1)
    echo "${filename#*.}"
}

function get_basename() {
    local filename=$(get_filename $1)
    echo "${filename%%.*}"
}

# Takes ID as first param and full path to output file as second for creating benchmark specific output file
#  input: [path]/[file_base].[extension]
# output: [path]/[file_base][id].[extension]
function get_path_with_id() {
    local id="${1}"
    local path="${2}"
    local job_dir=$(get_dirname "${path}")
    local job_base=$(get_basename "${path}")
    local job_ext=$(get_extension "${path}")
    echo "${job_dir}${job_base}${id}.${job_ext}"
}

# For using file customization user can use date formatting workflow
# File name is evaluated once at the begigning of sequence run
# %Y year, %m month, %d day, %H hour, %M minute, %S second, example:
#   - given path "/home/ubuntu/.hidden/plik_%Y%m%d_%H-%M-%S.scv"
#     will give  "/home/ubuntu/.hidden/plik_20220415_20-48-33.scv"
function eval_date_path() {
    local path=${1}
    echo $(date +"${path}")
}

# Execute any number of commands for output dump to file passed as first param
function get_debug_dump_info() {
    local out_path="${1}/debug.log"
    shift
    local cmds=(${@:-"env uname lspci"})

    for cmd in ${cmds[@]}; do
        echo "-----start---${cmd}-----" >> "${out_path}"
        $cmd >> "${out_path}"
        echo "------end----${cmd}-----" >> "${out_path}"
    done
}

function failure() {
    local -n _lineno="${1:-LINENO}"
    local -n _bash_lineno="${2:-BASH_LINENO}"
    local _last_command="${3:-${BASH_COMMAND}}"
    local _code="${4:-0}"
    local _last_command_height="$(wc -l <<<"${_last_command}")"
    local -a _output_array=()

    _output_array+=(
        '---'
        "lines_history: [${_lineno} ${_bash_lineno[*]}]"
        "function_trace: [${FUNCNAME[*]}]"
        "exit_code: ${_code}"
    )

    if [[ "${#BASH_SOURCE[@]}" -gt '1' ]]; then
        _output_array+=('source_trace:')
        for _item in "${BASH_SOURCE[@]}"; do
            _output_array+=("  - ${_item}")
        done
    else
        _output_array+=("source_trace: [${BASH_SOURCE[*]}]")
    fi

    if [[ "${_last_command_height}" -gt '1' ]]; then
        _output_array+=(
            'last_command: ->'
            "${_last_command}"
        )
    else
        _output_array+=("last_command: ${_last_command}")
    fi

    _output_array+=('---')
    _output_array+=('--- CRITICAL ERROR! ---')
    _output_array+=('---')
    printf '%s\n' "${_output_array[@]}" >&2
}
