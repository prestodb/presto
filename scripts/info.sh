#!/bin/bash
# Copyright (c) Facebook, Inc. and its affiliates.
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
set -e

version='0.0.1'
cb='```'

if [ ! -x "$(command -v cmake)" ]; then
  echo "Please install cmake to use this script."
  exit 1
fi

info=$(cmake --system-information)

ext() {
  grep -oP '\".+\"$' $1 | tr -d '\"'
}

print_info() {
echo "$info" | grep -P "$1" | ext
}

result="
Velox System Info v${version}
Commit: $(git rev-parse HEAD 2> /dev/null || echo "Not in a git repo.")
CMake Version: $(cmake --version | grep -oP '\d+\.\d+\.\d+')
System: $(print_info 'CMAKE_SYSTEM \"')
Arch: $(print_info 'CMAKE_SYSTEM_PROCESSOR')
C++ Compiler: $(print_info 'CMAKE_CXX_COMPILER ==')
C++ Compiler Version: $(print_info 'CMAKE_CXX_COMPILER_VERSION')
C Compiler: $(print_info 'CMAKE_C_COMPILER ==')
C Compiler Version: $(print_info 'CMAKE_C_COMPILER_VERSION')
CMake Prefix Path: $(print_info '_PREFIX_PATH ')
"
if [ "$CONDA_SHLVL" == 1 ]; then
  conda="
Conda Env
<details>

$cb
$(conda list)
$cb

</details>
"
fi

all="$result  $conda"
echo "$all"

if [ -x "$(command -v xclip)" ]; then
 clip="xclip -selection c"
elif [ -x "$(command -v pbcopy)" ]; then
  clip="pbcopy"
else
  echo "\nThe results will be copied to your clipboard if xclip is installed."
fi

if [ ! -z "$clip" ]; then
  echo "$all" | $clip
  echo "Result copied to clipboard!"
fi
