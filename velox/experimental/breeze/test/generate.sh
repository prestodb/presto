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

set -efx -o pipefail
SCRIPTDIR=$(dirname "${BASH_SOURCE[0]}")
cd "$SCRIPTDIR"

function generate {
  BACKEND=$1
  TYPE=$2
  EXT=$3
  DIR="$TYPE"s
  mkdir -p generated/"$DIR"
  ./kernel_generator.py --backend="$BACKEND" --template="$DIR"/"$TYPE"-kernels.template.h --out=generated/"$DIR"/kernels-"$BACKEND"."$EXT" ${LLVM_PATH:+-l "$LLVM_PATH"}
  ./test_fixture_generator.py --backend="$BACKEND" --template="$DIR"/"$TYPE"_test.template.h --out=generated/"$DIR"/"$TYPE"_test-"$BACKEND"."$EXT" ${LLVM_PATH:+-l "$LLVM_PATH"}
}

generate openmp "algorithm" h
generate openmp "function" h
generate cuda "algorithm" cuh
generate cuda "function" cuh
