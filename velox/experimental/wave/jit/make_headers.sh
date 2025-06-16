#!/bin/sh
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


# Generates the inlined headers for Wave Jit.
#
# Run in the valox checkout root.

JIT=velox/experimental/wave/jit
if [ -z "$STRINGIFY" ]; then
    STRINGIFY=stringify
fi

head --lines 16 velox/experimental/wave/common/Cuda.h > $JIT/Headers.h
echo "namespace facebook::velox::wave {" >> $JIT/Headers.h

echo "bool registerHeader(const char* text);" >> $JIT/Headers.h

$STRINGIFY velox/experimental/wave/common/BitUtil.cuh >> $JIT/Headers.h
$STRINGIFY velox/experimental/wave/common/Scan.cuh >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/vector/Operand.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/exec/ErrorCode.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/exec/WaveCore.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/exec/ExprKernel.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/exec/Accumulators.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/exec/Join.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/HashTable.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/HashTable.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/hash.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/StringView.cuh" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/StringView.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/Hash.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/CompilerDefines.h" >> $JIT/Headers.h
$STRINGIFY "velox/experimental/wave/common/Atomic.cuh" >> $JIT/Headers.h
cd velox/experimental/breeze || exit
$STRINGIFY "breeze/platforms/cuda.cuh" >> ../../../$JIT/Headers.h
$STRINGIFY "breeze/platforms/specialization/cuda-ptx.cuh" >> ../../../$JIT/Headers.h
$STRINGIFY "breeze/platforms/platform.h" >> ../../../$JIT/Headers.h
$STRINGIFY "breeze/utils/types.h" >> ../../../$JIT/Headers.h
cd - || exit

echo "}" >> $JIT/Headers.h

clang-format -i $JIT/Headers.h
