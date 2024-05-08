/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/dwio/decode/GpuDecoder.cuh"

namespace facebook::velox::wave {

int32_t GpuDecode::sharedMemorySize() const {
  return detail::sharedMemorySizeForDecode<kBlockSize>(step);
}

/// Describes multiple sequences of decode ops. Each TB executes a sequence of
/// decode steps. The data area starts with a range of instruction numbers for
/// each thread block. The first TB runs from 0 to ends[0]. The nth runs from
/// ends[nth-1] to ends[nth]. After gridDim.x ends, we round to an 8 aligned
/// offset and have an array of GpuDecodes.]
struct GpuDecodeParams {
  // If need to represent more than this many ops, use a dynamically allocated
  // external array in 'external'.
  static constexpr int32_t kMaxInlineOps = 50;

  // Pointer to standalone description of work. If nullptr, the description of
  // work fits inline in 'this'.
  GpuDecodeParams* external{nullptr};
  // The end of each decode program. The first starts at 0. The end is
  // ends[blockIdx.x].
  int32_t ends
      [kMaxInlineOps * (sizeof(GpuDecode) + sizeof(int32_t)) /
       sizeof(int32_t)] = {};
};

__global__ void decodeKernel(GpuDecodeParams inlineParams) {
  GpuDecodeParams* params =
      inlineParams.external ? inlineParams.external : &inlineParams;
  int32_t programStart = blockIdx.x == 0 ? 0 : params->ends[blockIdx.x - 1];
  int32_t programEnd = params->ends[blockIdx.x];
  GpuDecode* ops =
      reinterpret_cast<GpuDecode*>(&params->ends[0] + roundUp(gridDim.x, 2));
  for (auto i = programStart; i < programEnd; ++i) {
    detail::decodeSwitch<kBlockSize>(ops[i]);
  }
  __syncthreads();
}

void launchDecode(
    const DecodePrograms& programs,
    GpuArena* arena,
    WaveBufferPtr& extra,
    Stream* stream) {
  int32_t numBlocks = programs.programs.size();
  int32_t numOps = 0;
  int32_t shared = 0;
  for (auto& program : programs.programs) {
    numOps += program.size();
    for (auto& step : program) {
      shared = std::max(
          shared, detail::sharedMemorySizeForDecode<kBlockSize>(step->step));
    }
  }
  if (shared > 0) {
    shared += 15; // allow align at 16.
  }
  GpuDecodeParams localParams;
  GpuDecodeParams* params = &localParams;
  if (numOps > GpuDecodeParams::kMaxInlineOps) {
    extra = arena->allocate<char>(
        (numBlocks + 1) * (sizeof(GpuDecode) + sizeof(int32_t)));
    params = extra->as<GpuDecodeParams>();
  }
  int32_t end = programs.programs[0].size();
  GpuDecode* decodes =
      reinterpret_cast<GpuDecode*>(&params->ends[0] + roundUp(numBlocks, 2));
  int32_t fill = 0;
  for (auto i = 0; i < programs.programs.size(); ++i) {
    params->ends[i] =
        (i == 0 ? 0 : params->ends[i - 1]) + programs.programs[i].size();
    for (auto& op : programs.programs[i]) {
      decodes[fill++] = *op;
    }
  }
  if (extra) {
    localParams.external = params;
  }

  decodeKernel<<<numBlocks, kBlockSize, shared, stream->stream()->stream>>>(
      localParams);
  CUDA_CHECK(cudaGetLastError());
  if (programs.result) {
    if (!programs.hostResult) {
      stream->prefetch(
          nullptr, programs.result->as<char>(), programs.result->size());
    } else {
      stream->deviceToHostAsync(
          programs.hostResult->as<char>(),
          programs.result->as<char>(),
          programs.hostResult->size());
    }
  }
}

} // namespace facebook::velox::wave
