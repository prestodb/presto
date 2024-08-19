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

int32_t GpuDecode::tempSize() const {
  // 1 int32 per lane as an upper limit.
  return std::max<int32_t>(kBlockSize * sizeof(int32_t), sizeof(NonNullState));
}

int32_t GpuDecode::sharedMemorySize() const {
  return detail::sharedMemorySizeForDecode<kBlockSize>(step);
}

/// Describes multiple sequences of decode ops. Each TB executes a sequence of
/// decode steps. The data area starts with a range of instruction numbers for
/// each thread block. The first TB runs from 0 to ends[0]. The nth runs from
/// ends[nth-1] to ends[nth]. After gridDim.x ends, we round to an 8 aligned
/// offset and have an array of GpuDecodes.]
struct alignas(16) GpuDecodeParams {
  // If need to represent more than this many ops, use a dynamically allocated
  // external array in 'external'.
  static constexpr int32_t kMaxInlineOps = 19;

  // Pointer to standalone description of work. If nullptr, the description of
  // work fits inline in 'this'.
  GpuDecodeParams* external{nullptr};
  void* padding;
  // The end of each decode program. The first starts at 0. The end is
  // ends[blockIdx.x].
  int32_t ends
      [kMaxInlineOps * (sizeof(GpuDecode) + sizeof(int32_t)) /
       sizeof(int32_t)] = {};
};

void __global__ __launch_bounds__(1024)
    decodeKernel(GpuDecodeParams inlineParams) {
  __shared__ GpuDecodeParams* params;
  __shared__ int32_t programStart;
  __shared__ int32_t programEnd;
  __shared__ GpuDecode* ops;
  if (threadIdx.x == 0) {
    params = inlineParams.external ? inlineParams.external : &inlineParams;
    programStart = blockIdx.x == 0 ? 0 : params->ends[blockIdx.x - 1];
    programEnd = params->ends[blockIdx.x];
    ops =
        reinterpret_cast<GpuDecode*>(&params->ends[0] + roundUp(gridDim.x, 4));
  }
  __syncthreads();
  for (auto i = programStart; i < programEnd; ++i) {
    detail::decodeSwitch<kBlockSize>(ops[i]);
  }
  __syncthreads();
}

void launchDecode(
    const DecodePrograms& programs,
    LaunchParams& launchParams,
    Stream* stream) {
  int32_t numBlocks = programs.programs.size();
  int32_t numOps = 0;
  bool allSingle = true;
  int32_t shared = 0;
  for (auto& program : programs.programs) {
    int numSteps = program.size();
    ;
    if (numSteps != 1) {
      allSingle = false;
    }
    numOps += numSteps;
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
  char* host = nullptr;
  char* device = nullptr;
  if (numOps > GpuDecodeParams::kMaxInlineOps || allSingle) {
    auto pair = launchParams.setup(
        (numOps + 1) * (sizeof(GpuDecode) + sizeof(int32_t)) + 16);
    host = pair.first;
    device = pair.second;
    uintptr_t aligned = roundUp(reinterpret_cast<uintptr_t>(host), 16);
    params = reinterpret_cast<GpuDecodeParams*>(aligned);
  }
  int32_t end = programs.programs[0].size();
  GpuDecode* decodes =
      reinterpret_cast<GpuDecode*>(&params->ends[0] + roundUp(numBlocks, 4));
  uintptr_t decodeOffset = reinterpret_cast<char*>(decodes) - host;
  int32_t fill = 0;
  for (auto i = 0; i < programs.programs.size(); ++i) {
    params->ends[i] =
        (i == 0 ? 0 : params->ends[i - 1]) + programs.programs[i].size();
    for (auto& op : programs.programs[i]) {
      decodes[fill++] = *op;
    }
  }
  if (allSingle) {
    launchParams.transfer(*stream);
    detail::decodeGlobal<kBlockSize>
        <<<numBlocks, kBlockSize, shared, stream->stream()->stream>>>(
            reinterpret_cast<GpuDecode*>(device + decodeOffset));
    CUDA_CHECK(cudaGetLastError());
    programs.result.transfer(*stream);
    return;
  }
  if (launchParams.device) {
    localParams.external = reinterpret_cast<GpuDecodeParams*>(device);
    launchParams.transfer(*stream);
  }

  decodeKernel<<<numBlocks, kBlockSize, shared, stream->stream()->stream>>>(
      localParams);
  CUDA_CHECK(cudaGetLastError());
  programs.result.transfer(*stream);
}

REGISTER_KERNEL("decode", decodeKernel);
namespace {
static bool decSingles_reg = registerKernel(
    "decodeSingle",
    reinterpret_cast<const void*>(detail::decodeGlobal<kBlockSize>));
}

} // namespace facebook::velox::wave
