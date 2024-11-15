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

#pragma once

#include <cuda.h>
#include <cuda_runtime.h>
#include <cstdint>
#include "velox/experimental/wave/common/BitUtil.cuh"

/// Utilities header to include in Cuda code for Velox Wave. Do not combine with
/// Velox *.h.n
namespace facebook::velox::wave {

void cudaCheck(cudaError_t err, const char* file, int line);

void cudaCheckFatal(cudaError_t err, const char* file, int line);

void cuCheck(CUresult result, const char* file, int32_t line);

#define CUDA_CHECK(e) ::facebook::velox::wave::cudaCheck(e, __FILE__, __LINE__)

#define CU_CHECK(e) ::facebook::velox::wave::cuCheck(e, __FILE__, __LINE__)

#ifndef CUDA_CHECK_FATAL
#define CUDA_CHECK_FATAL(e) \
  ::facebook::velox::wave::cudaCheckFatal(e, __FILE__, __LINE__)
#endif

// Gets device and context for Driver API. Initializes on first use.
void getDeviceAndContext(CUdevice& device, CUcontext& context);

struct StreamImpl {
  cudaStream_t stream{};
};

bool registerKernel(const char* name, const void* func);

#define REGISTER_KERNEL(name, func)                              \
  namespace {                                                    \
  static bool func##_reg =                                       \
      registerKernel(name, reinterpret_cast<const void*>(func)); \
  }

} // namespace facebook::velox::wave
