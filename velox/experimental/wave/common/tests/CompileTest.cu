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

#include <cuda.h>
#include <cuda_runtime.h>

#include <gtest/gtest.h>
#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/CudaUtil.cuh"
#include "velox/experimental/wave/common/Exception.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/common/tests/BlockTest.h"

#include <iostream>

namespace facebook::velox::wave {

void testCuCheck(CUresult result) {
  if (result != CUDA_SUCCESS) {
    const char* str;
    cuGetErrorString(result, &str);
    waveError(std::string("Cuda error: ") + str);
  }
}

class CompileTest : public testing::Test {
 protected:
  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
    arena_ = std::make_unique<GpuArena>(1 << 28, allocator_);
    streams_.push_back(std::make_unique<BlockTestStream>());
  }

  Device* device_;
  GpuAllocator* allocator_;
  std::unique_ptr<GpuArena> arena_;
  std::vector<std::unique_ptr<BlockTestStream>> streams_;
};

struct KernelParams {
  int32_t* array;
  int32_t size;
};

const char* kernelText =
    "using int32_t = int; //#include <cstdint>\n"
    "namespace facebook::velox::wave {\n"
    "  struct KernelParams {\n"
    "    int32_t* array;\n"
    "    int32_t size;\n"
    "  };\n"
    "\n"
    "  void __global__ add1(KernelParams params) {\n"
    "    for (auto i = threadIdx.x; i < params.size; i += blockDim.x) {\n"
    "      ++params.array[i];\n"
    "    }\n"
    "  }\n"
    "\n"
    "  void __global__ add2(KernelParams params) {\n"
    "    for (auto i = threadIdx.x; i < params.size; i += blockDim.x) {\n"
    "      params.array[i] += 2;\n"
    "    }\n"
    "  }\n"
    "} // namespace\n";

void __global__ add3(KernelParams params) {
  for (auto i = threadIdx.x; i < params.size; i += blockDim.x) {
    params.array[i] += 3;
  }
}

TEST_F(CompileTest, module) {
  KernelSpec spec = KernelSpec{
      kernelText,
      {"facebook::velox::wave::add1", "facebook::velox::wave::add2"},
      "/tmp/add1.cu"};
  auto module = CompiledModule::create(spec);
  int32_t* ptr;
  testCuCheck(cuMemAllocManaged(
      reinterpret_cast<CUdeviceptr*>(&ptr),
      1000 * sizeof(int32_t),
      CU_MEM_ATTACH_GLOBAL));
  KernelParams record{ptr, 1000};
  memset(ptr, 0, 1000 * sizeof(int32_t));
  void* recordPtr = &record;
  auto impl = std::make_unique<StreamImpl>();
  testCuCheck(cuStreamCreate((CUstream*)&impl->stream, CU_STREAM_DEFAULT));
  auto stream = std::make_unique<Stream>(std::move(impl));
  module->launch(0, 1, 256, 0, stream.get(), &recordPtr);
  testCuCheck(cuStreamSynchronize((CUstream)stream->stream()->stream));
  EXPECT_EQ(1, ptr[0]);
  auto info = module->info(0);
  EXPECT_EQ(1024, info.maxThreadsPerBlock);

  // See if runtime API kernel works on driver API stream.
  add3<<<1, 256, 0, (cudaStream_t)stream->stream()->stream>>>(record);
  CUDA_CHECK(cudaGetLastError());
  testCuCheck(cuStreamSynchronize((CUstream)stream->stream()->stream));
  EXPECT_EQ(4, ptr[0]);

  auto stream2 = std::make_unique<Stream>();
  module->launch(1, 1, 256, 0, stream2.get(), &recordPtr);
  stream2->wait();
  EXPECT_EQ(6, ptr[0]);
}

TEST_F(CompileTest, cache) {
  KernelSpec spec = KernelSpec{
      kernelText,
      {"facebook::velox::wave::add1", "facebook::velox::wave::add2"},
      "/tmp/add1.cu"};
  auto kernel =
      CompiledKernel::getKernel("add1", [&]() -> KernelSpec { return spec; });
  auto buffer = arena_->allocate<int32_t>(1000);
  memset(buffer->as<int32_t>(), 0, sizeof(int32_t) * 1000);
  KernelParams record{buffer->as<int32_t>(), 1000};
  void* recordPtr = &record;
  auto stream = std::make_unique<Stream>();
  kernel->launch(1, 1, 256, 0, stream.get(), &recordPtr);
  stream->wait();
  EXPECT_EQ(2, buffer->as<int32_t>()[0]);
}

} // namespace facebook::velox::wave
