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

#include "velox/experimental/wave/common/tests/CudaTest.h"
#include <gtest/gtest.h>
#include "velox/common/base/BitUtil.h"
#include "velox/experimental/wave/common/GpuArena.h"

using namespace facebook::velox;
using namespace facebook::velox::wave;

class CudaTest : public testing::Test {
 protected:
  void SetUp() override {
    device_ = getDevice();
    setDevice(device_);
    allocator_ = getAllocator(device_);
  }
  Device* device_;
  GpuAllocator* allocator_;
};

TEST_F(CudaTest, stream) {
  constexpr int32_t kSize = 1000000;
  TestStream stream;
  auto ints =
      reinterpret_cast<int32_t*>(allocator_->allocate(kSize * sizeof(int32_t)));
  for (auto i = 0; i < kSize; ++i) {
    ints[i] = i;
  }
  stream.prefetch(device_, ints, kSize * sizeof(int32_t));
  stream.addOne(ints, kSize);
  stream.prefetch(nullptr, ints, kSize * sizeof(int32_t));
  stream.wait();
  for (auto i = 0; i < kSize; ++i) {
    ASSERT_EQ(ints[i], i + 1);
  }
  allocator_->free(ints, sizeof(int32_t) * kSize);
}
