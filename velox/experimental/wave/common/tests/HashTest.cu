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

#include <gtest/gtest.h>

#include "velox/experimental/wave/common/Cuda.h"
#include "velox/experimental/wave/common/Hash.h"

namespace facebook::velox::wave {
namespace {

__global__ void murmur3(const char* data, size_t len, uint32_t* out) {
  *out = Murmur3::hashBytes(data, len, 42);
}

void testMurmur3(const std::string& s, uint32_t expected) {
  SCOPED_TRACE(s);
  auto* allocator = getAllocator(getDevice());
  auto buf = allocator->allocate<char>(s.size() + 1);
  memcpy(&buf[1], s.data(), s.size());
  auto actual = allocator->allocate<uint32_t>();
  murmur3<<<1, 1>>>(&buf[1], s.size(), actual.get());
  ASSERT_EQ(cudaGetLastError(), cudaSuccess);
  ASSERT_EQ(cudaDeviceSynchronize(), cudaSuccess);
  ASSERT_EQ(*actual, expected);
}

TEST(HashTest, murmur3) {
  testMurmur3("foo", 1015597510u);
  testMurmur3("foobar", 3446066726u);
}

} // namespace
} // namespace facebook::velox::wave
