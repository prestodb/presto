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
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/MemoryInputStream.h"

using namespace facebook::velox::dwio::common;

TEST(TestBufferedInput, ZeroLengthStream) {
  MemoryInputStream stream{nullptr, 0};
  auto scopedPool = facebook::velox::memory::getDefaultScopedMemoryPool();
  auto& pool = scopedPool->getPool();
  BufferedInput input{stream, pool};
  auto ret = input.enqueue({0, 0});
  EXPECT_NE(ret, nullptr);
  const void* buf = nullptr;
  int32_t size = 1;
  EXPECT_FALSE(ret->Next(&buf, &size));
  EXPECT_EQ(size, 0);
}
