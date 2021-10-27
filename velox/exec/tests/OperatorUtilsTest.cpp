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
#include "velox/exec/OperatorUtils.h"
#include <gtest/gtest.h>

using namespace facebook::velox;

class OperatorUtilsTest : public ::testing::Test {
 protected:
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
};

TEST_F(OperatorUtilsTest, wrapChildConstant) {
  auto constant = BaseVector::createConstant(11, 1'000, pool_.get());

  BufferPtr mapping = allocateIndices(1'234, pool_.get());
  auto rawMapping = mapping->asMutable<vector_size_t>();
  for (auto i = 0; i < 1'234; i++) {
    rawMapping[i] = i / 2;
  }

  auto wrapped = exec::wrapChild(1'234, mapping, constant);
  ASSERT_EQ(wrapped->size(), 1'234);
  ASSERT_TRUE(wrapped->isConstantEncoding());
  ASSERT_TRUE(wrapped->equalValueAt(constant.get(), 100, 100));
}
