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

#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions {
namespace {

class LambdaFunctionUtilTest : public testing::Test,
                               public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(LambdaFunctionUtilTest, addNullsForUnselectedRows) {
  auto vector = makeFlatVector<int64_t>({1, 2, 3, 4, 5});

  SelectivityVector rows(3);
  auto nulls = addNullsForUnselectedRows(vector, rows);

  auto* rawNulls = nulls->as<uint64_t>();

  EXPECT_FALSE(bits::isBitNull(rawNulls, 0));
  EXPECT_FALSE(bits::isBitNull(rawNulls, 1));
  EXPECT_FALSE(bits::isBitNull(rawNulls, 2));
  EXPECT_TRUE(bits::isBitNull(rawNulls, 3));
  EXPECT_TRUE(bits::isBitNull(rawNulls, 4));
}

} // namespace
} // namespace facebook::velox::functions
