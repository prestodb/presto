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

#include "velox/dwio/common/ScanSpec.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

#include <gtest/gtest.h>

namespace facebook::velox::common {
namespace {

class ScanSpecTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }
};

TEST_F(ScanSpecTest, applyFilter) {
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(64, folly::identity),
      makeFlatVector<int64_t>(128, folly::identity),
  });
  ASSERT_EQ(rowVector->size(), 64);
  ScanSpec scanSpec("<root>");
  scanSpec.addAllChildFields(*rowVector->type());
  scanSpec.childByName("c1")->setFilter(createBigintValues({63, 64}, false));
  uint64_t result = -1ll;
  scanSpec.applyFilter(*rowVector, rowVector->size(), &result);
  ASSERT_EQ(result, 1ull << 63);
  result = -1ll;
  scanSpec.childByName("c1")->applyFilter(
      *rowVector->childAt("c1"), rowVector->size(), &result);
  ASSERT_EQ(result, 1ull << 63);
  rowVector = makeRowVector({
      makeFlatVector<int64_t>(128, folly::identity),
      makeFlatVector<int64_t>(64, folly::identity),
  });
  ASSERT_THROW(
      scanSpec.applyFilter(*rowVector, rowVector->size(), &result),
      VeloxRuntimeError);
}

} // namespace
} // namespace facebook::velox::common
