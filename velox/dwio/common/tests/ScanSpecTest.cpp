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

class TypedScanSpecTest : public testing::TestWithParam<TypePtr>,
                          public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  VectorPtr makeConstNullVector(TypePtr type, vector_size_t size) {
    return BaseVector::createNullConstant(type, size, pool());
  }

  void addIsNullFilterRecursive(ScanSpec& scanSpec) {
    scanSpec.setFilter(std::make_shared<velox::common::IsNull>());
    for (auto& child : scanSpec.children()) {
      addIsNullFilterRecursive(*child);
    }
  }

  void addIsNotNullFilterRecursive(ScanSpec& scanSpec) {
    scanSpec.setFilter(std::make_shared<velox::common::IsNotNull>());
    for (auto& child : scanSpec.children()) {
      addIsNullFilterRecursive(*child);
    }
  }

  void addIsNullFilterToLeaf(ScanSpec& scanSpec) {
    if (scanSpec.children().empty()) {
      scanSpec.setFilter(std::make_shared<velox::common::IsNull>());
    } else {
      for (auto& child : scanSpec.children()) {
        addIsNullFilterToLeaf(*child);
      }
    }
  }

  void addIsNotNullFilterToLeaf(ScanSpec& scanSpec) {
    if (scanSpec.children().empty()) {
      scanSpec.setFilter(std::make_shared<velox::common::IsNotNull>());
    } else {
      for (auto& child : scanSpec.children()) {
        addIsNotNullFilterToLeaf(*child);
      }
    }
  }
};

// Due to how subfield filters of maps and arrays are pruning
// and can't affect the row selectivity, the current test skips
// cases when maps and arrays are the lone child of (nested) structs.
INSTANTIATE_TEST_SUITE_P(
    TypedScanSpecTestSuite,
    TypedScanSpecTest,
    testing::Values(
        TINYINT(),
        SMALLINT(),
        INTEGER(),
        BIGINT(),
        REAL(),
        DOUBLE(),
        VARCHAR(),
        VARBINARY(),
        ROW({"int", "real"}, {INTEGER(), REAL()}),
        // TODO: the test cases fail when not specifying names for
        // the struct fields. This indicates bug in internal topology
        // when finding children of nested scan specs.
        ROW({"int", "map"}, {INTEGER(), MAP(INTEGER(), REAL())}),
        ROW({"int", "array"}, {INTEGER(), ARRAY(INTEGER())}),
        ROW({"int0", "array0", "row0"},
            {INTEGER(),
             ARRAY(INTEGER()),
             ROW({"int1", "real1", "row1"},
                 {INTEGER(),
                  REAL(),
                  ROW({"int2", "real2"}, {INTEGER(), REAL()})})})));

TEST_P(TypedScanSpecTest, applyFilterSchemaEvolution) {
  auto rowVector = makeRowVector({
      makeFlatVector<int64_t>(64, folly::identity),
      makeConstNullVector(GetParam(), 64),
  });
  ASSERT_EQ(rowVector->size(), 64);
  LOG(INFO) << "Testing with type: " << rowVector->type()->toString();

  {
    ScanSpec scanSpec("<root>");
    scanSpec.addAllChildFields(*rowVector->type());

    ASSERT_TRUE(scanSpec.childByName("c0"));
    scanSpec.childByName("c0")->setFilter(
        std::make_shared<BigintRange>(32, 64, false));

    ASSERT_TRUE(scanSpec.childByName("c1"));
    addIsNullFilterRecursive(*scanSpec.childByName("c1"));

    uint64_t result = -1ll;
    scanSpec.applyFilter(*rowVector, rowVector->size(), &result);
    ASSERT_EQ(result, -1ll << 32);

    // Now add a non-null filter on the missing column.
    ASSERT_TRUE(scanSpec.childByName("c1"));
    addIsNotNullFilterRecursive(*scanSpec.childByName("c1"));
    result = -1ll;
    scanSpec.applyFilter(*rowVector, rowVector->size(), &result);
    ASSERT_EQ(result, 0);
  }

  {
    ScanSpec scanSpec("<root>");
    scanSpec.addAllChildFields(*rowVector->type());

    ASSERT_TRUE(scanSpec.childByName("c0"));
    scanSpec.childByName("c0")->setFilter(
        std::make_shared<BigintRange>(32, 64, false));

    // Now add a null filter only on the innermost node of the missing column.
    // Should have the same result as recursive filters.
    ASSERT_TRUE(scanSpec.childByName("c1"));
    addIsNullFilterToLeaf(*scanSpec.childByName("c1"));
    uint64_t result = -1ll;
    scanSpec.applyFilter(*rowVector, rowVector->size(), &result);
    ASSERT_EQ(result, -1ll << 32);

    // Now add is not null filter only on the innermost node of the missing
    // column. Should have the same result as recursive filters.
    ASSERT_TRUE(scanSpec.childByName("c1"));
    addIsNotNullFilterToLeaf(*scanSpec.childByName("c1"));
    result = -1ll;
    scanSpec.applyFilter(*rowVector, rowVector->size(), &result);
    ASSERT_EQ(result, 0);
  }
}

} // namespace
} // namespace facebook::velox::common
