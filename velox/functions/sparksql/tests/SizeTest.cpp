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
#include <string>

#include <velox/core/QueryConfig.h>
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;

namespace facebook::velox::functions::sparksql::test {
class SizeTest : public SparkFunctionBaseTest {
 protected:
  std::function<vector_size_t(vector_size_t /* row */)> sizeAt =
      [](vector_size_t row) { return 1 + row % 7; };

  void testSize(VectorPtr vector, vector_size_t numRows) {
    auto result =
        evaluate<SimpleVector<int64_t>>("size(c0)", makeRowVector({vector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      if (vector->isNullAt(i)) {
        EXPECT_EQ(result->valueAt(i), -1) << "at " << i;
      } else {
        EXPECT_EQ(result->valueAt(i), sizeAt(i)) << "at " << i;
      }
    }
  }

  void testSizeLegacyNull(VectorPtr vector, vector_size_t numRows) {
    auto result =
        evaluate<SimpleVector<int64_t>>("size(c0)", makeRowVector({vector}));
    for (vector_size_t i = 0; i < numRows; ++i) {
      EXPECT_EQ(result->isNullAt(i), vector->isNullAt(i)) << "at " << i;
    }
  }

  void setConfig(std::string configStr, bool value) {
    execCtx_.queryCtx()->setConfigOverridesUnsafe({
        {configStr, std::to_string(value)},
    });
  }

  static inline vector_size_t valueAt(vector_size_t idx) {
    return idx + 1;
  }
};

TEST_F(SizeTest, sizetest) {
  vector_size_t numRows = 100;
  auto arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullptr);
  testSize(arrayVector, numRows);
  arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullEvery(5));
  testSize(arrayVector, numRows);
  auto mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullptr);
  testSize(mapVector, numRows);
  mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullEvery(5));
  testSize(mapVector, numRows);
}

// Ensure that out if set to -1 if `legacySizeOfNull`
// is specified.
TEST_F(SizeTest, legacySizeOfNull) {
  vector_size_t numRows = 100;
  setConfig("legacySizeOfNull", false);
  auto arrayVector =
      makeArrayVector<int64_t>(numRows, sizeAt, valueAt, nullEvery(1));
  testSizeLegacyNull(arrayVector, numRows);
  auto mapVector = makeMapVector<int64_t, int64_t>(
      numRows, sizeAt, valueAt, valueAt, nullEvery(1));
  testSizeLegacyNull(mapVector, numRows);
}

} // namespace facebook::velox::functions::sparksql::test
