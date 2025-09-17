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

#include "velox/experimental/cudf/exec/CudfFilterProject.h"
#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox;

namespace {

class CudfFilterProjectTest
    : public facebook::velox::functions::sparksql::test::SparkFunctionBaseTest {
 protected:
  static void SetUpTestCase() {
    facebook::velox::functions::sparksql::test::SparkFunctionBaseTest::
        SetUpTestCase();
    cudf_velox::registerCudf();
  }

  static void TearDownTestCase() {
    facebook::velox::functions::sparksql::test::SparkFunctionBaseTest::
        TearDownTestCase();
    cudf_velox::unregisterCudf();
  }

  CudfFilterProjectTest() {
    options_.parseIntegerAsBigint = false;
  }
};

TEST_F(CudfFilterProjectTest, hashWithSeed) {
  auto input = makeFlatVector<int64_t>({INT64_MAX, INT64_MIN});
  auto data = makeRowVector({input});
  auto hashPlan = PlanBuilder()
                      .setParseOptions(options_)
                      .values({data})
                      .project({"hash_with_seed(42, c0) AS c1"})
                      .planNode();
  auto hashResults = AssertQueryBuilder(hashPlan).copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({
          1049813396,
          1800792340,
      }),
  });
  facebook::velox::test::assertEqualVectors(expected, hashResults);
}

TEST_F(CudfFilterProjectTest, hashWithSeedMultiColumns) {
  auto input = makeFlatVector<int64_t>({INT64_MAX, INT64_MIN});
  auto data = makeRowVector({input, input});
  auto hashPlan = PlanBuilder()
                      .setParseOptions(options_)
                      .values({data})
                      .project({"hash_with_seed(42, c0, c1) AS c2"})
                      .planNode();
  auto hashResults = AssertQueryBuilder(hashPlan).copyResults(pool());

  auto expected = makeRowVector({
      makeFlatVector<int32_t>({
          -864217843,
          821064941,
      }),
  });
  facebook::velox::test::assertEqualVectors(expected, hashResults);
}
} // namespace
