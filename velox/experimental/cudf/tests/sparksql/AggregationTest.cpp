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

#include "velox/experimental/cudf/exec/ToCudf.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/sparksql/aggregates/Register.h"

using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::exec::sparksql::test {

class AggregationTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    functions::aggregate::sparksql::registerAggregateFunctions("");
    filesystems::registerLocalFileSystem();
    // After register supports function prefix, we could register the function
    // with spark_ to align with sparksql AverageAggregationTest, the
    // function name overwrite may not work well in some condition.
    cudf_velox::registerCudf();
  }

  void TearDown() override {
    cudf_velox::unregisterCudf();
  }
};

TEST_F(AggregationTest, sumReal) {
  // spark sum:
  // exec::AggregateFunctionSignatureBuilder()
  // .returnType("double")
  // .intermediateType("double")
  // .argumentType("real")
  // .build(),
  // presto sum:
  // exec::AggregateFunctionSignatureBuilder()
  //       .returnType("real")
  //       .intermediateType("double")
  //       .argumentType("real")
  //       .build(),
  // The sum(real) final result type is different.
  auto data = makeRowVector({makeFlatVector<float>({3.4028, 3.1})});
  auto vectors = {data};
  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"sum(c0)"})
                  .finalAggregation()
                  .planNode();
  auto expected = makeRowVector({"c0"}, {makeConstant<double>(6.5028, 1)});
  assertQuery(plan, expected);
}

} // namespace facebook::velox::exec::sparksql::test
