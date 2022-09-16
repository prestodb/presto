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
#include <folly/Random.h>
#include <folly/init/Init.h>

#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/vector/tests/utils/VectorMaker.h"

#include "velox/substrait/SubstraitToVeloxPlan.h"
#include "velox/substrait/VeloxToSubstraitPlan.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::substrait;

class VeloxSubstraitRoundTripPlanConverterTest : public OperatorTestBase {
 protected:
  /// Makes a vector of INTEGER type with 'size' RowVectorPtr.
  /// @param size The number of RowVectorPtr.
  /// @param childSize The number of columns for each row.
  /// @param batchSize The batch Size of the data.
  std::vector<RowVectorPtr>
  makeVectors(int64_t size, int64_t childSize, int64_t batchSize) {
    std::vector<RowVectorPtr> vectors;
    std::mt19937 gen(std::mt19937::default_seed);
    for (int i = 0; i < size; i++) {
      std::vector<VectorPtr> children;
      for (int j = 0; j < childSize; j++) {
        children.emplace_back(makeFlatVector<int32_t>(
            batchSize,
            [&](auto /*row*/) {
              return folly::Random::rand32(INT32_MAX / 4, INT32_MAX / 2, gen);
            },
            nullEvery(2)));
      }

      vectors.push_back(makeRowVector({children}));
    }
    return vectors;
  }

  void assertPlanConversion(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::string& duckDbSql) {
    assertQuery(plan, duckDbSql);

    // Convert Velox Plan to Substrait Plan.
    google::protobuf::Arena arena;
    auto substraitPlan = veloxConvertor_->toSubstrait(arena, plan);

    // Convert Substrait Plan to the same Velox Plan.
    auto samePlan =
        substraitConverter_->toVeloxPlan(substraitPlan, pool_.get());

    // Assert velox again.
    assertQuery(samePlan, duckDbSql);
  }

  std::shared_ptr<VeloxToSubstraitPlanConvertor> veloxConvertor_ =
      std::make_shared<VeloxToSubstraitPlanConvertor>();
  std::shared_ptr<SubstraitVeloxPlanConverter> substraitConverter_ =
      std::make_shared<SubstraitVeloxPlanConverter>();
};

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, project) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);
  auto plan =
      PlanBuilder().values(vectors).project({"c0 + c1", "c1 / c2"}).planNode();
  assertPlanConversion(plan, "SELECT c0 + c1, c1 / c2 FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, filter) {
  auto vectors = makeVectors(3, 4, 2);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder().values(vectors).filter("c2 < 1000").planNode();
  assertPlanConversion(plan, "SELECT * FROM tmp WHERE c2 < 1000");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, null) {
  auto vectors = makeRowVector(ROW({}, {}), 1);
  auto plan = PlanBuilder().values({vectors}).project({"NULL"}).planNode();
  assertPlanConversion(plan, "SELECT NULL ");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, values) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<int64_t>(
           {2499109626526694126, 2342493223442167775, 4077358421272316858}),
       makeFlatVector<int32_t>({581869302, -708632711, -133711905}),
       makeFlatVector<double>(
           {0.90579193414549275, 0.96886777112423139, 0.63235925003444637}),
       makeFlatVector<bool>({true, false, false}),
       makeFlatVector<int32_t>(3, nullptr, nullEvery(1))

      });
  createDuckDbTable({vectors});

  auto plan = PlanBuilder().values({vectors}).planNode();

  assertPlanConversion(plan, "SELECT * FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, count) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .singleAggregation({"c0", "c1"}, {"count(c4) as num_price"})
                  .project({"num_price"})
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT count(c4) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, countAll) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .filter("c6 < 24")
                  .singleAggregation({"c0", "c1"}, {"count(1) as num_price"})
                  .project({"num_price"})
                  .planNode();

  assertPlanConversion(
      plan,
      "SELECT count(*) as num_price FROM tmp WHERE c6 < 24 GROUP BY c0, c1");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, sum) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"sum(1)", "count(c4)"})
                  .planNode();

  assertPlanConversion(plan, "SELECT sum(1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, sumAndCount) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"sum(c1)", "count(c4)"})
                  .finalAggregation()
                  .planNode();

  assertPlanConversion(plan, "SELECT sum(c1), count(c4) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, sumGlobal) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  // Global final aggregation.
  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({"c0"}, {"sum(c0)", "sum(c1)"})
                  .intermediateAggregation()
                  .finalAggregation()
                  .planNode();
  assertPlanConversion(
      plan, "SELECT c0, sum(c0), sum(c1) FROM tmp GROUP BY c0");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, sumMask) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan =
      PlanBuilder()
          .values(vectors)
          .project({"c0", "c1", "c2 % 2 < 10 AS m0", "c3 % 3 = 0 AS m1"})
          .partialAggregation(
              {}, {"sum(c0)", "sum(c0)", "sum(c1)"}, {"m0", "m1", "m1"})
          .finalAggregation()
          .planNode();

  assertPlanConversion(
      plan,
      "SELECT sum(c0) FILTER (WHERE c2 % 2 < 10), "
      "sum(c0) FILTER (WHERE c3 % 3 = 0), sum(c1) FILTER (WHERE c3 % 3 = 0) "
      "FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, rowConstructor) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<double_t>({0.905791934145, 0.968867771124}),
       makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775}),
       makeFlatVector<int32_t>({581869302, -133711905})});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder()
                  .values({vectors})
                  .project({"row_constructor(c1, c2)"})
                  .planNode();
  assertPlanConversion(plan, "SELECT row(c1, c2) FROM tmp");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, projectAs) {
  RowVectorPtr vectors = makeRowVector(
      {makeFlatVector<double_t>({0.905791934145, 0.968867771124}),
       makeFlatVector<int64_t>({2499109626526694126, 2342493223442167775}),
       makeFlatVector<int32_t>({581869302, -133711905})});
  createDuckDbTable({vectors});

  auto plan = PlanBuilder()
                  .values({vectors})
                  .filter("c0 < 0.5")
                  .project({"c1 * c2 as revenue"})
                  .partialAggregation({}, {"sum(revenue)"})
                  .planNode();
  assertPlanConversion(
      plan, "SELECT sum(c1 * c2) as revenue FROM tmp WHERE c0 < 0.5");
}

TEST_F(VeloxSubstraitRoundTripPlanConverterTest, avg) {
  auto vectors = makeVectors(2, 7, 3);
  createDuckDbTable(vectors);

  auto plan = PlanBuilder()
                  .values(vectors)
                  .partialAggregation({}, {"avg(c4)"})
                  .finalAggregation()
                  .planNode();

  assertPlanConversion(plan, "SELECT avg(c4) FROM tmp");
}

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::init(&argc, &argv, false);
  return RUN_ALL_TESTS();
}
