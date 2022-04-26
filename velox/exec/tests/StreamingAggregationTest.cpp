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
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class StreamingAggregationTest : public OperatorTestBase {
 protected:
  static CursorParameters makeCursorParameters(
      const std::shared_ptr<const core::PlanNode>& planNode,
      uint32_t preferredOutputBatchSize) {
    auto queryCtx = core::QueryCtx::createForTest();
    queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kCreateEmptyFiles, "true"}});

    CursorParameters params;
    params.planNode = planNode;
    params.queryCtx = core::QueryCtx::createForTest();
    params.queryCtx->setConfigOverridesUnsafe(
        {{core::QueryConfig::kPreferredOutputBatchSize,
          std::to_string(preferredOutputBatchSize)}});
    return params;
  }

  void testAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize = 1'024) {
    std::vector<RowVectorPtr> data;

    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });
      data.push_back(makeRowVector({keyVector, payload}));
      totalSize += size;
    }
    createDuckDbTable(data);

    auto plan = PlanBuilder()
                    .values(data)
                    .partialStreamingAggregation(
                        {"c0"}, {"count(1)", "min(c1)", "max(c1)", "sum(c1)"})
                    .finalAggregation()
                    .planNode();

    assertQuery(
        makeCursorParameters(plan, outputBatchSize),
        "SELECT c0, count(1), min(c1), max(c1), sum(c1) FROM tmp GROUP BY 1");

    plan = PlanBuilder()
               .values(data)
               .project({"c1", "c0"})
               .partialStreamingAggregation(
                   {"c0"}, {"count(1)", "min(c1)", "max(c1)", "sum(c1)"})
               .finalAggregation()
               .planNode();

    assertQuery(
        makeCursorParameters(plan, outputBatchSize),
        "SELECT c0, count(1), min(c1), max(c1), sum(c1) FROM tmp GROUP BY 1");

    // Test aggregation masks: one aggregate without a mask, two with the same
    // mask, one with a different mask.
    plan = PlanBuilder()
               .values(data)
               .project({"c0", "c1", "c1 % 7 = 0 AS m1", "c1 % 11 = 0 AS m2"})
               .partialStreamingAggregation(
                   {"c0"},
                   {"count(1)", "min(c1)", "max(c1)", "sum(c1)"},
                   {"", "m1", "m2", "m1"})
               .finalAggregation()
               .planNode();

    assertQuery(
        makeCursorParameters(plan, outputBatchSize),
        "SELECT c0, count(1), min(c1) filter (where c1 % 7 = 0), "
        "max(c1) filter (where c1 % 11 = 0), sum(c1) filter (where c1 % 7 = 0) "
        "FROM tmp GROUP BY 1");
  }

  std::vector<RowVectorPtr> addPayload(const std::vector<RowVectorPtr>& keys) {
    auto numKeys = keys[0]->type()->size();

    std::vector<RowVectorPtr> data;

    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });

      auto children = keyVector->as<RowVector>()->children();
      VELOX_CHECK_EQ(numKeys, children.size());
      children.push_back(payload);
      data.push_back(makeRowVector(children));
      totalSize += size;
    }
    return data;
  }

  size_t numKeys(const std::vector<RowVectorPtr>& keys) {
    return keys[0]->type()->size();
  }

  void testMultiKeyAggregation(
      const std::vector<RowVectorPtr>& keys,
      uint32_t outputBatchSize = 1'024) {
    testMultiKeyAggregation(
        keys, keys[0]->type()->asRow().names(), outputBatchSize);
  }

  void testMultiKeyAggregation(
      const std::vector<RowVectorPtr>& keys,
      const std::vector<std::string>& preGroupedKeys,
      uint32_t outputBatchSize = 1'024) {
    auto data = addPayload(keys);
    createDuckDbTable(data);

    auto plan = PlanBuilder()
                    .values(data)
                    .aggregation(
                        keys[0]->type()->asRow().names(),
                        preGroupedKeys,
                        {"count(1)", "min(c1)", "max(c1)", "sum(c1)"},
                        {},
                        core::AggregationNode::Step::kPartial,
                        false)
                    .finalAggregation()
                    .planNode();

    // Generate a list of grouping keys to use in the query: c0, c1, c2,..
    std::ostringstream keySql;
    keySql << "c0";
    for (auto i = 1; i < numKeys(keys); i++) {
      keySql << ", c" << i;
    }

    assertQuery(
        makeCursorParameters(plan, outputBatchSize),
        fmt::format(
            "SELECT {}, count(1), min(c1), max(c1), sum(c1) FROM tmp GROUP BY {}",
            keySql.str(),
            keySql.str()));
  }
};

TEST_F(StreamingAggregationTest, smallInputBatches) {
  // Use grouping keys that span one or more batches.
  std::vector<VectorPtr> keys = {
      makeNullableFlatVector<int32_t>({1, 1, std::nullopt, 2, 2}),
      makeFlatVector<int32_t>({2, 3, 3, 4}),
      makeFlatVector<int32_t>({5, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 6, 6, 6}),
      makeFlatVector<int32_t>({6, 7, 8}),
  };

  testAggregation(keys);

  // Cut output into tiny batches of size 3.
  testAggregation(keys, 3);
}

TEST_F(StreamingAggregationTest, multipleKeys) {
  std::vector<RowVectorPtr> keys = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 20, 30, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({2, 3, 3, 3, 4}),
          makeFlatVector<int64_t>({30, 30, 40, 40, 40}),
      }),
      makeRowVector({
          makeNullableFlatVector<int32_t>({5, std::nullopt, 6, 6, 6}),
          makeNullableFlatVector<int64_t>({40, 50, 50, 50, std::nullopt}),
      }),
  };

  testMultiKeyAggregation(keys);

  // Cut output into tiny batches of size 3.
  testMultiKeyAggregation(keys, 3);
}

TEST_F(StreamingAggregationTest, regularSizeInputBatches) {
  auto size = 1'024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row / 5; }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (size + row) / 5; }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row) / 5; }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row) / 5; }),
  };

  testAggregation(keys);

  // Cut output into small batches of size 100.
  testAggregation(keys, 100);
}

TEST_F(StreamingAggregationTest, uniqueKeys) {
  auto size = 1'024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return 2 * size + row; }),
      makeFlatVector<int32_t>(78, [size](auto row) { return 3 * size + row; }),
  };

  testAggregation(keys);

  // Cut output into small batches of size 100.
  testAggregation(keys, 100);
}

TEST_F(StreamingAggregationTest, partialStreaming) {
  auto size = 1'024;

  // Generate 2 key columns. First key is clustered / pre-grouped. Second key is
  // not. Make one value of the clustered key last for exactly one batch,
  // another value span two bathes.
  auto keys = {
      makeRowVector({
          makeFlatVector<int32_t>({-10, -10, -5, -5, -5}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({-5, -5, -4, -3, -2}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({-1, -1, -1, -1, -1}),
          makeFlatVector<int32_t>({0, 1, 2, 1, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({0, 0, 0, 0, 0}),
          makeFlatVector<int32_t>({0, 4, 2, 3, 4}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(size, [](auto row) { return row / 7; }),
          makeFlatVector<int32_t>(size, [](auto row) { return row % 5; }),
      }),
      makeRowVector({
          makeFlatVector<int32_t>(
              size, [&](auto row) { return (size + row) / 7; }),
          makeFlatVector<int32_t>(
              size, [&](auto row) { return (size + row) % 5; }),
      }),
  };

  testMultiKeyAggregation(keys, {"c0"});
}
