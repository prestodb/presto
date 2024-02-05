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
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/SumNonPODAggregate.h"

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

class StreamingAggregationTest : public OperatorTestBase {
 protected:
  void SetUp() override {
    OperatorTestBase::SetUp();
    registerSumNonPODAggregate("sumnonpod", 64);
  }

  void testAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
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
                        {"c0"},
                        {"count(1)",
                         "min(c1)",
                         "max(c1)",
                         "sum(c1)",
                         "sumnonpod(1)",
                         "sum(cast(NULL as INT))",
                         "approx_percentile(c1, 0.95)"})
                    .finalAggregation()
                    .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .assertResults(
            "SELECT c0, count(1), min(c1), max(c1), sum(c1), sum(1), sum(cast(NULL as INT))"
            "     , approx_quantile(c1, 0.95) "
            "FROM tmp GROUP BY 1");

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

    plan =
        PlanBuilder()
            .values(data)
            .project({"c1", "c0"})
            .partialStreamingAggregation(
                {"c0"},
                {"count(1)", "min(c1)", "max(c1)", "sum(c1)", "sumnonpod(1)"})
            .finalAggregation()
            .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .assertResults(
            "SELECT c0, count(1), min(c1), max(c1), sum(c1), sum(1) FROM tmp GROUP BY 1");

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

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

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .assertResults(
            "SELECT c0, count(1), min(c1) filter (where c1 % 7 = 0), "
            "max(c1) filter (where c1 % 11 = 0), sum(c1) filter (where c1 % 7 = 0) "
            "FROM tmp GROUP BY 1");
  }

  void testSortedAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    std::vector<RowVectorPtr> data;

    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });
      data.push_back(makeRowVector({keyVector, payload, payload}));
      totalSize += size;
    }
    createDuckDbTable(data);

    auto plan = PlanBuilder()
                    .values(data)
                    .streamingAggregation(
                        {"c0"},
                        {"max(c1 order by c2)",
                         "max(c1 order by c2 desc)",
                         "array_agg(c1 order by c2)"},
                        {},
                        core::AggregationNode::Step::kSingle,
                        false)
                    .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .assertResults(
            "SELECT c0, max(c1 order by c2), max(c1 order by c2 desc), array_agg(c1 order by c2) FROM tmp GROUP BY c0");
  }

  void testDistinctAggregation(
      const std::vector<VectorPtr>& keys,
      uint32_t outputBatchSize) {
    std::vector<RowVectorPtr> data;

    vector_size_t totalSize = 0;
    for (const auto& keyVector : keys) {
      auto size = keyVector->size();
      auto payload = makeFlatVector<int32_t>(
          size, [totalSize](auto row) { return totalSize + row; });
      data.push_back(makeRowVector({keyVector, payload, payload}));
      totalSize += size;
    }
    createDuckDbTable(data);

    {
      auto plan = PlanBuilder()
                      .values(data)
                      .streamingAggregation(
                          {"c0"},
                          {"array_agg(distinct c1)",
                           "array_agg(c1 order by c2)",
                           "count(distinct c1)",
                           "array_agg(c2)"},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .planNode();

      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(
              core::QueryConfig::kPreferredOutputBatchRows,
              std::to_string(outputBatchSize))
          .assertResults(
              "SELECT c0, array_agg(distinct c1), array_agg(c1 order by c2), "
              "count(distinct c1), array_agg(c2) FROM tmp GROUP BY c0");
    }

    {
      auto plan =
          PlanBuilder()
              .values(data)
              .streamingAggregation(
                  {"c0"}, {}, {}, core::AggregationNode::Step::kSingle, false)
              .planNode();

      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(
              core::QueryConfig::kPreferredOutputBatchRows,
              std::to_string(outputBatchSize))
          .assertResults("SELECT distinct c0 FROM tmp");
    }
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
      uint32_t outputBatchSize) {
    testMultiKeyAggregation(
        keys, keys[0]->type()->asRow().names(), outputBatchSize);
  }

  void testMultiKeyAggregation(
      const std::vector<RowVectorPtr>& keys,
      const std::vector<std::string>& preGroupedKeys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys);
    createDuckDbTable(data);

    auto plan =
        PlanBuilder()
            .values(data)
            .aggregation(
                keys[0]->type()->asRow().names(),
                preGroupedKeys,
                {"count(1)", "min(c1)", "max(c1)", "sum(c1)", "sumnonpod(1)"},
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

    const auto sql = fmt::format(
        "SELECT {}, count(1), min(c1), max(c1), sum(c1), sum(1) FROM tmp GROUP BY {}",
        keySql.str(),
        keySql.str());

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(
            core::QueryConfig::kPreferredOutputBatchRows,
            std::to_string(outputBatchSize))
        .assertResults(sql);

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);

    // Force partial aggregation flush after every batch of input.
    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .config(core::QueryConfig::kMaxPartialAggregationMemory, "0")
        .assertResults(sql);

    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  void testMultiKeyDistinctAggregation(
      const std::vector<RowVectorPtr>& keys,
      uint32_t outputBatchSize) {
    auto data = addPayload(keys);
    createDuckDbTable(data);

    {
      auto plan =
          PlanBuilder()
              .values(data)
              .streamingAggregation(
                  keys[0]->type()->asRow().names(),
                  {"count(distinct c1)", "array_agg(c1)", "sumnonpod(1)"},
                  {},
                  core::AggregationNode::Step::kSingle,
                  false)
              .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format(
          "SELECT {}, count(distinct c1), array_agg(c1), sum(1) FROM tmp GROUP BY {}",
          keySql.str(),
          keySql.str());

      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(
              core::QueryConfig::kPreferredOutputBatchRows,
              std::to_string(outputBatchSize))
          .assertResults(sql);

      EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
    }

    {
      auto plan = PlanBuilder()
                      .values(data)
                      .streamingAggregation(
                          keys[0]->type()->asRow().names(),
                          {},
                          {},
                          core::AggregationNode::Step::kSingle,
                          false)
                      .planNode();

      // Generate a list of grouping keys to use in the query: c0, c1, c2,..
      std::ostringstream keySql;
      keySql << "c0";
      for (auto i = 1; i < numKeys(keys); i++) {
        keySql << ", c" << i;
      }

      const auto sql = fmt::format("SELECT distinct {} FROM tmp", keySql.str());

      AssertQueryBuilder(plan, duckDbQueryRunner_)
          .config(
              core::QueryConfig::kPreferredOutputBatchRows,
              std::to_string(outputBatchSize))
          .assertResults(sql);
    }
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

  testAggregation(keys, 1024);

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

  testMultiKeyAggregation(keys, 1024);

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

  testAggregation(keys, 1024);

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

  testAggregation(keys, 1024);

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

  testMultiKeyAggregation(keys, {"c0"}, 1024);
}

// Test StreamingAggregation being closed without being initialized. Create a
// pipeline with Project followed by StreamingAggregation. Make
// Project::initialize fail by using non-existent function.
TEST_F(StreamingAggregationTest, closeUninitialized) {
  auto data = makeRowVector({
      makeFlatVector<int64_t>({1, 2, 3}),
  });
  auto plan = PlanBuilder()
                  .values({data})
                  .addNode([](auto nodeId, auto source) -> core::PlanNodePtr {
                    return std::make_shared<core::ProjectNode>(
                        nodeId,
                        std::vector<std::string>{"c0", "x"},
                        std::vector<core::TypedExprPtr>{
                            std::make_shared<core::FieldAccessTypedExpr>(
                                BIGINT(), "c0"),
                            std::make_shared<core::CallTypedExpr>(
                                BIGINT(),
                                std::vector<core::TypedExprPtr>{},
                                "do-not-exist")},
                        source);
                  })
                  .partialStreamingAggregation({"c0"}, {"sum(x)"})
                  .planNode();

  VELOX_ASSERT_THROW(
      AssertQueryBuilder(plan).copyResults(pool()),
      "Scalar function name not registered: do-not-exist");
}

TEST_F(StreamingAggregationTest, sortedAggregations) {
  auto size = 1024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testSortedAggregation(keys, 1024);
  testSortedAggregation(keys, 32);
}

TEST_F(StreamingAggregationTest, distinctAggregations) {
  auto size = 1024;

  std::vector<VectorPtr> keys = {
      makeFlatVector<int32_t>(size, [](auto row) { return row; }),
      makeFlatVector<int32_t>(size, [size](auto row) { return (size + row); }),
      makeFlatVector<int32_t>(
          size, [size](auto row) { return (2 * size + row); }),
      makeFlatVector<int32_t>(
          78, [size](auto row) { return (3 * size + row); }),
  };

  testDistinctAggregation(keys, 1024);
  testDistinctAggregation(keys, 32);

  std::vector<RowVectorPtr> multiKeys = {
      makeRowVector({
          makeFlatVector<int32_t>({1, 1, 2, 2, 2}),
          makeFlatVector<int64_t>({10, 20, 20, 30, 30}),
      }),
      makeRowVector({
          makeFlatVector<int32_t>({2, 3, 3, 3, 4}),
          makeFlatVector<int64_t>({30, 30, 40, 40, 40}),
      }),
      makeRowVector({
          makeNullableFlatVector<int32_t>({5, 5, 6, 6, 6}),
          makeNullableFlatVector<int64_t>({40, 50, 50, 50, 50}),
      }),
  };

  testMultiKeyDistinctAggregation(multiKeys, 1024);
  testMultiKeyDistinctAggregation(multiKeys, 3);
}
