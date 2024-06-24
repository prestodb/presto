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

#include "velox/exec/tests/SimpleAggregateFunctionsRegistration.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/lib/aggregates/tests/utils/AggregationTestBase.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::functions::aggregate::test;

namespace facebook::velox::aggregate::test {

namespace {

class ArrayAggTest : public AggregationTestBase {
 protected:
  void SetUp() override {
    AggregationTestBase::SetUp();
    registerSimpleArrayAggAggregate("simple_array_agg");
    prestosql::registerInternalAggregateFunctions();
  }

  RowVectorPtr fuzzFlat(const RowTypePtr& rowType, size_t size) {
    VectorFuzzer::Options options;
    options.vectorSize = size;
    VectorFuzzer fuzzer(options, pool());
    return fuzzer.fuzzInputFlatRow(rowType);
  }
};

std::unordered_map<std::string, std::string> makeConfig(bool ignoreNulls) {
  if (ignoreNulls) {
    return {{"presto.array_agg.ignore_nulls", "true"}};
  }
  return {};
}

TEST_F(ArrayAggTest, groupBy) {
  auto testFunction = [this](
                          const std::string& functionName,
                          bool ignoreNulls,
                          bool testCompanionFunctions = true) {
    constexpr int32_t kNumGroups = 10;
    std::vector<RowVectorPtr> batches;
    // We make 10 groups. each with 10 arrays. Each array consists of n
    // arrays of varchar. These 10 groups are repeated 10 times. The
    // expected result is that there is, for each key, an array of 100
    // elements with, for key k, batch[k[, batch[k + 10], ... batch[k +
    // 90], repeated 10 times.
    batches.push_back(
        fuzzFlat(ROW({"c0", "a"}, {INTEGER(), ARRAY(VARCHAR())}), 100));
    // We divide the rows into 10 groups.
    auto keys = batches[0]->childAt(0)->as<FlatVector<int32_t>>();
    auto values = batches[0]->childAt(1)->as<ArrayVector>();
    for (auto i = 0; i < keys->size(); ++i) {
      if (i % 10 == 0) {
        keys->setNull(i, true);
      } else {
        keys->set(i, i % kNumGroups);
      }

      if (i % 7 == 0) {
        values->setNull(i, true);
      }
    }
    // We make 10 repeats of the first batch.
    for (auto i = 0; i < 9; ++i) {
      batches.push_back(batches[0]);
    }

    createDuckDbTable(batches);
    auto filter = ignoreNulls ? "filter (where a is not null)" : "";
    testAggregations(
        batches,
        {"c0"},
        {fmt::format("{}(a)", functionName)},
        {"c0", "array_sort(a0)"},
        fmt::format(
            "SELECT c0, array_sort(array_agg(a) {}) FROM tmp GROUP BY c0",
            filter),
        makeConfig(ignoreNulls));
    if (testCompanionFunctions) {
      testAggregationsWithCompanion(
          batches,
          [](auto& /*builder*/) {},
          {"c0"},
          {fmt::format("{}(a)", functionName)},
          {{ARRAY(VARCHAR())}},
          {"c0", "array_sort(a0)"},
          fmt::format(
              "SELECT c0, array_sort(array_agg(a) {}) FROM tmp GROUP BY c0",
              filter),
          makeConfig(ignoreNulls));
    }

    // Having one function supporting toIntermediate and one does not, make sure
    // the row container is recreated with only the function without
    // toIntermediate support.
    testAggregations(
        batches,
        {"c0"},
        {fmt::format("{}(a)", functionName), "max(c0)"},
        {"c0", "array_sort(a0)", "a1"},
        fmt::format(
            "SELECT c0, array_sort(array_agg(a) {}), max(c0) FROM tmp GROUP BY c0",
            filter),
        makeConfig(ignoreNulls));
  };

  testFunction("array_agg", true);
  testFunction("array_agg", false);
  testFunction("\"$internal$array_agg\"", false, false);
  testFunction("simple_array_agg", false);
}

TEST_F(ArrayAggTest, sortedGroupBy) {
  auto testFunction = [this](const std::string& functionName) {
    auto data = makeRowVector({
        makeFlatVector<int16_t>({1, 1, 2, 2, 1, 2, 1}),
        makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
        makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
        makeFlatVector<int32_t>({11, 44, 22, 55, 33, 66, 77}),
    });

    createDuckDbTable({data});

    // Sorted aggregations over same inputs.
    auto plan =
        PlanBuilder()
            .values({data})
            .singleAggregation(
                {"c0"},
                {
                    "sum(c1)",
                    fmt::format("{}(c1 ORDER BY c2 DESC)", functionName),
                    "avg(c1)",
                    fmt::format("{}(c1 ORDER BY c3)", functionName),
                })
            .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT c0, sum(c1), array_agg(c1 ORDER BY c2 DESC), avg(c1), array_agg(c1 ORDER BY c3) "
            " FROM tmp GROUP BY 1");

    // Sorted aggregations over different inputs.
    plan = PlanBuilder()
               .values({data})
               .singleAggregation(
                   {"c0"},
                   {
                       fmt::format("{}(c1 ORDER BY c2 DESC)", functionName),
                       "avg(c1)",
                       fmt::format("{}(c2 ORDER BY c3)", functionName),
                       "sum(c1)",
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT c0, array_agg(c1 ORDER BY c2 DESC), avg(c1), array_agg(c2 ORDER BY c3), sum(c1) "
            " FROM tmp GROUP BY 1");

    // Sorted aggregation with multiple sorting keys.
    plan = PlanBuilder()
               .values({data})
               .singleAggregation(
                   {"c0"},
                   {
                       fmt::format("{}(c1 ORDER BY c2 DESC, c3)", functionName),
                       "sum(c1)",
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT c0, array_agg(c1 ORDER BY c2 DESC, c3), sum(c1) "
            " FROM tmp GROUP BY 1");

    // Multiple sorted aggregations with same sorting keys.
    plan = PlanBuilder()
               .values({data})
               .singleAggregation(
                   {"c0"},
                   {
                       fmt::format("{}(c1 ORDER BY c3)", functionName),
                       fmt::format("{}(c2 ORDER BY c3)", functionName),
                       "sum(c1)",
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT c0, array_agg(c1 ORDER BY c3), array_agg(c2 ORDER BY c3), sum(c1) "
            " FROM tmp GROUP BY 1");

    // Sorted aggregation with mask.
    plan = PlanBuilder()
               .values({data})
               .project({"c0", "c1", "c2", "c1 % 2 = 1 as m"})
               .singleAggregation(
                   {"c0"},
                   {
                       fmt::format("{}(c1 ORDER BY c2 DESC)", functionName),
                       "sum(c1)",
                   },
                   {"m", "m"})
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT c0, array_agg(c1 ORDER BY c2 DESC) FILTER (WHERE c1 % 2 = 1), sum(c1)  FILTER (WHERE c1 % 2 = 1)"
            " FROM tmp GROUP BY 1");
  };

  testFunction("array_agg");
  testFunction("simple_array_agg");
}

TEST_F(ArrayAggTest, global) {
  auto testFunction = [this](
                          const std::string& functionName,
                          bool ignoreNulls,
                          bool testCompanionFunctions = true) {
    vector_size_t size = 10;

    std::vector<RowVectorPtr> vectors = {makeRowVector({makeFlatVector<int32_t>(
        size, [](vector_size_t row) { return row * 2; }, nullEvery(3))})};

    createDuckDbTable(vectors);
    auto filter = ignoreNulls ? "filter (where c0 is not null)" : "";
    testAggregations(
        vectors,
        {},
        {fmt::format("{}(c0)", functionName)},
        {"array_sort(a0)"},
        fmt::format("SELECT array_sort(array_agg(c0) {}) FROM tmp", filter),
        makeConfig(ignoreNulls));
    if (testCompanionFunctions) {
      testAggregationsWithCompanion(
          vectors,
          [](auto& /*builder*/) {},
          {},
          {fmt::format("{}(c0)", functionName)},
          {{INTEGER()}},
          {"array_sort(a0)"},
          fmt::format("SELECT array_sort(array_agg(c0) {}) FROM tmp", filter),
          makeConfig(ignoreNulls));
    }
  };

  testFunction("array_agg", true);
  testFunction("array_agg", false);
  testFunction("\"$internal$array_agg\"", false, false);
  testFunction("simple_array_agg", false);
}

TEST_F(ArrayAggTest, globalNoData) {
  auto testFunction = [this](
                          const std::string& functionName,
                          bool ignoreNulls,
                          bool testCompanionFunctions = true) {
    auto data = makeRowVector(ROW({"c0"}, {INTEGER()}), 0);
    testAggregations(
        {data}, {}, {fmt::format("{}(c0)", functionName)}, "SELECT null");

    std::vector<RowVectorPtr> allNulls = {
        makeRowVector({makeFlatVector<int32_t>(
            10, [](vector_size_t row) { return row; }, nullEvery(1))})};
    auto filter = ignoreNulls ? "filter (where c0 is not null)" : "";

    createDuckDbTable(allNulls);
    testAggregations(
        allNulls,
        {},
        {fmt::format("{}(c0)", functionName)},
        fmt::format("SELECT array_agg(c0) {} FROM tmp", filter),
        makeConfig(ignoreNulls));
    if (testCompanionFunctions) {
      testAggregationsWithCompanion(
          allNulls,
          [](auto& /*builder*/) {},
          {},
          {fmt::format("{}(c0)", functionName)},
          {{INTEGER()}},
          {"array_sort(a0)"},
          fmt::format("SELECT array_sort(array_agg(c0) {}) FROM tmp", filter),
          makeConfig(ignoreNulls));
    }
  };

  testFunction("array_agg", true);
  testFunction("array_agg", false);
  testFunction("\"$internal$array_agg\"", false, false);
  testFunction("simple_array_agg", false);
}

TEST_F(ArrayAggTest, sortedGlobal) {
  auto testFunction = [this](const std::string& functionName) {
    auto data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
        makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
        makeFlatVector<int32_t>({11, 33, 22, 44, 66, 55, 77}),
    });

    createDuckDbTable({data});

    auto plan =
        PlanBuilder()
            .values({data})
            .singleAggregation(
                {},
                {
                    "sum(c0)",
                    fmt::format("{}(c0 ORDER BY c1 DESC)", functionName),
                    "avg(c0)",
                    fmt::format("{}(c0 ORDER BY c2)", functionName),
                })
            .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC), avg(c0), array_agg(c0 ORDER BY c2) FROM tmp");

    // Sorted aggregations over different inputs.
    plan = PlanBuilder()
               .values({data})
               .singleAggregation(
                   {},
                   {
                       "sum(c0)",
                       fmt::format("{}(c0 ORDER BY c1 DESC)", functionName),
                       "avg(c0)",
                       fmt::format("{}(c1 ORDER BY c2)", functionName),
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC), avg(c0), array_agg(c1 ORDER BY c2) FROM tmp");

    // Sorted aggregation with multiple sorting keys.
    plan = PlanBuilder()
               .values({data})
               .singleAggregation(
                   {},
                   {
                       "sum(c0)",
                       fmt::format("{}(c0 ORDER BY c1 DESC, c2)", functionName),
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC, c2) FROM tmp");
  };

  testFunction("array_agg");
  testFunction("simple_array_agg");
}

TEST_F(ArrayAggTest, sortedGlobalWithMask) {
  auto testFunction = [this](const std::string& functionName) {
    auto data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5, 6, 7}),
        makeFlatVector<int64_t>({10, 20, 30, 40, 50, 60, 70}),
        makeFlatVector<int32_t>({11, 33, 22, 44, 66, 55, 77}),
    });

    createDuckDbTable({data});

    auto plan =
        PlanBuilder()
            .values({data})
            .project({"c0", "c1", "c2", "c0 % 2 = 1 as m1", "c0 % 3 = 0 as m2"})
            .singleAggregation(
                {},
                {
                    "sum(c0)",
                    fmt::format("{}(c0 ORDER BY c1 DESC)", functionName),
                    fmt::format("{}(c0 ORDER BY c2)", functionName),
                },
                {"", "m1", "m2"})
            .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC) FILTER (WHERE c0 % 2 = 1), array_agg(c0 ORDER BY c2) FILTER (WHERE c0 % 3 = 0) FROM tmp");

    // No rows excluded by the mask.
    plan = PlanBuilder()
               .values({data})
               .project({"c0", "c1", "c2", "c0 > 0 as m1"})
               .singleAggregation(
                   {},
                   {
                       "sum(c0)",
                       fmt::format("{}(c0 ORDER BY c1 DESC)", functionName),
                       fmt::format("{}(c0 ORDER BY c2)", functionName),
                   },
                   {
                       "",
                       "m1",
                   })
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC), array_agg(c0 ORDER BY c2) FROM tmp");

    // All rows are excluded by the mask.
    plan = PlanBuilder()
               .values({data})
               .project({"c0", "c1", "c2", "c0 < 0 as m1", "c0 % 3 = 0 as m2"})
               .singleAggregation(
                   {},
                   {
                       "sum(c0)",
                       fmt::format("{}(c0 ORDER BY c1 DESC)", functionName),
                       fmt::format("{}(c0 ORDER BY c2)", functionName),
                   },
                   {"", "m1", "m2"})
               .planNode();

    AssertQueryBuilder(plan, duckDbQueryRunner_)
        .assertResults(
            "SELECT sum(c0), array_agg(c0 ORDER BY c1 DESC) FILTER (WHERE c1 < 0), array_agg(c0 ORDER BY c2) FILTER (WHERE c1 % 3 = 0) FROM tmp");
  };

  testFunction("array_agg");
  testFunction("simple_array_agg");
}

TEST_F(ArrayAggTest, mask) {
  auto testFunction = [this](const std::string& functionName) {
    // Global aggregation with all-false mask.
    auto data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeConstant(false, 5),
    });

    // TODO: Add support for FILTER in testAggregationsWithCompanion() and test
    // queries with companion functions.
    testAggregations(
        split(data),
        {},
        {fmt::format("{}(c0) FILTER (WHERE c1)", functionName)},
        "SELECT null");

    // Global aggregation with a non-constant mask.
    data = makeRowVector({
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeFlatVector<bool>({true, false, true, false, true}),
    });

    testAggregations(
        split(data),
        {},
        {fmt::format("{}(c0) FILTER (WHERE c1)", functionName)},
        {"array_sort(a0)"},
        "SELECT [1, 3, 5]");

    // Group-by with all-false mask.
    data = makeRowVector({
        makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeConstant(false, 5),
    });

    testAggregations(
        split(data),
        {"c0"},
        {fmt::format("{}(c1) FILTER (WHERE c2)", functionName)},
        "VALUES (10, null), (20, null)");

    // Group-by with a non-constant mask.
    data = makeRowVector({
        makeFlatVector<int64_t>({10, 20, 10, 20, 20}),
        makeFlatVector<int64_t>({1, 2, 3, 4, 5}),
        makeFlatVector<bool>({true, false, true, false, true}),
    });

    testAggregations(
        split(data),
        {"c0"},
        {fmt::format("{}(c1) FILTER (WHERE c2)", functionName)},
        {"c0", "array_sort(a0)"},
        "VALUES (10, [1, 3]), (20, [5])");
  };

  testFunction("array_agg");
  testFunction("simple_array_agg");
}

} // namespace
} // namespace facebook::velox::aggregate::test
