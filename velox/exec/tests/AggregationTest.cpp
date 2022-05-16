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
#include "velox/dwio/dwrf/test/utils/BatchMaker.h"
#include "velox/exec/Aggregate.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/OperatorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/expression/FunctionSignature.h"

using facebook::velox::exec::Aggregate;
using facebook::velox::test::BatchMaker;

namespace facebook::velox::exec::test {
namespace {

struct NonPODInt64 {
  static int constructed;
  static int destructed;

  static void clearStats() {
    constructed = 0;
    destructed = 0;
  }

  int64_t value;

  NonPODInt64(int64_t value_ = 0) : value(value_) {
    ++constructed;
  }

  ~NonPODInt64() {
    value = -1;
    ++destructed;
  }

  // No move/copy constructor and assignment operator are used in this case.
  NonPODInt64(const NonPODInt64& other) = delete;
  NonPODInt64(NonPODInt64&& other) = delete;
  NonPODInt64& operator=(const NonPODInt64&) = delete;
  NonPODInt64& operator=(NonPODInt64&&) = delete;
};

int NonPODInt64::constructed = 0;
int NonPODInt64::destructed = 0;

// SumNonPODAggregate uses NonPODInt64 as accumulator which has external memory
// NonPODInt64::constructed and NonPODInt64::destructed. By asserting their
// equality, we make sure Velox calls constructor/destructor properly.
class SumNonPODAggregate : public Aggregate {
 public:
  explicit SumNonPODAggregate(velox::TypePtr resultType)
      : Aggregate(resultType) {}

  int32_t accumulatorFixedWidthSize() const override {
    return sizeof(NonPODInt64);
  }

  bool accumulatorUsesExternalMemory() const override {
    return true;
  }

  void initializeNewGroups(
      char** groups,
      folly::Range<const velox::vector_size_t*> indices) override {
    for (auto i : indices) {
      new (groups[i] + offset_) NonPODInt64(0);
    }
  }

  void destroy(folly::Range<char**> groups) override {
    for (auto group : groups) {
      value<NonPODInt64>(group)->~NonPODInt64();
    }
  }

  void extractAccumulators(
      char** groups,
      int32_t numGroups,
      velox::VectorPtr* result) override {
    auto vector = (*result)->as<FlatVector<int64_t>>();
    vector->resize(numGroups);
    int64_t* rawValues = vector->mutableRawValues();
    uint64_t* rawNulls = getRawNulls(vector);
    for (int32_t i = 0; i < numGroups; ++i) {
      char* group = groups[i];
      if (isNull(group)) {
        vector->setNull(i, true);
      } else {
        clearNull(rawNulls, i);
        rawValues[i] = value<NonPODInt64>(group)->value;
      }
    }
  }

  void extractValues(char** groups, int32_t numGroups, velox::VectorPtr* result)
      override {
    extractAccumulators(groups, numGroups, result);
  }

  void addIntermediateResults(
      char** groups,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(groups[i]);
      value<NonPODInt64>(groups[i])->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addRawInput(
      char** groups,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool mayPushdown) override {
    addIntermediateResults(groups, rows, args, mayPushdown);
  }

  void addSingleGroupIntermediateResults(
      char* group,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool /*mayPushdown*/) override {
    DecodedVector decoded(*args[0], rows);

    rows.applyToSelected([&](vector_size_t i) {
      if (decoded.isNullAt(i)) {
        return;
      }
      clearNull(group);
      value<NonPODInt64>(group)->value += decoded.valueAt<int64_t>(i);
    });
  }

  void addSingleGroupRawInput(
      char* group,
      const velox::SelectivityVector& rows,
      const std::vector<velox::VectorPtr>& args,
      bool mayPushdown) override {
    addSingleGroupIntermediateResults(group, rows, args, mayPushdown);
  }

  void finalize(char** /*groups*/, int32_t /*numGroups*/) override {}
};

bool registerSumNonPODAggregate(const std::string& name) {
  std::vector<std::shared_ptr<velox::exec::AggregateFunctionSignature>>
      signatures{
          velox::exec::AggregateFunctionSignatureBuilder()
              .returnType("bigint")
              .intermediateType("bigint")
              .argumentType("bigint")
              .build(),
      };

  velox::exec::registerAggregateFunction(
      name,
      std::move(signatures),
      [name](
          velox::core::AggregationNode::Step /*step*/,
          const std::vector<velox::TypePtr>& /*argTypes*/,
          const velox::TypePtr& /*resultType*/)
          -> std::unique_ptr<velox::exec::Aggregate> {
        return std::make_unique<SumNonPODAggregate>(velox::BIGINT());
      });
  return true;
}

static bool FB_ANONYMOUS_VARIABLE(g_AggregateFunction) =
    registerSumNonPODAggregate("sumnonpod");

class AggregationTest : public OperatorTestBase {
 protected:
  std::vector<RowVectorPtr>
  makeVectors(const RowTypePtr& rowType, vector_size_t size, int numVectors) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          velox::test::BatchMaker::createBatch(rowType, size, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  template <typename T>
  void testSingleKey(
      const std::vector<RowVectorPtr>& vectors,
      const std::string& keyName,
      bool ignoreNullKeys,
      bool distinct) {
    NonPODInt64::clearStats();
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)", "sum(0.1)", "sum(c1)",     "sum(c2)", "sum(c4)", "sum(c5)",
          "min(15)", "min(0.1)", "min(c1)",     "min(c2)", "min(c3)", "min(c4)",
          "min(c5)", "max(15)",  "max(0.1)",    "max(c1)", "max(c2)", "max(c3)",
          "max(c4)", "max(c5)",  "sumnonpod(1)"};
    }

    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {keyName},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause += " WHERE " + keyName + " IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct " + keyName + " " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT " + keyName +
              ", sum(15), sum(cast(0.1 as double)), sum(c1), sum(c2), sum(c4), sum(c5) , min(15), min(0.1), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(0.1), max(c1), max(c2), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY " + keyName);
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  void testMultiKey(
      const std::vector<RowVectorPtr>& vectors,
      bool ignoreNullKeys,
      bool distinct) {
    std::vector<std::string> aggregates;
    if (!distinct) {
      aggregates = {
          "sum(15)",
          "sum(0.1)",
          "sum(c4)",
          "sum(c5)",
          "min(15)",
          "min(0.1)",
          "min(c3)",
          "min(c4)",
          "min(c5)",
          "max(15)",
          "max(0.1)",
          "max(c3)",
          "max(c4)",
          "max(c5)",
          "sumnonpod(1)"};
    }
    auto op = PlanBuilder()
                  .values(vectors)
                  .aggregation(
                      {"c0", "c1", "c6"},
                      aggregates,
                      {},
                      core::AggregationNode::Step::kPartial,
                      ignoreNullKeys)
                  .planNode();

    std::string fromClause = "FROM tmp";
    if (ignoreNullKeys) {
      fromClause +=
          " WHERE c0 IS NOT NULL AND c1 IS NOT NULL AND c6 IS NOT NULL";
    }
    if (distinct) {
      assertQuery(op, "SELECT distinct c0, c1, c6 " + fromClause);
    } else {
      assertQuery(
          op,
          "SELECT c0, c1, c6, sum(15), sum(cast(0.1 as double)), sum(c4), sum(c5), min(15), min(0.1), min(c3), min(c4), min(c5), max(15), max(0.1), max(c3), max(c4), max(c5), sum(1) " +
              fromClause + " GROUP BY c0, c1, c6");
    }
    EXPECT_EQ(NonPODInt64::constructed, NonPODInt64::destructed);
  }

  template <typename T>
  void setTestKey(
      int64_t value,
      int32_t multiplier,
      vector_size_t row,
      FlatVector<T>* vector) {
    vector->set(row, value * multiplier);
  }

  template <typename T>
  void setKey(
      int32_t column,
      int32_t cardinality,
      int32_t multiplier,
      int32_t row,
      RowVector* batch) {
    auto vector = batch->childAt(column)->asUnchecked<FlatVector<T>>();
    auto value = folly::Random::rand32(rng_) % cardinality;
    setTestKey(value, multiplier, row, vector);
  }

  void makeModeTestKeys(
      TypePtr rowType,
      int32_t numRows,
      int32_t c0,
      int32_t c1,
      int32_t c2,
      int32_t c3,
      int32_t c4,
      int32_t c5,
      std::vector<RowVectorPtr>& batches) {
    RowVectorPtr rowVector;
    for (auto count = 0; count < numRows; ++count) {
      if (count % 1000 == 0) {
        rowVector = std::static_pointer_cast<RowVector>(BaseVector::create(
            rowType, std::min(1000, numRows - count), pool_.get()));
        batches.push_back(rowVector);
        for (auto& child : rowVector->children()) {
          child->resize(1000);
        }
      }
      setKey<int64_t>(0, c0, 6, count % 1000, rowVector.get());
      setKey<int16_t>(1, c1, 1, count % 1000, rowVector.get());
      setKey<int8_t>(2, c2, 1, count % 1000, rowVector.get());
      setKey<StringView>(3, c3, 2, count % 1000, rowVector.get());
      setKey<StringView>(4, c4, 5, count % 1000, rowVector.get());
      setKey<StringView>(5, c5, 8, count % 1000, rowVector.get());
    }
  }

  RowTypePtr rowType_{
      ROW({"c0", "c1", "c2", "c3", "c4", "c5", "c6"},
          {BIGINT(),
           SMALLINT(),
           INTEGER(),
           BIGINT(),
           REAL(),
           DOUBLE(),
           VARCHAR()})};
  folly::Random::DefaultGenerator rng_;
};

template <>
void AggregationTest::setTestKey(
    int64_t value,
    int32_t multiplier,
    vector_size_t row,
    FlatVector<StringView>* vector) {
  std::string chars;
  if (multiplier == 2) {
    chars.resize(2);
    chars[0] = (value % 64) + 32;
    chars[1] = ((value / 64) % 64) + 32;
  } else {
    chars = fmt::format("{}", value);
    for (int i = 2; i < multiplier; ++i) {
      chars = chars + fmt::format("{}", i * value);
    }
  }
  vector->set(row, StringView(chars));
}

TEST_F(AggregationTest, global) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {},
                    {"sum(15)",
                     "sum(c1)",
                     "sum(c2)",
                     "sum(c4)",
                     "sum(c5)",
                     "min(15)",
                     "min(c1)",
                     "min(c2)",
                     "min(c3)",
                     "min(c4)",
                     "min(c5)",
                     "max(15)",
                     "max(c1)",
                     "max(c2)",
                     "max(c3)",
                     "max(c4)",
                     "max(c5)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(
      op,
      "SELECT sum(15), sum(c1), sum(c2), sum(c4), sum(c5), min(15), min(c1), min(c2), min(c3), min(c4), min(c5), max(15), max(c1), max(c2), max(c3), max(c4), max(c5) FROM tmp");
}

TEST_F(AggregationTest, singleBigintKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(std::move(vectors), "c0", false, false);
  testSingleKey<int64_t>(std::move(vectors), "c0", true, false);
}

TEST_F(AggregationTest, singleBigintKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<int64_t>(vectors, "c0", false, true);
  testSingleKey<int64_t>(vectors, "c0", true, true);
}

TEST_F(AggregationTest, singleStringKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, false);
  testSingleKey<StringView>(vectors, "c6", true, false);
}

TEST_F(AggregationTest, singleStringKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testSingleKey<StringView>(vectors, "c6", false, true);
  testSingleKey<StringView>(vectors, "c6", true, true);
}

TEST_F(AggregationTest, multiKey) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, false);
  testMultiKey(vectors, true, false);
}

TEST_F(AggregationTest, multiKeyDistinct) {
  auto vectors = makeVectors(rowType_, 10, 100);
  createDuckDbTable(vectors);
  testMultiKey(vectors, false, true);
  testMultiKey(vectors, true, true);
}

TEST_F(AggregationTest, aggregateOfNulls) {
  auto rowType = ROW({"c0", "c1"}, {BIGINT(), SMALLINT()});

  auto children = {
      BatchMaker::createVector<TypeKind::BIGINT>(
          rowType_->childAt(0), 100, *pool_),
      BaseVector::createConstant(
          facebook::velox::variant(TypeKind::SMALLINT), 100, pool_.get()),
  };

  auto rowVector = std::make_shared<RowVector>(
      pool_.get(), rowType, BufferPtr(nullptr), 100, children);
  auto vectors = {rowVector};
  createDuckDbTable(vectors);

  auto op = PlanBuilder()
                .values(vectors)
                .aggregation(
                    {"c0"},
                    {"sum(c1)", "min(c1)", "max(c1)"},
                    {},
                    core::AggregationNode::Step::kPartial,
                    false)
                .planNode();

  assertQuery(op, "SELECT c0, sum(c1), min(c1), max(c1) FROM tmp GROUP BY c0");

  // global aggregation
  op = PlanBuilder()
           .values(vectors)
           .aggregation(
               {},
               {"sum(c1)", "min(c1)", "max(c1)"},
               {},
               core::AggregationNode::Step::kPartial,
               false)
           .planNode();

  assertQuery(op, "SELECT sum(c1), min(c1), max(c1) FROM tmp");
}

TEST_F(AggregationTest, hashmodes) {
  rng_.seed(1);
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});

  std::vector<RowVectorPtr> batches;

  // 20K rows with all at low cardinality.
  makeModeTestKeys(rowType, 20000, 2, 2, 2, 4, 4, 4, batches);
  // 20K rows with all at slightly higher cardinality, still in array range.
  makeModeTestKeys(rowType, 20000, 2, 2, 2, 4, 16, 4, batches);
  // 100K rows with cardinality outside of array range. We transit to
  // generic hash table from normalized keys when running out of quota
  // for distinct string storage for the sixth key.
  makeModeTestKeys(rowType, 100000, 1000000, 2, 2, 4, 4, 1000000, batches);
  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, C2, C3, C4, C5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, rangeToDistinct) {
  rng_.seed(1);
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {BIGINT(), SMALLINT(), TINYINT(), VARCHAR(), VARCHAR(), VARCHAR()});

  std::vector<RowVectorPtr> batches;
  // 20K rows with all at low cardinality. c0 is a range.
  makeModeTestKeys(rowType, 20000, 2000, 2, 2, 4, 4, 4, batches);
  // 20 rows that make c0 represented as distincts.
  makeModeTestKeys(rowType, 20, 200000000, 2, 2, 4, 4, 4, batches);
  // More keys in the low cardinality range. We see if these still hit
  // after the re-encoding of c0.
  makeModeTestKeys(rowType, 10000, 2000, 2, 2, 4, 4, 4, batches);

  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, c2, c3, c4, c5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, allKeyTypes) {
  // Covers different key types. Unlike the integer/string tests, the
  // hash table begins life in the generic mode, not array or
  // normalized key. Add types here as they become supported.
  auto rowType =
      ROW({"c0", "c1", "c2", "c3", "c4", "c5"},
          {DOUBLE(), REAL(), BIGINT(), INTEGER(), BOOLEAN(), VARCHAR()});

  std::vector<RowVectorPtr> batches;
  for (auto i = 0; i < 10; ++i) {
    batches.push_back(std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, 100, *pool_)));
  }
  createDuckDbTable(batches);
  auto op =
      PlanBuilder()
          .values(batches)
          .singleAggregation({"c0", "c1", "c2", "c3", "c4", "c5"}, {"sum(1)"})
          .planNode();

  assertQuery(
      op,
      "SELECT c0, c1, c2, c3, c4, c5, sum(1) FROM tmp "
      " GROUP BY c0, c1, c2, c3, c4, c5");
}

TEST_F(AggregationTest, partialAggregationMemoryLimit) {
  auto vectors = {
      makeRowVector({makeFlatVector<int32_t>(
          100, [](auto row) { return row; }, nullEvery(5))}),
      makeRowVector({makeFlatVector<int32_t>(
          110, [](auto row) { return row + 29; }, nullEvery(7))}),
      makeRowVector({makeFlatVector<int32_t>(
          90, [](auto row) { return row - 71; }, nullEvery(7))}),
  };

  createDuckDbTable(vectors);

  // Set an artificially low limit on the amount of data to accumulate in
  // the partial aggregation.

  // Distinct aggregation.
  AssertQueryBuilder(duckDbQueryRunner_)
      .config(core::QueryConfig::kMaxPartialAggregationMemory, "100")
      .plan(PlanBuilder()
                .values(vectors)
                .partialAggregation({"c0"}, {})
                .finalAggregation()
                .planNode())
      .assertResults("SELECT distinct c0 FROM tmp");

  // Count aggregation.
  AssertQueryBuilder(duckDbQueryRunner_)
      .config(core::QueryConfig::kMaxPartialAggregationMemory, "100")
      .plan(PlanBuilder()
                .values(vectors)
                .partialAggregation({"c0"}, {"count(1)"})
                .finalAggregation()
                .planNode())
      .assertResults("SELECT c0, count(1) FROM tmp GROUP BY 1");
}

/// Verify number of memory allocations in the HashAggregation operator.
TEST_F(AggregationTest, memoryAllocations) {
  vector_size_t size = 1'024;
  std::vector<RowVectorPtr> data;
  for (auto i = 0; i < 10; ++i) {
    data.push_back(makeRowVector({
        makeFlatVector<int64_t>(size, [](auto row) { return row; }),
        makeFlatVector<int64_t>(size, [](auto row) { return row + 3; }),
    }));
  }

  createDuckDbTable(data);

  core::PlanNodeId projectNodeId;
  core::PlanNodeId aggNodeId;
  auto plan = PlanBuilder()
                  .values(data)
                  .project({"c0 + c1"})
                  .capturePlanNodeId(projectNodeId)
                  .singleAggregation({}, {"sum(p0)"})
                  .capturePlanNodeId(aggNodeId)
                  .planNode();

  auto task = assertQuery(plan, "SELECT sum(c0 + c1) FROM tmp");

  // Verify memory allocations. Project operator should allocate a single vector
  // and re-use it. Aggregation should make 2 allocations: 1 for the
  // RowContainer holding single accumulator and 1 for the result.
  auto planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(1, planStats.at(projectNodeId).numMemoryAllocations);
  ASSERT_EQ(2, planStats.at(aggNodeId).numMemoryAllocations);

  plan = PlanBuilder()
             .values(data)
             .project({"c0", "c0 + c1"})
             .capturePlanNodeId(projectNodeId)
             .singleAggregation({"c0"}, {"sum(p1)"})
             .capturePlanNodeId(aggNodeId)
             .planNode();

  task = assertQuery(plan, "SELECT c0, sum(c0 + c1) FROM tmp GROUP BY 1");

  // Verify memory allocations. Project operator should allocate a single vector
  // and re-use it. Aggregation should make 5 allocations: 1 for the hash table,
  // 1 for the RowContainer holding accumulators, 3 for results (2 for values
  // and nulls buffers of the grouping key column, 1 for sum column).
  planStats = toPlanStats(task->taskStats());
  ASSERT_EQ(1, planStats.at(projectNodeId).numMemoryAllocations);
  ASSERT_EQ(5, planStats.at(aggNodeId).numMemoryAllocations);
}

} // namespace
} // namespace facebook::velox::exec::test
