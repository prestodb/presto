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
#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

/// Benchmark for running expressions on differently wrapped
/// columns. Runs a sequence of filters and projects wher where each
/// filter adds a row number mapping. Expressions are evaluated on
/// data filtered by one or more filters, of which each adds a
/// dictionary mapping output rows to input rows.
///
/// Benchmarks are run on 100K values, either in 10 x 10K or 2000 * 50 row
/// vectors. Benchmarks are run with 4 consecutive filters, each of which passes
/// either 95% or 50% of the input.  The sequences of 4 filters are compared to
/// a single filter followed by 4 projections to measure the cost of wrapping
/// data in dictionaries and subsequently peeling these off.
///
/// String data is benchmarked with either flat or dictionary encoded
/// input. The dictionary encoded case is either with a different set
/// of base values in each vector or each vector sharing the same base
/// values. The latter case allows memoization of expressions on
/// different elements of the base values.

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

namespace {
struct TestCase {
  // Dataset to be processed by the below plans.
  std::vector<RowVectorPtr> rows;

  // plan that processes data in one FilterProject selecting 80%
  std::shared_ptr<const core::PlanNode> baseline;
  // Plan that processes data in 4 stages, each selecting 95%
  std::shared_ptr<const core::PlanNode> plan95;
  // Plan that processes data in 4 stages, each selecting 50%
  std::shared_ptr<const core::PlanNode> plan50;
};

class FilterProjectBenchmark : public VectorTestBase {
 public:
  std::vector<RowVectorPtr>
  makeRows(RowTypePtr type, int32_t numVectors, int32_t rowsPerVector) {
    std::vector<RowVectorPtr> vectors;
    for (int32_t i = 0; i < numVectors; ++i) {
      auto vector = std::dynamic_pointer_cast<RowVector>(
          BatchMaker::createBatch(type, rowsPerVector, *pool_));
      vectors.push_back(vector);
    }
    return vectors;
  }

  template <typename T = int64_t>
  void
  setRandomInts(int32_t column, int32_t max, std::vector<RowVectorPtr> rows) {
    for (auto& r : rows) {
      auto values = r->childAt(column)->as<FlatVector<T>>();
      for (auto i = 0; i < values->size(); ++i) {
        values->set(i, folly::Random::rand32(rng_) % max);
      }
    }
  }

  std::shared_ptr<const core::PlanNode> makeFilterProjectPlan(
      int32_t numStages,
      int32_t passPct,
      std::vector<RowVectorPtr> data) {
    assert(!data.empty());
    exec::test::PlanBuilder builder;
    auto& type = data[0]->type()->as<TypeKind::ROW>();
    builder.values(data);
    for (auto level = 0; level < numStages; ++level) {
      builder.filter(fmt::format(
          "c0 >= {}",
          static_cast<int32_t>(
              1000000 - pow(passPct / 100.0, 1 + level) * 1000000)));
      std::vector<std::string> projections = {"c0"};
      int32_t nthBigint = 0;
      int32_t nthVarchar = 0;

      for (auto i = 1; i < type.size(); ++i) {
        projections.push_back(fmt::format("c{}", i));

        switch (type.childAt(i)->kind()) {
          case TypeKind::BIGINT:
            if (nthBigint++ % numStages == level) {
              projections.back() = fmt::format("c{} + 1 as c{}", i, i);
            }
            break;
          case TypeKind::VARCHAR:
            if (nthVarchar++ % numStages == level) {
              projections.back() = fmt::format(
                  "substr(c{}, 1, if (length(c{}) > 2, length(c{}) - 1, 0)) as c{}",
                  i,
                  i,
                  i,
                  i);
            }
            break;

          default:;
        }
      }
      builder.project(projections);
    }
    std::vector<std::string> aggregates = {"count(1)"};
    std::vector<std::string> finalProjection;
    bool needFinalProjection = false;
    for (auto i = 0; i < type.size(); ++i) {
      finalProjection.push_back(fmt::format("c{}", i));
      switch (type.childAt(i)->kind()) {
        case TypeKind::BIGINT:
          aggregates.push_back(fmt::format("max(c{})", i));
          break;
        case TypeKind::VARCHAR:
          needFinalProjection = true;
          finalProjection.back() = fmt::format("length(c{}) as c{}", i, i);
          aggregates.push_back(fmt::format("max(c{})", i));
          break;
        default:
          break;
      }
    }
    if (needFinalProjection) {
      builder.project(finalProjection);
    }
    builder.singleAggregation({}, aggregates);
    return builder.planNode();
  }

  std::string makeString(int32_t n) {
    static std::vector<std::string> tokens = {
        "epi",         "plectic",  "cary",    "ally",    "ously",
        "sly",         "suspect",  "account", "apo",     "thetic",
        "hypo",        "hyper",    "nice",    "fluffy",  "hippocampus",
        "comfortable", "cucurbit", "lemon",   "avocado", "specious",
        "phrenic"};
    std::string result;
    while (n > 0) {
      result = result + tokens[n % tokens.size()];
      n /= tokens.size();
    }
    return result;
  }

  VectorPtr randomStrings(int32_t size, int32_t cardinality) {
    std::string temp;
    return makeFlatVector<StringView>(size, [&](auto /*row*/) {
      temp = makeString(folly::Random::rand32() % cardinality);
      return StringView(temp);
    });
  }

  void prepareStringColumns(
      std::vector<RowVectorPtr> rows,
      int32_t cardinality,
      bool dictionaryStrings,
      bool shareStringDicts,
      bool stringNulls) {
    assert(!rows.empty());
    auto type = rows[0]->type()->as<TypeKind::ROW>();
    auto numColumns = rows[0]->type()->size();
    for (auto column = 0; column < numColumns; ++column) {
      if (type.childAt(column)->kind() == TypeKind::VARCHAR) {
        VectorPtr strings;
        if (dictionaryStrings && shareStringDicts) {
          strings = randomStrings(cardinality, cardinality * 2);
        }
        for (auto row : rows) {
          VectorPtr values;
          if (dictionaryStrings) {
            if (!shareStringDicts) {
              strings = randomStrings(cardinality, cardinality * 2);
            }
            auto indices = makeIndices(row->size(), [&](auto /*row*/) {
              return folly::Random::rand32() % strings->size();
            });
            values = BaseVector::wrapInDictionary(
                nullptr, indices, row->size(), strings);
          } else {
            values = randomStrings(row->size(), cardinality);
          }
          if (stringNulls) {
            setNulls(values, [&](auto row) { return row % 11 == 0; });
          }
          row->childAt(column) = values;
        }
      }
    }
  }

  void makeBenchmark(
      std::string name,
      RowTypePtr type,
      int64_t numVectors,
      int32_t numPerVector,
      int32_t stringCardinality = 1000,
      bool dictionaryStrings = false,
      bool shareStringDicts = false,
      bool stringNulls = false) {
    auto test = std::make_unique<TestCase>();
    test->rows = makeRows(type, numVectors, numPerVector);
    setRandomInts(0, 1000000, test->rows);
    prepareStringColumns(
        test->rows,
        stringCardinality,
        dictionaryStrings,
        shareStringDicts,
        stringNulls);
    test->baseline = makeFilterProjectPlan(1, 80, test->rows);
    test->plan95 = makeFilterProjectPlan(4, 95, test->rows);
    test->plan50 = makeFilterProjectPlan(4, 50, test->rows);
    folly::addBenchmark(
        __FILE__, name + "_base", [plan = &test->baseline, this]() {
          run(*plan);
          return 1;
        });
    folly::addBenchmark(
        __FILE__, name + "_95^4", [plan = &test->plan95, this]() {
          run(*plan);
          return 1;
        });
    folly::addBenchmark(
        __FILE__, name + "_50^4", [plan = &test->plan50, this]() {
          run(*plan);
          return 1;
        });
    cases_.push_back(std::move(test));
  }

  int64_t run(std::shared_ptr<const core::PlanNode> plan) {
    auto start = getCurrentTimeMicro();
    int32_t numRows = 0;
    auto result = exec::test::AssertQueryBuilder(plan).copyResults(pool_.get());
    numRows += result->childAt(0)->as<FlatVector<int64_t>>()->valueAt(0);
    auto elapsedMicros = getCurrentTimeMicro() - start;
    return elapsedMicros;
  }

  std::vector<std::unique_ptr<TestCase>> cases_;
  folly::Random::DefaultGenerator rng_;
};
} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  functions::prestosql::registerAllScalarFunctions();
  aggregate::prestosql::registerAllAggregateFunctions();
  parse::registerTypeResolver();

  FilterProjectBenchmark bm;

  auto bigint4 = ROW(
      {{"c0", BIGINT()},
       {"c1", BIGINT()},
       {"c2", BIGINT()},
       {"c3", BIGINT()},
       {"c4", BIGINT()}});

  auto varchar4 = ROW(
      {{"c0", BIGINT()},
       {"c1", VARCHAR()},
       {"c2", VARCHAR()},
       {"c3", VARCHAR()},
       {"c4", VARCHAR()}});

  // Integers.
  bm.makeBenchmark("Bigint4_10K", bigint4, 10, 10000);
  bm.makeBenchmark("Bigint4_50", bigint4, 2000, 50);

  // Flat strings.
  bm.makeBenchmark("Str4_10K", varchar4, 10, 10000);
  bm.makeBenchmark("Str4_50", varchar4, 2000, 50);

  // Strings dictionary encoded.
  bm.makeBenchmark("StrDict4_10K", varchar4, 10, 10000, 800, true, false, true);
  bm.makeBenchmark("StrDict4_50", varchar4, 2000, 50, 80, true, false, true);

  // Strings with dictionary base values shared between batches.
  bm.makeBenchmark(
      "StrRepDict4_10K", varchar4, 10, 10000, 8000, true, true, true);
  bm.makeBenchmark(
      "StrRepDict4_50", varchar4, 2000, 50, 8000, true, true, true);

  folly::runBenchmarks();
  return 0;
}
