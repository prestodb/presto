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
#include <fmt/format.h>
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include <string>

#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/tests/utils/Cursor.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/functions/prestosql/aggregates/AggregateNames.h"

using namespace facebook::velox::connector::hive;
using namespace facebook::velox::exec::test;

using facebook::velox::exec::Task;

static constexpr int32_t kNumVectors = 10;
static constexpr int32_t kRowsPerVector = 100'000;
static const std::string kTableBenchmarkTest = "TableBenchMarkTest.Write";

namespace facebook::velox::aggregate::test {

namespace {

class PushdownBenchmark : public HiveConnectorTestBase {
 public:
  explicit PushdownBenchmark(const std::shared_ptr<const RowType>& rowType)
      : rowType_(rowType) {
    HiveConnectorTestBase::SetUp();
    vectors_ = HiveConnectorTestBase::makeVectors(
        rowType_, kNumVectors, kRowsPerVector);
    filePath_ = TempFilePath::create();
    writeToFile(filePath_->path, kTableBenchmarkTest, vectors_);
  }

  ~PushdownBenchmark() override {
    HiveConnectorTestBase::TearDown();
  }

  void TestBody() override {}

  std::shared_ptr<TempFilePath> getFilePath() {
    return filePath_;
  }

  std::shared_ptr<core::PlanNode> makePushdownGroupByPlan(
      const std::string& aggName) {
    auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
        "hive_table", true, SubfieldFilters(), nullptr);

    std::unordered_map<std::string, std::shared_ptr<connector::ColumnHandle>>
        assignments;
    for (uint32_t i = 0; i < rowType_->size(); ++i) {
      const auto& name = rowType_->nameOf(i);
      assignments[name] = regularColumn(name, rowType_->childAt(i));
    }

    return PlanBuilder()
        .tableScan(rowType_, tableHandle, assignments)
        .partialAggregation({0}, {fmt::format("{}(c1)", aggName)})
        .finalAggregation()
        .planNode();
  }

  void runQuery(
      const std::shared_ptr<const core::PlanNode>& plan,
      const std::vector<std::shared_ptr<connector::ConnectorSplit>>& splits) {
    bool noMoreSplits = false;
    CursorParameters params;
    params.planNode = plan;
    readCursor(params, [&](Task* task) {
      if (noMoreSplits) {
        return;
      }
      for (auto& connectorSplit : splits) {
        task->addSplit("0", exec::Split(folly::copy(connectorSplit), -1));
      }
      task->noMoreSplits("0");
      noMoreSplits = true;
    });
  }

 private:
  std::vector<RowVectorPtr> vectors_;
  std::shared_ptr<const RowType> rowType_;
  std::shared_ptr<TempFilePath> filePath_;
};

void pushdown(
    uint32_t,
    const std::string& aggName,
    const std::shared_ptr<const Type>& type) {
  folly::BenchmarkSuspender kSuspender;
  PushdownBenchmark benchmark(ROW({"c0", "c1"}, {BIGINT(), type}));
  auto finalAgg = benchmark.makePushdownGroupByPlan(aggName);
  auto splits = benchmark.makeHiveSplits({benchmark.getFilePath()});
  kSuspender.dismiss();
  benchmark.runQuery(finalAgg, splits);
  kSuspender.rehire();
}

// Sum aggregate.
BENCHMARK_NAMED_PARAM(pushdown, SUM_TINYINT, kSum, TINYINT());
BENCHMARK_NAMED_PARAM(pushdown, SUM_SMALLINT, kSum, SMALLINT());
BENCHMARK_NAMED_PARAM(pushdown, SUM_INTEGER, kSum, INTEGER());
BENCHMARK_NAMED_PARAM(pushdown, SUM_REAL, kSum, REAL());
BENCHMARK_NAMED_PARAM(pushdown, SUM_DOUBLE, kSum, DOUBLE());
BENCHMARK_DRAW_LINE();

// Max aggregate.
BENCHMARK_NAMED_PARAM(pushdown, MAX_TINYINT, kMax, TINYINT());
BENCHMARK_NAMED_PARAM(pushdown, MAX_SMALLINT, kMax, SMALLINT());
BENCHMARK_NAMED_PARAM(pushdown, MAX_INTEGER, kMax, INTEGER());
BENCHMARK_NAMED_PARAM(pushdown, MAX_BIGINT, kMax, BIGINT());
BENCHMARK_NAMED_PARAM(pushdown, MAX_REAL, kMax, REAL());
BENCHMARK_NAMED_PARAM(pushdown, MAX_DOUBLE, kMax, DOUBLE());
BENCHMARK_DRAW_LINE();

// Min aggregate.
BENCHMARK_NAMED_PARAM(pushdown, MIN_TINYINT, kMin, TINYINT());
BENCHMARK_NAMED_PARAM(pushdown, MIN_SMALLINT, kMin, SMALLINT());
BENCHMARK_NAMED_PARAM(pushdown, MIN_INTEGER, kMin, INTEGER());
BENCHMARK_NAMED_PARAM(pushdown, MIN_BIGINT, kMin, BIGINT());
BENCHMARK_NAMED_PARAM(pushdown, MIN_REAL, kMin, REAL());
BENCHMARK_NAMED_PARAM(pushdown, MIN_DOUBLE, kMin, DOUBLE());
BENCHMARK_DRAW_LINE();

// Bitwise 0R aggregate.
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_OR_TINYINT, kBitwiseOr, TINYINT());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_OR_SMALLINT, kBitwiseOr, SMALLINT());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_OR_INTEGER, kBitwiseOr, INTEGER());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_OR_BIGINT, kBitwiseOr, BIGINT());
BENCHMARK_DRAW_LINE();

// Bitwise AND aggregate.
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_AND_TINYINT, kBitwiseAnd, TINYINT());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_AND_SMALLINT, kBitwiseAnd, SMALLINT());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_AND_INTEGER, kBitwiseAnd, INTEGER());
BENCHMARK_NAMED_PARAM(pushdown, BITWISE_AND_BIGINT, kBitwiseAnd, BIGINT());
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace facebook::velox::aggregate::test

int main(int argc, char** argv) {
  folly::init(&argc, &argv);
  folly::runBenchmarks();
  return 0;
}
