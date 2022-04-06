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
#include <gflags/gflags.h>

#include "velox/common/base/SuccinctPrinter.h"
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/exec/PlanNodeStats.h"
#include "velox/exec/Split.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/TpchQueryBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::exec::test;
using namespace facebook::velox::dwio::common;

namespace {
static bool notEmpty(const char* /*flagName*/, const std::string& value) {
  return !value.empty();
}

static bool validateDataFormat(const char* flagname, const std::string& value) {
  if ((value.compare("parquet") == 0) || (value.compare("orc") == 0)) {
    return true;
  }
  std::cout
      << fmt::format(
             "Invalid value for --{}: {}. Allowed values are [\"parquet\", \"orc\"]",
             flagname,
             value)
      << std::endl;
  return false;
}

void ensureTaskCompletion(exec::Task* task) {
  // ASSERT_TRUE requires a function with return type void.
  ASSERT_TRUE(waitForTaskCompletion(task));
}
} // namespace

DEFINE_string(data_path, "", "Root path of TPC-H data");
DEFINE_int32(
    run_query_verbose,
    -1,
    "Run a given query and print execution statistics");
DEFINE_bool(
    include_custom_stats,
    false,
    "Include custom statistics along with execution statistics");
DEFINE_int32(num_drivers, 4, "Number of drivers");
DEFINE_string(data_format, "parquet", "Data format");
DEFINE_int32(num_splits_per_file, 10, "Number of splits per file");

DEFINE_validator(data_path, &notEmpty);
DEFINE_validator(data_format, &validateDataFormat);

class TpchBenchmark {
 public:
  void initialize() {
    functions::prestosql::registerAllScalarFunctions();
    filesystems::registerLocalFileSystem();
    parquet::registerParquetReaderFactory();
    dwrf::registerDwrfReaderFactory();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(kHiveConnectorId, nullptr);
    connector::registerConnector(hiveConnector);
  }

  std::shared_ptr<Task> run(const TpchPlan& tpchPlan) {
    CursorParameters params;
    params.maxDrivers = FLAGS_num_drivers;
    params.numResultDrivers = 1;
    params.planNode = tpchPlan.plan;
    const int numSplitsPerFile = FLAGS_num_splits_per_file;

    bool noMoreSplits = false;
    auto addSplits = [&](exec::Task* task) {
      if (!noMoreSplits) {
        for (const auto& entry : tpchPlan.dataFiles) {
          for (const auto& path : entry.second) {
            auto const splits = HiveConnectorTestBase::makeHiveConnectorSplits(
                path, numSplitsPerFile, tpchPlan.dataFileFormat);
            for (const auto& split : splits) {
              task->addSplit(entry.first, exec::Split(split));
            }
          }
          task->noMoreSplits(entry.first);
        }
      }
      noMoreSplits = true;
    };
    auto [cursor, results] = readCursor(params, addSplits);
    return cursor->task();
  }
};

TpchBenchmark benchmark;
std::shared_ptr<TpchQueryBuilder> queryBuilder;

BENCHMARK(q1) {
  const auto planContext = queryBuilder->getQueryPlan(1);
  benchmark.run(planContext);
}

BENCHMARK(q6) {
  const auto planContext = queryBuilder->getQueryPlan(6);
  benchmark.run(planContext);
}

BENCHMARK(q18) {
  const auto planContext = queryBuilder->getQueryPlan(18);
  benchmark.run(planContext);
}

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);
  benchmark.initialize();
  queryBuilder =
      std::make_shared<TpchQueryBuilder>(toFileFormat(FLAGS_data_format));
  queryBuilder->initialize(FLAGS_data_path);
  if (FLAGS_run_query_verbose == -1) {
    folly::runBenchmarks();
  } else {
    const auto queryPlan = queryBuilder->getQueryPlan(FLAGS_run_query_verbose);
    const auto task = benchmark.run(queryPlan);
    ensureTaskCompletion(task.get());
    const auto stats = task->taskStats();
    std::cout << fmt::format(
                     "Execution time: {}",
                     succinctMillis(
                         stats.executionEndTimeMs - stats.executionStartTimeMs))
              << std::endl;
    std::cout << fmt::format(
                     "Splits total: {}, finished: {}",
                     stats.numTotalSplits,
                     stats.numFinishedSplits)
              << std::endl;
    std::cout << printPlanWithStats(
                     *queryPlan.plan, stats, FLAGS_include_custom_stats)
              << std::endl;
  }
}
