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
#include <folly/init/Init.h>
#include <chrono>

#include "velox/common/memory/Memory.h"
#include "velox/connectors/tpch/TpchConnector.h"
#include "velox/connectors/tpch/TpchConnectorSplit.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"

namespace {

/// This a utility binary that helps measure and evaluate how fast we can
/// generate TPC-H datasets using the TPC-H Connector. You can control the
/// generated table, scale factor, number of splits, and number of threads
/// (drivers) using the flags defined below.

DEFINE_string(table, "lineitem", "TPC-H table name to generate.");

DEFINE_int32(
    scale_factor,
    1,
    "Scale factor for the TPC-H table being generated.");

DEFINE_int32(
    num_splits,
    1,
    "Number of splits to generate for a particular TPC-H table scan.");

DEFINE_int32(
    max_drivers,
    1,
    "Maximum number of drivers (threads) per pipeline.");

using namespace facebook::velox;
using namespace facebook::velox::exec::test;

using std::chrono::system_clock;

class TpchSpeedTest {
 public:
  TpchSpeedTest() {
    auto tpchConnector =
        connector::getConnectorFactory(
            connector::tpch::TpchConnectorFactory::kTpchConnectorName)
            ->newConnector(kTpchConnectorId_, nullptr);
    connector::registerConnector(tpchConnector);
  }

  ~TpchSpeedTest() {
    connector::unregisterConnector(kTpchConnectorId_);
  }

  void run(tpch::Table table, size_t scaleFactor, size_t numSplits) {
    LOG(INFO) << "Generating table '" << toTableName(table) << "'.";

    // Create a simple plan composed of a single table scan.
    core::PlanNodeId scanId;
    auto plan =
        PlanBuilder()
            .tableScan(
                table, folly::copy(getTableSchema(table)->names()), scaleFactor)
            .capturePlanNodeId(scanId)
            .planNode();

    totalRows_ = 0;
    totalBytes_ = 0;
    intervalBytes_ = 0;
    intervalRows_ = 0;

    auto startTime = system_clock::now();
    intervalStart_ = startTime;

    CursorParameters params;
    params.planNode = plan;
    params.maxDrivers = FLAGS_max_drivers;

    TaskCursor taskCursor(params);
    taskCursor.start();

    auto task = taskCursor.task();
    addSplits(*task, scanId, numSplits);

    while (taskCursor.moveNext()) {
      processBatch(taskCursor.current());
    }

    // Wait for the task to finish.
    auto& inlineExecutor = folly::QueuedImmediateExecutor::instance();
    task->stateChangeFuture(0).via(&inlineExecutor).wait();

    std::chrono::duration<double> elapsed = system_clock::now() - startTime;
    LOG(INFO) << "Summary:";
    LOG(INFO) << "\tTotal rows generated: " << totalRows_;
    LOG(INFO) << "\tTotal bytes generated: " << totalBytes_;
    LOG(INFO) << "\tTotal time spent: " << elapsed.count() << "s";
  }

 private:
  void addSplits(exec::Task& task, core::PlanNodeId scanId, size_t numSplits) {
    for (size_t i = 0; i < numSplits; ++i) {
      task.addSplit(
          scanId,
          exec::Split(std::make_shared<connector::tpch::TpchConnectorSplit>(
              kTpchConnectorId_, numSplits, i)));
    }

    task.noMoreSplits(scanId);
    LOG(INFO) << "Added " << numSplits << " split(s) to node " << scanId << ".";
  }

  // Process batch size and prints ongoing stats every second.
  void processBatch(const RowVectorPtr& vector) {
    totalRows_ += vector->size();
    totalBytes_ += vector->retainedSize();
    intervalRows_ += vector->size();
    intervalBytes_ += vector->retainedSize();

    auto curTime = system_clock::now();

    if (curTime - intervalStart_ > std::chrono::seconds(1)) {
      size_t msElapsed = std::chrono::duration_cast<std::chrono::milliseconds>(
                             curTime - intervalStart_)
                             .count();

      // Normalize to one exact second (msElapsed will be *at least* one
      // second).
      size_t bytesPerSec = (intervalBytes_ * 1000) / msElapsed;
      size_t rowsPerSec = (intervalRows_ * 1000) / msElapsed;

      LOG(INFO) << "Current throughput: " << printThroughput(bytesPerSec)
                << " - " << rowsPerSec << " rows/s - " << totalRows_
                << " rows cummulative.";
      intervalStart_ = curTime;
      intervalRows_ = 0;
      intervalBytes_ = 0;
    }
  }

  std::string printThroughput(size_t bytesPerSec) {
    if (bytesPerSec < 1'000) {
      return fmt::format("{} b/s", bytesPerSec);
    } else if (bytesPerSec < 1'000'000) {
      return fmt::format("{} kB/s", bytesPerSec / 1'000);
    } else if (bytesPerSec < 1'000'000'000) {
      return fmt::format("{} MB/s", bytesPerSec / 1'000'000);
    } else {
      return fmt::format("{} GB/s", bytesPerSec / 1'000'000'000);
    }
  }

  const std::string kTpchConnectorId_{"test-tpch"};

  size_t totalRows_{0};
  size_t totalBytes_{0};

  size_t intervalRows_{0};
  size_t intervalBytes_{0};

  std::chrono::time_point<system_clock> intervalStart_;
};

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv, false);

  TpchSpeedTest speedTest;
  speedTest.run(
      tpch::fromTableName(FLAGS_table), FLAGS_scale_factor, FLAGS_num_splits);
  return 0;
}
