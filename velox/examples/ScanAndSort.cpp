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

#include "velox/common/base/Fs.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/Memory.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/reader/DwrfReader.h"
#include "velox/exec/Task.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

#include <folly/init/Init.h>
#include <algorithm>

using namespace facebook::velox;
using exec::test::HiveConnectorTestBase;

// This file contains a step-by-step minimal example of a workflow that:
//
// #1. generates a simple dataset in-memory using Vectors;
// #2. writes it to a local dwrf-encoded file using TableWrite operator;
// #3. executes a TableScan + OrderBy plan, printing the results to stdout.

int main(int argc, char** argv) {
  // Velox Tasks/Operators are based on folly's async framework, so we need to
  // make sure we initialize it first.
  folly::Init init{&argc, &argv};

  // Default memory allocator used throughout this example.
  memory::MemoryManager::initialize({});
  auto pool = memory::memoryManager()->addLeafPool();

  // For this example, the input dataset will be comprised of a single BIGINT
  // column ("my_col"), containing 10 rows.
  auto inputRowType = ROW({{"my_col", BIGINT()}});
  const size_t vectorSize = 10;

  // Create a base flat vector and fill it with consecutive integers, then
  // shuffle them.
  auto vector = BaseVector::create(BIGINT(), vectorSize, pool.get());
  auto rawValues = vector->values()->asMutable<int64_t>();

  std::iota(rawValues, rawValues + vectorSize, 0); // 0, 1, 2, 3, ...
  std::random_device rd;
  std::mt19937 g(rd());
  std::shuffle(rawValues, rawValues + vectorSize, g);

  // Wrap the vector (column) in a RowVector.
  auto rowVector = std::make_shared<RowVector>(
      pool.get(), // pool where allocations will be made.
      inputRowType, // input row type (defined above).
      BufferPtr(nullptr), // no nulls on this example.
      vectorSize, // length of the vectors.
      std::vector<VectorPtr>{vector}); // the input vector data.

  // For fun, let's print the shuffled data to stdout.
  LOG(INFO) << "Input vector generated:";
  for (vector_size_t i = 0; i < rowVector->size(); ++i) {
    LOG(INFO) << rowVector->toString(i);
  }

  // In order to read and write data and files from storage, we need to use a
  // Connector. Let's instantiate and register a HiveConnector for this
  // example:

  // We need a connector id string to identify the connector.
  const std::string kHiveConnectorId = "test-hive";

  // Create a new connector instance from the connector factory and register
  // it:
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(kHiveConnectorId, std::make_shared<core::MemConfig>());
  connector::registerConnector(hiveConnector);

  // To be able to read local files, we need to register the local file
  // filesystem. We also need to register the dwrf reader factory as well as a
  // write protocol, in this case commit is not required:
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();

  // Create a temporary dir to store the local file created. Note that this
  // directory is automatically removed when the `tempDir` object runs out of
  // scope.
  auto tempDir = exec::test::TempDirectoryPath::create();

  // Once we finalize setting up the Hive connector, let's define our query
  // plan. We use the helper `PlanBuilder` class to generate the query plan
  // for this example, but this is usually done programatically based on the
  // application's IR. Considering that the plan executed in a local host is
  // usually part of a larger and often distributed query, this local portion is
  // called a "query fragment" and described by a "plan fragment".
  //
  // The first part of this example creates a plan fragment that writes data
  // to a file using TableWriter operator. It uses a special `Values` operator
  // that allows us to insert our RowVector straight into the pipeline.
  auto writerPlanFragment =
      exec::test::PlanBuilder()
          .values({rowVector})
          .tableWrite("targetDirectory", dwio::common::FileFormat::DWRF)
          .planFragment();

  std::shared_ptr<folly::Executor> executor(
      std::make_shared<folly::CPUThreadPoolExecutor>(
          std::thread::hardware_concurrency()));

  // Task is the top-level execution concept. A task needs a taskId (as a
  // string), the plan fragment to execute, a destination (only used for
  // shuffles), and a QueryCtx containing metadata and configs for a query.
  auto writeTask = exec::Task::create(
      "my_write_task",
      writerPlanFragment,
      /*destination=*/0,
      std::make_shared<core::QueryCtx>(executor.get()));

  // next() starts execution using the client thread. The loop pumps output
  // vectors out of the task (there are none in this query fragment).
  while (auto result = writeTask->next())
    ;

  // At this point, the first part of the example is done; there is now a
  // file encoded using dwrf at `filePath`. The next part of the example
  // will create a query plan that reads and sorts the contents of that file.
  //
  // The second query fragment is composed of a simple TableScan (which will
  // read the file we just generated), and subsequently sort it. After we create
  // the query fragment and Task structures, we add input data to the Task by
  // adding "splits" to it. Splits are added to the TableScan operator, so
  // during creation we need to capture the TableScan planNodeId in the
  // `scanNodeId` variable.
  core::PlanNodeId scanNodeId;
  auto readPlanFragment = exec::test::PlanBuilder()
                              .tableScan(inputRowType)
                              .capturePlanNodeId(scanNodeId)
                              .orderBy({"my_col"}, /*isPartial=*/false)
                              .planFragment();

  // Create the reader task.
  auto readTask = exec::Task::create(
      "my_read_task",
      readPlanFragment,
      /*destination=*/0,
      std::make_shared<core::QueryCtx>(executor.get()));

  // Now that we have the query fragment and Task structure set up, we will
  // add data to it via `splits`.
  //
  // To pump data through a HiveConnector, we need to create a
  // HiveConnectorSplit for each file, using the same HiveConnector id defined
  // above, the local file path (the "file:" prefix specifies which FileSystem
  // to use; local, in this case), and the file format (DWRF/ORC).
  for (auto& filePath : fs::directory_iterator(tempDir->path)) {
    auto connectorSplit = std::make_shared<connector::hive::HiveConnectorSplit>(
        kHiveConnectorId,
        "file:" + filePath.path().string(),
        dwio::common::FileFormat::DWRF);
    // Wrap it in a `Split` object and add to the task. We need to specify to
    // which operator we're adding the split (that's why we captured the
    // TableScan's id above). Here we could pump subsequent split/files into the
    // TableScan.
    readTask->addSplit(scanNodeId, exec::Split{connectorSplit});
  }

  // Signal that no more splits will be added. After this point, calling next()
  // on the task will start the plan execution using the current thread.
  readTask->noMoreSplits(scanNodeId);

  // Read output vectors and print them.
  while (auto result = readTask->next()) {
    LOG(INFO) << "Vector available after processing (scan + sort):";
    for (vector_size_t i = 0; i < result->size(); ++i) {
      LOG(INFO) << result->toString(i);
    }
  }

  return 0;
}
