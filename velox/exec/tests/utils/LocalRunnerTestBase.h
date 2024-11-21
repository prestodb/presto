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

#pragma once

#include "velox/exec/ExchangeSource.h"
#include "velox/exec/tests/utils/HiveConnectorTestBase.h"
#include "velox/exec/tests/utils/LocalExchangeSource.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/runner/LocalRunner.h"

namespace facebook::velox::exec::test {

struct TableSpec {
  std::string name;
  RowTypePtr columns;
  int32_t rowsPerVector{10000};
  int32_t numVectorsPerFile{5};
  int32_t numFiles{5};

  /// Function  Applied to generated RowVectors for the table before writing.
  /// May be used to insert non-random data on top of the random datafrom
  /// HiveConnectorTestBase::makeVectors.
  std::function<void(const RowVectorPtr& vector)> customizeData;
};

/// Test helper class that manages a TestCase with a set of generated tables and
/// a LocalSchema and LocalSplitSource covering the test data. The lifetime of
/// the test data is the test case consisting of multiple TEST_F's.
class LocalRunnerTestBase : public HiveConnectorTestBase {
 protected:
  static void SetUpTestCase() {
    HiveConnectorTestBase::SetUpTestCase();
    schemaExecutor_ = std::make_unique<folly::CPUThreadPoolExecutor>(4);
  }

  static void TearDownTestCase() {
    files_.reset();
    HiveConnectorTestBase::TearDownTestCase();
  }

  void SetUp() override;

  void ensureTestData();
  void makeSchema();

  void makeTables(
      std::vector<TableSpec> specs,
      std::shared_ptr<TempDirectoryPath>& directory);

  std::shared_ptr<runner::SplitSourceFactory> splitSourceFactory(
      const runner::LocalSchema& schema);

  // Creates a QueryCtx with 'pool'. 'pool' must be a root pool.
  static std::shared_ptr<core::QueryCtx> makeQueryCtx(
      const std::string& queryId,
      memory::MemoryPool* pool);

  // Configs for creating QueryCtx.
  inline static std::unordered_map<std::string, std::string> config_;
  inline static std::unordered_map<std::string, std::string> hiveConfig_;

  // The specification of the test data. The data is created in ensureTestData()
  // called from each SetUp()(.
  inline static std::vector<TableSpec> testTables_;

  // The top level directory with the test data.
  inline static std::shared_ptr<TempDirectoryPath> files_;
  inline static std::unique_ptr<folly::CPUThreadPoolExecutor> schemaExecutor_;

  // The schema built from the data in 'files_'.
  std::shared_ptr<runner::LocalSchema> schema_;

  // Split source factory for making SplitSources that range over tables inside
  // 'files_'.
  std::shared_ptr<runner::SplitSourceFactory> splitSourceFactory_;

  // Leaf pool for schema.
  std::shared_ptr<memory::MemoryPool> schemaPool_;
};

/// Reads all results from 'runner'.
std::vector<RowVectorPtr> readCursor(
    std::shared_ptr<runner::LocalRunner> runner);

} // namespace facebook::velox::exec::test
