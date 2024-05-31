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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_set>
#include <vector>

#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/WriterFuzzer.h"
#include "velox/expression/fuzzer/FuzzerToolkit.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

namespace facebook::velox::exec::test {

/// WriterFuzzerRunner leverages WriterFuzzer and VectorFuzzer to
/// automatically generate and execute table writer tests.
/// It works in following steps:
///
///  1. Pick different table write properties. Eg: partitioned.
///  (TODO: bucketed, sorted).
///  2. Generate corresponding table write query plan.
///  3. Generate a random set of input data (vector).
///  4. Execute the query plan.
///  5. Generate corresponding sql in reference DB and execute the sql.
///  6. Assert results from step 4 and 5 are the same.
///  7. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./velox_writer_fuzzer_test --steps 10000
///
/// The important flags that control WriterFuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --batch_size: size of input vector batches generated.
///
/// e.g:
///
///  $ ./velox_writer_fuzzer_test \
///         --steps 10000 \
///         --seed 123

class WriterFuzzerRunner {
 public:
  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    filesystems::registerLocalFileSystem();
    auto hiveConnector =
        connector::getConnectorFactory(
            connector::hive::HiveConnectorFactory::kHiveConnectorName)
            ->newConnector(
                kHiveConnectorId, std::make_shared<core::MemConfig>());
    connector::registerConnector(hiveConnector);
    facebook::velox::exec::test::writerFuzzer(
        seed, std::move(referenceQueryRunner));
    // Calling gtest here so that it can be recognized as tests in CI systems.
    return RUN_ALL_TESTS();
  }
};

} // namespace facebook::velox::exec::test
