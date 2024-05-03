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

#include <gtest/gtest.h>

#include "velox/common/file/FileSystems.h"

#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/fuzzer/RowNumberFuzzer.h"
#include "velox/serializers/PrestoSerializer.h"

/// RowNumber FuzzerRunner leverages RowNumberFuzzer and VectorFuzzer to
/// automatically generate and execute tests. It works as follows:
///
///  1. Plan Generation: Generate two equivalent query plans, one is row-number
///     over ValuesNode and the other is over TableScanNode.
///  2. Executes a variety of logically equivalent query plans and checks the
///     results are the same.
///  3. Rinse and repeat.
///
/// It is used as follows:
///
///  $ ./velox_row_number_fuzzer_test --duration_sec 600
///
/// The flags that configure RowNumberFuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --batch_size: size of input vector batches generated.
///  --num_batches: number if input vector batches to generate.
///  --enable_spill: test plans with spilling enabled.
///  --enable_oom_injection: randomly trigger OOM while executing query plans.
/// e.g:
///
///  $ ./velox_row_number_fuzzer_test \
///         --seed 123 \
///         --duration_sec 600 \
///         --v=1

namespace facebook::velox::exec::test {

class RowNumberFuzzerRunner {
 public:
  static int run(
      size_t seed,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner) {
    serializer::presto::PrestoVectorSerde::registerVectorSerde();
    filesystems::registerLocalFileSystem();
    rowNumberFuzzer(seed, std::move(referenceQueryRunner));
    return RUN_ALL_TESTS();
  }
};

} // namespace facebook::velox::exec::test
