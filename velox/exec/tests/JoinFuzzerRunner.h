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
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/tests/JoinFuzzer.h"
#include "velox/serializers/PrestoSerializer.h"

/// Join FuzzerRunner leverages JoinFuzzer and VectorFuzzer to
/// automatically generate and execute join tests. It works by:
///
///  1. Picking a random join type.
///  2. Generating a random set of input data (vector), with a variety of
///     encodings and data layouts.
///  3. Executing a variety of logically equivalent query plans and
///     asserting results are the same.
///  4. Rinse and repeat.
///
/// The common usage pattern is as following:
///
///  $ ./velox_join_fuzzer_test --steps 10000
///
/// The important flags that control JoinFuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --batch_size: size of input vector batches generated.
///  --num_batches: number if input vector batches to generate.
///
/// e.g:
///
///  $ ./velox_join_fuzzer_test \
///         --steps 10000 \
///         --seed 123 \
///         --v=1

class JoinFuzzerRunner {
 public:
  static int run(size_t seed) {
    setupMemory();
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerVectorSerde();
    facebook::velox::filesystems::registerLocalFileSystem();

    facebook::velox::exec::test::joinFuzzer(seed);
    return RUN_ALL_TESTS();
  }

 private:
  // Invoked to set up memory system with arbitration.
  static void setupMemory() {
    FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
    FLAGS_velox_memory_leak_check_enabled = true;
    facebook::velox::memory::SharedArbitrator::registerFactory();
    facebook::velox::memory::MemoryManagerOptions options;
    options.allocatorCapacity = 8L << 30;
    options.arbitratorCapacity = 6L << 30;
    options.arbitratorKind = "SHARED";
    options.checkUsageLeak = true;
    options.arbitrationStateCheckCb =
        facebook::velox::exec::memoryArbitrationStateCheck;
    facebook::velox::memory::MemoryManager::initialize(options);
  }
};
