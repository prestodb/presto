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

#include <folly/init/Init.h>
#include <gflags/gflags.h>

#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/fuzzer/TopNRowNumberFuzzer.h"

/// TopNRowNumberFuzzerRunner leverages TopNRowNumberFuzzer and VectorFuzzer to
/// automatically generate and execute tests. It works as follows:
///
///  1. Plan Generation: Generate two equivalent query plans, one is
///     topn-row-number over ValuesNode and the other is over TableScanNode.
///  2. Executes a variety of logically equivalent query plans and checks the
///     results are the same.
///  3. Rinse and repeat.
///
/// It is used as follows:
///
///  $ ./velox_topn_row_number_fuzzer_test --duration_sec 600
///
/// The flags that configure TopNRowNumberFuzzer's behavior are:
///
///  --steps: how many iterations to run.
///  --duration_sec: alternatively, for how many seconds it should run (takes
///          precedence over --steps).
///  --seed: pass a deterministic seed to reproduce the behavior (each iteration
///          will print a seed as part of the logs).
///  --v=1: verbose logging; print a lot more details about the execution.
///  --batch_size: size of input vector batches generated.
///  --num_batches: number of input vector batches to generate.
///  --enable_spill: test plans with spilling enabled.
///  --enable_oom_injection: randomly trigger OOM while executing query plans.
/// e.g:
///
///  $ ./velox_topn_row_number_fuzzer_test \
///         --seed 123 \
///         --duration_sec 600 \
///         --v=1

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    presto_url,
    "",
    "Presto coordinator URI along with port. If set, we use Presto "
    "source of truth. Otherwise, use DuckDB. Example: "
    "--presto_url=http://127.0.0.1:8080");

DEFINE_uint32(
    req_timeout_ms,
    1000,
    "Timeout in milliseconds for HTTP requests made to reference DB, "
    "such as Presto. Example: --req_timeout_ms=2000");

DEFINE_int64(allocator_capacity, 8L << 30, "Allocator capacity in bytes.");

DEFINE_int64(arbitrator_capacity, 6L << 30, "Arbitrator capacity in bytes.");

using namespace facebook::velox;

int main(int argc, char** argv) {
  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);
  exec::test::setupMemory(FLAGS_allocator_capacity, FLAGS_arbitrator_capacity);
  std::shared_ptr<memory::MemoryPool> rootPool{
      memory::memoryManager()->addRootPool()};
  auto referenceQueryRunner = exec::test::setupReferenceQueryRunner(
      rootPool.get(),
      FLAGS_presto_url,
      "topn_row_number_fuzzer",
      FLAGS_req_timeout_ms);
  const size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  exec::topNRowNumberFuzzer(initialSeed, std::move(referenceQueryRunner));
}
