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

#include "velox/common/file/FileSystems.h"
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/JoinFuzzer.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/TypeResolver.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

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
///  $ ./velox_join_fuzzer --steps 10000
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
///  $ ./velox_join_fuzzer \
///         --steps 10000 \
///         --seed 123 \
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

using namespace facebook::velox::exec;

int main(int argc, char** argv) {
  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);
  test::setupMemory(FLAGS_allocator_capacity, FLAGS_arbitrator_capacity);
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  auto referenceQueryRunner = test::setupReferenceQueryRunner(
      rootPool.get(), FLAGS_presto_url, "join_fuzzer", FLAGS_req_timeout_ms);
  const size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  facebook::velox::serializer::presto::PrestoVectorSerde::registerVectorSerde();
  facebook::velox::filesystems::registerLocalFileSystem();
  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::parse::registerTypeResolver();
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kPresto)) {
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kCompactRow)) {
    facebook::velox::serializer::CompactRowVectorSerde::
        registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kUnsafeRow)) {
    facebook::velox::serializer::spark::UnsafeRowVectorSerde::
        registerNamedVectorSerde();
  }
  joinFuzzer(initialSeed, std::move(referenceQueryRunner));
}
