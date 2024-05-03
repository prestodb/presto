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
#include <gtest/gtest.h>
#include <unordered_set>
#include "velox/common/memory/SharedArbitrator.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/fuzzer/DuckQueryRunner.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/fuzzer/RowNumberFuzzerRunner.h"

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

using namespace facebook::velox::exec;

namespace {
std::unique_ptr<test::ReferenceQueryRunner> setupReferenceQueryRunner(
    const std::string& prestoUrl,
    const std::string& runnerName,
    const uint32_t& reqTimeoutMs) {
  if (prestoUrl.empty()) {
    auto duckQueryRunner = std::make_unique<test::DuckQueryRunner>();
    LOG(INFO) << "Using DuckDB as the reference DB.";
    return duckQueryRunner;
  }

  LOG(INFO) << "Using Presto as the reference DB.";
  return std::make_unique<test::PrestoQueryRunner>(
      prestoUrl,
      runnerName,
      static_cast<std::chrono::milliseconds>(reqTimeoutMs));
}

// Invoked to set up memory system with arbitration.
void setupMemory() {
  FLAGS_velox_enable_memory_usage_track_in_default_memory_pool = true;
  FLAGS_velox_memory_leak_check_enabled = true;
  facebook::velox::memory::SharedArbitrator::registerFactory();
  facebook::velox::memory::MemoryManagerOptions options;
  options.allocatorCapacity = 8L << 30;
  options.arbitratorCapacity = 6L << 30;
  options.arbitratorKind = "SHARED";
  options.checkUsageLeak = true;
  options.arbitrationStateCheckCb = memoryArbitrationStateCheck;
  facebook::velox::memory::MemoryManager::initialize(options);
}
} // namespace

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);
  setupMemory();
  auto referenceQueryRunner = setupReferenceQueryRunner(
      FLAGS_presto_url, "row_number_fuzzer", FLAGS_req_timeout_ms);
  const size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  return test::RowNumberFuzzerRunner::run(
      initialSeed, std::move(referenceQueryRunner));
}
