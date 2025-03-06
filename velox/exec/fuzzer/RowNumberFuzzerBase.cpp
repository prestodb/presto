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

#include "velox/exec/fuzzer/RowNumberFuzzerBase.h"

#include <utility>
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/exec/Spill.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(enable_spill, true, "Whether to test plans with spilling enabled.");

DEFINE_int32(
    max_spill_level,
    -1,
    "Max spill level, -1 means random [0, 7], otherwise the actual level.");

DEFINE_bool(
    enable_oom_injection,
    false,
    "When enabled OOMs will randomly be triggered while executing query "
    "plans. The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

namespace facebook::velox::exec {

RowNumberFuzzerBase::RowNumberFuzzerBase(
    size_t initialSeed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      referenceQueryRunner_{std::move(referenceQueryRunner)} {
  setupReadWrite();
  seed(initialSeed);
}

void RowNumberFuzzerBase::setupReadWrite() {
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();

  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }

  // Make sure not to run out of open file descriptors.
  std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};
  test::registerHiveConnector(hiveConfig);
}

// Sometimes we generate zero-column input of type ROW({}) or a column of type
// UNKNOWN(). Such data cannot be written to a file and therefore cannot
// be tested with TableScan.
bool RowNumberFuzzerBase::isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }
  if (type->kind() == TypeKind::HUGEINT) {
    return false;
  }
  // Disable testing with TableScan when input contains TIMESTAMP type, due to
  // the issue #8127.
  if (type->kind() == TypeKind::TIMESTAMP) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

void RowNumberFuzzerBase::validateExpectedResults(
    const core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& input,
    const RowVectorPtr& result) {
  if (!test::containsUnsupportedTypes(input[0]->type())) {
    auto [referenceResult, status] =
        test::computeReferenceResults(plan, referenceQueryRunner_.get());
    if (referenceResult.has_value()) {
      VELOX_CHECK(
          test::assertEqualResults(
              referenceResult.value(), plan->outputType(), {result}),
          "Velox and Reference results don't match");
    }
  }
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

void RowNumberFuzzerBase::run() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_USER_CHECK_GE(FLAGS_batch_size, 10, "Batch size must be at least 10.");

  const auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";
    runSingleIteration();
    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    reSeed();
    ++iteration;
  }
}

RowVectorPtr RowNumberFuzzerBase::execute(
    const PlanWithSplits& plan,
    const std::shared_ptr<memory::MemoryPool>& pool,
    bool injectSpill,
    bool injectOOM,
    const std::optional<std::string>& spillConfig,
    int maxSpillLevel) {
  LOG(INFO) << "Executing query plan: " << plan.plan->toString(true, true);

  test::AssertQueryBuilder builder(plan.plan);
  if (!plan.splits.empty()) {
    builder.splits(plan.splits);
  }

  int32_t spillPct{0};
  if (injectSpill) {
    VELOX_CHECK(
        spillConfig.has_value(),
        "Spill config not set for execute with spilling");
    VELOX_CHECK_GE(
        maxSpillLevel, 0, "Max spill should be set for execute with spilling");
    std::shared_ptr<test::TempDirectoryPath> spillDirectory;
    spillDirectory = exec::test::TempDirectoryPath::create();
    builder.config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kMaxSpillLevel, maxSpillLevel)
        .config(spillConfig.value(), true)
        .spillDirectory(spillDirectory->getPath());
    spillPct = 10;
  }

  test::ScopedOOMInjector oomInjector(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (injectOOM) {
    oomInjector.enable();
  }

  // Wait for the task to be destroyed before start next query execution to
  // avoid the potential interference of the background activities across query
  // executions.
  auto stopGuard =
      folly::makeGuard([&]() { test::waitForAllTasksToBeDeleted(); });

  TestScopedSpillInjection scopedSpillInjection(spillPct);
  RowVectorPtr result;
  try {
    result = builder.copyResults(pool.get());
  } catch (VeloxRuntimeError& e) {
    if (injectOOM &&
        e.errorCode() == facebook::velox::error_code::kMemCapExceeded &&
        e.message() == test::ScopedOOMInjector::kErrorMessage) {
      // If we enabled OOM injection we expect the exception thrown by the
      // ScopedOOMInjector.
      return nullptr;
    }

    throw e;
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << std::endl << result->toString(0, result->size());
  }

  return result;
}

void RowNumberFuzzerBase::testPlan(
    const PlanWithSplits& plan,
    int32_t testNumber,
    const RowVectorPtr& expected,
    const std::optional<std::string>& spillConfig) {
  LOG(INFO) << "Testing plan #" << testNumber;

  auto actual =
      execute(plan, pool_, /*injectSpill=*/false, FLAGS_enable_oom_injection);
  if (actual != nullptr && expected != nullptr) {
    VELOX_CHECK(
        test::assertEqualResults({expected}, {actual}),
        "Logically equivalent plans produced different results");
  } else {
    VELOX_CHECK(
        FLAGS_enable_oom_injection, "Got unexpected nullptr for results");
  }

  if (FLAGS_enable_spill) {
    LOG(INFO) << "Testing plan #" << testNumber << " with spilling";
    const auto fuzzMaxSpillLevel =
        FLAGS_max_spill_level == -1 ? randInt(0, 7) : FLAGS_max_spill_level;
    actual = execute(
        plan,
        pool_,
        /*=injectSpill=*/true,
        FLAGS_enable_oom_injection,
        spillConfig,
        fuzzMaxSpillLevel);
    if (actual != nullptr && expected != nullptr) {
      try {
        VELOX_CHECK(
            test::assertEqualResults({expected}, {actual}),
            "Logically equivalent plans produced different results");
      } catch (const VeloxException&) {
        LOG(ERROR) << "Expected\n"
                   << expected->toString(0, expected->size()) << "\nActual\n"
                   << actual->toString(0, actual->size());
        throw;
      }
    } else {
      VELOX_CHECK(
          FLAGS_enable_oom_injection, "Got unexpected nullptr for results");
    }
  }
}

} // namespace facebook::velox::exec
