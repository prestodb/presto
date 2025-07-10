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

#include "velox/exec/fuzzer/RowNumberFuzzer.h"

#include <utility>
#include "velox/common/file/FileSystems.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/RowNumberFuzzerBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"

namespace facebook::velox::exec {
namespace {

class RowNumberFuzzer : public RowNumberFuzzerBase {
 public:
  explicit RowNumberFuzzer(
      size_t initialSeed,
      std::unique_ptr<test::ReferenceQueryRunner>);

 private:
  // Runs one test iteration from query plans generations, executions and result
  // verifications.
  void runSingleIteration() override;

  std::pair<std::vector<std::string>, std::vector<TypePtr>>
  generatePartitionKeys();

  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes);

  void addPlansWithTableScan(
      const std::string& tableDir,
      const std::vector<std::string>& partitionKeys,
      const std::vector<RowVectorPtr>& input,
      std::vector<PlanWithSplits>& altPlans);

  // Makes the query plan with default settings in RowNumberFuzzer and value
  // inputs for both probe and build sides.
  //
  // NOTE: 'input' could either input rows with lazy
  // vectors or flatten ones.
  static PlanWithSplits makeDefaultPlan(
      const std::vector<std::string>& partitionKeys,
      const std::vector<RowVectorPtr>& input);

  static PlanWithSplits makePlanWithTableScan(
      const RowTypePtr& type,
      const std::vector<std::string>& partitionKeys,
      const std::vector<Split>& splits);
};

RowNumberFuzzer::RowNumberFuzzer(
    size_t initialSeed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner)
    : RowNumberFuzzerBase(initialSeed, std::move(referenceQueryRunner)) {
  // Set timestamp precision as milliseconds, as timestamp may be used as
  // paritition key, and presto doesn't supports nanosecond precision.
  vectorFuzzer_.getMutableOptions().timestampPrecision =
      fuzzer::FuzzerTimestampPrecision::kMilliSeconds;
}

std::pair<std::vector<std::string>, std::vector<TypePtr>>
RowNumberFuzzer::generatePartitionKeys() {
  const auto numKeys = randInt(1, 3);
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto i = 0; i < numKeys; ++i) {
    names.push_back(fmt::format("c{}", i));
    types.push_back(vectorFuzzer_.randType(/*maxDepth=*/1));
  }
  return std::make_pair(names, types);
}

std::vector<RowVectorPtr> RowNumberFuzzer::generateInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes) {
  std::vector<std::string> names = keyNames;
  std::vector<TypePtr> types = keyTypes;
  // Add up to 3 payload columns.
  const auto numPayload = randInt(0, 3);
  for (auto i = 0; i < numPayload; ++i) {
    names.push_back(fmt::format("c{}", i + keyNames.size()));
    types.push_back(vectorFuzzer_.randType(/*maxDepth=*/2));
  }

  const auto inputType = ROW(std::move(names), std::move(types));
  std::vector<RowVectorPtr> input;
  input.reserve(FLAGS_num_batches);
  for (auto i = 0; i < FLAGS_num_batches; ++i) {
    input.push_back(vectorFuzzer_.fuzzInputRow(inputType));
  }

  return input;
}

RowNumberFuzzerBase::PlanWithSplits RowNumberFuzzer::makeDefaultPlan(
    const std::vector<std::string>& partitionKeys,
    const std::vector<RowVectorPtr>& input) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<std::string> projectFields = partitionKeys;
  projectFields.emplace_back("row_number");
  auto plan = test::PlanBuilder()
                  .values(input)
                  .rowNumber(partitionKeys)
                  .project(projectFields)
                  .planNode();
  return PlanWithSplits{std::move(plan)};
}

RowNumberFuzzerBase::PlanWithSplits RowNumberFuzzer::makePlanWithTableScan(
    const RowTypePtr& type,
    const std::vector<std::string>& partitionKeys,
    const std::vector<Split>& splits) {
  std::vector<std::string> projectFields = partitionKeys;
  projectFields.emplace_back("row_number");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanId;
  auto plan = test::PlanBuilder(planNodeIdGenerator)
                  .tableScan(type)
                  .rowNumber(partitionKeys)
                  .project(projectFields)
                  .planNode();
  return PlanWithSplits{plan, splits};
}

void RowNumberFuzzer::addPlansWithTableScan(
    const std::string& tableDir,
    const std::vector<std::string>& partitionKeys,
    const std::vector<RowVectorPtr>& input,
    std::vector<PlanWithSplits>& altPlans) {
  VELOX_CHECK(!tableDir.empty());

  if (!isTableScanSupported(input[0]->type())) {
    return;
  }

  const std::vector<Split> inputSplits = test::makeSplits(
      input, fmt::format("{}/row_number", tableDir), writerPool_);
  altPlans.push_back(makePlanWithTableScan(
      asRowType(input[0]->type()), partitionKeys, inputSplits));
}

void RowNumberFuzzer::runSingleIteration() {
  const auto [keyNames, keyTypes] = generatePartitionKeys();

  const auto input = generateInput(keyNames, keyTypes);
  test::logVectors(input);

  auto defaultPlan = makeDefaultPlan(keyNames, input);
  const auto expected =
      execute(defaultPlan, pool_, /*injectSpill=*/false, false);

  if (expected != nullptr) {
    validateExpectedResults(defaultPlan.plan, input, expected);
  }

  std::vector<PlanWithSplits> altPlans;
  altPlans.push_back(std::move(defaultPlan));

  const auto tableScanDir = exec::test::TempDirectoryPath::create();
  addPlansWithTableScan(tableScanDir->getPath(), keyNames, input, altPlans);

  for (auto i = 0; i < altPlans.size(); ++i) {
    testPlan(
        altPlans[i], i, expected, "core::QueryConfig::kRowNumberSpillEnabled");
  }
}

} // namespace

void rowNumberFuzzer(
    size_t seed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner) {
  RowNumberFuzzer(seed, std::move(referenceQueryRunner)).run();
}
} // namespace facebook::velox::exec
