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

#include "velox/exec/fuzzer/TopNRowNumberFuzzer.h"

#include <utility>

#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/RowNumberFuzzerBase.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/vector/tests/utils/VectorMaker.h"

namespace facebook::velox::exec {
namespace {

class TopNRowNumberFuzzer : public RowNumberFuzzerBase {
 public:
  explicit TopNRowNumberFuzzer(
      size_t initialSeed,
      std::unique_ptr<test::ReferenceQueryRunner>);

 private:
  // Runs one test iteration from query plans generations, executions and result
  // verifications.
  void runSingleIteration() override;

  std::pair<std::vector<std::string>, std::vector<TypePtr>> generateKeys(
      const std::string& prefix);

  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys);

  // Makes the query plan with default settings in TopNRowNumberFuzzer.
  std::pair<PlanWithSplits, int32_t> makeDefaultPlan(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortKeys,
      const std::vector<std::string>& allKeys,
      const std::vector<RowVectorPtr>& input);

  PlanWithSplits makePlanWithTableScan(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortKeys,
      const std::vector<std::string>& allKeys,
      int limit,
      const std::vector<RowVectorPtr>& input,
      const std::string& tableDir);
};

TopNRowNumberFuzzer::TopNRowNumberFuzzer(
    size_t initialSeed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner)
    : RowNumberFuzzerBase(initialSeed, std::move(referenceQueryRunner)) {}

std::pair<std::vector<std::string>, std::vector<TypePtr>>
TopNRowNumberFuzzer::generateKeys(const std::string& prefix) {
  static const std::vector<TypePtr> kKeyTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      DATE(),
      REAL(),
      DOUBLE(),
  };

  auto numKeys = randInt(1, 5);
  std::vector<std::string> keys;
  std::vector<TypePtr> types;
  for (auto i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("{}{}", prefix, i));
    types.push_back(vectorFuzzer_.randOrderableType(kKeyTypes, 1));
  }

  return std::make_pair(keys, types);
}

std::vector<RowVectorPtr> TopNRowNumberFuzzer::generateInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes,
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys) {
  std::vector<RowVectorPtr> input;
  vector_size_t size = vectorFuzzer_.getOptions().vectorSize;
  velox::test::VectorMaker vectorMaker{pool_.get()};

  std::unordered_set<std::string> partitionKeySet{
      partitionKeys.begin(), partitionKeys.end()};
  std::unordered_set<std::string> sortingKeySet{
      sortingKeys.begin(), sortingKeys.end()};

  int64_t rowNumber = 0;
  for (auto j = 0; j < FLAGS_num_batches; ++j) {
    std::vector<VectorPtr> children;

    // Rank functions have semantics influenced by "peer" rows. Peer rows are
    // rows in the same partition having the same order by
    // key. In rank and dense_rank functions, peer rows have the same function
    // result value. This code influences the fuzzer to generate such data.
    //
    // To build such rows the code separates the notions of "peer" groups and
    // "partition" groups during data generation. A number of peers are chosen
    // between (1, size) of the input. Rows with the same peer number have the
    // same order by keys. This means that there are sets of rows in the input
    // data which will have the same order by key.
    //
    // Each peer is then mapped to a partition group. Rows in the same partition
    // group have the same partition keys. So a partition can contain a group of
    // rows with the same order by key and there can be multiple such groups
    // (each with different order by keys) in one partition.
    //
    // This style of data generation is preferable for ranking functions.
    auto numPeerGroups = size ? randInt(1, size) : 1;
    auto sortingIndices = vectorFuzzer_.fuzzIndices(size, numPeerGroups);
    auto rawSortingIndices = sortingIndices->as<vector_size_t>();
    auto sortingNulls = vectorFuzzer_.fuzzNulls(size);

    auto numPartitions = randInt(1, numPeerGroups);
    auto peerGroupToPartitionIndices =
        vectorFuzzer_.fuzzIndices(numPeerGroups, numPartitions);
    auto rawPeerGroupToPartitionIndices =
        peerGroupToPartitionIndices->as<vector_size_t>();
    auto partitionIndices =
        AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
    auto rawPartitionIndices = partitionIndices->asMutable<vector_size_t>();
    auto partitionNulls = vectorFuzzer_.fuzzNulls(size);
    for (auto i = 0; i < size; i++) {
      auto peerGroup = rawSortingIndices[i];
      rawPartitionIndices[i] = rawPeerGroupToPartitionIndices[peerGroup];
    }

    for (auto i = 0; i < keyTypes.size() - 1; ++i) {
      if (partitionKeySet.find(keyNames[i]) != partitionKeySet.end()) {
        // The partition keys are built with a dictionary over a smaller set of
        // values. This is done to introduce some repetition of key values for
        // windowing.
        auto baseVector = vectorFuzzer_.fuzz(keyTypes[i], numPartitions);
        children.push_back(BaseVector::wrapInDictionary(
            partitionNulls, partitionIndices, size, baseVector));
      } else if (sortingKeySet.find(keyNames[i]) != sortingKeySet.end()) {
        auto baseVector = vectorFuzzer_.fuzz(keyTypes[i], numPeerGroups);
        children.push_back(BaseVector::wrapInDictionary(
            sortingNulls, sortingIndices, size, baseVector));
      } else {
        children.push_back(vectorFuzzer_.fuzz(keyTypes[i], size));
      }
    }
    children.push_back(vectorMaker.flatVector<int32_t>(
        size, [&](auto /*row*/) { return rowNumber++; }));
    input.push_back(vectorMaker.rowVector(keyNames, children));
  }

  return input;
}

std::pair<RowNumberFuzzerBase::PlanWithSplits, int32_t>
TopNRowNumberFuzzer::makeDefaultPlan(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortKeys,
    const std::vector<std::string>& allKeys,
    const std::vector<RowVectorPtr>& input) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<std::string> projectFields = allKeys;
  projectFields.emplace_back("row_number");

  int32_t limit = randInt(1, FLAGS_batch_size);
  auto plan = test::PlanBuilder()
                  .values(input)
                  .topNRowNumber(partitionKeys, sortKeys, limit, true)
                  .project(projectFields)
                  .planNode();
  return std::make_pair(PlanWithSplits{std::move(plan)}, limit);
}

RowNumberFuzzerBase::PlanWithSplits TopNRowNumberFuzzer::makePlanWithTableScan(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortKeys,
    const std::vector<std::string>& allKeys,
    int limit,
    const std::vector<RowVectorPtr>& input,
    const std::string& tableDir) {
  VELOX_CHECK(!tableDir.empty());

  std::vector<std::string> projectFields = allKeys;
  projectFields.emplace_back("row_number");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  auto plan = test::PlanBuilder(planNodeIdGenerator)
                  .tableScan(asRowType(input[0]->type()))
                  .topNRowNumber(partitionKeys, sortKeys, limit, true)
                  .project(projectFields)
                  .planNode();

  const std::vector<Split> splits = test::makeSplits(
      input, fmt::format("{}/topn_row_number", tableDir), writerPool_);
  return PlanWithSplits{plan, splits};
}

void TopNRowNumberFuzzer::runSingleIteration() {
  const auto [partitionKeys, partitionTypes] = generateKeys("p");
  const auto [sortKeys, sortTypes] = generateKeys("s");

  std::vector<std::string> allSortKeys;
  std::vector<TypePtr> allSortTypes;
  allSortKeys.insert(allSortKeys.begin(), sortKeys.begin(), sortKeys.end());
  allSortTypes.insert(allSortTypes.begin(), sortTypes.begin(), sortTypes.end());
  allSortKeys.push_back("row_id");
  allSortTypes.push_back(INTEGER());

  std::vector<std::string> allKeys;
  std::vector<TypePtr> allTypes;
  allKeys.insert(allKeys.begin(), partitionKeys.begin(), partitionKeys.end());
  allTypes.insert(
      allTypes.begin(), partitionTypes.begin(), partitionTypes.end());
  allKeys.insert(allKeys.end(), allSortKeys.begin(), allSortKeys.end());
  allTypes.insert(allTypes.end(), allSortTypes.begin(), allSortTypes.end());

  const auto input = generateInput(allKeys, allTypes, partitionKeys, sortKeys);
  test::logVectors(input);

  auto [defaultPlan, limit] =
      makeDefaultPlan(partitionKeys, allSortKeys, allKeys, input);

  const auto expected =
      execute(defaultPlan, pool_, /*injectSpill=*/false, false);
  if (expected != nullptr) {
    validateExpectedResults(defaultPlan.plan, input, expected);
  }

  std::vector<PlanWithSplits> altPlans;
  altPlans.push_back(std::move(defaultPlan));

  const auto tableScanDir = exec::test::TempDirectoryPath::create();
  if (isTableScanSupported(input[0]->type())) {
    altPlans.push_back(makePlanWithTableScan(
        partitionKeys,
        allSortKeys,
        allKeys,
        limit,
        input,
        tableScanDir->getPath()));
  }

  for (auto i = 0; i < altPlans.size(); ++i) {
    testPlan(
        altPlans[i],
        i,
        expected,
        "core::QueryConfig::kTopNRowNumberSpillEnabled");
  }
}

} // namespace

void topNRowNumberFuzzer(
    size_t seed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner) {
  TopNRowNumberFuzzer(seed, std::move(referenceQueryRunner)).run();
}
} // namespace facebook::velox::exec
