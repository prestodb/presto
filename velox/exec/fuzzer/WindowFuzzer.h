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

#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/exec/fuzzer/AggregationFuzzerBase.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

DECLARE_bool(enable_window_reference_verification);

namespace facebook::velox::exec::test {

class WindowFuzzer : public AggregationFuzzerBase {
 public:
  WindowFuzzer(
      AggregateFunctionSignatureMap aggregationSignatureMap,
      WindowFunctionMap windowSignatureMap,
      size_t seed,
      const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
          customVerificationFunctions,
      const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
          customInputGenerators,
      const std::unordered_set<std::string>& orderDependentFunctions,
      VectorFuzzer::Options::TimestampPrecision timestampPrecision,
      const std::unordered_map<std::string, std::string>& queryConfigs,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
      : AggregationFuzzerBase{seed, customVerificationFunctions, customInputGenerators, timestampPrecision, queryConfigs, std::move(referenceQueryRunner)},
        orderDependentFunctions_{orderDependentFunctions} {
    VELOX_CHECK(
        !aggregationSignatureMap.empty() || !windowSignatureMap.empty(),
        "No function signatures available.");

    if (persistAndRunOnce_ && reproPersistPath_.empty()) {
      std::cerr
          << "--repro_persist_path must be specified if --persist_and_run_once is specified"
          << std::endl;
      exit(1);
    }

    // Presto doesn't allow complex-typed vectors with NULL elements to be
    // partition keys, so we disable it for all inputs.
    // TODO: allow complex-typed vectors with NULL elements as non-key columns.
    if (dynamic_cast<PrestoQueryRunner*>(referenceQueryRunner_.get())) {
      auto options = vectorFuzzer_.getOptions();
      options.containerHasNulls = false;
      vectorFuzzer_.setOptions(options);
    }

    addAggregationSignatures(aggregationSignatureMap);
    addWindowFunctionSignatures(windowSignatureMap);
    printStats(functionsStats);

    sortCallableSignatures(signatures_);
    sortSignatureTemplates(signatureTemplates_);

    signatureStats_.resize(signatures_.size() + signatureTemplates_.size());
  }

  void go();
  void go(const std::string& planPath);

 private:
  void addWindowFunctionSignatures(const WindowFunctionMap& signatureMap);

  // Return a randomly generated frame clause string together with a boolean
  // flag indicating whether it is a ROWS frame.
  std::tuple<std::string, bool> generateFrameClause();

  std::string generateOrderByClause(
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders);

  std::string getFrame(
      const std::vector<std::string>& partitionKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const std::string& frameClause);

  std::vector<SortingKeyAndOrder> generateSortingKeysAndOrders(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types);

  // Return 'true' if query plans failed.
  bool verifyWindow(
      const std::vector<std::string>& partitionKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const std::string& frameClause,
      const std::string& functionCall,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      const std::shared_ptr<ResultVerifier>& customVerifier,
      bool enableWindowVerification);

  void testAlternativePlans(
      const std::vector<std::string>& partitionKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const std::string& frame,
      const std::string& functionCall,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      const std::shared_ptr<ResultVerifier>& customVerifier,
      const velox::fuzzer::ResultOrError& expected);

  const std::unordered_set<std::string> orderDependentFunctions_;

  struct Stats : public AggregationFuzzerBase::Stats {
    std::unordered_set<std::string> verifiedFunctionNames;

    void print(size_t numIterations) const;
  } stats_;
};

/// Runs the window fuzzer.
/// @param aggregationSignatureMap Map of all aggregate function signatures.
/// @param windowSignatureMap Map of all window function signatures.
/// @param seed Random seed - Pass the same seed for reproducibility.
/// @param orderDependentFunctions Map of functions that depend on order of
/// input.
/// @param planPath Path to persisted plan information. If this is
/// supplied, fuzzer will only verify the plans.
/// @param referenceQueryRunner Reference query runner for results
/// verification.
void windowFuzzer(
    AggregateFunctionSignatureMap aggregationSignatureMap,
    WindowFunctionMap windowSignatureMap,
    size_t seed,
    const std::unordered_map<std::string, std::shared_ptr<ResultVerifier>>&
        customVerificationFunctions,
    const std::unordered_map<std::string, std::shared_ptr<InputGenerator>>&
        customInputGenerators,
    const std::unordered_set<std::string>& orderDependentFunctions,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

} // namespace facebook::velox::exec::test
