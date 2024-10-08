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
      const std::unordered_map<std::string, DataSpec>& functionDataSpec,
      VectorFuzzer::Options::TimestampPrecision timestampPrecision,
      const std::unordered_map<std::string, std::string>& queryConfigs,
      const std::unordered_map<std::string, std::string>& hiveConfigs,
      bool orderableGroupKeys,
      std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
      : AggregationFuzzerBase{seed, customVerificationFunctions, customInputGenerators, timestampPrecision, queryConfigs, hiveConfigs, orderableGroupKeys, std::move(referenceQueryRunner)},
        orderDependentFunctions_{orderDependentFunctions},
        functionDataSpec_{functionDataSpec} {
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
  struct FrameMetadata {
    core::WindowNode::WindowType windowType =
        core::WindowNode::WindowType::kRange;
    core::WindowNode::BoundType startBoundType =
        core::WindowNode::BoundType::kUnboundedPreceding;
    core::WindowNode::BoundType endBoundType =
        core::WindowNode::BoundType::kCurrentRow;
    std::string startBoundString = "";
    std::string endBoundString = "";
  };

  void addWindowFunctionSignatures(const WindowFunctionMap& signatureMap);

  // Generates the frame bound for k-ROWS frames, decides randomly if the frame
  // bound is a constant or a column. Returns the constant integer value when
  // K is a constant, otherwise returns the column name used as frame bound.
  std::string generateKRowsFrameBound(
      std::vector<std::string>& argNames,
      std::vector<TypePtr>& argTypes,
      const std::string& columnName);

  // Randomly generates the frame metadata, which includes the window type, the
  // frame start and end bound type, and the constant/column offset for k-ROWS
  // frames. For k-RANGE frames, the constant/column offset generation is
  // deferred till the ORDER-BY key is generated. The FrameMetadata returned by
  // this function is used in sorting key(s) generation, to ensure a single
  // ORDER-BY key exists for k-RANGE frames.
  FrameMetadata generateFrameClause(
      std::vector<std::string>& argNames,
      std::vector<TypePtr>& argTypes,
      const std::vector<std::string>& kBoundColumnNames);

  // Adds offset column(s) to input data for k-RANGE frames using the helper
  // function addKRangeOffsetColumnToInputImpl.
  template <TypeKind TKind>
  void addKRangeOffsetColumnToInput(
      std::vector<RowVectorPtr>& input,
      FrameMetadata& frameMetadata,
      SortingKeyAndOrder& orderByKey,
      const std::vector<std::string>& columnNames,
      const std::vector<std::string>& offsetColumnNames);

  // Adds offset column to input data for k-range frames. Randomly decides if
  // the frame bound will be a constant or a column.
  // 1. If the frame bound is a constant, the constant K value is randomly
  // generated to be of the same type as the ORDER-BY column. The offset column
  // value for a given row is then generated by helper function genOffsetAtIdx.
  // The constant K value is returned as a string.
  // 2. If the frame bound is a column, VectorFuzzer is used to generate a
  // column of the same type as the ORDER-BY key. Returns offset column name.
  template <typename T>
  std::string addKRangeOffsetColumnToInputImpl(
      std::vector<RowVectorPtr>& input,
      const core::WindowNode::BoundType& frameBoundType,
      const SortingKeyAndOrder& orderByKey,
      const std::string& columnName,
      const std::string& offsetColumnName);

  // Utility function to build column for kRange frames. When isOffsetColumn is
  // true, the offset column is generated. When isColumnBound is true, the
  // column used as frame bound is generated. The offset column (and frame bound
  // column, if used) will have nulls for rows where order by column is null.
  template <typename T>
  VectorPtr buildKRangeColumn(
      const VectorPtr& frameBound,
      const VectorPtr& orderByCol,
      const core::WindowNode::BoundType& frameBoundType,
      const core::SortOrder& sortOrder,
      bool isColumnBound,
      bool isOffsetColumn);

  // For frames with k RANGE PRECEDING/FOLLOWING, Velox requires the application
  // to add columns with the range frame boundary value computed according to
  // the frame type. frame_boundary_value = NULL iff current_order_by = NULL.
  // If the frame is k PRECEDING :
  // frame_boundary_value = current_order_by - k (for ascending ORDER BY)
  // frame_boundary_value = current_order_by + k (for descending ORDER BY)
  // If the frame is k FOLLOWING :
  // frame_boundary_value = current_order_by + k (for ascending ORDER BY)
  // frame_boundary_value = current_order_by - k (for descending ORDER BY)
  template <typename T>
  const T genOffsetAtIdx(
      const T& orderByValue,
      const T& frameBound,
      const core::WindowNode::BoundType& frameBoundType,
      const core::SortOrder& sortOrder);

  // Builds the frame clause string from frameMetadata. In case of k-RANGE frame
  // bounds, offset column names from kRangeOffsetColumnNames are used to build
  // the frame clause. If kRangeOffsetColumnNames is not specified, the frame
  // bound is obtained from frameMetadata.
  std::string frameClauseString(
      const FrameMetadata& frameMetadata,
      const std::vector<std::string>& kRangeOffsetColumns = {});

  std::string generateOrderByClause(
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders);

  std::string getFrame(
      const std::vector<std::string>& partitionKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const std::string& frameClause);

  std::vector<SortingKeyAndOrder> generateSortingKeysAndOrders(
      const std::string& prefix,
      std::vector<std::string>& names,
      std::vector<TypePtr>& types,
      const bool isKRangeFrame = false);

  // Return 'true' if query plans failed.
  bool verifyWindow(
      const std::vector<std::string>& partitionKeys,
      const std::vector<SortingKeyAndOrder>& sortingKeysAndOrders,
      const std::string& frameClause,
      const std::string& functionCall,
      const std::vector<RowVectorPtr>& input,
      bool customVerification,
      const std::shared_ptr<ResultVerifier>& customVerifier,
      bool enableWindowVerification,
      const std::string& prestoFrameClause);

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
  const std::unordered_map<std::string, DataSpec> functionDataSpec_;

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
    const std::unordered_map<std::string, DataSpec>& functionDataSpec,
    VectorFuzzer::Options::TimestampPrecision timestampPrecision,
    const std::unordered_map<std::string, std::string>& queryConfigs,
    const std::unordered_map<std::string, std::string>& hiveConfigs,
    bool orderableGroupKeys,
    const std::optional<std::string>& planPath,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner);

} // namespace facebook::velox::exec::test
