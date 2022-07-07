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

#include "velox/exec/HashBuild.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

// Probes a hash table made by HashBuild.
class HashProbe : public Operator {
 public:
  HashProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::HashJoinNode>& hashJoinNode);

  bool needsInput() const override {
    if (finished_ || noMoreInput_ || input_) {
      return false;
    }
    if (table_) {
      return true;
    }
    auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
        this, keyChannels_);
    return channels.empty();
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void clearDynamicFilters() override;

 private:
  // Sets up 'filter_' and related members.
  void initializeFilter(
      const std::shared_ptr<const core::ITypedExpr>& filter,
      const RowTypePtr& probeType,
      const RowTypePtr& tableType);

  // Check if output_ can be re-used and if not make a new one.
  void prepareOutput(vector_size_t size);

  // Populate output columns.
  void fillOutput(vector_size_t size);

  // Clears the columns of 'output_' that are projected from
  // 'input_'. This should be done when preparing to produce a next
  // batch of output to drop any lingering references to row
  // number mappings or input vectors. In this way input vectors do
  // not have to be copied and will be singly referenced by their
  // producer.
  void clearIdentityProjectedOutput();

  // Populate output columns with build-side rows that didn't match join
  // condition.
  RowVectorPtr getNonMatchingOutputForRightJoin();

  // Populate filter input columns.
  void fillFilterInput(vector_size_t size);

  // Applies 'filter_' to 'outputRows_' and updates 'outputRows_' and
  // 'rowNumberMapping_'. Returns the number of passing rows.
  vector_size_t evalFilter(vector_size_t numRows);

  void ensureLoadedIfNotAtEnd(column_index_t channel);

  // TODO: Define batch size as bytes based on RowContainer row sizes.
  const uint32_t outputBatchSize_;

  const core::JoinType joinType_;

  std::unique_ptr<HashLookup> lookup_;

  // Channel of probe keys in 'input_'.
  std::vector<column_index_t> keyChannels_;

  // True if the join can become a no-op starting with the next batch of input.
  bool canReplaceWithDynamicFilter_{false};

  // True if the join became a no-op after pushing down the filter.
  bool replacedWithDynamicFilter_{false};

  std::vector<std::unique_ptr<VectorHasher>> hashers_;

  // Table shared between other HashProbes in other Drivers of the
  // same pipeline.
  std::shared_ptr<BaseHashTable> table_;

  VectorHasher::ScratchMemory scratchMemory_;

  // Rows to apply 'filter_' to.
  SelectivityVector filterRows_;

  // Join filter.
  std::unique_ptr<ExprSet> filter_;
  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;

  // Type of the RowVector for filter inputs.
  RowTypePtr filterInputType_;

  // Maps input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterProbeInputs_;

  // Maps from column index in hash table to channel in 'filterInputType_'.
  std::vector<IdentityProjection> filterBuildInputs_;

  // Temporary projection from probe and build for evaluating
  // 'filter_'. This can always be reused since this does not escape
  // this operator.
  RowVectorPtr filterInput_;

  // Row number in 'input_' for each row of output.
  BufferPtr rowNumberMapping_;

  // maps from column index in 'table_' to channel in 'output_'.
  std::vector<IdentityProjection> tableResultProjections_;

  // Rows of table found by join probe, later filtered by 'filter_'.
  std::vector<char*> outputRows_;

  // Tracks probe side rows which had one or more matches on the build side, but
  // didn't pass the filter.
  class NoMatchDetector {
   public:
    // Called for each row that the filter was evaluated on. Expects that probe
    // side rows with multiple matches on the build side are next to each other.
    template <typename TOnMiss>
    void advance(vector_size_t row, bool passed, TOnMiss onMiss) {
      if (currentRow != row) {
        if (currentRow != -1 && !currentRowPassed) {
          onMiss(currentRow);
        }
        currentRow = row;
        currentRowPassed = false;
      }
      if (passed) {
        currentRowPassed = true;
      }
    }

    // Called when all rows from the current input batch were processed.
    template <typename TOnMiss>
    void finish(TOnMiss onMiss) {
      if (!currentRowPassed) {
        onMiss(currentRow);
      }

      currentRow = -1;
      currentRowPassed = false;
    }

   private:
    // Row number being processed.
    vector_size_t currentRow{-1};

    // True if currentRow has a match.
    bool currentRowPassed{false};
  };

  // For left semi join with extra filter, de-duplicates probe side rows with
  // multiple matches.
  class LeftSemiJoinTracker {
   public:
    // Called for each row that the filter passes. Expects that probe
    // side rows with multiple matches are next to each other. Calls onLastMatch
    // just once for each probe side row with at least one match.
    template <typename TOnLastMatch>
    void advance(vector_size_t row, TOnLastMatch onLastMatch) {
      if (currentRow != row) {
        if (currentRow != -1) {
          onLastMatch(currentRow);
        }
        currentRow = row;
      }
    }

    // Called when all rows from the current input batch were processed. Calls
    // onLastMatch for the last probe row with at least one match.
    template <typename TOnLastMatch>
    void finish(TOnLastMatch onLastMatch) {
      if (currentRow != -1) {
        onLastMatch(currentRow);
      }

      currentRow = -1;
    }

   private:
    // The last row number passed to advance for the current input batch.
    vector_size_t currentRow{-1};
  };

  /// True if this is the last HashProbe operator in the pipeline. It is
  /// responsible for producing non-matching build-side rows for the right join.
  bool lastRightJoinProbe_{false};

  BaseHashTable::NotProbedRowsIterator rightJoinIterator_;

  /// For left and anti join with filter, tracks the probe side rows which had
  /// matches on the build side but didn't pass the filter.
  NoMatchDetector noMatchDetector_;

  /// For left semi join with filter, de-duplicates probe side rows with
  /// multiple matches.
  LeftSemiJoinTracker leftSemiJoinTracker_;

  // Keeps track of returned results between successive batches of
  // output for a batch of input.
  BaseHashTable::JoinResultIterator results_;

  RowVectorPtr output_;

  // Input rows with no nulls in the join keys.
  SelectivityVector nonNullRows_;

  // Input rows with a hash match. This is a subset of rows with no nulls in the
  // join keys and a superset of rows that have a match on the build side.
  SelectivityVector activeRows_;

  bool finished_{false};

  // True if passingInputRows is up to date.
  bool passingInputRowsInitialized_;

  // Set of input rows for which there is at least one join hit. All
  // set if right side optional. Used when loading lazy vectors for
  // cases where there is more than one batch of output or join filter
  // input.
  SelectivityVector passingInputRows_;
};

} // namespace facebook::velox::exec
