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
#include "velox/exec/HashPartitionFunction.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"
#include "velox/exec/ProbeOperatorState.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

// Probes a hash table made by HashBuild.
class HashProbe : public Operator {
 public:
  HashProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::HashJoinNode>& hashJoinNode);

  void initialize() override;

  bool needsInput() const override {
    if (state_ == ProbeOperatorState::kFinish || noMoreInput_ ||
        noMoreSpillInput_ || input_ != nullptr) {
      return false;
    }
    if (table_) {
      return true;
    }
    // NOTE: if we can't apply dynamic filtering, then we can start early to
    // read input even before the hash table has been built.
    auto channels = operatorCtx_->driverCtx()->driver->canPushdownFilters(
        this, keyChannels_);
    return channels.empty();
  }

  void addInput(RowVectorPtr input) override;

  void noMoreInput() override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  /// NOTE: we can't reclaim memory from a hash probe operator. The disk
  /// spilling in hash probe is used to coordinate with the disk spilling
  /// triggered by the hash build operator.
  bool canReclaim() const override {
    return false;
  }

  void close() override;

  void clearDynamicFilters() override;

 private:
  void setState(ProbeOperatorState state);
  void checkStateTransition(ProbeOperatorState state);

  void setRunning();
  void checkRunning() const;
  bool isRunning() const;

  // Invoked to wait for the hash table to be built by the hash build operators
  // asynchronously.
  void asyncWaitForHashTable();

  // Invoked to set up spilling related input processing. The function sets up a
  // reader to read probe inputs from spilled data on disk if
  // 'restoredSpillPartitionId' is not null. If 'spillPartitionIds' is not
  // empty, then spilling has been triggered at the build side and the function
  // will set up a spiller and the associated data structures to spill probe
  // inputs.
  void maybeSetupSpillInput(
      const std::optional<SpillPartitionId>& restoredSpillPartitionId,
      const SpillPartitionIdSet& spillPartitionIds);

  // Sets up 'filter_' and related members.p
  void initializeFilter(
      const core::TypedExprPtr& filter,
      const RowTypePtr& probeType,
      const RowTypePtr& tableType);

  // Check if output_ can be re-used and if not make a new one.
  void prepareOutput(vector_size_t size);

  // Populate output columns.
  void fillOutput(vector_size_t size);

  // Populate 'match' output column for the left semi join project,
  void fillLeftSemiProjectMatchColumn(vector_size_t size);

  // Clears the columns of 'output_' that are projected from
  // 'input_'. This should be done when preparing to produce a next
  // batch of output to drop any lingering references to row
  // number mappings or input vectors. In this way input vectors do
  // not have to be copied and will be singly referenced by their
  // producer.
  void clearIdentityProjectedOutput();

  // Populate output columns with matching build-side rows
  // for the right semi join and non-matching build-side rows
  // for right join and full join.
  RowVectorPtr getBuildSideOutput();

  // Applies 'filter_' to 'outputTableRows_' and updates 'outputRowMapping_'.
  // Returns the number of passing rows.
  vector_size_t evalFilter(vector_size_t numRows);

  inline bool filterPassed(vector_size_t row) {
    return filterInputRows_.isValid(row) &&
        !decodedFilterResult_.isNullAt(row) &&
        decodedFilterResult_.valueAt<bool>(row);
  }

  // Populate filter input columns.
  void fillFilterInput(vector_size_t size);

  // Prepare filter row selectivity for null-aware join. 'numRows'
  // specifies the number of rows in 'filterInputRows_' to process. If
  // 'filterPropagateNulls' is true, the probe input row which has null in any
  // probe filter column can't pass the filter.
  void prepareFilterRowsForNullAwareJoin(
      vector_size_t numRows,
      bool filterPropagateNulls);

  // Evaluate the filter for null-aware anti or left semi project join.
  SelectivityVector evalFilterForNullAwareJoin(
      vector_size_t numRows,
      bool filterPropagateNulls);

  // Combine the selected probe-side rows with all or null-join-key (depending
  // on the iterator) build side rows and evaluate the filter.  Mark probe rows
  // that pass the filter in 'filterPassedRows'. Used in null-aware join
  // processing.
  void applyFilterOnTableRowsForNullAwareJoin(
      const SelectivityVector& rows,
      SelectivityVector& filterPassedRows,
      std::function<int32_t(char**, int32_t)> iterator);

  void ensureLoadedIfNotAtEnd(column_index_t channel);

  // Indicates if the operator has more probe inputs from either the upstream
  // operator or the spill input reader.
  bool hasMoreInput() const;

  // Indicates if the join type such as right, right semi and full joins,
  // require to produce the output results from the join results after all the
  // probe operators have finished processing the probe inputs.
  bool needLastProbe() const;

  // Indicates if the join type can skip processing probe inputs with empty
  // build table such as inner, right and semi joins.
  //
  // NOTE: if spilling is triggered at the build side, then we still need to
  // process probe inputs to spill the probe rows if the corresponding
  // partitions have been spilled at the build side.
  bool skipProbeOnEmptyBuild() const;

  bool spillEnabled() const;

  // Indicates if the probe input is read from spilled data or not.
  bool isSpillInput() const;

  // Indicates if there is more spill data to restore after finishes processing
  // the current probe inputs.
  bool hasMoreSpillData() const;

  // Indicates if the operator needs to spill probe inputs. It is true if parts
  // of the build-side rows have been spilled. Hence, the probe operator needs
  // to spill the corresponding probe-side rows as well.
  bool needSpillInput() const;

  // Invoked after finishes processing the probe inputs and there is spill data
  // remaining to restore. The function will reset the internal states which
  // are relevant to the last finished probe run. The last finished probe
  // operator will also notify the hash build operators to build the next hash
  // table from spilled data.
  void prepareForSpillRestore();

  // Invoked to read next batch of spilled probe inputs from disk to process.
  void addSpillInput();

  // Invoked to spill rows in 'input' to disk directly if the corresponding
  // partitions have been spilled at the build side.
  //
  // NOTE: this method keeps 'input' as is if no row needs spilling; resets it
  // to null if all rows have been spilled; wraps in a dictionary using rows
  // number that do not need spilling otherwise.
  void spillInput(RowVectorPtr& input);

  // Invoked to prepare indices buffers for input spill processing.
  void prepareInputIndicesBuffers(
      vector_size_t numInput,
      const folly::F14FastSet<uint32_t>& spillPartitions);

  /// Decode join key inputs and populate 'nonNullInputRows_'.
  void decodeAndDetectNonNullKeys();

  // Invoked when there is no more input from either upstream task or spill
  // input. If there is remaining spilled data, then the last finished probe
  // operator is responsible for notifying the hash build operators to build the
  // next hash table from the spilled data.
  void noMoreInputInternal();

  void recordSpillStats();

  // Returns the index of the 'match' column in the output for semi project
  // joins.
  VectorPtr& matchColumn() const {
    VELOX_DCHECK(
        isRightSemiProjectJoin(joinType_) || isLeftSemiProjectJoin(joinType_));
    return output_->children().back();
  }

  // Returns true if build side has no data.
  // NOTE: if build side has triggered spilling, then the first hash table
  // might be empty, but we still have spilled partition data remaining to
  // restore. Also note that the spilled partition at build side must not be
  // empty.
  bool emptyBuildSide() const {
    return table_->numDistinct() == 0 && spillPartitionSet_.empty() &&
        spillInputPartitionIds_.empty();
  }

  // TODO: Define batch size as bytes based on RowContainer row sizes.
  const uint32_t outputBatchSize_;

  const std::shared_ptr<const core::HashJoinNode> joinNode_;

  const core::JoinType joinType_;

  const bool nullAware_;

  const RowTypePtr probeType_;

  std::shared_ptr<HashJoinBridge> joinBridge_;

  ProbeOperatorState state_{ProbeOperatorState::kWaitForBuild};

  // Used for synchronization with the hash probe operators of the same pipeline
  // to handle the last probe processing for certain types of join and notify
  // the hash build operators to build the next hash table from spilled data if
  // disk spilling has been triggered.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  // Used with 'future_' for the same purpose. 'promises_' will be fulfilled
  // after finishes the last probe processing and hash build operator
  // notification.
  //
  // NOTE: for a given probe operator, it has either 'future_' or 'promises_'
  // set, but not both. All but last probe operators get futures that are
  // fulfilled when the last probe operator finishes. The last probe operator
  // doesn't get a future, but collects the promises from the Task and uses
  // those to wake up peers.
  std::vector<ContinuePromise> promises_;

  // True if this is the last hash probe operator in the pipeline that finishes
  // probe input processing. It is responsible for producing matching build-side
  // rows for the right semi join and non-matching build-side rows for right
  // join and full join. If the disk spilling is enabled, the last operator is
  // also responsible to notify the hash build operators to build the next hash
  // table from the previously spilled data.
  bool lastProber_{false};

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

  // Indicates whether there was no input. Used for right semi join project.
  bool noInput_{true};

  // Indicates whether to skip probe input data processing or not. It only
  // applies for a specific set of join types (see skipProbeOnEmptyBuild()), and
  // the build table is empty and the probe input is read from non-spilled
  // source. This ensures the hash probe operator keeps running until all the
  // probe input from the sources have been processed. It prevents the exchange
  // hanging problem at the producer side caused by the early query finish.
  bool skipInput_{false};

  // Indicates whether there are rows with null join keys on the build
  // side. Used by anti and left semi project join.
  bool buildSideHasNullKeys_{false};

  // Indicates whether there are rows with null join keys on the probe
  // side. Used by right semi project join.
  bool probeSideHasNullKeys_{false};

  // Rows in 'filterInput_' to apply 'filter_' to.
  SelectivityVector filterInputRows_;

  // Join filter.
  std::unique_ptr<ExprSet> filter_;

  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;

  // Type of the RowVector for filter inputs.
  RowTypePtr filterInputType_;

  // Maps input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterInputProjections_;

  // Maps from column index in hash table to channel in 'filterInputType_'.
  std::vector<IdentityProjection> filterTableProjections_;

  // Temporary projection from probe and build for evaluating
  // 'filter_'. This can always be reused since this does not escape
  // this operator.
  RowVectorPtr filterInput_;

  // The following six fields are used in null-aware anti join filter
  // processing.

  // Used to decode a probe side filter input column to check nulls.
  DecodedVector filterInputColumnDecodedVector_;

  // Rows that have null value in any probe side filter columns to skip the
  // null-propagating filter evaluation. The corresponding probe input rows can
  // be added to the output result directly as they won't pass the filter.
  SelectivityVector nullFilterInputRows_;

  // Used to store the null-key joined rows for filter processing.
  RowVectorPtr filterTableInput_;
  SelectivityVector filterTableInputRows_;

  // Used to store the filter result for null-key joined rows.
  std::vector<VectorPtr> filterTableResult_;
  DecodedVector decodedFilterTableResult_;

  // Row number in 'input_' for each output row.
  BufferPtr outputRowMapping_;

  // maps from column index in 'table_' to channel in 'output_'.
  std::vector<IdentityProjection> tableOutputProjections_;

  // Rows of table found by join probe, later filtered by 'filter_'.
  std::vector<char*> outputTableRows_;

  // Indicates probe-side rows which should produce a NULL in left semi project
  // with filter.
  SelectivityVector leftSemiProjectIsNull_;

  // Tracks probe side rows which had one or more matches on the build side, but
  // didn't pass the filter.
  class NoMatchDetector {
   public:
    // Called for each row that the filter was evaluated on. Expects that probe
    // side rows with multiple matches on the build side are next to each other.
    template <typename TOnMiss>
    void advance(vector_size_t row, bool passed, TOnMiss onMiss) {
      if (currentRow != row) {
        // Check if 'currentRow' is the same input row as the last missed row
        // from a previous output batch.  If so finishIteration will call
        // onMiss.
        if (currentRow != -1 && !currentRowPassed &&
            (!lastMissedRow || currentRow != lastMissedRow)) {
          onMiss(currentRow);
        }
        currentRow = row;
        currentRowPassed = false;
      }

      if (passed) {
        // lastMissedRow can only be a row that has never passed the filter.  If
        // it passes there's no need to continue carrying it forward.
        if (lastMissedRow && currentRow == lastMissedRow) {
          lastMissedRow.reset();
        }

        currentRowPassed = true;
      }
    }

    // Invoked at the end of one output batch processing. 'end' is set to true
    // at the end of processing an input batch. 'freeOutputRows' is the number
    // of rows that can still be written to the output batch.
    template <typename TOnMiss>
    void
    finishIteration(TOnMiss onMiss, bool endOfData, size_t freeOutputRows) {
      if (endOfData) {
        if (!currentRowPassed && currentRow != -1) {
          // If we're at the end of the input batch and the current row hasn't
          // passed the filter, it never will, process it as a miss.
          // We're guaranteed to have space, at least the last row was never
          // written out since it was a miss.
          VELOX_CHECK_GE(freeOutputRows, 0);
          onMiss(currentRow);
          freeOutputRows--;
        }

        // We no longer need to carry the current row since we already called
        // onMiss on it.
        if (lastMissedRow && currentRow == lastMissedRow) {
          lastMissedRow.reset();
        }

        currentRow = -1;
        currentRowPassed = false;
      }

      // If there's space left in the output batch, write out the last missed
      // row.
      if (lastMissedRow && currentRow != lastMissedRow && freeOutputRows > 0) {
        onMiss(*lastMissedRow);
        lastMissedRow.reset();
      }

      // If the current row hasn't passed the filter, we need to carry it
      // forward in case it never passes the filter.
      if (!currentRowPassed && currentRow != -1) {
        lastMissedRow = currentRow;
      }
    }

    // Returns if we're carrying forward a missed input row. Notably, if this is
    // true, we're not yet done processing the input batch.
    bool hasLastMissedRow() {
      return lastMissedRow.has_value();
    }

   private:
    // Row number being processed.
    vector_size_t currentRow{-1};

    // True if currentRow has a match.
    bool currentRowPassed{false};

    // If set, it points to the last missed (input) row carried over from
    // previous output batch processing. The last missed row is either written
    // as a passed row if the same input row has a hit in the next output batch
    // processed or written to the first output batch which has space at
    // the end if it never has a hit.
    std::optional<vector_size_t> lastMissedRow;
  };

  // For left semi join filter with extra filter, de-duplicates probe side rows
  // with multiple matches.
  class LeftSemiFilterJoinTracker {
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

  // For left semi join project with filter, de-duplicates probe side rows
  // with multiple matches.
  class LeftSemiProjectJoinTracker {
   public:
    // Called for each row and indicates whether the filter passed or not.
    // Expects that probe side rows with multiple matches are next to each
    // other. Calls onLast just once for each probe side row.
    template <typename TOnLast>
    void
    advance(vector_size_t row, std::optional<bool> passed, TOnLast onLast) {
      if (currentRow != row) {
        if (currentRow != -1) {
          onLast(currentRow, currentRowPassed);
        }
        currentRow = row;
        currentRowPassed = std::nullopt;
      }

      if (passed.has_value()) {
        if (currentRowPassed.has_value()) {
          currentRowPassed = currentRowPassed.value() || passed.value();
        } else {
          currentRowPassed = passed;
        }
      }
    }

    // Called when all rows from the current input batch were processed. Calls
    // onLast for the last probe row.
    template <typename TOnLast>
    void finish(TOnLast onLast) {
      if (currentRow != -1) {
        onLast(currentRow, currentRowPassed);
      }

      currentRow = -1;
      currentRowPassed = std::nullopt;
    }

   private:
    // The last row number passed to advance for the current input batch.
    vector_size_t currentRow{-1};

    // True if currentRow has a match.
    std::optional<bool> currentRowPassed;
  };

  BaseHashTable::RowsIterator lastProbeIterator_;

  /// For left and anti join with filter, tracks the probe side rows which had
  /// matches on the build side but didn't pass the filter.
  NoMatchDetector noMatchDetector_;

  /// For left semi join filter with extra filter, de-duplicates probe side rows
  /// with multiple matches.
  LeftSemiFilterJoinTracker leftSemiFilterJoinTracker_;

  /// For left semi join project with filter, de-duplicates probe side rows with
  /// multiple matches.
  LeftSemiProjectJoinTracker leftSemiProjectJoinTracker_;

  // Keeps track of returned results between successive batches of
  // output for a batch of input.
  BaseHashTable::JoinResultIterator results_;

  RowVectorPtr output_;

  // Input rows with no nulls in the join keys.
  SelectivityVector nonNullInputRows_;

  // Input rows with a hash match. This is a subset of rows with no nulls in the
  // join keys and a superset of rows that have a match on the build side.
  SelectivityVector activeRows_;

  // True if passingInputRows is up to date.
  bool passingInputRowsInitialized_;

  // Set of input rows for which there is at least one join hit. All
  // set if right side optional. Used when loading lazy vectors for
  // cases where there is more than one batch of output or join filter
  // input.
  SelectivityVector passingInputRows_;

  // 'spiller_' is only created if some part of build-side rows have been
  // spilled. It is used to spill probe-side rows if the corresponding
  // build-side rows have been spilled.
  std::unique_ptr<Spiller> spiller_;

  // If not empty, the probe inputs with partition id set in
  // 'spillInputPartitionIds_' needs to spill. It is set along with 'spiller_'
  // to the partition ids that have been spilled at build side when built
  // 'table_'.
  SpillPartitionIdSet spillInputPartitionIds_;

  // Used to calculate the spill partition numbers of the probe inputs.
  std::unique_ptr<HashPartitionFunction> spillHashFunction_;

  // Reusable memory for spill hash partition calculation.
  std::vector<uint32_t> spillPartitions_;

  // Reusable memory for probe input spilling processing.
  std::vector<vector_size_t> numSpillInputs_;
  std::vector<BufferPtr> spillInputIndicesBuffers_;
  std::vector<vector_size_t*> rawSpillInputIndicesBuffers_;
  BufferPtr nonSpillInputIndicesBuffer_;
  vector_size_t* rawNonSpillInputIndicesBuffer_;

  // 'spillInputReader_' is only created if 'table_' is built from the
  // previously spilled data. It is used to read the probe inputs from the
  // corresponding spilled data on disk.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillInputReader_;

  // Sets to true after read all the probe inputs from 'spillInputReader_'.
  bool noMoreSpillInput_{false};

  // The spilled probe partitions remaining to restore.
  SpillPartitionSet spillPartitionSet_;
};

inline std::ostream& operator<<(std::ostream& os, ProbeOperatorState state) {
  os << probeOperatorStateName(state);
  return os;
}

} // namespace facebook::velox::exec
