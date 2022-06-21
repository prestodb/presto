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
#include "velox/exec/MergeSource.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
class MergeJoin : public Operator {
 public:
  MergeJoin(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeJoinNode>& joinNode);

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool needsInput() const override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

  void close() override {
    if (rightSource_) {
      rightSource_->close();
    }
    Operator::close();
  }

 private:
  // Sets up 'filter_' and related member variables.
  void initializeFilter(
      const std::shared_ptr<const core::ITypedExpr>& filter,
      const RowTypePtr& leftType,
      const RowTypePtr& rightType);

  RowVectorPtr doGetOutput();

  static int32_t compare(
      const std::vector<column_index_t>& keys,
      const RowVectorPtr& batch,
      vector_size_t index,
      const std::vector<column_index_t>& otherKeys,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex);

  // Compare rows on the left and right at index_ and rightIndex_ respectively.
  int32_t compare() const {
    return compare(
        leftKeys_, input_, index_, rightKeys_, rightInput_, rightIndex_);
  }

  // Compare two rows on the left: index_ and index.
  int32_t compareLeft(vector_size_t index) const {
    return compare(leftKeys_, input_, index_, leftKeys_, input_, index);
  }

  // Compare two rows on the right: rightIndex_ and index.
  int32_t compareRight(vector_size_t index) const {
    return compare(
        rightKeys_, rightInput_, rightIndex_, rightKeys_, rightInput_, index);
  }

  // Compare two rows from the left side.
  int32_t compareLeft(
      const RowVectorPtr& batch,
      vector_size_t index,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex) const {
    return compare(leftKeys_, batch, index, leftKeys_, otherBatch, otherIndex);
  }

  // Compare two rows from the right side.
  int32_t compareRight(
      const RowVectorPtr& batch,
      vector_size_t index,
      const RowVectorPtr& otherBatch,
      vector_size_t otherIndex) const {
    return compare(
        rightKeys_, batch, index, rightKeys_, otherBatch, otherIndex);
  }

  /// Describes a contiguous set of rows on the left or right side of the join
  /// with all join keys being the same. The set of rows may span multiple
  /// batches of input.
  struct Match {
    // One or more batches of inputs that contain rows with matching keys.
    std::vector<RowVectorPtr> inputs;

    // Row number in the first batch pointing to the first row with matching
    // keys.
    vector_size_t startIndex{0};

    // Row number in the last batch pointing to the row just past the row with
    // matching keys.
    vector_size_t endIndex{0};

    // True if all matching rows have been collected. False, if more batches
    // need to be processed to identify all matching rows.
    bool complete{false};

    /// Identifies a particular row in a set rows with matching keys (Match).
    /// Used to store a restart position for when output vector filled up before
    /// the full set of matching rows was added. The next call to getOutput will
    /// continue filling up next output batch from that place.
    struct Cursor {
      // Index of the batch.
      size_t batchIndex{0};

      // Row number in the batch specified by batchIndex.
      vector_size_t index{0};
    };

    /// A position to continue producing output from. Set if complete set of
    /// rows with matching keys didn't fit into output batch.
    std::optional<Cursor> cursor;

    /// A convenience method to set or update 'cursor'.
    void setCursor(size_t batchIndex, vector_size_t index) {
      cursor = Cursor{batchIndex, index};
    }
  };

  /// Given a partial set of rows with matching keys (match) finds all rows from
  /// the start of the 'input' batch that also have matching keys. Updates
  /// 'match' to include the newly identified rows. Returns true if found the
  /// last matching row and set match.complete to true. If all rows in 'input'
  /// have matching keys, adds 'input' to 'match' and returns false to ensure
  /// that next batch of input is checked for more matching rows.
  bool findEndOfMatch(
      Match& match,
      const RowVectorPtr& input,
      const std::vector<column_index_t>& keys);

  /// Initialize 'output_' vector using 'ouputType_' and 'outputBatchSize_' if
  /// it is null.
  void prepareOutput();

  // Appends a cartesian product of the current set of matching rows, leftMatch_
  // x rightMatch_, to output_. Returns true if output_ is full. Sets
  // leftMatchCursor_ and rightMatchCursor_ if output_ filled up before all the
  // rows were added. Fills up output starting from leftMatchCursor_ and
  // rightMatchCursor_ positions if these are set. Clears leftMatch_ and
  // rightMatch_ if all rows were added. Updates leftMatchCursor_ and
  // rightMatchCursor_ if output_ filled up before all rows were added.
  bool addToOutput();

  // Adds one row of output by copying values from left and right batches at the
  // specified rows. Advances outputSize_. Assumes that output_ has room.
  //
  // TODO: Copying is inefficient especially for complex type values. Consider
  // an optimization of using dictionary wrapping when full batch of output can
  // be produced using single batch of input from the left side and single batch
  // of input from the right side.
  void addOutputRow(
      const RowVectorPtr& left,
      vector_size_t leftIndex,
      const RowVectorPtr& right,
      vector_size_t rightIndex);

  /// Adds one row of output for a left-side row with no right-side match.
  /// Copies values from the 'index_' row on the left side and fills in nulls
  /// for columns that correspond to the right side.
  void addOutputRowForLeftJoin();

  /// Evaluates join filter on 'filterInput_' and returns 'output' that contains
  /// a subset of rows on which the filter passed. Returns nullptr if no rows
  /// passed the filter.
  RowVectorPtr applyFilter(const RowVectorPtr& output);

  /// Evaluates 'filter_' on the specified rows of 'filterInput_' and decodes
  /// the result using 'decodedFilterResult_'.
  void evaluateFilter(const SelectivityVector& rows);

  /// As we populate the results of the left join, we track whether a given
  /// output row is a result of a match between left and right sides or a miss.
  /// We use LeftJoinTracker::addMatch and addMiss methods for that.
  ///
  /// Once we have a batch of output, we evaluate the filter on a subset of rows
  /// which correspond to matches between left and right sides. There is no
  /// point evaluating filters on misses as these need to be included in the
  /// output regardless of whether filter passes or fails.
  ///
  /// We also track blocks of consecutive output rows that correspond to the
  /// same left-side row. If the filter passes on at least one row in such a
  /// block, we keep the subset of passing rows. However, if the filter failed
  /// on all rows in such a block, we add one of these rows back and update
  /// build-side columns to null.
  struct LeftJoinTracker {
    LeftJoinTracker(vector_size_t numRows, memory::MemoryPool* pool)
        : matchingRows_{numRows, false} {
      leftRowNumbers_ = AlignedBuffer::allocate<vector_size_t>(numRows, pool);
      rawLeftRowNumbers_ = leftRowNumbers_->asMutable<vector_size_t>();
    }

    /// Records a row of output that corresponds to a match between a left-side
    /// row and a right-side row. Assigns synthetic number to uniquely identify
    /// the corresponding left-side row. The caller must call addMatch or
    /// addMiss method for each row of output in order, starting with the first
    /// row.
    void addMatch(
        const VectorPtr& left,
        vector_size_t leftIndex,
        vector_size_t outputIndex) {
      matchingRows_.setValid(outputIndex, true);

      if (lastVector_ != left || lastIndex_ != leftIndex) {
        // New left-side row.
        ++lastLeftRowNumber_;
        lastVector_ = left;
        lastIndex_ = leftIndex;
      }

      rawLeftRowNumbers_[outputIndex] = lastLeftRowNumber_;
    }

    /// Returns a subset of "match" rows in [0, numRows) range that were
    /// recorded by addMatch.
    const SelectivityVector& matchingRows(vector_size_t numRows) {
      matchingRows_.setValidRange(numRows, matchingRows_.size(), false);
      matchingRows_.updateBounds();
      return matchingRows_;
    }

    /// Records a row of output that corresponds to a left-side
    /// row that has no match on the right-side. The caller must call addMatch
    /// or addMiss method for each row of output in order, starting with the
    /// first row.
    void addMiss(vector_size_t outputIndex) {
      matchingRows_.setValid(outputIndex, false);
      resetLastVector();
    }

    /// Clear the left-side vector and index of the last added output row. The
    /// left-side vector has been fully processed and is now available for
    /// re-use, hence, need to make sure that new rows won't be confused with
    /// the old ones.
    void resetLastVector() {
      lastVector_.reset();
      lastIndex_ = -1;
    }

    /// Called for each row that the filter was evaluated on in order starting
    /// with the first row. Calls 'onMiss' if the filter failed on all output
    /// rows that correspond to a single left-side row. Use
    /// 'noMoreFilterResults' to make sure 'onMiss' is called for the last
    /// left-side row.
    template <typename TOnMiss>
    void processFilterResult(
        vector_size_t outputIndex,
        bool passed,
        TOnMiss onMiss) {
      auto rowNumber = rawLeftRowNumbers_[outputIndex];
      if (currentLeftRowNumber_ != rowNumber) {
        if (currentRow_ != -1 && !currentRowPassed_) {
          onMiss(currentRow_);
        }
        currentRow_ = outputIndex;
        currentLeftRowNumber_ = rowNumber;
        currentRowPassed_ = false;
      } else {
        currentRow_ = outputIndex;
      }

      if (passed) {
        currentRowPassed_ = true;
      }
    }

    /// Called when all rows from the current output batch are processed and the
    /// next batch of output will start with a new left-side row or there will
    /// be no more batches. Calls 'onMiss' for the last left-side row if the
    /// filter failed for all matches of that row.
    template <typename TOnMiss>
    void noMoreFilterResults(TOnMiss onMiss) {
      if (!currentRowPassed_) {
        onMiss(currentRow_);
      }

      currentRow_ = -1;
      currentRowPassed_ = false;
    }

   private:
    /// A subset of output rows where left side matched right side on the join
    /// keys. Used in filter evaluation.
    SelectivityVector matchingRows_;

    /// The left-side vector and index of the last added row. Used to identify
    /// the end of a block of output rows that correspond to the same left-side
    /// row.
    VectorPtr lastVector_{nullptr};
    vector_size_t lastIndex_{-1};

    /// Synthetic numbers used to uniquely identify a left-side row. We cannot
    /// use row number from the left-side vector because a given batch of output
    /// may contains rows from multiple left-side batches. Only "match" rows
    /// added via addMatch are being tracked. The values for "miss" rows are
    /// not defined.
    BufferPtr leftRowNumbers_;
    vector_size_t* rawLeftRowNumbers_;

    /// Synthetic number assigned to the last added "match" row or zero if
    /// no row has been added yet.
    vector_size_t lastLeftRowNumber_{0};

    /// Output index of the last output row for which filter result was
    /// recorded.
    vector_size_t currentRow_{-1};

    /// Synthetic number for the 'currentRow'.
    vector_size_t currentLeftRowNumber_{-1};

    /// True if at least one row in a block of output rows corresponding a
    /// single left-side row identified by 'currentRowNumber' passed the filter.
    bool currentRowPassed_{false};
  };

  std::optional<LeftJoinTracker> leftJoinTracker_{std::nullopt};

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  /// Type of join.
  const core::JoinType joinType_;

  /// Number of join keys.
  const size_t numKeys_;

  std::vector<column_index_t> leftKeys_;
  std::vector<column_index_t> rightKeys_;
  std::vector<IdentityProjection> leftProjections_;
  std::vector<IdentityProjection> rightProjections_;

  /// Join filter.
  std::unique_ptr<ExprSet> filter_;

  /// Join filter input type.
  RowTypePtr filterInputType_;

  /// Maps left-side input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterLeftInputs_;

  /// Maps right-side input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterRightInputs_;

  /// Reusable memory for filter evaluation.
  RowVectorPtr filterInput_;
  SelectivityVector filterRows_;
  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;

  /// An instance of MergeJoinSource to pull batches of right side input from.
  std::shared_ptr<MergeJoinSource> rightSource_;

  /// Latest batch of input from the right side.
  RowVectorPtr rightInput_;

  /// Row number on the left side (input_) to process next.
  vector_size_t index_{0};

  /// Row number on the right side (rightInput_) to process next.
  vector_size_t rightIndex_{0};

  /// A set of rows with matching keys on the left side.
  std::optional<Match> leftMatch_;

  /// A set of rows with matching keys on the right side.
  std::optional<Match> rightMatch_;

  RowVectorPtr output_;

  /// Number of rows accumulated in the output_.
  vector_size_t outputSize_;

  /// A future that will be completed when right side input becomes available.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  /// True if all the right side data has been received.
  bool noMoreRightInput_{false};
};
} // namespace facebook::velox::exec
