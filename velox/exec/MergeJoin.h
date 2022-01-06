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

  void close() override {
    if (rightSource_) {
      rightSource_->close();
    }
    Operator::close();
  }

 private:
  RowVectorPtr doGetOutput();

  static int32_t compare(
      const std::vector<ChannelIndex>& keys,
      const RowVectorPtr& batch,
      vector_size_t index,
      const std::vector<ChannelIndex>& otherKeys,
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
  /// the start of the 'input' batch that also have maching keys. Updates
  /// 'match' to include the newly identified rows. Returns true if found the
  /// last matching row and set match.complete to true. If all rows in 'input'
  /// have matching keys, add 'input' to 'match' and returns false to ensure
  /// that next batch of input is checked for more matching rows.
  bool findEndOfMatch(Match& match, const RowVectorPtr& input);

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

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  /// Type of join.
  const core::JoinType joinType_;

  /// Number of join keys.
  const size_t numKeys_;

  std::vector<ChannelIndex> leftKeys_;
  std::vector<ChannelIndex> rightKeys_;
  std::vector<IdentityProjection> leftProjections_;
  std::vector<IdentityProjection> rightProjections_;

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

  /// Number of rows accumulated in the output_.
  vector_size_t outputSize_;

  /// A future that will be completed when right side input becomes available.
  ContinueFuture future_{false};

  /// Boolean indicating whether 'future_' is set.
  bool hasFuture_{false};

  /// True if all the right side data has been received.
  bool noMoreRightInput_{false};
};
} // namespace facebook::velox::exec
