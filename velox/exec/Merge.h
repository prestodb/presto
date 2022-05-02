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

#include "velox/exec/Exchange.h"
#include "velox/exec/MergeSource.h"
#include "velox/exec/TreeOfLosers.h"

namespace facebook::velox::exec {

class SourceStream;

// Merge operator Implementation: This implementation uses priority queue
// to perform a k-way merge of its inputs. It stops merging if any one of
// its inputs is blocked.
class Merge : public SourceOperator {
 public:
  Merge(
      int32_t operatorId,
      DriverCtx* driverCtx,
      RowTypePtr outputType,
      const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
          sortingKeys,
      const std::vector<core::SortOrder>& sortingOrders,
      const std::string& planNodeId,
      const std::string& operatorType);

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  RowVectorPtr getOutput() override;

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  memory::MappedMemory* mappedMemory() const {
    return operatorCtx_->mappedMemory();
  }

 protected:
  virtual BlockingReason addMergeSources(ContinueFuture* future) = 0;

  std::vector<std::shared_ptr<MergeSource>> sources_;

 private:
  void initializeTreeOfLosers();

  /// Maximum number of rows in the output batch.
  const uint32_t outputBatchSize_;

  std::vector<std::pair<ChannelIndex, CompareFlags>> sortingKeys_;

  /// A list of cursors over batches of ordered source data. One per source.
  /// Aligned with 'sources'.
  std::vector<SourceStream*> streams_;

  /// Used to merge data from two or more sources.
  std::unique_ptr<TreeOfLosers<SourceStream>> treeOfLosers_;

  RowVectorPtr output_;

  /// Number of rows accumulated in 'output_' so far.
  vector_size_t outputSize_{0};

  bool finished_{false};

  /// A list of blocking futures for sources. These are populates when a given
  /// source is blocked waiting for the next batch of data.
  std::vector<ContinueFuture> sourceBlockingFutures_;
};

class SourceStream final : public MergeStream {
 public:
  SourceStream(
      MergeSource* source,
      const std::vector<std::pair<ChannelIndex, CompareFlags>>& sortingKeys,
      uint32_t outputBatchSize)
      : source_{source},
        sortingKeys_{sortingKeys},
        outputRows_(outputBatchSize, false),
        sourceRows_(outputBatchSize) {
    keyColumns_.reserve(sortingKeys.size());
  }

  /// Returns true and appends a future to 'futures' if needs to wait for the
  /// source to produce data.
  bool isBlocked(std::vector<ContinueFuture>& futures) {
    if (needData_) {
      return fetchMoreData(futures);
    }
    return false;
  }

  bool hasData() const override {
    return !atEnd_;
  }

  /// Returns true if current source row is less then current source row in
  /// 'other'.
  bool operator<(const MergeStream& other) const override;

  /// Advances to the next row. Returns true and appends a future to 'futures'
  /// if runs out of rows in the current batch and needs to wait for the
  /// source to produce the next batch. The return flag has the meaning of
  /// 'is-blocked'.
  bool pop(std::vector<ContinueFuture>& futures);

  /// Records the output row number for the current row. Returns true if
  /// current row is the last row in the current batch, in which case the
  /// caller must call 'copyToOutput' before calling pop(). The caller must
  /// call 'setOutputRow' before calling 'pop'. The output rows must
  /// monotonically increase in between calls to 'copyToOutput'.
  bool setOutputRow(vector_size_t row) {
    outputRows_.setValid(row, true);
    return currentSourceRow_ == data_->size() - 1;
  }

  /// Called if either current row is the last row in the current batch or the
  /// caller accumulated enough output rows across all sources to produce an
  /// output batch.
  void copyToOutput(RowVectorPtr& output);

 private:
  bool fetchMoreData(std::vector<ContinueFuture>& futures);

  MergeSource* source_;

  const std::vector<std::pair<ChannelIndex, CompareFlags>>& sortingKeys_;

  /// Ordered source rows.
  RowVectorPtr data_;

  /// Raw pointers to vectors corresponding to sorting key columns in the same
  /// order as 'sortingKeys_'.
  std::vector<BaseVector*> keyColumns_;

  /// Index of the current row.
  vector_size_t currentSourceRow_{0};

  /// True if source has been exhausted.
  bool atEnd_{false};

  /// True if ran out of rows in 'data_' and needs to wait for the future
  /// returned by 'source_->next()'.
  bool needData_{true};

  /// First source row that hasn't been copied out yet.
  vector_size_t firstSourceRow_{0};

  /// Output row numbers for source rows that haven't been copied out yet.
  SelectivityVector outputRows_;

  /// Reusable memory.
  std::vector<vector_size_t> sourceRows_;
};

// LocalMerge merges its source's output into a single stream of
// sorted rows. It runs single threaded. The sources may run multi-threaded and
// in the same task.
class LocalMerge : public Merge {
 public:
  LocalMerge(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::LocalMergeNode>& localMergeNode);

 protected:
  BlockingReason addMergeSources(ContinueFuture* future) override;
};

// MergeExchange merges its sources' outputs into a single stream of
// sorted rows similar to local merge. However, the sources are splits
// and may be generated by a different task.
class MergeExchange : public Merge {
 public:
  MergeExchange(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::MergeExchangeNode>& orderByNode);

 protected:
  BlockingReason addMergeSources(ContinueFuture* future) override;

 private:
  bool noMoreSplits_ = false;
  size_t numSplits_{0}; // Number of splits we took to process so far.
};

} // namespace facebook::velox::exec
