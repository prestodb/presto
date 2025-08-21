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
#include "velox/exec/Spill.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/TreeOfLosers.h"

namespace facebook::velox::exec {

class SourceStream;
class SourceMerger;
class SpillMerger;

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
      const std::string& operatorType,
      const std::optional<common::SpillConfig>& spillConfig = std::nullopt);

  void initialize() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  RowVectorPtr getOutput() override;

  void close() override;

  const RowTypePtr& outputType() const {
    return outputType_;
  }

  /// The name of runtime stats specific to merge.
  /// The running wall time of the merge operator reading from the streaming
  /// source. If spilling is enabled for local merge, this also includes the
  /// time that writes to the spilled source.
  static inline const std::string kStreamingSourceReadWallNanos{
      "streamingSourceReadWallNanos"};
  /// The running wall time of the merge operator reading from the spilled
  /// source to produce the final output. This only applies when spilling is
  /// enabled for local merge.
  static inline const std::string kSpilledSourceReadWallNanos{
      "spilledSourceReadWallNanos"};

 protected:
  virtual BlockingReason addMergeSources(ContinueFuture* future) = 0;

  std::vector<std::shared_ptr<MergeSource>> sources_;
  size_t numStartedSources_{0};
  /// Maximum number of merge sources per run.
  uint32_t maxNumMergeSources_{std::numeric_limits<uint32_t>::max()};

 private:
  // Tracks the internal execution stats for a merge operator.
  struct Stats {
    // The time point that a merge operator starts reading from the streaming
    // source.
    uint64_t streamingSourceReadStartTimeUs{0};
    // The time point that a merge operator finishes read from the streaming
    // source. This includes the time that writes to the spilled source for
    // recursive merge when spilling is enabled for local merge.
    uint64_t streamingSourceReadEndTimeUs{0};
    // The time point that a merge operator finishes read from the spilled
    // source. This only applies when spilling is enabled for local merge.
    uint64_t spilledSourceReadEndTimeUs{0};
  };
  void recordMergeStats();

  // Start sources for this merge run, it may start either all the sources at
  // once or a portion of the sources at a time to cap the memory usage.
  void maybeStartNextMergeSourceGroup();

  // Returns true if needs to spill the merged source output if all sources can
  // not be merged at once.
  bool needSpill() const {
    return maxNumMergeSources_ < sources_.size();
  }

  void maybeSetupOutputSpiller();

  // Spill the output of a partial merge sources.
  void spill();

  // Invoked at the end for each partial merge run to ensure the order within
  // each spill file.
  void finishMergeSourceGroup();

  // Create spillMerger_ exactly once if spill has happened.
  void setupSpillMerger();

  RowVectorPtr getOutputFromSpill();

  RowVectorPtr getOutputFromSource();

  // Maximum number of rows in the output batch.
  const vector_size_t maxOutputBatchRows_;
  // Maximum number of bytes in the output batch.
  const uint64_t maxOutputBatchBytes_;
  const std::vector<SpillSortKey> sortingKeys_;

  Stats mergeStats_;

  RowVectorPtr output_;
  /// Number of rows accumulated in 'output_' so far.
  vector_size_t outputSize_{0};
  bool finished_{false};

  /// A list of blocking futures for sources. These are populates when a given
  /// source is blocked waiting for the next batch of data.
  std::vector<ContinueFuture> sourceBlockingFutures_;

  std::unique_ptr<SourceMerger> sourceMerger_;
  std::shared_ptr<SpillMerger> spillMerger_;
  std::unique_ptr<MergeSpiller> mergeOutputSpiller_;
  // Number of total spilled rows, it must be equal to the input rows.
  uint64_t numSpilledRows_{0};
  // SpillFiles group for all the partial merge runs.
  std::vector<SpillFiles> spillFileGroups_;
};

/// A utility class for sort-merging data from upstream sources of the
/// `LocalMerge` operator. The `LocalMerge` operator may start only a portion of
/// the sources at a time to cap the memory usage, hence it might perform
/// multiple sort-merge operations with a subset of merge sources.
class SourceMerger {
 public:
  SourceMerger(
      const RowTypePtr& type,
      std::vector<std::unique_ptr<SourceStream>> sourceStreams,
      vector_size_t maxOutputBatchRows,
      uint64_t maxOutputBatchBytes,
      velox::memory::MemoryPool* pool);

  void isBlocked(std::vector<ContinueFuture>& sourceBlockingFutures) const;

  RowVectorPtr getOutput(
      std::vector<ContinueFuture>& sourceBlockingFutures,
      bool& atEnd);

 private:
  void setOutputBatchSize();

  const RowTypePtr type_;
  const vector_size_t maxOutputBatchRows_;
  const uint64_t maxOutputBatchBytes_;
  const std::vector<SourceStream*> streams_;
  const std::unique_ptr<TreeOfLosers<SourceStream>> merger_;
  velox::memory::MemoryPool* const pool_;

  // The max number of rows in an output vector which is determined by
  // 'setOutputBatchSize'. The calculation is based on the actual estimated row
  // size and capped by 'maxOutputBatchRows_' and 'maxOutputBatchBytes_'.
  vector_size_t outputBatchRows_{0};

  // Reusable output vector.
  RowVectorPtr output_;
  // The number of rows in 'output_' vector.
  uint64_t outputRows_{0};
};

class SourceStream final : public MergeStream {
 public:
  SourceStream(
      MergeSource* source,
      const std::vector<SpillSortKey>& sortingKeys,
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

  // Returns the estimated row size based on the vector received from the
  // merge source.
  std::optional<int64_t> estimateRowSize() const {
    if (data_ == nullptr || data_->size() == 0) {
      return std::nullopt;
    }
    return data_->estimateFlatSize() / data_->size();
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

  const std::vector<SpillSortKey>& sortingKeys_;

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

/// A utility class for sort-merging data from data spilled by the `LocalMerge`
/// operator.
class SpillMerger : public std::enable_shared_from_this<SpillMerger> {
 public:
  SpillMerger(
      const std::vector<SpillSortKey>& sortingKeys,
      const RowTypePtr& type,
      std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
          spillReadFilesGroup,
      vector_size_t maxOutputBatchRows,
      uint64_t maxOutputBatchBytes,
      int mergeSourceQueueSize,
      const common::SpillConfig* spillConfig,
      const std::shared_ptr<folly::Synchronized<common::SpillStats>>&
          spillStats,
      velox::memory::MemoryPool* pool);

  ~SpillMerger();

  void start();

  RowVectorPtr getOutput(
      std::vector<ContinueFuture>& sourceBlockingFutures,
      bool& atEnd) const;

 private:
  static std::vector<std::shared_ptr<MergeSource>> createMergeSources(
      size_t numSpillSources,
      int queueSize);

  static std::vector<std::unique_ptr<BatchStream>> createBatchStreams(
      std::vector<std::vector<std::unique_ptr<SpillReadFile>>>
          spillReadFilesGroup);

  static std::unique_ptr<SourceMerger> createSourceMerger(
      const std::vector<SpillSortKey>& sortingKeys,
      const RowTypePtr& type,
      const std::vector<std::shared_ptr<MergeSource>>& sources,
      vector_size_t maxOutputBatchRows,
      uint64_t maxOutputBatchBytes,
      velox::memory::MemoryPool* pool);

  static void asyncReadFromSpillFileStream(
      const std::weak_ptr<SpillMerger>& mergeHolder,
      size_t streamIdx);

  void readFromSpillFileStream(size_t streamIdx);

  void scheduleAsyncSpillFileStreamReads();

  folly::Executor* const executor_;
  const std::shared_ptr<folly::Synchronized<common::SpillStats>> spillStats_;
  const std::shared_ptr<memory::MemoryPool> pool_;

  std::vector<std::shared_ptr<MergeSource>> sources_;
  std::vector<std::unique_ptr<BatchStream>> batchStreams_;
  std::unique_ptr<SourceMerger> sourceMerger_;
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

  VectorSerde* serde() const {
    return serde_;
  }

  VectorSerde::Options* serdeOptions() const {
    return serdeOptions_.get();
  }

  void close() override;

 protected:
  BlockingReason addMergeSources(ContinueFuture* future) override;

 private:
  VectorSerde* const serde_;
  const std::unique_ptr<VectorSerde::Options> serdeOptions_;
  bool noMoreSplits_ = false;
  // Task Ids from all the splits we took to process so far.
  std::vector<std::string> remoteSourceTaskIds_;
};

} // namespace facebook::velox::exec
