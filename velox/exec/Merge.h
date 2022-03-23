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

#include <memory>
#include "velox/exec/Exchange.h"
#include "velox/exec/MergeSource.h"
#include "velox/exec/RowContainer.h"

namespace facebook::velox::exec {

// Merge operator Implementation: This implementation uses priority queue
// to perform a k-way merge of its inputs. It stops merging if any one of
// its inputs is blocked.
class Merge : public SourceOperator {
 public:
  Merge(
      int32_t operatorId,
      DriverCtx* ctx,
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
  static const size_t kBatchSizeInBytes{2 * 1024 * 1024};
  using SourceRow = std::pair<size_t, char*>;

  class Comparator {
   public:
    Comparator(
        const RowTypePtr& outputType,
        const std::vector<std::shared_ptr<const core::FieldAccessTypedExpr>>&
            sortingKeys,
        const std::vector<core::SortOrder>& sortingOrders,
        RowContainer* rowContainer);

    // Returns true if lhs > rhs, false otherwise.
    bool operator()(const SourceRow& lhs, const SourceRow& rhs) {
      for (auto& key : keyInfo_) {
        if (auto result = rowContainer_->compare(
                lhs.second, rhs.second, key.first, key.second)) {
          return result > 0;
        }
      }
      return false;
    }

   private:
    std::vector<std::pair<ChannelIndex, CompareFlags>> keyInfo_;
    RowContainer* rowContainer_;
  };

  /// Appends next row from the specified source to 'candidates_'. If
  /// corresponding CursorSource has next row, returns that row and advances the
  /// cursor. Otherwise, fetches next batch of source rows from MergeSource,
  /// copies them to rowContainer_ and resets the cursor.
  BlockingReason pushSource(ContinueFuture* future, size_t sourceId);

  BlockingReason ensureSourcesReady(ContinueFuture* future);

  /// Returns "max" row from 'candidates_' and removes that row from
  /// 'candidates_'. Assumes 'candidates_' is not empty.
  SourceRow nextOutputRow();

  /// A cursor over an ordered batch of source rows copied into 'rowContainer_'.
  struct SourceCursor {
    /// Ordered source rows.
    std::vector<char*> rows;
    /// Index of the next row.
    vector_size_t index{0};
    /// True if source has been exhausted.
    bool atEnd{false};

    /// Returns true if there is a next row.
    bool hasNext() {
      return !atEnd && index < rows.size();
    }

    /// Returns next row and advances the cursor.
    char* nextUnchecked() {
      return rows[index++];
    }

    /// Copies the 'data' into 'rowContainer' and resets the cursor to point to
    /// the first row.
    void reset(const RowVectorPtr& data, RowContainer* rowContainer);
  };

  /// A list of cursors over batches of ordered source data. One per source.
  /// Aligned with 'sources'.
  std::vector<SourceCursor> sourceCursors_;

  /// Ordered list of output rows.
  std::vector<char*> rows_;

  /// Row container to store incoming batches of source data.
  std::unique_ptr<RowContainer> rowContainer_;

  /// STL-compatible comparator to compare rows by sorting keys.
  Comparator comparator_;

  /// A list of "max" rows from each source. Used to pick the next output row.
  std::vector<SourceRow> candidates_;

  BlockingReason blockingReason_{BlockingReason::kNotBlocked};
  ContinueFuture future_;
  size_t numSourcesAdded_ = 0;
  size_t currentSourcePos_ = 0;
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
