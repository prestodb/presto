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

#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {

/// Partitions the input using specified partitioning keys, sorts rows within
/// partitions using specified sorting keys, assigns row numbers and returns up
/// to specified number of rows per partition.
///
/// It is allowed to not specify partitioning keys. In this case the whole input
/// is treated as a single partition.
///
/// At least one sorting key must be specified.
///
/// The limit (maximum number of rows to return per partition) must be greater
/// than zero.
///
/// This is an optimized version of a Window operator with a single row_number
/// window function followed by a row_number <= N filter.
class TopNRowNumber : public Operator {
 public:
  TopNRowNumber(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::TopNRowNumberNode>& node);

  bool needsInput() const override {
    return true;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  void noMoreInput() override;

  bool isFinished() override;

 private:
  /// A priority queue to keep track of top 'limit' rows for a given partition.
  struct TopRows {
    struct Compare {
      RowComparator& comparator;

      bool operator()(const char* lhs, const char* rhs) {
        return comparator(lhs, rhs);
      }
    };

    std::priority_queue<char*, std::vector<char*, StlAllocator<char*>>, Compare>
        rows;

    TopRows(HashStringAllocator* allocator, RowComparator& comparator)
        : rows{{comparator}, StlAllocator<char*>(allocator)} {}
  };

  void initializeNewPartitions();

  TopRows& partitionAt(char* group) {
    return *reinterpret_cast<TopRows*>(group + partitionOffset_);
  }

  /// Adds input row to a partition or discards the row.
  void processInputRow(
      const RowVectorPtr& input,
      vector_size_t index,
      TopRows& partition);

  /// Returns next partition to add to output or nullptr if there are no
  /// partitions left.
  TopRows* nextPartition();

  /// Returns partition that was partially added to the previous output batch.
  TopRows& currentPartition();

  /// Appends partition rows to outputRows_ and optionally populates row
  /// numbers.
  void appendPartitionRows(
      TopRows& partition,
      vector_size_t start,
      vector_size_t size,
      vector_size_t outputOffset,
      FlatVector<int64_t>* rowNumbers);

  const int32_t limit_;
  const bool generateRowNumber_;
  const RowTypePtr inputType_;

  /// Hash table to keep track of partitions. Not used if there are no
  /// partitioning keys. For each partition, stores an instance of TopRows
  /// struct.
  std::unique_ptr<BaseHashTable> table_;
  std::unique_ptr<HashLookup> lookup_;
  int32_t partitionOffset_;

  /// TopRows struct to keep track of top rows for a single partition, when
  /// there are no partitioning keys.
  std::unique_ptr<HashStringAllocator> allocator_;
  std::unique_ptr<TopRows> singlePartition_;

  /// Stores row data. For each partition, only up to 'limit' rows are stored.
  std::unique_ptr<RowContainer> data_;

  RowComparator comparator_;

  std::vector<DecodedVector> decodedVectors_;

  bool finished_{false};

  /// Maximum number of rows in the output batch.
  vector_size_t outputBatchSize_;
  std::vector<char*> outputRows_;

  /// Number of partitions to fetch from a HashTable in a single listAllRows
  /// call.
  static const size_t kPartitionBatchSize = 100;

  BaseHashTable::RowsIterator partitionIt_;
  std::vector<char*> partitions_{kPartitionBatchSize};
  size_t numPartitions_{0};
  std::optional<int32_t> currentPartition_;
  vector_size_t remainingRowsInPartition_{0};
};
} // namespace facebook::velox::exec
