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

#include "velox/core/PlanNode.h"
#include "velox/exec/GroupingSet.h"

namespace facebook::velox::exec {

/// ColumnStatsCollector collects column statistics during table write
/// operations by leveraging the GroupingSet aggregation framework. It supports
/// both partitioned and unpartitioned tables, computing aggregated statistics
/// such as min, max, count, etc. for specified columns.
///
/// Usage pattern:
/// 1. Create instance with ColumnStatsSpec defining desired statistics
/// 2. Call initialize() once before processing any input
/// 3. Call addInput() repeatedly with data batches
/// 4. Call noMoreInput() when all input has been provided
/// 5. Call getOutput() until finished() returns true to retrieve all results
/// 6. Call close() to cleanup resources
class ColumnStatsCollector {
 public:
  /// Constructs a ColumnStatsCollector for collecting statistics during table
  /// writes.
  /// @param statsSpec Specification defining grouping keys, aggregation step,
  /// and aggregation functions to compute
  /// @param inputType Schema of the input data that will be processed
  /// @param queryConfig Query configuration settings
  /// @param pool Memory pool for allocations
  /// @param nonReclaimableSection Atomic flag indicating non-reclaimable memory
  /// section
  ColumnStatsCollector(
      const core::ColumnStatsSpec& statsSpec,
      const RowTypePtr& inputType,
      const core::QueryConfig* queryConfig,
      memory::MemoryPool* pool,
      tsan_atomic<bool>* nonReclaimableSection);

  /// Returns the output row type that will be produced by this collector.
  /// The output type is determined by the grouping keys and aggregate functions
  /// specified in the ColumnStatsSpec.
  static RowTypePtr outputType(const core::ColumnStatsSpec& statsSpec);

  /// Initializes the stats collector. Must be called exactly once before
  /// adding any input data. Sets up internal aggregation structures based
  /// on the provided ColumnStatsSpec.
  void initialize();

  /// Adds a batch of input data for statistics collection. Can be called
  /// multiple times with different batches until all input data has been
  /// processed.
  /// @param input Batch of input data to process for statistics collection
  void addInput(RowVectorPtr input);

  /// Signals that no more input data will be provided. Must be called after
  /// all addInput() calls to finalize the aggregation computation.
  void noMoreInput();

  /// Retrieves the computed column statistics. For partitioned tables, results
  /// are returned one partition at a time, so this method may need to be called
  /// multiple times until finished() returns true.
  RowVectorPtr getOutput();

  /// Checks whether all computed statistics have been returned. For partitioned
  /// tables, there is one output row per partition, so multiple getOutput()
  /// calls may be required. Returns true when all statistics have been
  /// retrieved.
  bool finished() const;

  /// Cleans up and releases all resources used by the stats collector.
  /// Should be called after all statistics have been retrieved and the
  /// collector is no longer needed.
  void close();

 private:
  void setOutputType();

  // Creates the grouping key channel projections for the column stats
  // collection for partitioned table write with one group per each table
  // partition.
  std::pair<std::vector<column_index_t>, std::vector<column_index_t>>
  setupGroupingKeyChannelProjections() const;

  void createGroupingSet();

  std::vector<AggregateInfo> createAggregates(size_t numGroupingKeys);

  void prepareOutput();

  static const int kMaxOutputBatchRows = 512;
  static const int kMaxOutputBatchBytes = 128 << 20;

  const core::ColumnStatsSpec statsSpec_;
  const RowTypePtr inputType_;
  const core::QueryConfig* const queryConfig_;
  memory::MemoryPool* const pool_;
  tsan_atomic<bool>* const nonReclaimableSection_;
  const vector_size_t maxOutputBatchRows_;

  bool initialized_{false};
  bool noMoreInput_{false};
  bool finished_{false};

  std::unique_ptr<GroupingSet> groupingSet_;
  RowTypePtr outputType_;
  RowVectorPtr output_;
  RowContainerIterator outputIterator_;
};

} // namespace facebook::velox::exec
