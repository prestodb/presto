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
#include "velox/exec/JoinBridge.h"
#include "velox/exec/Spill.h"

namespace facebook::velox::exec {

/// Hands over a hash table from a multi-threaded build pipeline to a
/// multi-threaded probe pipeline. This is owned by shared_ptr by all the build
/// and probe Operator instances concerned. Corresponds to the Presto concept of
/// the same name.
class HashJoinBridge : public JoinBridge {
 public:
  void start() override;

  /// Invoked by HashBuild operator ctor to add to this bridge by incrementing
  /// 'numBuilders_'. The latter is used to split the spill partition data among
  /// HashBuild operators to parallelize the restoring operation.
  void addBuilder();

  /// 'spillPartitionSet' contains the spilled partitions while building
  /// 'table'. The function returns true if there is spill data to restore
  /// after HashProbe operators process 'table', otherwise false. This only
  /// applies if the disk spilling is enabled.
  bool setHashTable(
      std::unique_ptr<BaseHashTable> table,
      SpillPartitionSet spillPartitionSet);

  void setAntiJoinHasNullKeys();

  /// Represents the result of HashBuild operators: a hash table, an optional
  /// restored spill partition id associated with the table, and the spilled
  /// partitions while building the table if not empty. In case of an anti join,
  /// a build side entry with a null in a join key makes the join return
  /// nothing. In this case, HashBuild operators finishes early without
  /// processing all the input and without finishing building the hash table.
  struct HashBuildResult {
    HashBuildResult(
        std::shared_ptr<BaseHashTable> _table,
        std::optional<SpillPartitionId> _restoredPartitionId,
        SpillPartitionIdSet _spillPartitionIds)
        : antiJoinHasNullKeys(false),
          table(std::move(_table)),
          restoredPartitionId(std::move(_restoredPartitionId)),
          spillPartitionIds(std::move(_spillPartitionIds)) {}

    HashBuildResult() : antiJoinHasNullKeys(true) {}

    bool antiJoinHasNullKeys{false};
    std::shared_ptr<BaseHashTable> table;
    std::optional<SpillPartitionId> restoredPartitionId;
    SpillPartitionIdSet spillPartitionIds;
  };

  /// Invoked by HashProbe operator to get the table to probe which is built by
  /// HashBuild operators. If HashProbe operator calls this early, 'future' will
  /// be set to wait asynchronously, otherwise the built table along with
  /// optional spilling related information will be returned in HashBuildResult.
  std::optional<HashBuildResult> tableOrFuture(
      ContinueFuture* FOLLY_NONNULL future);

  /// Invoked by HashProbe operator after finishes probing the built table to
  /// set one of the previously spilled partition to restore. The HashBuild
  /// operators will then build the next hash table from the selected spilled
  /// one. The function returns true if there is spill data to be restored by
  /// HashBuild operators next.
  bool probeFinished();

  /// Contains the spill input for one HashBuild operator: a shard of previously
  /// spilled partition data. 'spillPartition' is null if there is no more spill
  /// data to restore.
  struct SpillInput {
    explicit SpillInput(
        std::unique_ptr<SpillPartition> spillPartition = nullptr)
        : spillPartition(std::move(spillPartition)) {}

    std::unique_ptr<SpillPartition> spillPartition;
  };

  /// Invoked by HashBuild operator to get one of previously spilled partition
  /// shard to restore. The spilled partition to restore is set by HashProbe
  /// operator after finishes probing on the previously built hash table.
  /// If HashBuild operator calls this early, 'future' will be set to wait
  /// asynchronously. If there is no more spill data to restore, then
  /// 'spillPartition' will be set to null in the returned SpillInput.
  std::optional<SpillInput> spillInputOrFuture(
      ContinueFuture* FOLLY_NONNULL future);

 private:
  uint32_t numBuilders_{0};

  std::optional<HashBuildResult> buildResult_;

  // restoringSpillPartitionXxx member variables are populated by the
  // bridge itself. When probe side finished processing, the bridge picks the
  // first partition from 'spillPartitionSets_', splits it into "even" shards
  // among the HashBuild operators and notifies these operators that they can
  // start building HashTables from these shards.

  // If not null, set to the currently restoring spill partition id.
  std::optional<SpillPartitionId> restoringSpillPartitionId_;

  // If 'restoringSpillPartitionId_' is not null, this set to the restoring
  // spill partition data shards. Each shard is expected to have the same number
  // of spill files and will be processed by one of the HashBuild operator.
  std::vector<std::unique_ptr<SpillPartition>> restoringSpillShards_;

  // The spill partitions remaining to restore. This set is populated using
  // information provided by the HashBuild operators if spilling is enabled.
  // This set can grow if HashBuild operator cannot load full partition in
  // memory and engages in recursive spilling.
  SpillPartitionSet spillPartitionSets_;
};

// Indicates if 'joinNode' is null-aware anti-join type and has filter set.
bool isNullAwareAntiJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode);
} // namespace facebook::velox::exec
