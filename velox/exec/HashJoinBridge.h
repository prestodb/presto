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

#include "velox/exec/HashBitRange.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/JoinBridge.h"
#include "velox/exec/MemoryReclaimer.h"
#include "velox/exec/Spill.h"

namespace facebook::velox::wave {
struct HashTableHolder;
}

namespace facebook::velox::exec {
class HashBuildSpiller;

namespace test {
class HashJoinBridgeTestHelper;
}

using HashJoinTableSpillFunc =
    std::function<SpillPartitionSet(std::shared_ptr<BaseHashTable>)>;

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

  void reclaim();

  /// Invoked by the build operator to set the built hash table.
  /// 'spillPartitionSet' contains the spilled partitions while building
  /// 'table' which only applies if the disk spilling is enabled.
  void setHashTable(
      std::unique_ptr<BaseHashTable> table,
      SpillPartitionSet spillPartitionSet,
      bool hasNullKeys,
      HashJoinTableSpillFunc&& tableSpillFunc);

  void setHashTable(
      std::shared_ptr<wave::HashTableHolder> table,
      bool hasNullKeys);

  /// Invoked by the probe operator to append the spilled hash table partitions
  /// while probing. The function appends the spilled table partitions into
  /// 'spillPartitionSets_' stack. This only applies if the disk spilling is
  /// enabled.
  void appendSpilledHashTablePartitions(SpillPartitionSet spillPartitionSet);

  void setAntiJoinHasNullKeys();

  /// Represents the result of HashBuild operators. In case of an anti join, a
  /// build side entry with a null in a join key makes the join return nothing.
  /// In this case, HashBuild operators finishes early without processing all
  /// the input and without finishing building the hash table.
  struct HashBuildResult {
    HashBuildResult(
        std::shared_ptr<BaseHashTable> _table,
        std::optional<SpillPartitionId> _restoredPartitionId,
        SpillPartitionIdSet _spillPartitionIds,
        bool _hasNullKeys)
        : hasNullKeys(_hasNullKeys),
          table(std::move(_table)),
          restoredPartitionId(std::move(_restoredPartitionId)),
          spillPartitionIds(std::move(_spillPartitionIds)) {}

    HashBuildResult() : hasNullKeys(true) {}

    HashBuildResult(
        std::shared_ptr<wave::HashTableHolder> _table,
        bool _hasNullKeys)
        : hasNullKeys(_hasNullKeys), waveTable(std::move(_table)) {}

    bool hasNullKeys;
    std::shared_ptr<BaseHashTable> table;

    std::shared_ptr<wave::HashTableHolder> waveTable;

    /// Restored spill partition id associated with 'table', null if 'table' is
    /// not built from restoration.
    std::optional<SpillPartitionId> restoredPartitionId;

    /// Spilled partitions while building hash table. Since we don't support
    /// fine-grained spilling for hash table, either 'table' is empty or
    /// 'spillPartitionIds' is empty.
    SpillPartitionIdSet spillPartitionIds;
  };

  /// Invoked by HashProbe operator to get the table to probe which is built by
  /// HashBuild operators. If HashProbe operator calls this early, 'future' will
  /// be set to wait asynchronously, otherwise the built table along with
  /// optional spilling related information will be returned in HashBuildResult.
  std::optional<HashBuildResult> tableOrFuture(ContinueFuture* future);

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
  std::optional<SpillInput> spillInputOrFuture(ContinueFuture* future);

 private:
  void appendSpilledHashTablePartitionsLocked(
      SpillPartitionSet&& spillPartitionSet);

  uint32_t numBuilders_{0};

  // The result of the build side. It is set by the last build operator when
  // build is done.
  std::optional<HashBuildResult> buildResult_;

  // Spill function that lets hash join bridge spill the hash table on behalf of
  // the hash build operator after the table ownership transfer, and before
  // probing.
  HashJoinTableSpillFunc tableSpillFunc_{nullptr};

  // restoringSpillPartitionXxx member variables are populated by the
  // bridge itself. When probe side finished processing, the bridge picks the
  // first partition from 'spillPartitionSets_', splits it into "even" shards
  // among the HashBuild operators and notifies these operators that they can
  // start building HashTables from these shards.

  // If not null, set to the currently restoring table spill partition id.
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

  // A flag indicating if any probe operator has poked 'this' join bridge to
  // attempt to get table. It is reset after probe side finish the (sub) table
  // processing.
  bool probeStarted_;

  friend test::HashJoinBridgeTestHelper;
};

// Indicates if 'joinNode' is null-aware anti or left semi project join type and
// has filter set.
bool isLeftNullAwareJoinWithFilter(
    const std::shared_ptr<const core::HashJoinNode>& joinNode);

class HashJoinMemoryReclaimer final : public MemoryReclaimer {
 public:
  static std::unique_ptr<memory::MemoryReclaimer> create(
      std::shared_ptr<HashJoinBridge> joinBridge,
      int32_t priority = 0) {
    return std::unique_ptr<memory::MemoryReclaimer>(
        new HashJoinMemoryReclaimer(joinBridge, priority));
  }

  uint64_t reclaim(
      memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) final;

 private:
  HashJoinMemoryReclaimer(
      const std::shared_ptr<HashJoinBridge>& joinBridge,
      int32_t priority)
      : MemoryReclaimer(priority), joinBridge_(joinBridge) {}
  std::weak_ptr<HashJoinBridge> joinBridge_;
};

/// Returns true if 'pool' is a hash build operator's memory pool. The check is
/// currently based on the pool name.
bool isHashBuildMemoryPool(const memory::MemoryPool& pool);

/// Returns true if 'pool' is a hash probe operator's memory pool. The check is
/// currently based on the pool name.
bool isHashProbeMemoryPool(const memory::MemoryPool& pool);

bool needRightSideJoin(core::JoinType joinType);

/// Returns the type of the hash table associated with this join.
RowTypePtr hashJoinTableType(
    const std::shared_ptr<const core::HashJoinNode>& joinNode);

struct HashJoinTableSpillResult {
  HashBuildSpiller* spiller{nullptr};
  const std::exception_ptr error{nullptr};

  explicit HashJoinTableSpillResult(std::exception_ptr _error)
      : error(_error) {}
  explicit HashJoinTableSpillResult(HashBuildSpiller* _spiller)
      : spiller(_spiller) {}
};

/// Invoked to spill the hash table from a set of spillers. If 'spillExecutor'
/// is provided, then we do parallel spill. This is used by hash build to spill
/// a partially built hash join table.
std::vector<std::unique_ptr<HashJoinTableSpillResult>> spillHashJoinTable(
    const std::vector<HashBuildSpiller*>& spillers,
    const common::SpillConfig* spillConfig);

/// Invoked to spill 'table' and returns spilled partitions. This is used by
/// hash probe or hash join bridge to spill a fully built table.
SpillPartitionSet spillHashJoinTable(
    std::shared_ptr<BaseHashTable> table,
    const HashBitRange& hashBitRange,
    const std::shared_ptr<const core::HashJoinNode>& joinNode,
    const common::SpillConfig* spillConfig,
    folly::Synchronized<common::SpillStats>* stats);

/// Returns the type used to spill a given hash table type. The function
/// might attach a boolean column at the end of 'tableType' if 'joinType' needs
/// right side join processing. It is used by the hash join table spilling
/// triggered at the probe side to record if each row has been probed or not.
RowTypePtr hashJoinTableSpillType(
    const RowTypePtr& tableType,
    core::JoinType joinType);

/// Checks if a given type is a hash table spill type or not based on
/// 'joinType'.
bool isHashJoinTableSpillType(
    const RowTypePtr& spillType,
    core::JoinType joinType);
} // namespace facebook::velox::exec
