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

#include "velox/exec/HashJoinBridge.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"
#include "velox/exec/Spill.h"
#include "velox/exec/Spiller.h"
#include "velox/exec/UnorderedStreamReader.h"
#include "velox/exec/VectorHasher.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

// Builds a hash table for use in HashProbe. This is the final
// Operator in a build side Driver. The build side pipeline has
// multiple Drivers, each with its own HashBuild. The build finishes
// when the last Driver of the build pipeline finishes. Hence finish()
// has a barrier where the last one to enter gathers the data
// accumulated by the other Drivers and makes the join hash
// table. This table is then passed to the probe side pipeline via
// JoinBridge. After this, all build side Drivers finish and free
// their state.
class HashBuild final : public Operator {
 public:
  /// Define the internal execution state for hash build.
  enum class State {
    /// The running state.
    kRunning = 1,
    /// The yield state that voluntarily yield cpu after running too long when
    /// processing input from spilled file.
    kYield = 2,
    /// The state that waits for the pending group spill to finish. This state
    /// only applies if disk spilling is enabled.
    kWaitForSpill = 3,
    /// The state that waits for the hash tables to be merged together.
    kWaitForBuild = 4,
    /// The state that waits for the hash probe to finish before start to build
    /// the hash table for one of previously spilled partition. This state only
    /// applies if disk spilling is enabled.
    kWaitForProbe = 5,
    /// The finishing state.
    kFinish = 6,
  };
  static std::string stateName(State state);

  HashBuild(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::HashJoinNode> joinNode);

  void initialize() override;

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  bool canReclaim() const override;

  void reclaim(uint64_t targetBytes, memory::MemoryReclaimer::Stats& stats)
      override;

  void close() override;

 private:
  void setState(State state);
  void checkStateTransition(State state);

  void setRunning();
  bool isRunning() const;
  void checkRunning() const;

  // Invoked to set up hash table to build.
  void setupTable();

  // Invoked when operator has finished processing the build input and wait for
  // all the other drivers to finish the processing. The last driver that
  // reaches to the hash build barrier, is responsible to build the hash table
  // merged from all the other drivers.
  bool finishHashBuild();

  // Invoked after the hash table has been built. It waits for any spill data to
  // process after the probe side has finished processing the previously built
  // hash table. If disk spilling is not enabled or there is no more spill data,
  // then the operator will transition to 'kFinish' state to finish. Otherwise,
  // it will transition to 'kWaitForProbe' to wait for the next spill data to
  // process which will be set by the join probe side.
  void postHashBuildProcess();

  bool spillEnabled() const {
    return canReclaim();
  }

  // Indicates if the input is read from spill data or not.
  bool isInputFromSpill() const;

  // Returns the type of data fed into 'addInput()'. The column orders will be
  // different from the build source data type if the input is read from spill
  // data during restoring.
  RowTypePtr inputType() const;

  // Invoked to setup spiller if disk spilling is enabled. If 'spillPartition'
  // is not null, then the input is from the spilled data instead of from build
  // source. The function will need to setup a spill input reader to read input
  // from the spilled data for restoring. If the spilled data can't still fit
  // in memory, then we will recursively spill part(s) of its data on disk.
  void setupSpiller(SpillPartition* spillPartition = nullptr);

  // Invoked when either there is no more input from the build source or from
  // the spill input reader during the restoring.
  void noMoreInputInternal();

  // Invoked to ensure there is a sufficient memory to process 'input' by
  // reserving a sufficient amount of memory in advance if disk spilling is
  // enabled.
  void ensureInputFits(RowVectorPtr& input);

  // Invoked to ensure there is sufficient memory to build the join table with
  // the specified 'numRows' if spilling is enabled. The function throws to fail
  // the query if the memory reservation fails.
  void ensureTableFits(uint64_t numRows);

  // Invoked to compute spill partitions numbers for each row 'input' and spill
  // rows to spiller directly if the associated partition(s) is spilling. The
  // function will skip processing if disk spilling is not enabled or there is
  // no spilling partition.
  void spillInput(const RowVectorPtr& input);

  // Invoked to spill a number of rows from 'input' to a spill 'partition'.
  // 'size' is the number of rows. 'indices' is the row indices in 'input'.
  void spillPartition(
      uint32_t partition,
      vector_size_t size,
      const BufferPtr& indices,
      const RowVectorPtr& input);

  // Invoked to compute spill partition numbers for 'input' if disk spilling is
  // enabled. The computed partition numbers are stored in 'spillPartitions_'.
  void computeSpillPartitions(const RowVectorPtr& input);

  // Invoked to set up 'spillChildVectors_' for spill if 'input' is from build
  // source.
  void maybeSetupSpillChildVectors(const RowVectorPtr& input);

  // Invoked to prepare indices buffers for input spill processing.
  void prepareInputIndicesBuffers(
      vector_size_t numInput,
      const SpillPartitionNumSet& spillPartitions);

  // Invoked to reset the operator state to restore previously spilled data. It
  // setup (recursive) spiller and spill input reader from 'spillInput' received
  // from 'joinBride_'. 'spillInput' contains a shard of previously spilled
  // partition data. 'spillInput' also indicates if there is no more spill data,
  // then this operator will transition to 'kFinish' state to finish.
  void setupSpillInput(HashJoinBridge::SpillInput spillInput);

  // Invoked to process data from spill input reader on restoring.
  void processSpillInput();

  // Set up for null-aware and regular anti-join with filter processing.
  void setupFilterForAntiJoins(
      const folly::F14FastMap<column_index_t, column_index_t>& keyChannelMap);

  // Invoked when preparing for null-aware and regular anti join with
  // null-propagating filter. The function deselects the input rows which have
  // any null in the filter input columns. This is an optimization for
  // null-aware and regular anti join processing at the probe side as any probe
  // matches with the deselected rows can't pass the null-propagating filter and
  // will be added to the joined output.
  void removeInputRowsForAntiJoinFilter();

  void addRuntimeStats();

  // Indicates if this hash build operator is under non-reclaimable state or
  // not.
  bool nonReclaimableState() const;

  const std::shared_ptr<const core::HashJoinNode> joinNode_;

  const core::JoinType joinType_;

  const bool nullAware_;

  // Sets to true for join type which needs right side join processing. The hash
  // table spiller then needs to record the probed flag, and the spilled input
  // reader also needs to restore the recorded probed flag. This is used to
  // support probe side spilling to record if a spilled row has been probed or
  // not.
  const bool needProbedFlagSpill_;

  std::shared_ptr<HashJoinBridge> joinBridge_;

  bool exceededMaxSpillLevelLimit_{false};

  State state_{State::kRunning};

  // The row type used for hash table build and disk spilling.
  RowTypePtr tableType_;

  // Used to serialize access to internal state including 'table_' and
  // 'spiller_'. This is only required when variables are accessed
  // concurrently, that is, when a thread tries to close the operator while
  // another thread is building the hash table. Refer to 'close()' and
  // finishHashBuild()' for more details.
  std::mutex mutex_;

  // Indicates if the intermediate state ('table_' and 'spiller_') has
  // been cleared. This can happen either when the operator is closed or when
  // the last hash build operator transfers ownership of them to itself while
  // building the final hash table.
  bool stateCleared_{false};

  // Container for the rows being accumulated.
  std::unique_ptr<BaseHashTable> table_;

  // Key channels in 'input_'
  std::vector<column_index_t> keyChannels_;

  // Non-key channels in 'input_'.
  std::vector<column_index_t> dependentChannels_;

  // Corresponds 1:1 to 'dependentChannels_'.
  std::vector<std::unique_ptr<DecodedVector>> decoders_;

  // Future for synchronizing with other Drivers of the same pipeline. All build
  // Drivers must be completed before making the hash table.
  ContinueFuture future_{ContinueFuture::makeEmpty()};

  // True if we are considering use of normalized keys or array hash tables. Set
  // to false when the dataset is no longer suitable.
  bool analyzeKeys_;

  // Temporary space for hash numbers.
  raw_vector<uint64_t> hashes_;

  // Set of active rows during addInput().
  SelectivityVector activeRows_;

  // True if this is a build side of an anti or left semi project join and has
  // at least one entry with null join keys.
  bool joinHasNullKeys_{false};

  // The type used to spill hash table which might attach a boolean column to
  // record the probed flag if 'needProbedFlagSpill_' is true.
  RowTypePtr spillType_;
  // Specifies the column index in 'spillType_' which records the probed flag
  // for each spilled row.
  column_index_t spillProbedFlagChannel_;
  // Used to set the probed flag vector at the build side which is always false.
  std::shared_ptr<ConstantVector<bool>> spillProbedFlagVector_;

  // This can be nullptr if either spilling is not allowed or it has been
  // transferred to the last hash build operator while in kWaitForBuild state or
  // it has been cleared to set up a new one for recursive spilling.
  std::unique_ptr<Spiller> spiller_;

  // Used to read input from previously spilled data for restoring.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillInputReader_;
  // Vector used to read from spilled input with type of 'spillType_'.
  RowVectorPtr spillInput_;

  // Reusable memory for spill partition calculation for input data.
  std::vector<uint32_t> spillPartitions_;

  // Reusable memory for input spilling processing.
  std::vector<vector_size_t> numSpillInputs_;
  std::vector<BufferPtr> spillInputIndicesBuffers_;
  std::vector<vector_size_t*> rawSpillInputIndicesBuffers_;
  std::vector<VectorPtr> spillChildVectors_;

  // Indicates whether the filter is null-propagating.
  bool filterPropagatesNulls_{false};

  // Indices of key columns used by the filter in build side table.
  std::vector<column_index_t> keyFilterChannels_;
  // Indices of dependent columns used by the filter in 'decoders_'.
  std::vector<column_index_t> dependentFilterChannels_;

  // Maps key channel in 'input_' to channel in key.
  folly::F14FastMap<column_index_t, column_index_t> keyChannelMap_;
};

inline std::ostream& operator<<(std::ostream& os, HashBuild::State state) {
  os << HashBuild::stateName(state);
  return os;
}
} // namespace facebook::velox::exec

template <>
struct fmt::formatter<facebook::velox::exec::HashBuild::State>
    : formatter<std::string> {
  auto format(facebook::velox::exec::HashBuild::State s, format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::exec::HashBuild::stateName(s), ctx);
  }
};
