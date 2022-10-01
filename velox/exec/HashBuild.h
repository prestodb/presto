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
#include "velox/exec/SpillOperatorGroup.h"
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
    /// The state that waits for the pending group spill to finish. This state
    /// only applies if disk spilling is enabled.
    kWaitForSpill = 2,
    /// The state that waits for the hash tables to be merged together.
    kWaitForBuild = 3,
    /// The state that waits for the hash probe to finish before start to build
    /// the hash table for one of previously spilled partition. This state only
    /// applies if disk spilling is enabled.
    kWaitForProbe = 4,
    /// The finishing state.
    kFinish = 5,
  };
  static std::string stateName(State state);

  HashBuild(
      int32_t operatorId,
      DriverCtx* FOLLY_NONNULL driverCtx,
      std::shared_ptr<const core::HashJoinNode> joinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* FOLLY_NONNULL future) override;

  bool isFinished() override;

  void close() override {}

 private:
  void setState(State state);
  void stateTransitionCheck(State state);

  void setRunning();
  bool isRunning() const;
  void checkRunning() const;

  // Invoked to set up hash table to build.
  void setupTable();

  // Invoked when operator has finished processing the build input and wait for
  // all the other drivers to finish the processing. The last driver that
  // reaches to the hash build barrier, is responsible to build the hash table
  // merged from all the other drivers. If the disk spilling is enabled, the
  // last driver will also restart 'spillGroup_' and add a new hash build
  // barrier for the next round of hash table build operation if it needs.
  bool finishHashBuild();

  // Invoked after the hash table has been built. It waits for any spill data to
  // process after the probe side has finished processing the previously built
  // hash table. If disk spilling is not enabled or there is no more spill data,
  // then the operator will transition to 'kFinish' state to finish. Otherwise,
  // it will transition to 'kWaitForProbe' to wait for the next spill data to
  // process which will be set by the join probe side.
  void postHashBuildProcess();

  bool spillEnabled() const {
    return spillConfig_.has_value();
  }

  const Spiller::Config* FOLLY_NULLABLE spillConfig() const {
    return spillConfig_.has_value() ? &spillConfig_.value() : nullptr;
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
  void setupSpiller(SpillPartition* FOLLY_NULLABLE spillPartition = nullptr);

  // Invoked when either there is no more input from the build source or from
  // the spill input reader during the restoring.
  void noMoreInputInternal();

  // Invoked to ensure there is a sufficient memory to process 'input' by
  // reserving a sufficient amount of memory in advance if disk spilling is
  // enabled. The function returns true if the disk spilling is not enabled, or
  // the memory reservation succeeds. If the memory reservation fails, the
  // function will trigger a group spill which needs coordination among the
  // other build drivers in the same group. The function returns true if the
  // group spill has been inline executed which could happen if there is only
  // one driver in the group, or it happens that all the other drivers have
  // also requested group spill and this driver is the last one to reach the
  // group spill barrier. Otherwise, the function returns false to wait for the
  // group spill to run. The operator will transition to 'kWaitForSpill' state
  // accordingly.
  bool ensureInputFits(RowVectorPtr& input);

  // Invoked to reserve memory for 'input' if disk spilling is enabled. The
  // function returns true on success, otherwise false.
  bool reserveMemory(const RowVectorPtr& input);

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

  // Invoked to send group spill request to 'spillGroup_'. The function returns
  // true if group spill has been inline executed, otherwise returns false. In
  // the latter case, the operator will transition to 'kWaitForSpill' state and
  // 'input' will be saved in 'input_' to be processed after the group spill has
  // been executed.
  bool requestSpill(RowVectorPtr& input);

  // Invoked to check if it needs to wait for any pending group spill to run.
  // The function returns true if it needs to wait, otherwise false. The latter
  // case is either because there is no pending group spill or this operator is
  // the last one to reach to the group spill barrier and execute the group
  // spill inline.
  bool waitSpill(RowVectorPtr& input);

  // The callback registered to 'spillGroup_' to run group spill on
  // 'spillOperators'.
  void runSpill(const std::vector<Operator*>& spillOperators);

  // Invoked by 'runSpill' to sum up the spill targets from all the operators in
  // 'numRows' and 'numBytes'.
  void addAndClearSpillTarget(uint64_t& numRows, uint64_t& numBytes);

  // Invoked to reset the operator state to restore previously spilled data. It
  // setup (recursive) spiller and spill input reader from 'spillInput' received
  // from 'joinBride_'. 'spillInput' contains a shard of previously spilled
  // partition data. 'spillInput' also indicates if there is no more spill data,
  // then this operator will transition to 'kFinish' state to finish.
  void setupSpillInput(HashJoinBridge::SpillInput spillInput);

  // Invoked to process data from spill input reader on restoring.
  void processSpillInput();

  void addRuntimeStats();

  // Invoked to check if it needs to trigger spilling for test purpose only.
  bool testingTriggerSpill();

  const std::shared_ptr<const core::HashJoinNode> joinNode_;

  const core::JoinType joinType_;

  // Holds the areas in RowContainer of 'table_'
  memory::MappedMemory* const FOLLY_NONNULL mappedMemory_;

  const std::shared_ptr<HashJoinBridge> joinBridge_;

  const std::optional<Spiller::Config> spillConfig_;

  const std::shared_ptr<SpillOperatorGroup> spillGroup_;

  State state_{State::kRunning};

  // The row type used for hash table build and disk spilling.
  RowTypePtr tableType_;

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

  // True if this is a build side of an anti join and has at least one entry
  // with null join keys.
  bool antiJoinHasNullKeys_{false};

  // Counts input batches and triggers spilling if folly hash of this % 100 <=
  // 'testSpillPct_';.
  uint64_t spillTestCounter_{0};

  // The spill targets set by 'requestSpill()' to request group spill.
  uint64_t numSpillRows_{0};
  uint64_t numSpillBytes_{0};

  std::unique_ptr<Spiller> spiller_;

  // Used to read input from previously spilled data for restoring.
  std::unique_ptr<UnorderedStreamReader<BatchStream>> spillInputReader_;

  // Reusable memory for spill partition calculation for input data.
  std::vector<uint32_t> spillPartitions_;

  // Reusable memory for input spilling processing.
  std::vector<vector_size_t> numSpillInputs_;
  std::vector<BufferPtr> spillInputIndicesBuffers_;
  std::vector<vector_size_t*> rawSpillInputIndicesBuffers_;
  std::vector<VectorPtr> spillChildVectors_;
};

inline std::ostream& operator<<(std::ostream& os, HashBuild::State state) {
  os << HashBuild::stateName(state);
  return os;
}

} // namespace facebook::velox::exec
