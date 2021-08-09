/*
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
#include "velox/exec/VectorHasher.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

// Hands over a hash table from a multithreaded build pipeline to a
// multithreaded probe pipeline. This is owned by shared_ptr by all the build
// and probe Operator instances concerned. Corresponds to the Presto concept of
// the same name.
class JoinBridge {
 public:
  void setHashTable(std::unique_ptr<BaseHashTable> table);

  // Sets this to a cancelled state and unblocks any waiting
  // activity. This may happen asynchronously before or after the hash
  // table has been set.
  void cancel();

  std::shared_ptr<BaseHashTable> tableOrFuture(ContinueFuture* future);

 private:
  std::mutex mutex_;
  std::shared_ptr<BaseHashTable> table_;
  std::vector<VeloxPromise<bool>> promises_;
  bool cancelled_{false};
};

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
  HashBuild(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::HashJoinNode> joinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override {
    return nullptr;
  }

  bool needsInput() const override {
    return !isFinishing_;
  }

  void finish() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  void close() override {}

 private:
  // Container for the rows being accumulated.
  std::unique_ptr<HashTable<true>> table_;

  // Key channels in 'input_'
  std::vector<ChannelIndex> keyChannels_;

  // Non-key channels in 'input_'.
  std::vector<ChannelIndex> dependentChannels_;

  // Corresponds 1:1 to 'dependentChannels_'.
  std::vector<std::unique_ptr<DecodedVector>> decoders_;

  // Holds the areas in RowContainer of 'table_'
  memory::MappedMemory* mappedMemory_;

  // Future for synchronizing with other Drivers of the same pipeline. All build
  // Drivers must be completed before making the hash table.
  ContinueFuture future_;
  bool hasFuture_ = false;

  // True if we are considering use of normalized keys or array hash tables. Set
  // to false when the dataset is no longer suitable.
  bool analyzeKeys_;

  // Temporary space for hash numbers.
  std::vector<uint64_t> hashes_;

  // Set of active rows during addInput().
  SelectivityVector activeRows_;
};

} // namespace facebook::velox::exec
