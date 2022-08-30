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
#include "velox/exec/Operator.h"
#include "velox/exec/VectorHasher.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::exec {

// Hands over a hash table from a multi-threaded build pipeline to a
// multi-threaded probe pipeline. This is owned by shared_ptr by all the build
// and probe Operator instances concerned. Corresponds to the Presto concept of
// the same name.
class HashJoinBridge : public JoinBridge {
 public:
  void setHashTable(std::unique_ptr<BaseHashTable> table);

  void setAntiJoinHasNullKeys();

  // Represents the result of a HashBuild operator: a hash table. In case of an
  // anti join, a build side entry with a null in a join key makes the join
  // return nothing. In this case, HashBuild operator finishes early without
  // processing all the input and without finishing building the hash table.
  struct HashBuildResult {
    std::shared_ptr<BaseHashTable> table;
    bool antiJoinHasNullKeys;
  };

  std::optional<HashBuildResult> tableOrFuture(ContinueFuture* future);

 private:
  std::shared_ptr<BaseHashTable> table_;
  bool antiJoinHasNullKeys_{false};
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
    return !noMoreInput_;
  }

  void noMoreInput() override;

  BlockingReason isBlocked(ContinueFuture* future) override;

  bool isFinished() override;

  void close() override {}

 private:
  // Invoked to setup hash table to build.
  void setupTable();

  void addRuntimeStats();

  const std::shared_ptr<const core::HashJoinNode> joinNode_;

  const core::JoinType joinType_;

  // Holds the areas in RowContainer of 'table_'
  memory::MappedMemory* const mappedMemory_; // Not owned.

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
};

} // namespace facebook::velox::exec
