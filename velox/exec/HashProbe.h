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

#include "velox/exec/HashBuild.h"
#include "velox/exec/HashTable.h"
#include "velox/exec/Operator.h"
#include "velox/exec/VectorHasher.h"

namespace facebook::velox::exec {

// Probes a hash table made by HashBuild.
class HashProbe : public Operator {
 public:
  HashProbe(
      int32_t operatorId,
      DriverCtx* driverCtx,
      std::shared_ptr<const core::HashJoinNode> hashJoinNode);

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool needsInput() const override {
    return !isFinishing_ && !input_;
  }

  BlockingReason isBlocked(ContinueFuture* future) override;

  void close() override {}

 private:
  // TODO: Define batch size as bytes based on RowContainer row sizes.
  static constexpr int32_t kOutputBatchSize = 1'000;

  // Sets up 'filter_' and related members.
  void initializeFilter(
      const std::shared_ptr<const core::ITypedExpr>& filter,
      const RowTypePtr& probeType,
      const RowTypePtr& tableType);

  // Applies 'filter_' to 'outputRows_' and updates 'outputRows_' and
  // 'rowNumberMapping_'. Returns the number of passing rows.
  vector_size_t evalFilter(vector_size_t numRows);

  std::unique_ptr<HashLookup> lookup_;

  // Channel of probe keys in 'input_'.
  std::vector<ChannelIndex> keyChannels_;

  std::vector<std::unique_ptr<VectorHasher>> hashers_;

  // Table shared between other HashProbes in other Drivers of the
  // same pipeline.
  std::shared_ptr<BaseHashTable> table_;

  // Decodes probe keys when 'table_' is in normalized key  or array hash mode.
  DecodedVector valueIdDecoder_;

  // Temporary for de-duplicating value ids for dictionary inputs.
  std::vector<uint64_t> deduppedHashes_;

  // Rows to apply 'filter_' to.
  SelectivityVector filterRows_;

  // Join filter.
  std::unique_ptr<ExprSet> filter_;
  std::vector<VectorPtr> filterResult_;
  DecodedVector decodedFilterResult_;

  // Type of the RowVector for filter inputs.
  RowTypePtr filterInputType_;

  // Maps input channels to channels in 'filterInputType_'.
  std::vector<IdentityProjection> filterProbeInputs_;

  // Maps from column index in hash table to channel in 'filterInputType_'.
  std::vector<IdentityProjection> filterBuildInputs_;

  // Temporary projection from probe and build for evaluating
  // 'filter_'. This can always be reused since this does not escape
  // this operator.
  RowVectorPtr filterInput_;

  // Row number in 'input_' for each row of output.
  BufferPtr rowNumberMapping_;

  // maps from column index in 'table_' to channel in 'output_'.
  std::vector<IdentityProjection> tableResultProjections_;

  // Rows of table found by join probe, later filtered by 'filter_'.
  std::vector<char*> outputRows_;

  // Keeps track of returned results between successive batches of
  // output for a batch of input.
  BaseHashTable::JoinResultIterator results;

  // Active rows in the current batch of input.
  SelectivityVector activeRows_;
};

} // namespace facebook::velox::exec
