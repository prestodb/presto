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
#include "velox/exec/Operator.h"

namespace facebook::velox::exec {
class Unnest : public Operator {
 public:
  Unnest(
      int32_t operatorId,
      DriverCtx* driverCtx,
      const std::shared_ptr<const core::UnnestNode>& unnestNode);

  BlockingReason isBlocked(ContinueFuture* /*future*/) override {
    return BlockingReason::kNotBlocked;
  }

  bool needsInput() const override {
    return input_ == nullptr;
  }

  void addInput(RowVectorPtr input) override;

  RowVectorPtr getOutput() override;

  bool isFinished() override;

 private:
  // Generate output for 'size' input rows starting from 'start' input row.
  //
  // @param start First input row to include in the output.
  // @param size Number of input rows to include in the output.
  // @param outputSize Pre-computed number of output rows.
  RowVectorPtr generateOutput(
      vector_size_t start,
      vector_size_t size,
      vector_size_t outputSize);

  // Invoked by generateOutput function above to generate the repeated output
  // columns.
  void generateRepeatedColumns(
      vector_size_t start,
      vector_size_t size,
      vector_size_t numElements,
      std::vector<VectorPtr>& outputs);

  struct UnnestChannelEncoding {
    BufferPtr indices;
    BufferPtr nulls;
    bool identityMapping;

    VectorPtr wrap(const VectorPtr& base, vector_size_t wrapSize) const;
  };

  // Invoked by generateOutput above to generate the encoding for the unnested
  // Array or Map.
  const UnnestChannelEncoding generateEncodingForChannel(
      column_index_t channel,
      vector_size_t start,
      vector_size_t size,
      vector_size_t numElements);

  // Invoked by generateOutput for the ordinality column.
  VectorPtr generateOrdinalityVector(
      vector_size_t start,
      vector_size_t size,
      vector_size_t numElements);

  const bool withOrdinality_;
  std::vector<column_index_t> unnestChannels_;

  std::vector<DecodedVector> unnestDecoded_;

  BufferPtr maxSizes_;
  vector_size_t* rawMaxSizes_{nullptr};

  std::vector<const vector_size_t*> rawSizes_;
  std::vector<const vector_size_t*> rawOffsets_;
  std::vector<const vector_size_t*> rawIndices_;

  // Next 'input_' row to process in getOutput().
  vector_size_t nextInputRow_{0};
};
} // namespace facebook::velox::exec
