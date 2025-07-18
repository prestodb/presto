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

#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/FormatData.h"
#include "velox/experimental/wave/dwio/decode/DecodeStep.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

class NimbleChunkDecodePipeline {
 public:
  NimbleChunkDecodePipeline(std::unique_ptr<NimbleEncoding> encoding);
  NimbleChunkDecodePipeline(NimbleChunk const& chunk);
  NimbleEncoding* next();
  bool finished() const;
  uint32_t size() const {
    return pipeline_.size();
  }
  NimbleEncoding& operator[](uint32_t index) {
    return *pipeline_[index];
  }
  NimbleEncoding& rootEncoding() const {
    return *encoding_;
  }

 private:
  std::unique_ptr<NimbleEncoding> encoding_;
  std::vector<NimbleEncoding*> pipeline_;
  typename std::vector<NimbleEncoding*>::iterator currentEncoding_;
};

class NimbleFormatData : public wave::FormatData {
 public:
  static constexpr int32_t kNotRegistered = -1;

  NimbleFormatData(
      OperandId operand,
      int32_t totalRows,
      std::vector<NimbleChunkDecodePipeline>&& pipelines)
      : operand_(operand),
        totalRows_(totalRows),
        pipelines_(std::move(pipelines)) {
    offsets_.resize(pipelines_.size());
    int32_t sum = 0;
    for (size_t i = 0; i < pipelines_.size(); ++i) {
      offsets_[i] = sum;
      sum += pipelines_[i].rootEncoding().numValues();
    }
  }

  bool hasNulls() const override {
    for (int i = 0; i < pipelines_.size(); i++) {
      auto& pipeline = pipelines_[i];
      if (pipeline.rootEncoding().isNullableEncoding()) {
        return true;
      }
    }
    return false;
  }

  int32_t totalRows() const override {
    return totalRows_;
  }

  void newBatch(int32_t startRow) override {
    currentRow_ = startRow;
    queued_ = false;
  }

  void griddize(
      ColumnOp& op,
      int32_t blockSize,
      int32_t numBlocks,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& programs,
      ReadStream& stream) override;

  void startOp(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& program,
      ReadStream& stream) override;

 private:
  // Stages movement of nulls to device if any. Returns the id of the buffer or
  // kNotRegisterd.
  int32_t stageNulls(ResultStaging& deviceStaging, SplitStaging& splitStaging);

  const OperandId operand_;
  bool queued_{false};
  int32_t totalRows_{0};
  bool nullsStaged_{false};
  bool streamStaged_{false};

  const NimbleEncoding* encoding_;
  std::vector<NimbleChunkDecodePipeline> pipelines_;
  std::vector<int32_t> offsets_;
};

class NimbleFormatParams : public wave::FormatParams {
 public:
  NimbleFormatParams(
      memory::MemoryPool& pool,
      dwio::common::ColumnReaderStatistics& stats,
      NimbleStripe& stripe)
      : FormatParams(pool, stats), stripe_(stripe) {}

  std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const velox::common::ScanSpec& scanSpec,
      OperandId operand) override;

  NimbleChunkedStream* getChunkedStream(
      const dwio::common::TypeWithId& child) const {
    return stripe_.findStream(child);
  }

  NimbleStripe& stripe() {
    return stripe_;
  }

 private:
  NimbleStripe& stripe_;
};

} // namespace facebook::velox::wave
