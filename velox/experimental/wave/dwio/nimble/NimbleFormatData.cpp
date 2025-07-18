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

#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"

DECLARE_int32(wave_reader_rows_per_tb);

namespace facebook::velox::wave {
std::unique_ptr<FormatData> NimbleFormatParams::toFormatData(
    const std::shared_ptr<const dwio::common::TypeWithId>& type,
    const velox::common::ScanSpec& scanSpec,
    OperandId operand) {
  std::vector<NimbleChunkDecodePipeline> pipelines;
  if (type->type()->kind() == TypeKind::ROW) {
    return std::make_unique<NimbleFormatData>(
        operand, stripe_.totalRows(), std::move(pipelines));
  }
  auto* stream = getChunkedStream(*type);
  VELOX_CHECK_NOT_NULL(stream, "NimbleChunkedStream is null.");
  while (stream->hasNext()) {
    auto chunk = stream->nextChunk();
    pipelines.emplace_back(
        NimbleChunk::parseEncodingFromChunk(chunk.chunkData()));
  }
  int32_t totalRows = 0;
  for (auto& pipeline : pipelines) {
    totalRows += pipeline[pipeline.size() - 1].numValues();
  }

  VELOX_CHECK(totalRows == stripe_.totalRows());
  return std::make_unique<NimbleFormatData>(
      operand, totalRows, std::move(pipelines));
}

BufferId NimbleFormatData::stageNulls(
    ResultStaging& deviceStaging,
    SplitStaging& splitStaging) {
  if (!hasNulls()) {
    nullsStaged_ = true;
    return kNotRegistered;
  } else {
    VELOX_NYI("Nulls are not supported yet");
  }
}

void NimbleFormatData::griddize(
    ColumnOp& op,
    int32_t blockSize,
    int32_t numBlocks,
    ResultStaging& deviceStaging,
    ResultStaging& resultStaging,
    SplitStaging& staging,
    DecodePrograms& programs,
    ReadStream& stream) {
  griddized_ = true;
  return;
}

void NimbleFormatData::startOp(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ResultStaging& deviceStaging,
    ResultStaging& resultStaging,
    SplitStaging& splitStaging,
    DecodePrograms& program,
    ReadStream& stream) {
  op.isFinal = true;

  for (size_t i = 0; i < pipelines_.size(); ++i) {
    auto& pipeline = pipelines_[i];
    auto& rootEncoding = pipeline.rootEncoding();
    BufferId id = kNoBufferId;
    if (!streamStaged_) {
      Staging staging(
          rootEncoding.encodedDataPtr(),
          rootEncoding.encodedData().size(),
          common::Region{0, 0}); // TODO(bowenwu): revisit the
                                 // region construction here
      id = splitStaging.add(staging);
      lastStagingId_ = splitStaging.id();
      splitStaging.registerPointer(
          id, rootEncoding.deviceEncodedDataPtrPtr(), true);
    } else {
      VELOX_CHECK_NOT_NULL(rootEncoding.deviceEncodedDataPtr());
    }

    while (auto encoding = pipeline.next()) {
      auto offset = encoding == &rootEncoding ? offsets_[i] : 0;
      auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
      int32_t numBlocks =
          bits::roundUp(encoding->numValues(), rowsPerBlock) / rowsPerBlock;
      if (numBlocks > program.programs.size()) {
        program.programs.resize(numBlocks);
      }
      if (numBlocks > 1) {
        VELOX_CHECK(griddized_);
      }
      VELOX_CHECK_LT(numBlocks, 256 * 256, "Overflow 16 bit block number");
      for (auto blockIdx = 0; blockIdx < numBlocks; ++blockIdx) {
        auto step = encoding->makeStep(
            op,
            stream,
            splitStaging,
            deviceStaging,
            id,
            rootEncoding,
            blockIdx,
            offset);
        if (step) {
          program.programs[blockIdx].push_back(std::move(step));
        }
      }
    }

    op.isFinal &= pipeline.finished();
  }
  streamStaged_ = true;
}

// NimbleChunkDecodePipeline implementation
NimbleChunkDecodePipeline::NimbleChunkDecodePipeline(
    std::unique_ptr<NimbleEncoding> encoding)
    : encoding_(std::move(encoding)) {
  if (!encoding_) {
    return;
  }

  std::queue<NimbleEncoding*> queue;
  queue.push(encoding_.get());

  while (!queue.empty()) {
    NimbleEncoding* node = queue.front();
    queue.pop();
    pipeline_.push_back(node);

    for (uint8_t i = 0; i < node->childrenCount(); ++i) {
      queue.push(node->childAt(i));
    }
  }

  std::reverse(pipeline_.begin(), pipeline_.end());

  // Reorder pipeline to ensure all leaves come first
  std::stable_partition(
      pipeline_.begin(), pipeline_.end(), [](NimbleEncoding* node) {
        return node->childrenCount() == 0;
      });

  currentEncoding_ = pipeline_.begin();
}

NimbleEncoding* NimbleChunkDecodePipeline::next() {
  if (finished() || !((*currentEncoding_)->isReadyToDecode())) {
    for (auto it = currentEncoding_ - 1;
         it >= pipeline_.begin() && !(*it)->isDecoded();
         --it) {
      (*it)->setDecoded();
    }
    return nullptr;
  }

  return *currentEncoding_++;
}

bool NimbleChunkDecodePipeline::finished() const {
  return currentEncoding_ == pipeline_.end();
}

} // namespace facebook::velox::wave
