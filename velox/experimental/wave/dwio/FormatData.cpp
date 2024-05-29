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

#include "velox/experimental/wave/dwio/FormatData.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"

DECLARE_int32(wave_reader_rows_per_tb);

namespace facebook::velox::wave {

BufferId SplitStaging::add(Staging& staging) {
  VELOX_CHECK_NULL(deviceBuffer_);
  staging_.push_back(staging);
  offsets_.push_back(fill_);
  fill_ += bits::roundUp(staging.size, 8);
  return offsets_.size() - 1;
}

void SplitStaging::registerPointerInternal(
    BufferId id,
    void** ptr,
    bool clear) {
  VELOX_CHECK_NOT_NULL(ptr);
  VELOX_CHECK_NULL(deviceBuffer_);
  if (clear) {
    *ptr = nullptr;
  }
  patch_.push_back(std::make_pair(id, ptr));
}

// Starts the transfers registered with add(). 'stream' is set to a stream
// where operations depending on the transfer may be queued.
void SplitStaging::transfer(WaveStream& waveStream, Stream& stream) {
  if (fill_ == 0) {
    return;
  }
  deviceBuffer_ = waveStream.arena().allocate<char>(fill_);
  auto universal = deviceBuffer_->as<char>();
  for (auto i = 0; i < offsets_.size(); ++i) {
    memcpy(universal + offsets_[i], staging_[i].hostData, staging_[i].size);
  }
  stream.prefetch(
      getDevice(), deviceBuffer_->as<char>(), deviceBuffer_->size());
  for (auto& pair : patch_) {
    *reinterpret_cast<int64_t*>(pair.second) +=
        reinterpret_cast<int64_t>(universal) + offsets_[pair.first];
  }
}

BufferId ResultStaging::reserve(int32_t bytes) {
  offsets_.push_back(fill_);
  fill_ += bits::roundUp(bytes, 8);
  return offsets_.size() - 1;
}

void ResultStaging::registerPointerInternal(
    BufferId id,
    void** pointer,
    bool clear) {
  VELOX_CHECK_LT(id, offsets_.size());
  VELOX_CHECK_NOT_NULL(pointer);
#ifndef NDEBUG
  for (auto& pair : patch_) {
    VELOX_CHECK(
        pair.second != pointer, "Must not register the same pointer twice");
  }
#endif
  if (clear) {
    *pointer = nullptr;
  }
  patch_.push_back(std::make_pair(id, pointer));
}

void ResultStaging::makeDeviceBuffer(GpuArena& arena) {
  if (fill_ == 0) {
    return;
  }
  WaveBufferPtr buffer = arena.allocate<char>(fill_);
  auto address = reinterpret_cast<int64_t>(buffer->as<char>());
  // Patch all the registered pointers to point to buffer at offset offset_[id]
  // + the offset already in the registered pointer.
  for (auto& pair : patch_) {
    int64_t* pointer = reinterpret_cast<int64_t*>(pair.second);
    *pointer += address + offsets_[pair.first];
  }
  patch_.clear();
  offsets_.clear();
  fill_ = 0;
  buffers_.push_back(std::move(buffer));
}

void ResultStaging::setReturnBuffer(GpuArena& arena, DecodePrograms& programs) {
  if (fill_ == 0) {
    programs.result = nullptr;
    programs.hostResult = nullptr;
    return;
  }
  programs.result = arena.allocate<char>(fill_);
  auto address = reinterpret_cast<int64_t>(programs.result->as<char>());
  // Patch all the registered pointers to point to buffer at offset offset_[id]
  // + the offset already in the registered pointer.
  for (auto& pair : patch_) {
    int64_t* pointer = reinterpret_cast<int64_t*>(pair.second);
    *pointer += address + offsets_[pair.first];
  }
  patch_.clear();
  offsets_.clear();
  fill_ = 0;
}

namespace {
void setFilter(GpuDecode* step, ColumnReader* reader, Stream* stream) {
  auto* veloxFilter = reader->scanSpec().filter();
  if (!veloxFilter) {
    step->filterKind = WaveFilterKind::kAlwaysTrue;
    return;
  }
  switch (veloxFilter->kind()) {
    case common::FilterKind::kBigintRange: {
      step->filterKind = WaveFilterKind::kBigintRange;
      step->nullsAllowed = veloxFilter->testNull();
      step->filter._.int64Range[0] =
          reinterpret_cast<common::BigintRange*>(veloxFilter)->lower();
      step->filter._.int64Range[1] =
          reinterpret_cast<common::BigintRange*>(veloxFilter)->upper();
      break;
    }

    default:
      VELOX_UNSUPPORTED(
          "Unsupported filter kind", static_cast<int32_t>(veloxFilter->kind()));
  }
}
} // namespace

std::unique_ptr<GpuDecode> FormatData::makeStep(
    ColumnOp& op,
    const ColumnOp* previousFilter,
    ResultStaging& deviceStaging,
    ReadStream& stream,
    WaveTypeKind columnKind,
    int32_t blockIdx) {
  auto rowsPerBlock = FLAGS_wave_reader_rows_per_tb;
  int32_t numBlocks =
      bits::roundUp(op.rows.size(), rowsPerBlock) / rowsPerBlock;

  auto rowsInBlock = std::min<int32_t>(
      rowsPerBlock, op.rows.size() - (blockIdx * rowsPerBlock));

  auto step = std::make_unique<GpuDecode>();
  if (grid_.nulls) {
    step->nonNullBases = grid_.numNonNull;
  }
  step->numRowsPerThread = rowsPerBlock / kBlockSize;
  setFilter(step.get(), op.reader, nullptr);
  bool dense = previousFilter == nullptr &&
      simd::isDense(op.rows.data(), op.rows.size());
  step->nullMode = grid_.nulls
      ? (dense ? NullMode::kDenseNullable : NullMode::kSparseNullable)
      : (dense ? NullMode::kDenseNonNull : NullMode::kSparseNonNull);
  step->nthBlock = blockIdx;
  if (step->filterKind != WaveFilterKind::kAlwaysTrue && op.waveVector) {
    /// Filtres get to record an extra copy of their passing rows if they make
    /// values.
    if (blockIdx == 0) {
      op.extraRowCountId = deviceStaging.reserve(
          numBlocks * step->numRowsPerThread * sizeof(int32_t));
      deviceStaging.registerPointer(
          op.extraRowCountId, &step->filterRowCount, true);
      deviceStaging.registerPointer(
          op.extraRowCountId, &op.extraRowCount, true);
    } else {
      step->filterRowCount = reinterpret_cast<int32_t*>(
          blockIdx * sizeof(int32_t) * step->numRowsPerThread);
      deviceStaging.registerPointer(
          op.extraRowCountId, &step->filterRowCount, false);
    }
  }
  step->dataType = columnKind;
  auto kindSize = waveTypeKindSize(columnKind);
  step->step =
      kindSize == 4 ? DecodeStep::kSelective32 : DecodeStep::kSelective64;
  if (previousFilter) {
    step->maxRow = GpuDecode::kFilterHits;
  } else {
    step->maxRow = currentRow_ + (blockIdx * rowsPerBlock) + rowsInBlock;
  }
  step->baseRow = currentRow_ + rowsPerBlock * blockIdx;

  if (op.waveVector) {
    if (blockIdx == 0) {
      op.waveVector->resize(op.rows.size(), false);
    }
    step->result =
        op.waveVector->values<char>() + kindSize * blockIdx * rowsPerBlock;
    step->resultNulls = op.waveVector->nulls()
        ? op.waveVector->nulls() + rowsPerBlock + blockIdx
        : nullptr;

    if (previousFilter) {
      if (previousFilter->deviceResult) {
        // This is when the previous filter is in the previous kernel and its
        // device side result is allocated.
        step->rows = previousFilter->deviceResult + blockIdx * rowsPerBlock;
      } else {
        step->rows = reinterpret_cast<int32_t*>(
            blockIdx * rowsPerBlock * sizeof(int32_t));
        deviceStaging.registerPointer(
            previousFilter->deviceResultId, &step->rows, false);
      }
    }
    if (op.reader->scanSpec().filter()) {
      if (blockIdx == 0) {
        op.deviceResultId =
            deviceStaging.reserve(op.rows.size() * sizeof(int32_t));
        deviceStaging.registerPointer(
            op.deviceResultId, &step->resultRows, true);
        deviceStaging.registerPointer(
            op.deviceResultId, &op.deviceResult, true);
      } else {
        step->resultRows = reinterpret_cast<int32_t*>(
            blockIdx * rowsPerBlock * sizeof(int32_t));
        deviceStaging.registerPointer(
            op.deviceResultId, &step->resultRows, false);
      }
    }
  }
  return step;
}

} // namespace facebook::velox::wave
