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
#include "velox/experimental/wave/dwio/decode/DecodeStep.h"
#include "velox/experimental/wave/vector/WaveVector.h"

#include <folly/Range.h>

namespace facebook::velox::wave {
using BufferId = int32_t;
constexpr BufferId kNoBufferId = -1;

class ReadStream;
class WaveStream;

// Describes how a column is staged on GPU, for example, copy from host RAM,
// direct read, already on device etc.
struct Staging {
  // Pointer to data in pageable host memory, if applicable.
  const void* hostData{nullptr};

  //  Size in bytes.
  size_t size;

  // Add members here to describe locations in storage for GPU direct transfer.
};

/// Describes how columns to be read together are staged on device. This is
/// anything from a set of host to device copies, GPU direct IO, or no-op if
/// data already on device.
class SplitStaging {
 public:
  /// Adds a transfer described by 'staging'. Returns an id of the
  /// device side buffer. The id will be mapped to an actual buffer
  /// when the transfers are queud. At this time, pointers that
  /// are registered to the id are patched to the actual device side
  /// address.
  BufferId add(Staging& staging);

  /// Registers '*ptr' to be patched to the device side address of the transfer
  /// identified by 'id'. The *ptr is an offset into the buffer identified by
  /// id, so that the actual start of the area is added to the offset at *ptr.
  template <typename T>
  void registerPointer(BufferId id, T pointer) {
    registerPointerInternal(
        id, reinterpret_cast<void**>(reinterpret_cast<uint64_t>(pointer)));
  }

  // Starts the transfers registered with add( on 'stream').
  void transfer(WaveStream& waveStream, Stream& stream);

 private:
  void registerPointerInternal(BufferId id, void** ptr);

  // Pinned host memory for transfer to device. May be nullptr if using unified
  // memory.
  WaveBufferPtr hostBuffer_;

  // Device accessible memory (device or unified) with the data to read.
  WaveBufferPtr deviceBuffer_;

  std::vector<Staging> staging_;
  // Offsets into device buffer for each id returned by add().
  std::vector<int64_t> offsets_;

  // List of pointers to patch to places inside deviceBuffer once this is
  // allocated.
  std::vector<std::pair<int32_t, void**>> patch_;

  // Total device side space reserved so farr.
  int64_t fill_{0};
};

class ResultStaging {
 public:
  /// Reserves 'bytes' bytes in result buffer to be brought to host after
  /// Decodeprograms completes on device.
  BufferId reserve(int32_t bytes);

  /// Registers '*pointer' to be patched to the buffer. The starting address of
  /// the buffer is added to *pointer, so that if *pointer was 16, *pointer will
  /// come to point to the 16th byte in the buffer.
  template <typename T>
  void registerPointer(BufferId id, T pointer) {
    registerPointerInternal(
        id, reinterpret_cast<void**>(reinterpret_cast<uint64_t>(pointer)));
  }

  void setReturnBuffer(GpuArena& arena, DecodePrograms& programs);

 private:
  void registerPointerInternal(BufferId id, void** pointer);

  // Offset of each result in either buffer.
  std::vector<int32_t> offsets_;
  // Patch addresses. The int64_t* is updated to point to the result buffer once
  // it is allocated.
  std::vector<std::pair<int32_t, void**>> patch_;
  int32_t fill_{0};
  WaveBufferPtr deviceBuffer_;
  WaveBufferPtr hostBuffer_;
};

using RowSet = folly::Range<const int32_t*>;
class ColumnReader;

// Specifies an action on a column. A column is not indivisible. It
// has parts and another column's decode may depend on one part of
// another column but not another., e.g. a child of a nullable struct
// needs the nulls of the struct but no other parts to decode.
enum class ColumnAction { kNulls, kFilter, kLengths, kValues };

/// A generic description of a decode step. The actual steps are
/// provided by FormatData specializations but this captures
/// dependences, e.g. filters before non-filters, nulls and lengths
/// of repeated containers before decoding the values. A dependency
/// can be device side only or may need host decision. Items that
/// depend device side can be made into consecutive decode ops in
/// one kernel launch or can be in consecutively queued
/// kernels. dependences which need host require the prerequisite
/// kernel to ship data to host, which will sync on the stream and
/// only then may schedule the dependents in another kernel.
struct ColumnOp {
  static constexpr int32_t kNoPrerequisite = -1;
  static constexpr int32_t kNoOperand = -1;

  // Is the op completed after this? If so, any dependent action can be
  // queued as soon as this is set.
  bool isFinal{false};
  // True if needs a result on the host before proceeding.
  bool needsResult{false};
  OperandId producesOperand{kNoOperand};
  // Index of another op in column ops array in ReadStream.
  int32_t prerequisite{kNoPrerequisite};
  ColumnAction action;
  // Non-owning view on rows to read.
  RowSet rows;
  ColumnReader* reader{nullptr};
  // Vector completed by arrival of this. nullptr if no vector.
  WaveVector* waveVector{nullptr};
  // Host side result size. 0 for unconditional decoding. Can be buffer size for
  // passing rows, length/offset array etc.
  int32_t resultSize{0};

  // Device side non-vector result, like set of passing rows, array of
  // lengths/starts etc.
  int32_t* deviceResult{nullptr};
  int32_t* hostResult{nullptr};
};

/// Operations on leaf columns. This is specialized for each file format.
class FormatData {
 public:
  virtual ~FormatData() = default;

  virtual int32_t totalRows() const = 0;

  virtual bool hasNulls() const = 0;

  /// Enqueues read of 'numRows' worth of null flags.  Returns the id of the
  /// result area allocated from 'deviceStaging'.
  virtual BufferId readNulls(
      int32_t numRows,
      ResultStaging& deviceStaging,
      SplitStaging& stageing,
      DecodePrograms& programs) {
    VELOX_NYI();
  }

  /// Sets how many TBs will be scheduled at a time for this column.
  void setBlocks(int32_t numBlocks) {
    VELOX_NYI();
  }

  /// Returns estimate of sequential instructions needed to decode one value.
  /// Used to decide how many TBs to use for each column.
  virtual float cost(const ColumnOp& op) {
    return 10;
  }

  /// Prepares a new batch of reads. The batch starts at 'startRiw', which is a
  /// row number in terms of the column of 'this'. The row number for a nested
  /// column is in terms of the column, not in terms of top level rows.
  virtual void newBatch(int32_t startRow) = 0;

  /// Adds the next read of the column. If the column is a filter depending on
  /// another filter, the previous filter is given on the first call. Updates
  /// status of 'op'.
  virtual void startOp(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& program,
      ReadStream& stream) = 0;
};

class FormatParams {
 public:
  explicit FormatParams(
      memory::MemoryPool& pool,
      dwio::common::ColumnReaderStatistics& stats)
      : pool_(pool), stats_(stats) {}

  virtual ~FormatParams() = default;

  /// Makes format-specific structures for the column given by  'type'.
  /// 'scanSpec' is given as extra context.
  virtual std::unique_ptr<FormatData> toFormatData(
      const std::shared_ptr<const dwio::common::TypeWithId>& type,
      const velox::common::ScanSpec& scanSpec,
      OperandId operand) = 0;

  memory::MemoryPool& pool() {
    return pool_;
  }

  dwio::common::ColumnReaderStatistics& runtimeStatistics() {
    return stats_;
  }

 private:
  memory::MemoryPool& pool_;
  dwio::common::ColumnReaderStatistics& stats_;
  int32_t currentRow_{0};
};

}; // namespace facebook::velox::wave
