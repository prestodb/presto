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

#include <folly/Range.h>
#include "velox/common/base/Semaphore.h"
#include "velox/common/caching/AsyncDataCache.h"
#include "velox/common/file/Region.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/experimental/wave/dwio/decode/DecodeStep.h"
#include "velox/experimental/wave/exec/OperandSet.h"
#include "velox/experimental/wave/vector/WaveVector.h"

namespace facebook::velox::wave {

class ReadStream;
class WaveStream;

/// Use generic bit set to track depemdemce pon staging.
using StagingSet = OperandSet;

// Describes how a column is staged on GPU, for example, copy from host RAM,
// direct read, already on device etc.
struct Staging {
  Staging(const void* hostData, int32_t size, const common::Region& region)
      : hostData(hostData),
        size(hostData ? size : region.length),
        fileOffset(region.offset) {}

  // Pointer to data in pageable host memory, if applicable.
  const void* hostData{nullptr};

  //  Size in bytes.
  size_t size;

  /// If 'hostData' is nullptr, this is the start offset for 'size'
  /// bytes in 'fileInfo_' of containing SplitStaging.
  int64_t fileOffset{0};
};

struct FileInfo {
  ReadFile* file{nullptr};
  StringIdLease* fileId{nullptr};
  cache::AsyncDataCache* cache{nullptr};
};

/// Describes how columns to be read together are staged on device. This is
/// anything from a set of host to device copies, GPU direct IO, or no-op if
/// data already on device.
class SplitStaging {
 public:
  /// id indicating no dependence on other staging.
  static constexpr int32_t kNoStaging = ~0;

  SplitStaging(FileInfo& fileInfo, int32_t id) : id_(id), fileInfo_(fileInfo) {}

  /// Adds a transfer described by 'staging'. Returns an id of the
  /// device side buffer. The id will be mapped to an actual buffer
  /// when the transfers are queud. At this time, pointers that
  /// are registered to the id are patched to the actual device side
  /// address.
  BufferId add(Staging& staging);

  /// Registers '*ptr' to be patched to the device side address of the transfer
  /// identified by 'id'. The *ptr is an offset into the buffer identified by
  /// id, so that the actual start of the area is added to the offset at *ptr.
  /// If 'clear' is true, *ptr is set to nullptr first. This may not be called
  /// after transfer().
  template <typename T>
  void registerPointer(BufferId id, T pointer, bool clear) {
    registerPointerInternal(
        id,
        reinterpret_cast<void**>(reinterpret_cast<uint64_t>(pointer)),
        clear);
  }

  int64_t bytesToDevice() const {
    return fill_;
  }
  // Starts the transfers registered with add( on 'stream'). Does
  // nothing after first call or if no pointers are registered. If
  // 'recordEvent' is true, records an event that is completed after
  // the transfer arrives. Use event() to access the event. If
  // 'asyncTail' is non-nullptr, it is called after the data transfer
  // is enqueued. The call is on an executor and transfer() returns as
  // soon as the work is enqueud. If asyncTail is not given,
  // transfer() returns after the transfer is enqueued on
  // 'stream'. event() is not set until the transfer is enqueued.
  void transfer(
      WaveStream& waveStream,
      Stream& stream,
      bool recordEvent = false,
      std::function<void(WaveStream&, Stream&)> asyncTail = nullptr);

  Event* event() const {
    return event_.get();
  }

  int32_t id() const {
    return id_;
  }

  void addDependency(int32_t id) {
    dependsOn_.add(id);
  }

  const StagingSet& dependsOn() {
    return dependsOn_;
  }

 private:
  void registerPointerInternal(BufferId id, void** ptr, bool clear);

  void copyColumns(int32_t begin, int32_t end, char* destination, bool release);

  const int32_t id_;

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

  // Optional event recorded after end of transfer. Use to sync dependent
  // kernels on other streams.
  std::unique_ptr<Event> event_;

  // Synchronizes arrival of multithreaded memcpy
  Semaphore sem_{0};

  FileInfo& fileInfo_;

  // Set of other SplitStaging ids of which 'this' is a
  // duplicate. These need to be complete efore dependents of 'this'
  // can run. A staging can both have transfers and dependencies.
  StagingSet dependsOn_;
};

using RowSet = folly::Range<const int32_t*>;
class ColumnReader;

/// Information that allows a column to be read in parallel independent thread
/// blocks. This represents an array of starting points inside the encoded
/// column.
struct ColumnGridInfo {
  /// Number of independently schedulable blocks.
  int32_t numBlocks;

  ///
  BlockStatus* status{nullptr};

  /// Device readable nulls as a flat bitmap. 1 is non-null. nullptr means
  /// non-null.
  char* nulls{nullptr};

  /// Device side array of non-null counts. Decoding for values for the ith
  /// block starts at index 'nonNullCount[i - 1]' in encoded values. nullptr if
  /// non nulls.
  int32_t* numNonNull{nullptr};
};

// Specifies an action on a column. A column is not indivisible. It
// has parts and another column's decode may depend on one part of
// another column but not another., e.g. a child of a nullable struct
// needs the nulls of the struct but no other parts to decode.
enum class ColumnAction { kNulls = 1, kLengths = 2, kFilter = 4, kValues = 8 };

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

  // Id of 'deviceResult' from resultStaging. A subsequent op must refer to the
  // result of the previous one before the former is allocated.
  BufferId deviceResultId{kNoBufferId};

  // Id of extra filter passing row count. Needed for aligning values from
  // non-last filtered columns to final.
  int32_t* extraRowCount{nullptr};

  BufferId extraRowCountId{kNoBufferId};

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

  /// Schedules operations for preparing the encoded data to be
  /// consumed in 'numBlocks' parallel blocks of 'blockSize' rows. For
  /// example, for a column of 11M nullable varints, this with 1024
  /// blocksize and 2048 blocks, this would count 2M bits and write a
  /// prefix sum every 1K bits, so that we know the corresponding
  /// position in the varints for non-nulls. Then for the varints, we
  /// write the starting offset every 1K nulls, e.g, supposing 2 bytes
  /// per varint and 800 non-nulls for every 1K bits, we get 0, 1600,
  /// 3600, ... as starts for the varints. The FormatData stores the
  /// intermediates. This is a no-op for encodings that are random
  /// access capable, e.g. non-null bit packings. this is a also a
  /// no-op if there are less than 'blockSize' rows left.
  virtual void griddize(
      int32_t blockSize,
      int32_t numBlocks,
      ResultStaging& deviceStaging,
      ResultStaging& resultStaging,
      SplitStaging& staging,
      DecodePrograms& program,
      ReadStream& stream) = 0;

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

 protected:
  std::unique_ptr<GpuDecode> makeStep(
      ColumnOp& op,
      const ColumnOp* previousFilter,
      ResultStaging& deviceStaging,
      SplitStaging& splitStaging,
      ReadStream& stream,
      WaveTypeKind columnKind,
      int32_t blockIdx);

  // Staging id for nulls.
  int32_t nullsStagingId_{SplitStaging::kNoStaging};
  // Id for nulls buffer. The nulls buffer has no address at time of scheduling
  // if nulls decode is in the same kernel as decoding.
  BufferId nullsBufferId_{kNoBufferId};
  // id of last splitStaging 'this' depends on.
  int32_t lastStagingId_{SplitStaging::kNoStaging};

  // First unaccessed row number relative to start of 'this'.
  int32_t currentRow_{0};

  ColumnGridInfo grid_;
  bool griddized_{false};
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
