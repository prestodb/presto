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

#include "velox/experimental/wave/exec/Wave.h"

namespace facebook::velox::wave {

std::string rowTypeString(const Type& type) {
  return "";
}

std::string AdvanceResult::toString() const {
  if (empty()) {
    return "AdvanceResult::empty";
  }
  return fmt::format(
      "AdvanceResult(.numRows={}, .isRetry={}, .sync={})",
      numRows,
      isRetry,
      syncDrivers       ? "drivers"
          : syncStreams ? "streams"
                        : "none");
}

void AbstractAggregation::reserveState(InstructionStatus& reservedState) {
  instructionStatus = reservedState;
  // A group by produces 8 bytes of grid level state and uses the main main
  // BlockStatus for lane status.
  reservedState.gridState += sizeof(AggregateReturn);
}

int32_t countErrors(BlockStatus* status, int32_t numBlocks, ErrorCode error) {
  int32_t count = 0;
  for (auto i = 0; i < numBlocks; ++i) {
    for (auto j = 0; j < status[i].numRows; ++j) {
      count += status[i].errors[j] == error;
    }
  }
  return count;
}

void restockAllocator(
    AggregateOperatorState& state,
    GpuArena& arena,
    int32_t size,
    HashPartitionAllocator* allocator) {
  // If we can get rows by raising the row limit we do this first.
  int32_t adjustedSize = size - allocator->raiseRowLimits(size);
  if (adjustedSize <= 0) {
    return;
  }
  if (allocator->ranges[0].fixedFull) {
    state.ranges.push_back(std::move(allocator->ranges[0]));
    allocator->ranges[0] = std::move(allocator->ranges[1]);
  }
  auto buffer = arena.allocate<char>(size);
  state.buffers.push_back(buffer);
  AllocationRange newRange(
      reinterpret_cast<uintptr_t>(buffer->as<char>()),
      size,
      size,
      allocator->rowSize);
  if (allocator->ranges[0].empty()) {
    allocator->ranges[0] = std::move(newRange);
  } else {
    allocator->ranges[1] = std::move(newRange);
  }
}

void AggregateOperatorState::setSizesToSafe() {
  GpuHashTableBase* hashTable =
      reinterpret_cast<GpuHashTableBase*>(alignedHead + 1);
  auto* allocators = reinterpret_cast<HashPartitionAllocator*>(hashTable + 1);
  int32_t numPartitions = hashTable->partitionMask + 1;
  int32_t rowSize = allocators[0].rowSize;
  int32_t spaceInTable = hashTable->maxEntries - hashTable->numDistinct;
  auto allowedPerPartition = spaceInTable / numPartitions;
  for (auto i = 0; i < numPartitions; ++i) {
    auto availableInAllocator = allocators[i].availableFixed() / rowSize;
    if (availableInAllocator > allowedPerPartition) {
      allocators[i].trimRows(allowedPerPartition * rowSize);
    }
  }
}

void resupplyHashTable(WaveStream& stream, AbstractInstruction& inst) {
  auto* agg = &inst.as<AbstractAggregation>();
  auto deviceStream = WaveStream::streamFromReserve();
  auto stateId = agg->state->id;
  auto* state = stream.operatorState(stateId)->as<AggregateOperatorState>();
  auto* head = state->alignedHead;
  auto* hashTable = reinterpret_cast<GpuHashTableBase*>(head + 1);
  auto* gridState = stream.gridStatus<AggregateReturn>(agg->instructionStatus);
  auto* blockStatus = stream.hostBlockStatus();
  int32_t numBlocks = bits::roundUp(stream.numRows(), kBlockSize) / kBlockSize;
  int32_t numFailed =
      countErrors(blockStatus, numBlocks, ErrorCode::kInsufficientMemory);
  int32_t rowSize = agg->rowSize();
  int32_t numPartitions = hashTable->partitionMask + 1;
  int64_t newSize =
      bits::nextPowerOfTwo(numFailed + hashTable->numDistinct * 2);
  int64_t increment =
      rowSize * (newSize - hashTable->numDistinct) / numPartitions;
  for (auto i = 0; i < numPartitions; ++i) {
    auto* allocator =
        &reinterpret_cast<HashPartitionAllocator*>(hashTable + 1)[i];
    // Many concurrent failed allocation attempts can leave the fill way past
    // limit. Reset fills to limits if over limit.
    allocator->clearOverflows();
    if (allocator->availableFixed() < increment) {
      restockAllocator(*state, stream.arena(), increment, allocator);
    }
  }
  bool rehash = false;
  WaveBufferPtr oldBuckets;
  int32_t numOldBuckets;
  // Rehash if close to max. We can have growth from variable length
  // accumulators so rehash is not always right.
  if (gridState->numDistinct >= hashTable->maxEntries / 10 * 9) {
    oldBuckets = state->buffers[1];
    numOldBuckets = hashTable->sizeMask + 1;
    state->buffers[1] = stream.arena().allocate<GpuBucketMembers>(
        newSize / GpuBucketMembers::kNumSlots);
    hashTable->sizeMask = (newSize / GpuBucketMembers::kNumSlots) - 1;
    hashTable->buckets = state->buffers[1]->as<GpuBucket>();
    hashTable->maxEntries = newSize / 6 * 5;
    rehash = true;
  }
  state->setSizesToSafe();
  deviceStream->prefetch(
      getDevice(), state->alignedHead, state->alignedHeadSize);
  if (rehash) {
    AggregationControl control;
    control.head = head;
    control.oldBuckets = oldBuckets->as<char>();
    control.numOldBuckets = numOldBuckets;
    reinterpret_cast<WaveKernelStream*>(deviceStream.get())
        ->setupAggregation(control);
  }
  deviceStream->wait();
  WaveStream::releaseStream(std::move(deviceStream));
}

AdvanceResult AbstractAggregation::canAdvance(
    WaveStream& stream,
    LaunchControl* control,
    OperatorState* state,
    int32_t instructionIdx) const {
  if (keys.empty()) {
    return {};
  }
  auto gridState = stream.gridStatus<AggregateReturn>(instructionStatus);
  if (gridState->numDistinct) {
    // The hash table needs memory or rehash. Request a Task-wide break to
    // resupply the device side hash table.
    return {
        .numRows = stream.numRows(),
        .instructionIdx = instructionIdx,
        .isRetry = true,
        .syncDrivers = true,
        .updateStatus = resupplyHashTable,
        .reason = state};
  }
  return {};
}

std::pair<int64_t, int64_t> countResultRows(
    std::vector<AllocationRange>& ranges,
    int32_t rowSize) {
  int64_t count = 0;
  int64_t bytes = 0;
  for (auto& range : ranges) {
    auto bits = reinterpret_cast<uint64_t*>(range.base);
    int32_t numFree = bits::countBits(bits, 0, range.firstRowOffset * 8);
    auto n = ((range.rowOffset - range.firstRowOffset) / rowSize) - numFree;
    count += n;
    bytes += n * rowSize + (range.capacity - range.stringOffset);
  }
  return {count, bytes};
}

int32_t makeResultRows(
    AllocationRange* ranges,
    int32_t numRanges,
    int32_t rowSize,
    int32_t maxRows,
    int32_t& startRange,
    int32_t& startRow,
    uintptr_t* result) {
  int32_t fill = 0;
  for (; startRange < numRanges; ++startRange) {
    auto& range = ranges[startRange];
    uint64_t* bits = reinterpret_cast<uint64_t*>(range.base);
    auto firstRowOffset = range.firstRowOffset;
    uint32_t offset = startRow * rowSize + firstRowOffset;
    uint32_t limit = range.rowOffset - rowSize;
    for (; offset <= limit; offset += rowSize, ++startRow) {
      if (bits::isBitSet(bits, startRow)) {
        continue;
      }
      result[fill++] = range.base + offset;
      if (fill >= maxRows) {
        return fill;
      }
    }
    startRow = 0;
  }
  return fill;
}

AdvanceResult AbstractReadAggregation::canAdvance(
    WaveStream& stream,
    LaunchControl* control,
    OperatorState* state,
    int32_t instructionIdx) const {
  auto* aggState = reinterpret_cast<AggregateOperatorState*>(state);
  int32_t batchSize = 100'000;
  int32_t rowSize = aggState->instruction->rowSize();
  std::lock_guard<std::mutex> l(aggState->mutex);
  if (!aggState->instruction->keys.empty()) {
    auto deviceStream = WaveStream::streamFromReserve();
    auto deviceAgg = aggState->alignedHead;
    // On first continue set up the device side row ranges.
    if (aggState->isNew) {
      aggState->isNew = false;
      auto* hashTable =
          reinterpret_cast<GpuHashTableBase*>(aggState->alignedHead + 1);
      auto* allocators =
          reinterpret_cast<HashPartitionAllocator*>(hashTable + 1);
      int32_t numPartitions = hashTable->partitionMask + 1;
      for (auto i = 0; i < numPartitions; ++i) {
        for (auto j = 0; j < 2; j++) {
          if (!allocators[i].ranges[j].empty()) {
            aggState->ranges.push_back(std::move(allocators[i].ranges[j]));
          }
        }
      }
      aggState->rangeIdx = 0;
      aggState->rowIdx = 0;
      auto [r, b] = countResultRows(aggState->ranges, rowSize);
      aggState->numRows = r;
      aggState->bytes = b;
      auto maxReadStreams = aggState->instruction->maxReadStreams;
      aggState->resultRowPointers =
          stream.arena().allocate<int64_t*>(maxReadStreams);
      deviceAgg->numReadStreams = maxReadStreams;
      deviceAgg->resultRowPointers =
          aggState->resultRowPointers->as<uintptr_t*>();
      aggState->resultRows.resize(maxReadStreams);
      deviceStream->memset(
          aggState->resultRowPointers->as<char>(),
          0,
          maxReadStreams * sizeof(void*));
      aggState->alignedHead->resultRowPointers =
          aggState->resultRowPointers->as<uintptr_t*>();
      deviceStream->prefetch(
          getDevice(), aggState->alignedHead, aggState->alignedHeadSize);
      aggState->temp =
          getSmallTransferArena().allocate<int64_t*>(batchSize + 1);
    }
    int64_t* tempPtr;
    auto streamIdx = stream.streamIdx();
    if (!aggState->resultRows[streamIdx]) {
      aggState->resultRows[streamIdx] =
          stream.arena().allocate<int64_t*>(batchSize + 1);

      // Put the new array in the per-stream array in device side state.
      tempPtr = aggState->resultRows[streamIdx]->as<int64_t>();
      deviceStream->hostToDeviceAsync(
          aggState->resultRowPointers->as<int64_t*>() + streamIdx,
          &tempPtr,
          sizeof(tempPtr));
    }
    auto numRows = makeResultRows(
        aggState->ranges.data(),
        aggState->ranges.size(),
        rowSize,
        batchSize,
        aggState->rangeIdx,
        aggState->rowIdx,
        aggState->temp->as<uintptr_t>() + 1);
    aggState->temp->as<uintptr_t>()[0] = numRows;
    if (numRows == 0) {
      return {};
    }
    deviceStream->hostToDeviceAsync(
        aggState->resultRows[streamIdx]->as<char>(),
        aggState->temp->as<char>(),
        (numRows + 1) * sizeof(int64_t*));
    deviceStream->wait();
    return {.numRows = numRows};
  }

  // Single row case.
  if (aggState->isNew) {
    aggState->isNew = false;
    return {.numRows = 1};
  }
  return {};
}

} // namespace facebook::velox::wave
