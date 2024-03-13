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

#include <cub/cub.cuh> // @manual

namespace facebook::velox::wave {

namespace detail {

template <typename T>
__device__ void decodeTrivial(GpuDecode::Trivial& op) {
  auto address = reinterpret_cast<uint64_t>(op.input);
  constexpr uint64_t kMask = sizeof(T) - 1;
  int32_t lowShift = (address & kMask) * 8;
  int32_t highShift = sizeof(T) * 8 - lowShift;
  auto source = reinterpret_cast<T*>(address & ~kMask);
  int32_t end = op.end;
  T* result = reinterpret_cast<T*>(op.result);
  auto scatter = op.scatter;
  if (scatter) {
    for (auto i = op.begin + threadIdx.x; i < end; i += blockDim.x) {
      result[scatter[i]] =
          (source[i] >> lowShift) | (source[i + 1] << highShift);
    }
  } else {
    for (auto i = op.begin + threadIdx.x; i < end; i += blockDim.x) {
      result[i] = (source[i] >> lowShift) | (source[i + 1] << highShift);
    }
  }
}

__device__ inline void decodeTrivial(GpuDecode& plan) {
  auto& op = plan.data.trivial;
  switch (op.dataType) {
    case WaveTypeKind::TINYINT:
      decodeTrivial<uint8_t>(op);
      break;
    case WaveTypeKind::SMALLINT:
      decodeTrivial<uint16_t>(op);
      break;
    case WaveTypeKind::INTEGER:
    case WaveTypeKind::REAL:
      decodeTrivial<uint32_t>(op);
      break;
    case WaveTypeKind::BIGINT:
    case WaveTypeKind::DOUBLE:
      decodeTrivial<uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for Trivial\n");
      }
  }
}

enum class ResultOp { kDict, kBaseline, kDictScatter, kBaselineScatter };

template <typename T, ResultOp op>
__device__ void storeResult(
    int32_t i,
    int32_t bitfield,
    T baseline,
    const T* dict,
    const int32_t* scatter,
    T* result) {
  if constexpr (op == ResultOp::kBaseline) {
    result[i] = bitfield + baseline;
  } else if constexpr (op == ResultOp::kBaselineScatter) {
    result[scatter[i]] = bitfield + baseline;
  } else if constexpr (op == ResultOp::kDict) {
    result[i] = dict[bitfield + baseline];
  } else if constexpr (op == ResultOp::kDictScatter) {
    result[scatter[i]] = dict[bitfield + baseline];
  }
}

template <typename T, ResultOp resultOp>
__device__ void decodeDictionaryOnBitpack(GpuDecode::DictionaryOnBitpack& op) {
  int32_t i = op.begin + threadIdx.x;
  auto end = op.end;
  auto address = reinterpret_cast<uint64_t>(op.indices);
  int32_t alignOffset = (address & 7) * 8;
  address &= ~7UL;
  auto words = reinterpret_cast<uint64_t*>(address);
  const T* dict = reinterpret_cast<const T*>(op.alphabet);
  auto scatter = op.scatter;
  auto baseline = op.baseline;
  auto bitWidth = op.bitWidth;
  uint64_t mask = (1LU << bitWidth) - 1;
  auto result = reinterpret_cast<T*>(op.result);
  for (; i < end; i += blockDim.x) {
    int32_t bitIndex = i * bitWidth + alignOffset;
    int32_t wordIndex = bitIndex >> 6;
    int32_t bit = bitIndex & 63;
    uint64_t word = words[wordIndex];
    uint64_t low = word >> bit;
    if (bitWidth + bit <= 64) {
      int32_t index = low & mask;
      storeResult<T, resultOp>(i, index, baseline, dict, scatter, result);
    } else {
      uint64_t nextWord = words[wordIndex + 1];
      int32_t bitsFromNext = bitWidth - (64 - bit);
      int32_t index =
          low | ((nextWord & ((1 << bitsFromNext) - 1)) << (64 - bit));
      storeResult<T, resultOp>(i, index, baseline, dict, scatter, result);
    }
  }
}

template <typename T>
__device__ void decodeDictionaryOnBitpack(GpuDecode::DictionaryOnBitpack& op) {
  if (!op.scatter) {
    if (op.alphabet) {
      decodeDictionaryOnBitpack<T, ResultOp::kDict>(op);
    } else {
      decodeDictionaryOnBitpack<T, ResultOp::kBaseline>(op);
    }
  } else {
    if (op.alphabet) {
      decodeDictionaryOnBitpack<T, ResultOp::kDictScatter>(op);
    } else {
      decodeDictionaryOnBitpack<T, ResultOp::kBaselineScatter>(op);
    }
  }
}

__device__ inline void decodeDictionaryOnBitpack(GpuDecode& plan) {
  auto& op = plan.data.dictionaryOnBitpack;
  switch (op.dataType) {
    case WaveTypeKind::TINYINT:
      decodeDictionaryOnBitpack<int8_t>(op);
      break;
    case WaveTypeKind::SMALLINT:
      decodeDictionaryOnBitpack<int16_t>(op);
      break;
    case WaveTypeKind::INTEGER:
      decodeDictionaryOnBitpack<int32_t>(op);
      break;
    case WaveTypeKind::BIGINT:
      decodeDictionaryOnBitpack<int64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for DictionaryOnBitpack\n");
        assert(false);
      }
  }
}

template <int32_t kBlockSize, typename T>
__device__ int scatterIndices(
    const T* values,
    T value,
    int32_t begin,
    int32_t end,
    int32_t* indices) {
  typedef cub::BlockScan<int32_t, kBlockSize> BlockScan;
  extern __shared__ __align__(
      alignof(typename BlockScan::TempStorage)) char smem[];
  auto* scanStorage = reinterpret_cast<typename BlockScan::TempStorage*>(smem);
  int numMatch;
  bool match;
  int32_t k = 0;
  for (auto j = begin; j < end; j += kBlockSize) {
    auto jt = j + threadIdx.x;
    numMatch = match = (jt < end && values[jt] == value);
    int subtotal;
    BlockScan(*scanStorage).ExclusiveSum(numMatch, numMatch, subtotal);
    __syncthreads();
    if (match) {
      indices[k + numMatch] = jt - begin;
    }
    k += subtotal;
  }
  return k;
}

template <int kBlockSize>
__device__ int scatterIndices(
    const uint8_t* bits,
    bool value,
    int32_t begin,
    int32_t end,
    int32_t* indices) {
  typedef cub::BlockScan<int32_t, kBlockSize> BlockScan;
  extern __shared__ __align__(
      alignof(typename BlockScan::TempStorage)) char smem[];
  auto* scanStorage = reinterpret_cast<typename BlockScan::TempStorage*>(smem);
  constexpr int kPerThread = 8;
  int numMatch[kPerThread];
  bool match[kPerThread];
  int32_t k = 0;
  constexpr auto kBitsPerBlock = kBlockSize * kPerThread;
  for (auto j = begin; j < end; j += kBitsPerBlock) {
    auto jt = j + threadIdx.x * kPerThread;
    for (auto i = 0; i < kPerThread; ++i) {
      numMatch[i] = match[i] = jt + i < end && isSet(bits, jt + i) == value;
    }
    int subtotal;
    BlockScan(*scanStorage).ExclusiveSum(numMatch, numMatch, subtotal);
    __syncthreads();
    for (auto i = 0; i < kPerThread; ++i) {
      if (match[i]) {
        indices[k + numMatch[i]] = jt + i - begin;
      }
    }
    k += subtotal;
  }
  return k;
}

__device__ inline void
bitpackBools(const bool* input, int count, uint8_t* out) {
  int nbytes = count / 8;
  for (auto i = threadIdx.x; i < nbytes; i += blockDim.x) {
    uint8_t value = 0;
    for (int j = 0; j < 8; ++j) {
      value |= input[8 * i + j] << j;
    }
    out[i] = value;
  }
  if (threadIdx.x == 0 && count % 8 != 0) {
    auto extra = count % 8;
    for (int j = count - extra; j < count; ++j) {
      setBit(out, j, input[j]);
    }
  }
}

__device__ inline void decodeSparseBool(GpuDecode::SparseBool& op) {
  for (int i = threadIdx.x; i < op.totalCount; i += blockDim.x) {
    op.bools[i] = !op.sparseValue;
  }
  __syncthreads();
  for (int i = threadIdx.x; i < op.sparseCount; i += blockDim.x) {
    op.bools[op.sparseIndices[i]] = op.sparseValue;
  }
  __syncthreads();
  bitpackBools(op.bools, op.totalCount, op.result);
}

__device__ inline uint32_t readVarint32(const char** pos) {
  uint32_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (*((*pos)++) & 127) << 28;
  return value;
}

__device__ inline uint64_t readVarint64(const char** pos) {
  uint64_t value = (**pos) & 127;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 7;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 14;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= (**pos & 127) << 21;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 28;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 35;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 42;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 49;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(**pos & 127) << 56;
  if (!(*((*pos)++) & 128)) {
    return value;
  }
  value |= static_cast<uint64_t>(*((*pos)++) & 127) << 63;
  return value;
}

template <int kBlockSize, typename T>
__device__ int decodeVarint(
    const char* input,
    int size,
    bool* ends,
    int32_t* endPos,
    T* output) {
  for (auto i = threadIdx.x; i < size; i += blockDim.x) {
    ends[i] = ~input[i] & 0x80;
  }
  auto numOut = scatterIndices<kBlockSize>(ends, true, 0, size, endPos);
  for (auto i = threadIdx.x; i < numOut; i += blockDim.x) {
    auto* pos = input + (i == 0 ? 0 : (endPos[i - 1] + 1));
    if constexpr (sizeof(T) == 4) {
      output[i] = readVarint32(&pos);
    } else {
      static_assert(sizeof(T) == 8);
      output[i] = readVarint64(&pos);
    }
  }
  return numOut;
}

template <int kBlockSize>
__device__ void decodeVarint(GpuDecode& plan) {
  auto& op = plan.data.varint;
  int resultSize;
  switch (op.resultType) {
    case WaveTypeKind::INTEGER:
      resultSize = decodeVarint<kBlockSize, uint32_t>(
          op.input, op.size, op.ends, op.endPos, (uint32_t*)op.result);
      break;
    case WaveTypeKind::BIGINT:
      resultSize = decodeVarint<kBlockSize, uint64_t>(
          op.input, op.size, op.ends, op.endPos, (uint64_t*)op.result);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported result type for varint decoder\n");
        assert(false);
      }
  }
  if (threadIdx.x == 0) {
    op.resultSize = resultSize;
  }
}

template <int kBlockSize, typename T>
__device__ void decodeMainlyConstant(GpuDecode::MainlyConstant& op) {
  auto otherCount = scatterIndices<kBlockSize>(
      op.isCommon, false, 0, op.count, op.otherIndices);
  auto commonValue = *(const T*)op.commonValue;
  auto* otherValues = (const T*)op.otherValues;
  auto* result = (T*)op.result;
  for (int i = threadIdx.x; i < op.count; i += blockDim.x) {
    result[i] = commonValue;
  }
  __syncthreads();
  for (int i = threadIdx.x; i < otherCount; i += blockDim.x) {
    result[op.otherIndices[i]] = otherValues[i];
  }
  if (threadIdx.x == 0 && op.otherCount) {
    *op.otherCount = otherCount;
  }
}

template <int kBlockSize>
__device__ void decodeMainlyConstant(GpuDecode& plan) {
  auto& op = plan.data.mainlyConstant;
  switch (op.dataType) {
    case WaveTypeKind::TINYINT:
      decodeMainlyConstant<kBlockSize, uint8_t>(op);
      break;
    case WaveTypeKind::SMALLINT:
      decodeMainlyConstant<kBlockSize, uint16_t>(op);
      break;
    case WaveTypeKind::INTEGER:
    case WaveTypeKind::REAL:
      decodeMainlyConstant<kBlockSize, uint32_t>(op);
      break;
    case WaveTypeKind::BIGINT:
    case WaveTypeKind::DOUBLE:
      decodeMainlyConstant<kBlockSize, uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported data type for MainlyConstant\n");
        assert(false);
      }
  }
}

template <int kBlockSize, typename T, typename U>
__device__ T sum(const U* values, int size) {
  using Reduce = cub::BlockReduce<T, kBlockSize>;
  extern __shared__ __align__(
      alignof(typename Reduce::TempStorage)) char smem[];
  auto* reduceStorage = reinterpret_cast<typename Reduce::TempStorage*>(smem);
  T total = 0;
  for (int i = 0; i < size; i += kBlockSize) {
    auto numValid = min(size - i, kBlockSize);
    T value;
    if (threadIdx.x < numValid) {
      value = values[i + threadIdx.x];
    }
    total += Reduce(*reduceStorage).Sum(value, numValid);
    __syncthreads();
  }
  return total;
}

template <int kBlockSize>
__device__ void rleTotalLength(GpuDecode::RleTotalLength& op) {
  auto result = sum<kBlockSize, int64_t>(op.input, op.count);
  if (threadIdx.x == 0) {
    *op.result = result;
  }
}

template <typename T>
__device__ int upperBound(const T* data, int size, T target) {
  int lo = 0, hi = size;
  while (lo < hi) {
    int i = (lo + hi) / 2;
    if (data[i] <= target) {
      lo = i + 1;
    } else {
      hi = i;
    }
  }
  return lo;
}

template <int kBlockSize, typename T>
__device__ void decodeRle(GpuDecode::Rle& op) {
  using BlockScan = cub::BlockScan<int32_t, kBlockSize>;
  extern __shared__ __align__(
      alignof(typename BlockScan::TempStorage)) char smem[];
  auto* scanStorage = reinterpret_cast<typename BlockScan::TempStorage*>(smem);

  static_assert(sizeof(*scanStorage) >= sizeof(int32_t) * kBlockSize);
  auto* offsets = (int32_t*)scanStorage;
  auto* values = (const T*)op.values;
  auto* result = (T*)op.result;
  int total = 0;
  for (int i = 0; i < op.count; i += blockDim.x) {
    auto ti = threadIdx.x + i;
    auto len = ti < op.count ? op.lengths[ti] : 0;
    int32_t offset, subtotal;
    __syncthreads();
    BlockScan(*scanStorage).InclusiveSum(len, offset, subtotal);
    __syncthreads();
    offsets[threadIdx.x] = offset;
    __syncthreads();
    for (int j = threadIdx.x; j < subtotal; j += blockDim.x) {
      result[total + j] = values[i + upperBound(offsets, blockDim.x, j)];
    }
    total += subtotal;
  }
}

template <int kBlockSize>
__device__ void decodeRle(GpuDecode& plan) {
  auto& op = plan.data.rle;
  switch (op.valueType) {
    case WaveTypeKind::TINYINT:
      decodeRle<kBlockSize, uint8_t>(op);
      break;
    case WaveTypeKind::SMALLINT:
      decodeRle<kBlockSize, uint16_t>(op);
      break;
    case WaveTypeKind::INTEGER:
    case WaveTypeKind::REAL:
      decodeRle<kBlockSize, uint32_t>(op);
      break;
    case WaveTypeKind::BIGINT:
    case WaveTypeKind::DOUBLE:
      decodeRle<kBlockSize, uint64_t>(op);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported value type for Rle\n");
        assert(false);
      }
  }
}

template <int kBlockSize>
__device__ void makeScatterIndices(GpuDecode::MakeScatterIndices& op) {
  auto indicesCount = scatterIndices<kBlockSize>(
      op.bits, op.findSetBits, op.begin, op.end, op.indices);
  if (threadIdx.x == 0 && op.indicesCount) {
    *op.indicesCount = indicesCount;
  }
}
template <int32_t kBlockSize>
__device__ void decodeSwitch(GpuDecode& op) {
  switch (op.step) {
    case DecodeStep::kTrivial:
      detail::decodeTrivial(op);
      break;
    case DecodeStep::kDictionaryOnBitpack:
      detail::decodeDictionaryOnBitpack(op);
      break;
    case DecodeStep::kSparseBool:
      detail::decodeSparseBool(op.data.sparseBool);
      break;
    case DecodeStep::kMainlyConstant:
      detail::decodeMainlyConstant<kBlockSize>(op);
      break;
    case DecodeStep::kVarint:
      detail::decodeVarint<kBlockSize>(op);
      break;
    case DecodeStep::kRleTotalLength:
      detail::rleTotalLength<kBlockSize>(op.data.rleTotalLength);
      break;
    case DecodeStep::kRle:
      detail::decodeRle<kBlockSize>(op);
      break;
    case DecodeStep::kMakeScatterIndices:
      detail::makeScatterIndices<kBlockSize>(op.data.makeScatterIndices);
      break;
    default:
      if (threadIdx.x == 0) {
        printf("ERROR: Unsupported DecodeStep (with shared memory)\n");
      }
  }
}

template <int kBlockSize>
__global__ void decodeGlobal(GpuDecode* plan) {
  decodeSwitch<kBlockSize>(plan[blockIdx.x]);
}

template <int32_t kBlockSize>
int32_t sharedMemorySizeForDecode(DecodeStep step) {
  using Reduce32 = cub::BlockReduce<int32_t, kBlockSize>;
  using BlockScan32 = cub::BlockScan<int32_t, kBlockSize>;
  switch (step) {
    case DecodeStep::kTrivial:
    case DecodeStep::kDictionaryOnBitpack:
    case DecodeStep::kSparseBool:
      return 0;
      break;

    case DecodeStep::kRleTotalLength:
      return sizeof(typename Reduce32::TempStorage);
    case DecodeStep::kMainlyConstant:
    case DecodeStep::kRleBool:
    case DecodeStep::kRle:
    case DecodeStep::kVarint:
    case DecodeStep::kMakeScatterIndices:
    case DecodeStep::kLengthToOffset:
      return sizeof(typename BlockScan32::TempStorage);
    default:
      assert(false); // Undefined.
      return 0;
  }
}

} // namespace detail

template <int kBlockSize>
void decodeGlobal(GpuDecode* plan, int numBlocks, cudaStream_t stream) {
  int32_t sharedSize = 0;
  for (auto i = 0; i < numBlocks; ++i) {
    sharedSize = std::max(
        sharedSize,
        detail::sharedMemorySizeForDecode<kBlockSize>(plan[i].step));
  }
  if (sharedSize > 0) {
    sharedSize += 15; // allow align at 16.
  }

  detail::decodeGlobal<kBlockSize>
      <<<numBlocks, kBlockSize, sharedSize, stream>>>(plan);
}

} // namespace facebook::velox::wave
