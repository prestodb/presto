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
#include "velox/experimental/wave/common/Bits.cuh"

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
    uint64_t bitfield,
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
      uint64_t index = low & mask;
      storeResult<T, resultOp>(i, index, baseline, dict, scatter, result);
    } else {
      uint64_t nextWord = words[wordIndex + 1];
      int32_t bitsFromNext = bitWidth - (64 - bit);
      uint64_t index =
          low | ((nextWord & ((1LU << bitsFromNext) - 1)) << (64 - bit));
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
        assert(false);
        printf("ERROR: Unsupported data type for DictionaryOnBitpack\n");
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
  extern __shared__ char smem[];
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
  extern __shared__ char smem[];
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
  extern __shared__ char smem[];
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
  extern __shared__ char smem[];
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

template <typename T>
inline __device__ T randomAccessDecode(const GpuDecode* op, int32_t idx) {
  switch (op->encoding) {
    case DecodeStep::kDictionaryOnBitpack: {
      const auto& d = op->data.dictionaryOnBitpack;
      auto width = d.bitWidth;
      if (sizeof(T) == 4 || width <= 32) {
        return loadBits32(d.indices, idx * width, width) + d.baseline;
      } else {
        return loadBits64(d.indices, idx * width, width) + d.baseline;
      }
    }
  }
}

template <typename T, WaveFilterKind kFilterKind, bool kFixedFilter = true>
__device__ bool testFilter(const GpuDecode* op, T data) {
  switch (kFixedFilter ? kFilterKind : op->filterKind) {
    case WaveFilterKind::kBigintRange: {
      long2 bounds = *reinterpret_cast<const long2*>(&op->filter);
      return data >= bounds.x && data <= bounds.y;
    }
    default:
      return true;
  }
}

template <typename T, int32_t kBlockSize>
__device__ void makeResult(
    const GpuDecode* op,
    T data,
    int32_t row,
    bool filterPass,
    int32_t nthLoop,
    uint8_t nullFlag,
    int32_t* temp) {
  auto base = nthLoop * kBlockSize;
  if (op->filterKind != WaveFilterKind::kAlwaysTrue) {
    int32_t resultIdx = exclusiveSum<int16_t, kBlockSize>(
        static_cast<int16_t>(filterPass),
        nullptr,
        reinterpret_cast<int16_t*>(temp));
    if (threadIdx.x == kBlockSize - 1) {
      op->blockStatus[nthLoop].numRows = resultIdx + filterPass;
      if (op->filterRowCount) {
        op->filterRowCount[nthLoop] = resultIdx + filterPass;
      }
    }
    if (filterPass) {
      resultIdx += base;
      op->resultRows[resultIdx] = row;
      if (op->result) {
        reinterpret_cast<T*>(op->result)[resultIdx] = data;
        if (op->resultNulls) {
          op->resultNulls[resultIdx] = nullFlag;
        }
      }
    }
  } else {
    if (!filterPass) {
      // In the no filter case, filterPass is false for lanes that are after the
      // last row.
      return;
    }
    auto resultIdx = base + threadIdx.x;
    reinterpret_cast<T*>(op->result)[resultIdx] = data;
    if (op->resultNulls) {
      op->resultNulls[resultIdx] = nullFlag;
    }
  }
}

template <typename T, int32_t kBlockSize>
__device__ void decodeSelective(GpuDecode* op) {
  int32_t dataIdx;

  int32_t nthLoop = 0;
  switch (op->nullMode) {
    case NullMode::kDenseNonNull: {
      do {
        int32_t row = threadIdx.x + op->baseRow + nthLoop * kBlockSize;
        bool filterPass = false;
        T data{};
        if (row < op->maxRow) {
          data = randomAccessDecode<T>(op, row);
          filterPass =
              testFilter<T, WaveFilterKind::kAlwaysTrue, false>(op, data);
        }
        makeResult<T, kBlockSize>(
            op, data, row, filterPass, nthLoop, kNotNull, op->temp);
      } while (++nthLoop < op->numRowsPerThread);
      break;
    }
    case NullMode::kSparseNonNull:
      do {
        int32_t numRows = op->blockStatus[nthLoop].numRows;
        bool filterPass = false;
        T data{};
        int32_t row = 0;
        if (threadIdx.x < numRows) {
          row = op->rows[threadIdx.x + nthLoop * kBlockSize];
          data = randomAccessDecode<T>(op, row);
          filterPass =
              testFilter<T, WaveFilterKind::kAlwaysTrue, false>(op, data);
        }
        makeResult<T, kBlockSize>(
            op, data, row, filterPass, nthLoop, kNotNull, op->temp);
      } while (++nthLoop < op->numRowsPerThread);
      break;
    case NullMode::kDenseNullable: {
      int32_t maxRow = op->maxRow;
      int32_t dataIdx = 0;
      auto* state = reinterpret_cast<NonNullState*>(op->temp);
      if (threadIdx.x == 0) {
        state->nonNullsBelow =
            op->nthBlock == 0 ? 0 : op->nonNullBases[op->nthBlock - 1];
        state->nonNullsBelowRow =
            op->numRowsPerThread * op->nthBlock * kBlockSize;
      }
      __syncthreads();
      do {
        int32_t base = op->baseRow + nthLoop * kBlockSize;
        bool filterPass = false;
        int32_t dataIdx;
        T data{};
        if (base < maxRow) {
          dataIdx = nonNullIndex256(
              op->nulls, base, min(kBlockSize, maxRow - base), state);
          filterPass = base + threadIdx.x < maxRow;
          if (filterPass) {
            if (dataIdx == -1) {
              if (!op->nullsAllowed) {
                filterPass = false;
              }
            } else {
              data = randomAccessDecode<T>(op, dataIdx);
              filterPass =
                  testFilter<T, WaveFilterKind::kAlwaysTrue, false>(op, data);
            }
          }
        }
        makeResult<T, kBlockSize>(
            op,
            data,
            base + threadIdx.x,
            filterPass,
            nthLoop,
            dataIdx == -1 ? kNull : kNotNull,
            state->temp);
      } while (++nthLoop < op->numRowsPerThread);
      break;
    }
    case NullMode::kSparseNullable: {
      auto state = reinterpret_cast<NonNullState*>(op->temp);
      if (threadIdx.x == 0) {
        state->nonNullsBelow =
            op->nthBlock == 0 ? 0 : op->nonNullBases[op->nthBlock - 1];
        state->nonNullsBelowRow =
            op->numRowsPerThread * op->nthBlock * kBlockSize;
      }
      __syncthreads();
      do {
        int32_t base = kBlockSize * nthLoop;
        int32_t numRows = op->blockStatus[nthLoop].numRows;
        if (numRows == 0) {
        } else {
          bool filterPass = true;
          T data{};
          dataIdx =
              nonNullIndex256Sparse(op->nulls, op->rows + base, numRows, state);
          filterPass = threadIdx.x < numRows;
          if (filterPass) {
            if (dataIdx == -1) {
              if (!op->nullsAllowed) {
                filterPass = false;
              }
            } else {
              data = randomAccessDecode<T>(op, dataIdx);
              filterPass =
                  testFilter<T, WaveFilterKind::kAlwaysTrue, false>(op, data);
            }
          }
          makeResult<T, kBlockSize>(
              op,
              data,
              op->rows[base + threadIdx.x],
              filterPass,
              nthLoop,
              dataIdx == -1 ? kNull : kNotNull,
              state->temp);
        }
      } while (++nthLoop < op->numRowsPerThread);
      break;
    }
  }
  __syncthreads();
}

// Returns the position of 'target' in 'data' to 'data + size'. Not finding the
// value is an error and the values are expected to be unique.
inline __device__ int
findRow(const int32_t* rows, int32_t size, int32_t row, GpuDecode* op) {
  int lo = 0, hi = size;
  while (lo < hi) {
    int i = (lo + hi) / 2;
    if (rows[i] == row) {
      return i;
    }
    if (rows[i] < row) {
      lo = i + 1;
    } else {
      hi = i;
    }
  }
  printf("Expecting to find  row %d in findRow() size %d %p\n", row, size, op);
  assert(false);
}

template <typename T, int32_t kBlockSize>
__device__ void compactValues(GpuDecode* op) {
  auto& compact = op->data.compact;
  int32_t nthLoop = 0;
  do {
    auto numRows = op->blockStatus[nthLoop].numRows;
    T sourceValue;
    uint8_t sourceNull;
    int32_t base;
    if (threadIdx.x < numRows) {
      base = nthLoop * kBlockSize;
      auto row = compact.finalRows[base + threadIdx.x];
      auto numSource = compact.sourceNumRows[nthLoop];
      auto sourceRow = findRow(compact.sourceRows + base, numSource, row, op);
      sourceValue =
          reinterpret_cast<const T*>(compact.source)[base + sourceRow];
      if (compact.sourceNull) {
        sourceNull = compact.sourceNull[base + sourceRow];
      }
    }
    __syncthreads();
    if (threadIdx.x < numRows) {
      reinterpret_cast<T*>(compact.source)[base + threadIdx.x] = sourceValue;
      if (compact.sourceNull) {
        compact.sourceNull[base + threadIdx.x] = sourceNull;
      }
    }
  } while (++nthLoop < op->numRowsPerThread);
}

template <int kBlockSize>
__device__ void setRowCountNoFilter(GpuDecode::RowCountNoFilter& op) {
  auto numRows = op.numRows;
  auto* status = op.status;
  auto numCounts = roundUp(numRows, kBlockSize) / kBlockSize;
  for (auto base = 0; base < numCounts; base += kBlockSize) {
    auto idx = threadIdx.x + base;
    if (idx < numCounts) {
      // Every thread writes a row count and errors for kBlockSize rows. All
      // errors are cleared and all row counts except the last are kBlockSize.
      status[idx].numRows =
          idx < numCounts - 1 ? kBlockSize : numRows - idx * kBlockSize;
      memset(&status[base + threadIdx.x].errors, 0, sizeof(status->errors));
    }
  }
}

template <int32_t kBlockSize, int32_t kWidth>
inline __device__ void reduceCase(
    int32_t cnt,
    int32_t nthLoop,
    int32_t numResults,
    int32_t* results,
    int32_t* temp) {
  static_assert(kWidth == 4 || kWidth == 8 || kWidth == 16 || kWidth == 32);
  using Reduce = cub::WarpReduce<int32_t, kWidth>;
  auto sum =
      Reduce(*reinterpret_cast<typename Reduce::TempStorage*>(temp)).Sum(cnt);
  constexpr int32_t kResultsPerLoop = kBlockSize / kWidth;

  if ((threadIdx.x & (kWidth - 1)) == 0) {
    temp[threadIdx.x / kWidth] = sum;
  }
  __syncthreads();
  // Add up the temps.
  sum = threadIdx.x < kResultsPerLoop ? temp[threadIdx.x] : 0;
  if (threadIdx.x == 0 && nthLoop > 0) {
    sum += results[nthLoop * kResultsPerLoop - 1];
  }
  auto result = inclusiveSum<int32_t, kBlockSize / kWidth>(
      threadIdx.x < kResultsPerLoop ? sum : 0, nullptr, temp);
  auto resultIdx = threadIdx.x + nthLoop * kResultsPerLoop;
  if (resultIdx < numResults) {
    results[resultIdx] = result;
  }
}

template <int kBlockSize>
__device__ void countBits(GpuDecode& step) {
  auto& op = step.data.countBits;
  auto numBits = op.numBits;
  bool aligned = (reinterpret_cast<uintptr_t>(op.bits) & 7) == 0;
  int32_t numWords = roundUp(op.numBits, 64) / 64;
  int32_t numResults = (numBits - 1) / op.resultStride;
  auto* bits = reinterpret_cast<const uint64_t*>(op.bits);
  for (auto i = 0; i < numBits; i += 64 * kBlockSize) {
    int32_t idx = threadIdx.x + (i / 64);
    int32_t cnt = 0;
    if (idx < numWords) {
      if (aligned) {
        cnt = __popcll(bits[idx]);
      } else {
        cnt = __popcll(unalignedLoad64(bits + idx));
      }
    }
    switch (op.resultStride) {
      case 256:
        reduceCase<kBlockSize, 4>(
            cnt,
            i / (64 * kBlockSize),
            numResults,
            reinterpret_cast<int32_t*>(step.result),
            step.temp);
        break;
      case 512:
        reduceCase<kBlockSize, 8>(
            cnt,
            i / (64 * kBlockSize),
            numResults,
            reinterpret_cast<int32_t*>(step.result),
            step.temp);
        break;
      case 1024:
        reduceCase<kBlockSize, 16>(
            cnt,
            i / (64 * kBlockSize),
            numResults,
            reinterpret_cast<int32_t*>(step.result),
            step.temp);
        break;
      case 2048:
        reduceCase<kBlockSize, 32>(
            cnt,
            i / (64 * kBlockSize),
            numResults,
            reinterpret_cast<int32_t*>(step.result),
            step.temp);
        break;
    }
  }
}

template <int32_t kBlockSize>
__device__ void decodeSwitch(GpuDecode& op) {
  switch (op.step) {
    case DecodeStep::kSelective32:
      detail::decodeSelective<int32_t, kBlockSize>(&op);
      break;
    case DecodeStep::kSelective64:
      detail::decodeSelective<int64_t, kBlockSize>(&op);
      break;
    case DecodeStep::kCompact64:
      detail::compactValues<int64_t, kBlockSize>(&op);
      break;
    case DecodeStep::kCountBits:
      countBits<kBlockSize>(op);
      break;
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
    case DecodeStep::kRowCountNoFilter:
      detail::setRowCountNoFilter<kBlockSize>(op.data.rowCountNoFilter);
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
    case DecodeStep::kSelective32:
    case DecodeStep::kSelective64:
    case DecodeStep::kCompact64:
    case DecodeStep::kTrivial:
    case DecodeStep::kDictionaryOnBitpack:
    case DecodeStep::kCountBits:
    case DecodeStep::kSparseBool:
    case DecodeStep::kRowCountNoFilter:
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
