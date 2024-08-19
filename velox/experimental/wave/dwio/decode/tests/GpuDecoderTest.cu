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

#include <fmt/format.h>
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/experimental/gpu/Common.h"
#include "velox/experimental/wave/dwio/decode/GpuDecoder.cuh"

DEFINE_int32(device_id, 0, "");
DEFINE_bool(benchmark, false, "");
DEFINE_bool(print_kernels, false, "Print register and smem usage");
DEFINE_bool(use_selective, false, "Use selective path for test");

namespace facebook::velox::wave {
namespace {

using namespace facebook::velox;

// define to use the flexible call path wiht multiple ops per TB
#define USE_PROGRAM_API
#define USE_SEL_BITPACK true

// Returns the number of bytes the "values" will occupy after varint encoding.
uint64_t bulkVarintSize(const uint64_t* values, int count) {
  constexpr uint8_t kLookupSizeTable64[64] = {
      10, 9, 9, 9, 9, 9, 9, 9, 8, 8, 8, 8, 8, 8, 8, 7, 7, 7, 7, 7, 7, 7,
      6,  6, 6, 6, 6, 6, 6, 5, 5, 5, 5, 5, 5, 5, 4, 4, 4, 4, 4, 4, 4, 3,
      3,  3, 3, 3, 3, 3, 2, 2, 2, 2, 2, 2, 2, 1, 1, 1, 1, 1, 1, 1};
  uint64_t size = 0;
  for (int i = 0; i < count; ++i) {
    size += kLookupSizeTable64[__builtin_clzll(values[i] | 1ULL)];
  }
  return size;
}

template <typename T>
void writeVarint(T val, char** pos) noexcept {
  while (val >= 128) {
    *((*pos)++) = 0x80 | (val & 0x7f);
    val >>= 7;
  }
  *((*pos)++) = val;
}

template <typename T>
gpu::CudaPtr<T[]> allocate(int n) {
  T* ptr;
  CUDA_CHECK_FATAL(cudaMallocManaged(&ptr, n * sizeof(T)));
  return gpu::CudaPtr<T[]>(ptr);
}

template <typename T>
void fillRandom(T* values, int32_t numValues) {
  uint64_t seed = 0xafbe1647deba879LU;
  for (auto i = 0; i < numValues; ++i) {
    values[i] = seed;
    seed = (seed * 0x5def1) ^ (seed >> 21);
  }
}

// Generate random bits with probability "p" being true and "1 - p" being false.
void fillRandomBits(uint8_t* bits, double p, int numValues) {
  for (int i = 0; i < numValues; ++i) {
    setBit(bits, i, (double)rand() / RAND_MAX < p);
  }
}

template <typename T>
inline T* addBytes(T* ptr, int bytes) {
  return reinterpret_cast<T*>(reinterpret_cast<char*>(ptr) + bytes);
}

template <typename T>
inline const T* addBytes(const T* ptr, int bytes) {
  return reinterpret_cast<const T*>(reinterpret_cast<const char*>(ptr) + bytes);
}

void prefetchToDevice(void* ptr, size_t size) {
  CUDA_CHECK_FATAL(cudaMemPrefetchAsync(ptr, size, FLAGS_device_id, nullptr));
}

template <typename T>
void makeBitpackDict(
    int32_t bitWidth,
    int32_t numValues,
    gpu::CudaPtr<char[]>& cudaPtr,
    T*& dict,
    uint64_t*& bits,
    T*& result,
    int32_t** scatter,
    bool bitsOnly,
    BlockStatus*& blockStatus,
    int32_t numBlocks,
    int32_t blockSize) {
  int64_t dictBytes = bitsOnly ? 0 : sizeof(T) << bitWidth;
  int64_t bitBytes = (roundUp(numValues * bitWidth, 128) / 8) + 16;
  int64_t resultBytes = numValues * sizeof(T);
  int scatterBytes =
      scatter ? roundUp(numValues * sizeof(int32_t), sizeof(T)) : 0;
  int32_t statusBytes = sizeof(BlockStatus) * numBlocks;
  if (scatterBytes) {
    resultBytes += resultBytes / 2;
  }
  cudaPtr = allocate<char>(
      dictBytes + bitBytes + scatterBytes + resultBytes + statusBytes);
  T* memory = (T*)cudaPtr.get();

  dict = bitsOnly ? nullptr : memory;

  static int sequence = 1;
  ++sequence;
  for (auto i = 0; i < dictBytes / sizeof(T); ++i) {
    dict[i] = (10 + sequence) * i;
  }

  // The bit packed data does not start at a word boundary.
  bits = addBytes(reinterpret_cast<uint64_t*>(memory), dictBytes + 1);
  fillRandom(bits, bitBytes / 8);

  if (scatterBytes) {
    // Make a scatter vector that makes gaps in the result sequence.
    *scatter =
        addBytes(reinterpret_cast<int32_t*>(memory), dictBytes + bitBytes);
    for (auto i = 0; i < numValues; ++i) {
      (*scatter)[i] = i + i / 4;
    }
  }
  result = addBytes(
      reinterpret_cast<T*>(memory), dictBytes + bitBytes + scatterBytes);
  blockStatus =
      reinterpret_cast<BlockStatus*>(addBytes(result, numValues * sizeof(T)));
  for (auto i = 0; i < numBlocks; ++i) {
    blockStatus[i].numRows =
        i < numBlocks - 1 ? blockSize : numValues - (i * blockSize);
  }
  prefetchToDevice(
      memory, dictBytes + bitBytes + scatterBytes + resultBytes + statusBytes);
}

class GpuDecoderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    if (int device; cudaGetDevice(&device) != cudaSuccess) {
      GTEST_SKIP() << "No CUDA detected, skipping all tests";
    }
    arena_ = std::make_unique<GpuArena>(100000000, getAllocator(getDevice()));
    CUDA_CHECK_FATAL(cudaEventCreate(&startEvent_));
    CUDA_CHECK_FATAL(cudaEventCreate(&stopEvent_));
  }

  void TearDown() override {
    CUDA_CHECK_FATAL(cudaEventDestroy(startEvent_));
    CUDA_CHECK_FATAL(cudaEventDestroy(stopEvent_));
  }

  void testCase(
      const std::string& label,
      std::function<void()> func,
      int64_t bytes,
      int32_t numReps) {
    func();
    CUDA_CHECK_FATAL(cudaGetLastError());
    if (!FLAGS_benchmark) {
      CUDA_CHECK_FATAL(cudaDeviceSynchronize());
      return;
    }
    CUDA_CHECK_FATAL(cudaEventRecord(startEvent_, 0));
    for (auto count = 0; count < numReps; ++count) {
      func();
    }
    CUDA_CHECK_FATAL(cudaEventRecord(stopEvent_, 0));
    CUDA_CHECK_FATAL(cudaEventSynchronize(stopEvent_));
    float ms;
    CUDA_CHECK_FATAL(cudaEventElapsedTime(&ms, startEvent_, stopEvent_));
    printf(
        "%s %.2f (%d at %.2f us each)\n",
        label.c_str(),
        bytes * numReps * 1e-6 / ms,
        numReps,
        ms * 1000 / numReps);
  }

  template <typename T, int32_t kBlockSize>
  void testCopyPlan(int64_t numValues, int numBlocks, bool useScatter) {
    auto source = allocate<T>(numValues);
    auto result = allocate<T>(numValues * 4 / 3);
    gpu::CudaPtr<int32_t[]> scatter;
    if (useScatter) {
      scatter = allocate<int32_t>(numValues);
      for (auto i = 0; i < numValues; ++i) {
        scatter[i] = i * 5 / 4;
      }
    }
    fillRandom(source.get(), numValues);
    result[numValues] = 0xdeadbeef;
    int valuesPerOp = roundUp(numValues / numBlocks, kBlockSize);
    int numOps = roundUp(numValues, valuesPerOp) / valuesPerOp;
    auto ops = allocate<GpuDecode>(numOps);
    for (auto i = 0; i < numOps; ++i) {
      int32_t begin = i * valuesPerOp;
      ops[i].step = DecodeStep::kTrivial;
      auto& op = ops[i].data.trivial;
      op.dataType = WaveTypeTrait<T>::typeKind;
      op.begin = begin;
      op.end = std::min<int32_t>(numValues, (i + 1) * valuesPerOp);
      op.result = result.get();
      op.input = source.get();
      op.scatter = scatter.get();
    }
    testCase(
        fmt::format(
            "copy plan {} numValues={} useScatter={}",
            sizeof(T) * 8,
            numValues,
            useScatter),
        [&] { decodeGlobal<kBlockSize>(ops.get(), numOps); },
        numValues * sizeof(T),
        10);
    if (!scatter) {
      EXPECT_EQ(0xdeadbeef, result[numValues]);
    }
    for (auto i = 0; i < numValues; ++i) {
      ASSERT_EQ(source[i], result[scatter ? scatter[i] : i]);
    }
  }

  template <typename T, int kBlockSize>
  void dictTestPlan(
      int32_t bitWidth,
      int64_t numValues,
      int numBlocks,
      bool useScatter,
      bool bitsOnly = false,
      bool useSelective = false) {
    gpu::CudaPtr<char[]> ptr;
    T* dict;
    uint64_t* bits;
    T* result;
    int32_t* scatter = nullptr;
    BlockStatus* blockStatus;
    makeBitpackDict(
        bitWidth,
        numValues,
        ptr,
        dict,
        bits,
        result,
        useScatter ? &scatter : nullptr,
        bitsOnly,
        blockStatus,
        roundUp(numValues, kBlockSize) / kBlockSize,
        kBlockSize);
    result[numValues] = 0xdeadbeef;
    int valuesPerOp = roundUp(numValues / numBlocks, kBlockSize);
    int numOps = roundUp(numValues, valuesPerOp) / valuesPerOp;
    auto valuesPerThread = valuesPerOp / kBlockSize;
    auto ops = allocate<GpuDecode>(numOps);
    for (auto i = 0; i < numOps; ++i) {
      int32_t begin = i * valuesPerOp;
      ops[i].step = useSelective ? (sizeof(T) == 8 ? DecodeStep::kSelective64
                                                   : DecodeStep::kSelective32)
                                 : DecodeStep::kDictionaryOnBitpack;
      ops[i].encoding = DecodeStep::kDictionaryOnBitpack;
      ops[i].dataType = WaveTypeTrait<T>::typeKind;
      ops[i].nullMode = NullMode::kDenseNonNull;
      ops[i].nthBlock = i;
      ops[i].numRowsPerThread = i == numOps - 1
          ? roundUp(numValues - (valuesPerOp * i), kBlockSize) / kBlockSize
          : valuesPerThread;
      ops[i].baseRow = i * valuesPerOp;
      ops[i].maxRow = std::min<int32_t>((i + 1) * valuesPerOp, numValues);
      ops[i].result = reinterpret_cast<T*>(result) + i * valuesPerOp;

      ops[i].blockStatus = blockStatus + (i * valuesPerThread);
      auto& op = ops[i].data.dictionaryOnBitpack;
      op.begin = begin;
      op.end = std::min<int>(numValues, (i + 1) * valuesPerOp);
      op.result = result;
      op.bitWidth = bitWidth;
      op.indices = bits;
      op.alphabet = dict;
      op.scatter = scatter;
      op.baseline = 0;
      op.dataType = WaveTypeTrait<T>::typeKind;
    }
    testCase(
        fmt::format(
            "bitpack dictplan {} -> {} numValues={} useScatter={}",
            bitWidth,
            sizeof(T) * 8,
            numValues,
            useScatter),
        [&] {
#ifdef USE_PROGRAM_API
          callViaPrograms(ops.get(), numOps);
#else
          decodeGlobal<kBlockSize>(ops.get(), numOps);
#endif
        },
        numValues * sizeof(T),
        10);
    if (!scatter) {
      EXPECT_EQ(0xdeadbeef, result[numValues]);
    }
    auto mask = (1uL << bitWidth) - 1;
    for (auto i = 0; i < numValues; ++i) {
      int32_t bit = i * bitWidth;
      uint64_t word = *addBytes(bits, bit / 8);
      uint64_t index = (word >> (bit & 7)) & mask;
      T expected = bitsOnly ? index : dict[index];
      ASSERT_EQ(result[scatter ? scatter[i] : i], expected) << i;
    }
  }

  template <int kBlockSize>
  void testSparseBool(int numValues, int numBlocks) {
    auto expected = allocate<uint8_t>((numValues + 7) / 8);
    fillRandomBits(expected.get(), 0.99, numValues);
    auto indices = allocate<int32_t>(numValues);
    int indicesCount = 0;
    for (int i = 0; i < numValues; ++i) {
      if (!isSet(expected.get(), i)) {
        indices[indicesCount++] = i;
      }
    }
    auto bools = allocate<bool>(numValues * numBlocks);
    auto resultSize = (numValues + 7) / 8;
    auto result = allocate<uint8_t>(resultSize * numBlocks);
    auto ops = allocate<GpuDecode>(numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kSparseBool;
      auto& op = ops[i].data.sparseBool;
      op.totalCount = numValues;
      op.sparseValue = false;
      op.sparseIndices = indices.get();
      op.sparseCount = indicesCount;
      op.bools = bools.get() + i * numValues;
      op.result = result.get() + i * resultSize;
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        resultSize * numBlocks,
        3);
    for (int j = 0; j < numBlocks; ++j) {
      auto* actual = ops[j].data.sparseBool.result;
      for (int i = 0; i < numValues; ++i) {
        ASSERT_EQ(isSet(actual, i), isSet(expected.get(), i)) << i;
      }
    }
  }

  template <int kBlockSize>
  void testVarint(int numValues, int numBlocks) {
    std::vector<uint64_t> expected(numValues);
    fillRandom(expected.data(), numValues);
    for (int i = 0; i < numValues; ++i) {
      if (i % 100 != 0) {
        expected[i] %= 128;
      }
    }
    auto inputSize = bulkVarintSize(expected.data(), numValues);
    auto input = allocate<char>(inputSize);
    auto* rawInput = input.get();
    for (int i = 0; i < numValues; ++i) {
      writeVarint(expected[i], &rawInput);
    }
    auto ends = allocate<bool>(inputSize * numBlocks);
    auto endPos = allocate<int32_t>(inputSize * numBlocks);
    auto result = allocate<uint64_t>(inputSize * numBlocks);
    auto ops = allocate<GpuDecode>(numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kVarint;
      auto& op = ops[i].data.varint;
      op.input = input.get();
      op.size = inputSize;
      op.ends = ends.get() + i * inputSize;
      op.endPos = endPos.get() + i * inputSize;
      op.resultType = WaveTypeKind::BIGINT;
      op.result = result.get() + i * inputSize;
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        numValues * sizeof(uint64_t) * numBlocks,
        3);
    for (int j = 0; j < numBlocks; ++j) {
      auto& op = ops[j].data.varint;
      ASSERT_EQ(op.resultSize, numValues);
      for (int i = 0; i < numValues; ++i) {
        ASSERT_EQ(reinterpret_cast<const uint64_t*>(op.result)[i], expected[i]);
      }
    }
  }

  template <typename T, int kBlockSize>
  void testMainlyConstant(int numValues, int numBlocks) {
    auto isCommon = allocate<uint8_t>((numValues + 7) / 8);
    fillRandomBits(isCommon.get(), 0.99, numValues);
    auto values = allocate<T>(numValues + 1);
    fillRandom(values.get(), numValues + 1);
    auto otherIndices = allocate<int32_t>(numValues * numBlocks);
    auto result = allocate<T>(numValues * numBlocks);
    auto otherCounts = allocate<int32_t>(numBlocks);
    auto ops = allocate<GpuDecode>(numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kMainlyConstant;
      auto& op = ops[i].data.mainlyConstant;
      op.dataType = WaveTypeTrait<T>::typeKind;
      op.count = numValues;
      op.commonValue = &values[numValues];
      op.otherValues = values.get();
      op.isCommon = isCommon.get();
      op.otherIndices = otherIndices.get() + i * numValues;
      op.result = result.get() + i * numValues;
      op.otherCount = otherCounts.get() + i;
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        numValues * numBlocks * sizeof(T),
        3);
    for (int k = 0; k < numBlocks; ++k) {
      auto& op = ops[k].data.mainlyConstant;
      auto* result = (const T*)op.result;
      int j = 0;
      for (int i = 0; i < numValues; ++i) {
        if (isSet(isCommon.get(), i)) {
          ASSERT_EQ(result[i], values[numValues]);
        } else {
          ASSERT_EQ(result[i], values[j++]);
        }
      }
      ASSERT_EQ(*op.otherCount, j);
    }
  }

  template <int kBlockSize>
  void testRleTotalLength(int numValues, int numBlocks) {
    auto values = allocate<int32_t>(numValues);
    fillRandom(values.get(), numValues);
    int valuesPerOp = (numValues + numBlocks - 1) / numBlocks;
    auto ops = allocate<GpuDecode>(numBlocks);
    auto lengths = allocate<int64_t>(numBlocks);
    for (auto i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kRleTotalLength;
      auto& op = ops[i].data.rleTotalLength;
      op.input = values.get() + i * valuesPerOp;
      op.count = std::min(valuesPerOp, numValues - i * valuesPerOp);
      op.result = &lengths[i];
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        numValues * sizeof(int32_t),
        5);
    for (int i = 0; i < numBlocks; ++i) {
      auto& op = ops[i].data.rleTotalLength;
      int64_t expected = 0;
      for (int j = 0; j < op.count; ++j) {
        expected += op.input[j];
      }
      ASSERT_EQ(*op.result, expected);
    }
  }

  template <typename T, int kBlockSize>
  void testRle(int numValues, int numBlocks) {
    auto values = allocate<T>(numValues);
    auto lengths = allocate<int32_t>(numValues);
    int totalLength = 0;
    fillRandom(values.get(), numValues);
    fillRandom(lengths.get(), numValues);
    for (int i = 0; i < numValues; ++i) {
      int limit = i % 1000 == 0 ? 1000 : 10;
      lengths[i] = (uint32_t)lengths[i] % limit;
      totalLength += lengths[i];
    }
    auto ops = allocate<GpuDecode>(numBlocks);
    auto results = allocate<int64_t>(numBlocks);
    int valuesPerOp = (numValues + numBlocks - 1) / numBlocks;
    for (int i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kRleTotalLength;
      auto& op = ops[i].data.rleTotalLength;
      op.input = lengths.get() + i * valuesPerOp;
      op.count = std::min(valuesPerOp, numValues - i * valuesPerOp);
      op.result = &results[i];
    }
    decodeGlobal<kBlockSize>(ops.get(), numBlocks);
    CUDA_CHECK_FATAL(cudaGetLastError());
    CUDA_CHECK_FATAL(cudaDeviceSynchronize());
    auto result = allocate<T>(totalLength);
    int lengthSofar = 0;
    for (int i = 0; i < numBlocks; ++i) {
      int subtotal = *ops[i].data.rleTotalLength.result;
      ops[i].step = DecodeStep::kRle;
      auto& op = ops[i].data.rle;
      op.valueType = WaveTypeTrait<T>::typeKind;
      op.values = values.get() + i * valuesPerOp;
      op.lengths = lengths.get() + i * valuesPerOp;
      op.count = std::min(valuesPerOp, numValues - i * valuesPerOp);
      op.result = result.get() + lengthSofar;
      lengthSofar += subtotal;
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        totalLength * sizeof(T),
        3);
    for (int i = 0; i < numBlocks; ++i) {
    }
    for (int i = 0, j = 0; i < numValues; ++i) {
      for (int k = 0; k < lengths[i]; ++k) {
        ASSERT_EQ(result[j++], values[i]);
      }
    }
  }

  template <int kBlockSize>
  void testMakeScatterIndices(int numValues, int numBlocks) {
    auto bits = allocate<uint8_t>((numValues * numBlocks + 7) / 8);
    fillRandomBits(bits.get(), 0.5, numValues * numBlocks);
    auto indices = allocate<int32_t>(numValues * numBlocks);
    auto indicesCounts = allocate<int32_t>(numBlocks);
    auto ops = allocate<GpuDecode>(numBlocks);
    for (int i = 0; i < numBlocks; ++i) {
      ops[i].step = DecodeStep::kMakeScatterIndices;
      auto& op = ops[i].data.makeScatterIndices;
      op.bits = bits.get();
      op.findSetBits = true;
      op.begin = i * numValues;
      op.end = op.begin + numValues;
      op.indices = indices.get() + i * numValues;
      op.indicesCount = indicesCounts.get() + i;
    }
    testCase(
        "",
        [&] { decodeGlobal<kBlockSize>(ops.get(), numBlocks); },
        numValues * numBlocks * sizeof(int32_t),
        3);
    for (int i = 0; i < numBlocks; ++i) {
      auto& op = ops[i].data.makeScatterIndices;
      int k = 0;
      for (int j = 0; j < numValues; ++j) {
        if (isSet(bits.get(), j + i * numValues)) {
          ASSERT_LT(k, *op.indicesCount);
          ASSERT_EQ(op.indices[k++], j);
        }
      }
      ASSERT_EQ(k, *op.indicesCount);
    }
  }

  void callViaPrograms(GpuDecode* ops, int32_t numOps) {
    auto stream = std::make_unique<Stream>();
    LaunchParams params(*arena_);
    DecodePrograms programs;
    for (int i = 0; i < numOps; ++i) {
      programs.programs.emplace_back();
      programs.programs.back().push_back(std::make_unique<GpuDecode>());
      auto opPtr = programs.programs.back().front().get();
      *opPtr = ops[i];
    }
    launchDecode(programs, params, stream.get());
    stream->wait();
  }

  void testMakeScatterIndicesStream(int numValues, int numBlocks) {
    auto bits = allocate<uint8_t>((numValues * numBlocks + 7) / 8);
    fillRandomBits(bits.get(), 0.5, numValues * numBlocks);
    auto indices = allocate<int32_t>(numValues * numBlocks);
    auto indicesCounts = allocate<int32_t>(numBlocks);
    DecodePrograms programs;
    for (int i = 0; i < numBlocks; ++i) {
      programs.programs.emplace_back();
      programs.programs.back().push_back(std::make_unique<GpuDecode>());
      auto opPtr = programs.programs.back().front().get();
      opPtr->step = DecodeStep::kMakeScatterIndices;
      auto& op = opPtr->data.makeScatterIndices;
      op.bits = bits.get();
      op.findSetBits = true;
      op.begin = i * numValues;
      op.end = op.begin + numValues;
      op.indices = indices.get() + i * numValues;
      op.indicesCount = indicesCounts.get() + i;
    }
    auto stream = std::make_unique<Stream>();
    LaunchParams params(*arena_);
    launchDecode(programs, params, stream.get());
    stream->wait();
    for (int i = 0; i < numBlocks; ++i) {
      auto& op = programs.programs[i].front()->data.makeScatterIndices;
      int k = 0;
      for (int j = 0; j < numValues; ++j) {
        if (isSet(bits.get(), j + i * numValues)) {
          ASSERT_LT(k, *op.indicesCount);
          ASSERT_EQ(op.indices[k++], j);
        }
      }
      ASSERT_EQ(k, *op.indicesCount);
    }
  }

  void testCountBits(int32_t numWords, int32_t stride) {
    auto bits = allocate<uint8_t>(numWords * 8);
    fillRandomBits(bits.get(), 0.5, numWords * 64);
    auto result = allocate<int32_t>(numWords * 64 / stride);
    // One int per warp.
    auto temp = allocate<int32_t>(8);
    DecodePrograms programs;
    programs.programs.emplace_back();
    programs.programs.back().push_back(std::make_unique<GpuDecode>());
    auto opPtr = programs.programs.back().front().get();
    opPtr->step = DecodeStep::kCountBits;
    auto& op = opPtr->data.countBits;
    opPtr->temp = temp.get();
    op.bits = bits.get();
    op.numBits = numWords * 64;
    op.resultStride = stride;
    opPtr->result = result.get();
    auto stream = std::make_unique<Stream>();
    LaunchParams params(*arena_);
    launchDecode(programs, params, stream.get());
    stream->wait();
    auto numResults = ((numWords * 64) - 1) / stride;
    auto* rawResult = result.get();
    int32_t count = 0;
    for (auto i = 0; i < numResults; ++i) {
      for (auto j = 0; j < stride / 64; j++) {
        count += __builtin_popcountl(
            reinterpret_cast<const uint64_t*>(op.bits)[i * (stride / 64) + j]);
      }
      EXPECT_EQ(count, rawResult[i]);
    }
  }

 private:
  std::unique_ptr<GpuArena> arena_;

  cudaEvent_t startEvent_;
  cudaEvent_t stopEvent_;
};

TEST_F(GpuDecoderTest, trivial) {
  testCopyPlan<uint64_t, 128>(40'000'003, 1024, false);
  testCopyPlan<uint64_t, 128>(40'000'003, 1024, true);
}

TEST_F(GpuDecoderTest, dictionaryOnBitpack) {
  dictTestPlan<int32_t, 256>(11, 4'000'037, 1024, false);
  dictTestPlan<int64_t, 256>(11, 4'000'037, 1024, false);
  dictTestPlan<int32_t, 256>(11, 40'000'003, 1024, false);
  dictTestPlan<int64_t, 256>(11, 40'000'003, 1024, false);
  dictTestPlan<int64_t, 256>(11, 40'000'003, 1024, true);
}

TEST_F(GpuDecoderTest, bitpack) {
  bool useSelective = FLAGS_use_selective;
  dictTestPlan<int32_t, 256>(27, 4000001, 1024, false, true, useSelective);
  dictTestPlan<int64_t, 256>(28, 4'000'037, 1024, false, true, useSelective);
  dictTestPlan<int32_t, 256>(26, 40'000'003, 1024, false, true, useSelective);
  dictTestPlan<int64_t, 256>(30, 40'000'003, 1024, false, true, useSelective);
  dictTestPlan<int64_t, 256>(47, 40'000'003, 1024, false, true, useSelective);
  dictTestPlan<int64_t, 256>(22, 40'000'003, 1024, true, true, false);
}

TEST_F(GpuDecoderTest, sparseBool) {
  testSparseBool<256>(40013, 1024);
}

TEST_F(GpuDecoderTest, varint) {
  testVarint<256>(4001, 1024);
}

TEST_F(GpuDecoderTest, mainlyConstant) {
  testMainlyConstant<int64_t, 256>(40013, 1024);
}

TEST_F(GpuDecoderTest, rleTotalLength) {
  testRleTotalLength<256>(40'000'003, 1024);
}

TEST_F(GpuDecoderTest, rle) {
  testRle<int64_t, 256>(40'000'003, 1024);
}

TEST_F(GpuDecoderTest, makeScatterIndices) {
  testMakeScatterIndices<256>(40013, 1024);
}

TEST_F(GpuDecoderTest, countBits) {
  testCountBits(10000, 256);
  testCountBits(20000, 512);
  testCountBits(30000, 1024);
  testCountBits(100000, 2048);
}

TEST_F(GpuDecoderTest, streamApi) {
  //  One call with few blocks, another with many, to cover inlined and out of
  //  line params.
  testMakeScatterIndicesStream(100, 20);
  testMakeScatterIndicesStream(999, 999);
}
} // namespace
} // namespace facebook::velox::wave

void printFuncAttrs(
    const std::string& heading,
    const cudaFuncAttributes& attrs) {
  std::cout << heading << " sharedSizeBytes=" << attrs.sharedSizeBytes
            << " constSizeBytes" << attrs.constSizeBytes
            << " localSizeBytes =" << attrs.localSizeBytes
            << "maxThreadsPerBlock=" << attrs.maxThreadsPerBlock
            << " numRegs=" << attrs.numRegs
            << " maxDynamicSharedSizeBytes=" << attrs.maxDynamicSharedSizeBytes
            << std::endl;
}
using namespace facebook::velox::wave;

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  folly::Init init{&argc, &argv};

  if (int device; cudaGetDevice(&device) != cudaSuccess) {
    std::cerr << "No CUDA detected, skipping all tests" << std::endl;
    return 0;
  }

  cudaDeviceProp prop;
  CUDA_CHECK_FATAL(cudaGetDeviceProperties(&prop, FLAGS_device_id));
  printf("Running on device: %s\n", prop.name);
  CUDA_CHECK_FATAL(cudaSetDevice(FLAGS_device_id));
  if (FLAGS_print_kernels) {
    cudaFuncAttributes attrs;
    CUDA_CHECK_FATAL(cudaFuncGetAttributes(&attrs, detail::decodeGlobal<128>));
    printFuncAttrs("decode blocksize 128", attrs);
    CUDA_CHECK_FATAL(cudaFuncGetAttributes(&attrs, detail::decodeGlobal<256>));
    printFuncAttrs("decode blocksize 256", attrs);
    CUDA_CHECK_FATAL(cudaFuncGetAttributes(&attrs, detail::decodeGlobal<512>));
    printFuncAttrs("decode blocksize 512", attrs);
    CUDA_CHECK_FATAL(cudaFuncGetAttributes(&attrs, detail::decodeGlobal<1024>));
    printFuncAttrs("decode blocksize 1024", attrs);
    printFuncAttrs("decode2", attrs);

    printKernels();
  }
  return RUN_ALL_TESTS();
}
