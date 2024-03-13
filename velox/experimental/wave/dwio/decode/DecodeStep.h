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

#include "velox/experimental/wave/common/Buffer.h"
#include "velox/experimental/wave/common/GpuArena.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

/// Instructions for GPU decode.  This can be decoding,
/// or pre/post processing other than decoding.
enum class DecodeStep {
  kConstant32,
  kConstant64,
  kConstantChar,
  kConstantBool,
  kConstantBytes,
  kTrivial,
  kTrivialNoOp,
  kMainlyConstant,
  kBitpack32,
  kBitpack64,
  kRleTotalLength,
  kRleBool,
  kRle,
  kDictionary,
  kDictionaryOnBitpack,
  kVarint,
  kNullable,
  kSentinel,
  kSparseBool,
  kMakeScatterIndices,
  kScatter32,
  kScatter64,
  kLengthToOffset,
  kMissing,
  kStruct,
  kArray,
  kMap,
  kFlatMap,
  kFlatMapNode,
  kUnsupported,
};

/// Describes a decoding loop's input and result disposition.
struct GpuDecode {
  // The operation to perform. Decides which branch of the union to use.
  DecodeStep step;

  struct Trivial {
    // Type of the input and result data.
    WaveTypeKind dataType;
    // Input data.
    const void* input;
    // Begin position for input, scatter and result.
    int begin;
    // End position (exclusive) for input, scatter and result.
    int end;
    // If not null, contains the output position relative to result pointer.
    const int32_t* scatter;
    // Starting address of the result.
    void* result;
  };

  struct MainlyConstant {
    // Type of the values and result.
    WaveTypeKind dataType;
    // Number of total values that should be written to result.
    int count;
    // Common value that is repeated.
    const void* commonValue;
    // Sparse values that will be picked up when isCommon is false.
    const void* otherValues;
    // Bitmask indicating whether a values is common or not.
    const uint8_t* isCommon;
    // Temporary storage to keep non-common value indices.  Should be
    // preallocated to at least count large.
    int32_t* otherIndices;
    // Starting address of the result.
    void* result;
    // If non-null, the count of non-common elements will be written to this
    // address.
    int32_t* otherCount;
  };

  struct DictionaryOnBitpack {
    // Type of the alphabet and result.
    WaveTypeKind dataType;
    // Dictionary alphabet.
    const void* alphabet;
    // Indices into the alphabet.
    const uint64_t* indices;
    // Begin position for indices, scatter and result.
    int begin;
    // End position (exclusive) for indices, scatter and result.
    int end;
    // Bit width of each index.
    int bitWidth;
    // If not null, contains the output position relative to result pointer.
    const int32_t* scatter;
    // All indices should be offseted by this baseline.
    int64_t baseline;
    // Starting address of the result.
    void* result;
  };

  struct Varint {
    // Address of the input data.
    const char* input;
    // Byte size of the input data.
    int size;
    // Temporary storage to keep whether each byte is a terminal for a number.
    // Should be allocated at least "size" large.
    bool* ends;
    // Temporary storage to keep the location of end position of each number.
    // Should be allocated at least "size" large.
    int32_t* endPos;
    // Type of the result number.
    WaveTypeKind resultType;
    // Starting address of the result.
    void* result;
    // Count of the numbers in the result.
    int resultSize;
  };

  struct SparseBool {
    // Number of bits in the result.
    int totalCount;
    // Sparse value; common value is the opposite of this.
    bool sparseValue;
    // Bit indices where sparse value should be stored.
    const int32_t* sparseIndices;
    // Number of sparse values.
    int sparseCount;
    // Temporary storage to keep bool representation of bits.  Should be at
    // least totalCount large.
    bool* bools;
    // Address of the result bits.  We do not support unaligned result because
    // different blocks could access the same byte and cause racing condition.
    uint8_t* result;
  };

  struct RleTotalLength {
    // Input data to be summed.
    const int32_t* input;
    // Number of input data.
    int count;
    // The sum of input data.
    int64_t* result;
  };

  struct Rle {
    // Type of values and result.
    WaveTypeKind valueType;
    // Values that will be repeated.
    const void* values;
    // Length of each value.
    const int32_t* lengths;
    // Number of values and lengths.
    int count;
    // Starting address of the result.
    const void* result;
  };

  struct MakeScatterIndices {
    // Input bits.
    const uint8_t* bits;
    // Whether set or unset bits should be found in the input.
    bool findSetBits;
    // Begin bit position of the input.
    int begin;
    // End bit position (exclusive) of the input.
    int end;
    // Address of the result indices.
    int32_t* indices;
    // If non-null, store the number of indices being written.
    int32_t* indicesCount;
  };

  union {
    Trivial trivial;
    MainlyConstant mainlyConstant;
    DictionaryOnBitpack dictionaryOnBitpack;
    Varint varint;
    SparseBool sparseBool;
    RleTotalLength rleTotalLength;
    Rle rle;
    MakeScatterIndices makeScatterIndices;
  } data;

  /// Returns the amount f shared memory for standard size thread block for
  /// 'step'.
  int32_t sharedMemorySize() const;
};

struct DecodePrograms {
  // Set of decode programs submitted as a unit. Each vector<DecodeStep> is run
  // on its own thread block. The consecutive DecodeSteps in the same program
  // are consecutive and the next one can depend on a previous one.
  std::vector<std::vector<std::unique_ptr<GpuDecode>>> programs;

  /// Unified or device memory   buffer where steps in 'programs' write results
  /// for the host. Decode results stay on device, only control information like
  /// filter result counts or length sums come to the host via this buffer. If
  /// nullptr, no data transfer is scheduled. 'result' should be nullptr if all
  /// steps are unconditional, like simple decoding.
  WaveBufferPtr result;
  /// Host addressable copy of 'result'. If result is unified memory
  /// this can be nullptr and we just enqueue a prefetch. to host. If
  /// 'result' is device memory, this should be pinned host memory
  /// with the same size.
  WaveBufferPtr hostResult;
};

void launchDecode(
    const DecodePrograms& programs,
    GpuArena* arena,
    WaveBufferPtr& extra,
    Stream* stream);

} // namespace facebook::velox::wave
