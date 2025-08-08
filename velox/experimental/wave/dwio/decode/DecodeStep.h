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
#include "velox/experimental/wave/common/ResultStaging.h"
#include "velox/experimental/wave/vector/Operand.h"

namespace facebook::velox::wave {

enum class NullMode : uint8_t {
  kDenseNonNull,
  kSparseNonNull,
  kDenseNullable,
  kSparseNullable
};

enum class WaveFilterKind : uint8_t {
  kAlwaysTrue,
  kNotNull,
  kNull,
  kBigintRange,
  kDoubleRange,
  kFloatRange,
  kBigintValues,
  kDictFilter
};

struct alignas(16) WaveFilterBase {
  union {
    int64_t int64Range[2];
    float floatRange[2];
    double doubleRange[2];
    struct {
      int32_t size;
      void* table;
    } values;
  } _;
  // flags for float/double range.
  bool lowerUnbounded;
  bool upperUnbounded;
  bool lowerExclusive;
  bool upperExclusive;
};

/// Instructions for GPU decode.  This can be decoding,
/// or pre/post processing other than decoding.
enum class DecodeStep {
  kSelective32,
  kCompact64,
  kSelective64,
  kSelective32Chunked,
  kSelective64Chunked,
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
  kRowCountNoFilter,
  kCountBits,
  kUnsupported,
};

enum class DictMode {
  // Decoded values are returned as is
  kNone,
  // Decoded values are indices into a dictionary.
  kDict,
  // Decoded values are indices into a dictionary and into a bitmap where 1
  // means filter passed.
  kDictFilter,
  // Decoded values are raw values and the filter result is to be stored into a
  // filter pass bitmap.
  kRecordFilter
};

class ColumnReader;

/// Describes a decoding loop's input and result disposition.
struct alignas(16) GpuDecode {
  /// Constant in 'numRows' to signify the number comes from 'blockstatus'.
  static constexpr int32_t kFilterHits = -1;

  // The operation to perform. Decides which branch of the union to use.
  DecodeStep step;
  DecodeStep encoding;

  WaveTypeKind dataType;

  /// If false, implies a not null filter. If there is a filter, specifies
  /// whether nulls pass.
  bool nullsAllowed{true};

  WaveFilterKind filterKind{WaveFilterKind::kAlwaysTrue};

  NullMode nullMode;

  /// Specifies use of dictionary.
  DictMode dictMode{DictMode::kNone};

  /// Number of chunks (e.g. Parquet pages). If > 1, different rows row ranges
  /// have different encodings. The first chunk's encoding is in 'data'. The
  /// next chunk's encoding is in the next GpuDecode's 'data'. Each chunk has
  /// its own 'nulls'. The input row numbers and output data/row numbers are
  /// given by the first GpuDecode.
  uint8_t numChunks{1};

  // Ordinal number of TB in TBs working on the same column. Each TB does a
  // multiple of TB width rows. The TBs for different ranges of rows are
  // launched in the same grid but are independent. The ordinal for non-first
  // TBs gets the base index for values.
  uint16_t nthBlock{0};
  /// Number of rows to process per thread of this block. This is equal across
  /// the grid, except for last block.
  uint16_t numRowsPerThread{1};

  /// Number of rows per thread in the grid, same for all blocks including the
  /// last one.
  uint16_t gridNumRowsPerThread{1};

  /// Number of rows to decode. if kFilterHits, the previous GpuDecode gives
  /// this number in BlockStatus. If 'rows' is set, this is the number of valid
  /// elements in 'rows'. If 'rows' is not set, the start is ''baseRow'
  int32_t maxRow{0};

  // If rows are densely decoded, this is the first row in terms of nullable
  // rows to decode in this TB.
  int32_t baseRow{0};

  /// Row count from filter. If a filter precedes this, the row count
  /// is read from this and 'rows' is set to the result row
  /// numbers. If this is a filter, the row count is written to
  /// 'blockStatus' and the row numbers are written to
  /// 'resultRows'. There is one BlockStatus for each kBlockSize rows
  /// in the grid. One TB covers 'numRowsPerThread' consecutive
  /// BlockStatuses. Because the previous filter for the range of rows
  /// is in the same TB read and write of row count is ordered by
  /// syncthreads at the end of each filter. The subscript is
  /// 'nthBlock * numRowsPerThread + <nth loop>'.
  BlockStatus* blockStatus{nullptr};

  /// Extra copy of result row count for each of kBlockSize rows if
  /// this is a filter. This is needed for non-last filters that
  /// extract values. The values have to be aligned after the final
  /// filtering result is known. This is no longer available in
  /// blockStatus.
  int32_t* filterRowCount{nullptr};

  // If multiple TBs on the same column and there are nulls, this is the start
  // offset of the TB's range of rows in non-null values. nullptr if no nulls.
  int32_t* nonNullBases{nullptr};

  /// If there are multiple chunks, this is an array of starts of non-first
  /// chunks.
  int32_t* chunkBounds{nullptr};

  /// If results will be scattered because of nulls, this is the bitmap with a 0
  /// for null. Subscripted with row number in encoding unit.
  char* nulls{nullptr};

  // If rows are sparsely decoded, this is the array of row numbers to extract.
  // The numbers are in terms of nullable rows.
  int32_t* rows{nullptr};

  // Data for pushed down filter. Interpretation depends on 'filterKind'.
  WaveFilterBase filter;

  // Temp storage. Requires 2 + (kBlockSize / kWarpThreads) ints for each TB.
  int32_t* temp;

  /// Row numbers that pass 'filter'. nullptr if no filter.
  int32_t* resultRows{nullptr};

  /// Result nulls.
  uint8_t* resultNulls{nullptr};

  /// Result array. nullptr if filter only.
  void* result{nullptr};

  // Bitmap of filter pass flags indexed by dictionary index. Filled in if
  // 'dictMode' is kDictFilter or kRecordFilter.
  uint32_t* filterBitmap{nullptr};

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

  struct SelectiveChunked {
    int32_t chunkStart;
    const void* input;
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

  struct RowCountNoFilter {
    int32_t numRows;
    BlockStatus* status;
    int32_t gridStatusSize;
    bool gridOnly;
  };

  struct CountBits {
    const uint8_t* bits;
    // Number of bits. A count is produced for each run of 'resultStride' bits
    // fully included in 'numBits'. If numBits is 600 and resultStride is 256
    // then result[0] is the count of ones in the first 256, then result[1] is
    // the count of ones in the first 512 and result[2] is unset.
    int32_t numBits;
    // 256/512/1024/2048.
    int32_t resultStride;
    // One int per warp (blockDim.x/32).
  };

  struct CompactValues {
    // Selected row numbers from the source filtered column.
    int32_t* sourceRows;
    // Row count for each kBlockSize rows of the source.
    int32_t* sourceNumRows;
    /// The rows selected by the last filter. blockStatus has the count.
    int32_t* finalRows;
    /// The results produced by the first filtered column.
    void* source;
    /// Null flags. nullptr if not nullable.
    uint8_t* sourceNull;
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
    RowCountNoFilter rowCountNoFilter;
    CountBits countBits;
    CompactValues compact;
    SelectiveChunked selectiveChunked;
  } data;

  /// True if 'nulls' is a bitmap of nulls. False if 'nulls' an array of uint8_t
  bool isNullsBitmap{true};

  /// Returns the amount of int aligned global memory per TB needed in 'temp'
  /// for standard size TB.
  int32_t tempSize() const;

  /// Returns the amount of shared memory for standard size thread block for
  /// 'step'.
  int32_t sharedMemorySize() const;

  /// Sets the pushed down filter from ScanSpec of 'reader'. Uses 'stream' to
  /// setup device-side data like hash tables.
  void setFilter(ColumnReader* reader, Stream* stream);
};

struct DecodePrograms {
  void clear() {
    programs.clear();
    result.clear();
  }

  // Set of decode programs submitted as a unit. Each vector<DecodeStep> is run
  // on its own thread block. The consecutive DecodeSteps in the same program
  // are consecutive and the next one can depend on a previous one.
  std::vector<std::vector<std::unique_ptr<GpuDecode>>> programs;

  /// Unified or device memory   buffer where steps in 'programs' write results
  /// for the host. Decode results stay on device, only control information like
  /// filter result counts or length sums come to the host via this buffer. If
  /// nullptr, no data transfer is scheduled. 'result' should be nullptr if all
  /// steps are unconditional, like simple decoding.
  ResultBuffer result;
};

void launchDecode(
    const DecodePrograms& programs,
    LaunchParams& params,
    Stream* stream);

} // namespace facebook::velox::wave
