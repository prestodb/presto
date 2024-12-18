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

// Imported from Apache Impala (incubating) on 2016-01-29 and modified for use
// in parquet-cpp, Arrow

// Adapted from Apache Arrow.

#pragma once

#include <algorithm>
#include <cmath>
#include <limits>
#include <vector>

#include "arrow/util/bit_block_counter.h"
#include "arrow/util/bit_run_reader.h"

#include "velox/dwio/parquet/common/BitStreamUtilsInternal.h"

namespace facebook::velox::parquet {
/// Utility classes to do run length encoding (RLE) for fixed bit width values.
/// If runs are sufficiently long, RLE is used, otherwise, the values are just
/// bit-packed (literal encoding). For both types of runs, there is a
/// byte-aligned indicator which encodes the length of the run and the type of
/// the run. This encoding has the benefit that when there aren't any long
/// enough runs, values are always decoded at fixed (can be precomputed) bit
/// offsets OR both the value and the run length are byte aligned. This allows
/// for very efficient decoding implementations. The encoding is:
///    encoded-block := run*
///    run := literal-run | repeated-run
///    literal-run := literal-indicator < literal bytes >
///    repeated-run := repeated-indicator < repeated value. padded to byte
///    boundary > literal-indicator := varint_encode( number_of_groups << 1 | 1)
///    repeated-indicator := varint_encode( number_of_repetitions << 1 )
//
/// Each run is preceded by a varint. The varint's least significant bit is
/// used to indicate whether the run is a literal run or a repeated run. The
/// rest of the varint is used to determine the length of the run (eg how many
/// times the value repeats).
//
/// In the case of literal runs, the run length is always a multiple of 8 (i.e.
/// encode in groups of 8), so that no matter the bit-width of the value, the
/// sequence will end on a byte boundary without padding. Given that we know it
/// is a multiple of 8, we store the number of 8-groups rather than the actual
/// number of encoded ints. (This means that the total number of encoded values
/// cannot be determined from the encoded data, since the number of values in
/// the last group may not be a multiple of 8). For the last group of literal
/// runs, we pad the group to 8 with zeros. This allows for 8 at a time decoding
/// on the read side without the need for additional checks.
//
/// There is a break-even point when it is more storage efficient to do run
/// length encoding.  For 1 bit-width values, that point is 8 values.  They
/// require 2 bytes for both the repeated encoding or the literal encoding. This
/// value can always be computed based on the bit-width.
/// TODO: think about how to use this for strings.  The bit packing isn't quite
/// the same.
//
/// Examples with bit-width 1 (eg encoding booleans):
/// ----------------------------------------
/// 100 1s followed by 100 0s:
/// <varint(100 << 1)> <1, padded to 1 byte> <varint(100 << 1)> <0, padded to 1
/// byte>
///  - (total 4 bytes)
//
/// alternating 1s and 0s (200 total):
/// 200 ints = 25 groups of 8
/// <varint((25 << 1) | 1)> <25 bytes of values, bitpacked>
/// (total 26 bytes, 1 byte overhead)
//

/// Decoder class for RLE encoded data.
class RleDecoder {
 public:
  /// Create a decoder object. buffer/bufferLen is the decoded data.
  /// bitWidth is the width of each value (before encoding).
  RleDecoder(const uint8_t* buffer, int bufferLen, int bitWidth)
      : bitReader_(buffer, bufferLen),
        bitWidth_(bitWidth),
        currentValue_(0),
        repeatCount_(0),
        literalCount_(0) {
    VELOX_DCHECK_GE(bitWidth_, 0);
    VELOX_DCHECK_LE(bitWidth_, 64);
  }

  RleDecoder() : bitWidth_(-1) {}

  void Reset(const uint8_t* buffer, int bufferLen, int bitWidth) {
    VELOX_DCHECK_GE(bitWidth, 0);
    VELOX_DCHECK_LE(bitWidth, 64);
    bitReader_.Reset(buffer, bufferLen);
    bitWidth_ = bitWidth;
    currentValue_ = 0;
    repeatCount_ = 0;
    literalCount_ = 0;
  }

  /// Gets the next value.  Returns false if there are no more.
  template <typename T>
  bool Get(T* val);

  /// Gets a batch of values.  Returns the number of decoded elements.
  template <typename T>
  int GetBatch(T* values, int batchSize);

  /// Like GetBatch but add spacing for null entries
  template <typename T>
  int GetBatchSpaced(
      int batchSize,
      int nullCount,
      const uint8_t* validBits,
      int64_t validBitsOffset,
      T* out);

  /// Like GetBatch but the values are then decoded using the provided
  /// dictionary
  template <typename T>
  int GetBatchWithDict(
      const T* dictionary,
      int32_t dictionaryLength,
      T* values,
      int batchSize);

  /// Like GetBatchWithDict but add spacing for null entries
  ///
  /// Null entries will be zero-initialized in `values` to avoid leaking
  /// private data.
  template <typename T>
  int GetBatchWithDictSpaced(
      const T* dictionary,
      int32_t dictionaryLength,
      T* values,
      int batchSize,
      int nullCount,
      const uint8_t* validBits,
      int64_t validBitsOffset);

 protected:
  BitReader bitReader_;
  /// Number of bits needed to encode the value. Must be between 0 and 64.
  int bitWidth_;
  uint64_t currentValue_;
  int32_t repeatCount_;
  int32_t literalCount_;

 private:
  /// Fills literalCount_ and repeatCount_ with next values. Returns false if
  /// there are no more.
  template <typename T>
  bool NextCounts();

  /// Utility methods for retrieving spaced values.
  template <typename T, typename RunType, typename Converter>
  int GetSpaced(
      Converter converter,
      int batchSize,
      int nullCount,
      const uint8_t* validBits,
      int64_t validBitsOffset,
      T* out);
};

/// Class to incrementally build the rle data.   This class does not allocate
/// any memory. The encoding has two modes: encoding repeated runs and literal
/// runs. If the run is sufficiently short, it is more efficient to encode as a
/// literal run. This class does so by buffering 8 values at a time.  If they
/// are not all the same they are added to the literal run.  If they are the
/// same, they are added to the repeated run.  When we switch modes, the
/// previous run is flushed out.
class RleEncoder {
 public:
  /// buffer/bufferLen: preallocated output buffer.
  /// bitWidth: max number of bits for value.
  /// TODO: consider adding a min_repeated_run_length so the caller can control
  /// when values should be encoded as repeated runs.  Currently this is derived
  /// based on the bitWidth, which can determine a storage optimal choice.
  /// TODO: allow 0 bitWidth (and have dict encoder use it)
  RleEncoder(uint8_t* buffer, int bufferLen, int bitWidth)
      : bitWidth_(bitWidth), bitWriter_(buffer, bufferLen) {
    VELOX_DCHECK_GE(bitWidth_, 0);
    VELOX_DCHECK_LE(bitWidth_, 64);
    maxRunByteSize_ = MinBufferSize(bitWidth);
    VELOX_DCHECK_GE(bufferLen, maxRunByteSize_, "Input buffer not big enough.");
    Clear();
  }

  /// Returns the minimum buffer size needed to use the encoder for 'bitWidth'
  /// This is the maximum length of a single run for 'bitWidth'.
  /// It is not valid to pass a buffer less than this length.
  static int MinBufferSize(int bitWidth) {
    /// 1 indicator byte and MAX_VALUES_PER_LITERAL_RUN 'bitWidth' values.
    int maxLiteralRunSize = 1 +
        static_cast<int>(::arrow::bit_util::BytesForBits(
            MAX_VALUES_PER_LITERAL_RUN * bitWidth));
    /// Up to kMaxVlqByteLength indicator and a single 'bitWidth' value.
    int maxRepeatedRunSize = BitReader::kMaxVlqByteLength +
        static_cast<int>(::arrow::bit_util::BytesForBits(bitWidth));
    return std::max(maxLiteralRunSize, maxRepeatedRunSize);
  }

  /// Returns the maximum byte size it could take to encode 'numValues'.
  static int MaxBufferSize(int bitWidth, int numValues) {
    // For a bitWidth > 1, the worst case is the repetition of "literal run of
    // length 8 and then a repeated run of length 8". 8 values per smallest run,
    // 8 bits per byte
    int bytesPerRun = bitWidth;
    int numRuns = static_cast<int>(::arrow::bit_util::CeilDiv(numValues, 8));
    int literalMaxSize = numRuns + numRuns * bytesPerRun;

    // In the very worst case scenario, the data is a concatenation of repeated
    // runs of 8 values. Repeated run has a 1 byte varint followed by the
    // bit-packed repeated value
    int minRepeatedRunSize =
        1 + static_cast<int>(::arrow::bit_util::BytesForBits(bitWidth));
    int repeatedMaxSize = numRuns * minRepeatedRunSize;

    return std::max(literalMaxSize, repeatedMaxSize);
  }

  /// Encode value.  Returns true if the value fits in buffer, false otherwise.
  /// This value must be representable with bitWidth_ bits.
  bool Put(uint64_t value);

  /// Flushes any pending values to the underlying buffer.
  /// Returns the total number of bytes written
  int Flush();

  /// Resets all the state in the encoder.
  void Clear();

  /// Returns pointer to underlying buffer
  uint8_t* buffer() {
    return bitWriter_.buffer();
  }
  int32_t len() {
    return bitWriter_.bytesWritten();
  }

 private:
  /// Flushes any buffered values.  If this is part of a repeated run, this is
  /// largely a no-op. If it is part of a literal run, this will call
  /// FlushLiteralRun, which writes out the buffered literal values. If 'done'
  /// is true, the current run would be written even if it would normally have
  /// been buffered more.  This should only be called at the end, when the
  /// encoder has received all values even if it would normally continue to be
  /// buffered.
  void FlushBufferedValues(bool done);

  /// Flushes literal values to the underlying buffer.  If
  /// updateIndicatorByte, then the current literal run is complete and the
  /// indicator byte is updated.
  void FlushLiteralRun(bool updateIndicatorByte);

  /// Flushes a repeated run to the underlying buffer.
  void FlushRepeatedRun();

  /// Checks and sets bufferFull_. This must be called after flushing a run to
  /// make sure there are enough bytes remaining to encode the next run.
  void CheckBufferFull();

  /// The maximum number of values in a single literal run
  /// (number of groups encodable by a 1-byte indicator * 8)
  static const int MAX_VALUES_PER_LITERAL_RUN = (1 << 6) * 8;

  /// Number of bits needed to encode the value. Must be between 0 and 64.
  const int bitWidth_;

  /// Underlying buffer.
  BitWriter bitWriter_;

  /// If true, the buffer is full and subsequent Put()'s will fail.
  bool bufferFull_;

  /// The maximum byte size a single run can take.
  int maxRunByteSize_;

  /// We need to buffer at most 8 values for literals.  This happens when the
  /// bitWidth is 1 (so 8 values fit in one byte).
  /// TODO: generalize this to other bit widths
  int64_t bufferedValues_[8];

  /// Number of values in bufferedValues_
  int numBufferedValues_;

  /// The current (also last) value that was written and the count of how
  /// many times in a row that value has been seen.  This is maintained even
  /// if we are in a literal run.  If the repeatCount_ get high enough, we
  /// switch to encoding repeated runs.
  uint64_t currentValue_;
  int repeatCount_;

  /// Number of literals in the current run.  This does not include the literals
  /// that might be in bufferedValues_.  Only after we've got a group big
  /// enough can we decide if they should part of the literalCount_ or
  /// repeatCount_
  int literalCount_;

  /// Pointer to a byte in the underlying buffer that stores the indicator byte.
  /// This is reserved as soon as we need a literal run but the value is written
  /// when the literal run is complete.
  uint8_t* literalIndicatorByte_;
};

template <typename T>
inline bool RleDecoder::Get(T* val) {
  return GetBatch(val, 1) == 1;
}

template <typename T>
inline int RleDecoder::GetBatch(T* values, int batchSize) {
  VELOX_DCHECK_GE(bitWidth_, 0);
  int valuesRead = 0;

  auto* out = values;

  while (valuesRead < batchSize) {
    int remaining = batchSize - valuesRead;

    if (repeatCount_ > 0) { // Repeated value case.
      int repeatBatch = std::min(remaining, repeatCount_);
      std::fill(out, out + repeatBatch, static_cast<T>(currentValue_));

      repeatCount_ -= repeatBatch;
      valuesRead += repeatBatch;
      out += repeatBatch;
    } else if (literalCount_ > 0) {
      int literalBatch = std::min(remaining, literalCount_);
      int actualRead = bitReader_.GetBatch(bitWidth_, out, literalBatch);
      if (actualRead != literalBatch) {
        return valuesRead;
      }

      literalCount_ -= literalBatch;
      valuesRead += literalBatch;
      out += literalBatch;
    } else {
      if (!NextCounts<T>())
        return valuesRead;
    }
  }

  return valuesRead;
}

template <typename T, typename RunType, typename Converter>
inline int RleDecoder::GetSpaced(
    Converter converter,
    int batchSize,
    int nullCount,
    const uint8_t* validBits,
    int64_t validBitsOffset,
    T* out) {
  if (FOLLY_UNLIKELY(nullCount == batchSize)) {
    converter.FillZero(out, out + batchSize);
    return batchSize;
  }

  VELOX_DCHECK_GE(bitWidth_, 0);
  int valuesRead = 0;
  int valuesRemaining = batchSize - nullCount;

  // Assume no bits to start.
  ::arrow::internal::BitRunReader bitReader(
      validBits,
      validBitsOffset,
      /*length=*/batchSize);
  ::arrow::internal::BitRun validRun = bitReader.NextRun();
  while (valuesRead < batchSize) {
    if (FOLLY_UNLIKELY(validRun.length == 0)) {
      validRun = bitReader.NextRun();
    }

    VELOX_DCHECK_GT(batchSize, 0);
    VELOX_DCHECK_GT(validRun.length, 0);

    if (validRun.set) {
      if ((repeatCount_ == 0) && (literalCount_ == 0)) {
        if (!NextCounts<RunType>())
          return valuesRead;
        VELOX_DCHECK((repeatCount_ > 0) ^ (literalCount_ > 0));
      }

      if (repeatCount_ > 0) {
        int repeatBatch = 0;
        // Consume the entire repeat counts incrementing repeatBatch to
        // be the total of nulls + values consumed, we only need to
        // get the total count because we can fill in the same value for
        // nulls and non-nulls. This proves to be a big efficiency win.
        while (repeatCount_ > 0 && (valuesRead + repeatBatch) < batchSize) {
          VELOX_DCHECK_GT(validRun.length, 0);
          if (validRun.set) {
            int updateSize =
                std::min(static_cast<int>(validRun.length), repeatCount_);
            repeatCount_ -= updateSize;
            repeatBatch += updateSize;
            validRun.length -= updateSize;
            valuesRemaining -= updateSize;
          } else {
            // We can consume all nulls here because we would do so on
            //  the next loop anyways.
            repeatBatch += static_cast<int>(validRun.length);
            validRun.length = 0;
          }
          if (validRun.length == 0) {
            validRun = bitReader.NextRun();
          }
        }
        RunType currentValue = static_cast<RunType>(currentValue_);
        if (FOLLY_UNLIKELY(!converter.IsValid(currentValue))) {
          return valuesRead;
        }
        converter.Fill(out, out + repeatBatch, currentValue);
        out += repeatBatch;
        valuesRead += repeatBatch;
      } else if (literalCount_ > 0) {
        int literalBatch = std::min(valuesRemaining, literalCount_);
        VELOX_DCHECK_GT(literalBatch, 0);

        // Decode the literals
        constexpr int kBufferSize = 1024;
        RunType indices[kBufferSize];
        literalBatch = std::min(literalBatch, kBufferSize);
        int actualRead = bitReader_.GetBatch(bitWidth_, indices, literalBatch);
        if (FOLLY_UNLIKELY(actualRead != literalBatch)) {
          return valuesRead;
        }
        if (!converter.IsValid(indices, /*length=*/actualRead)) {
          return valuesRead;
        }
        int skipped = 0;
        int literalsRead = 0;
        while (literalsRead < literalBatch) {
          if (validRun.set) {
            int updateSize = std::min(
                literalBatch - literalsRead, static_cast<int>(validRun.length));
            converter.Copy(out, indices + literalsRead, updateSize);
            literalsRead += updateSize;
            out += updateSize;
            validRun.length -= updateSize;
          } else {
            converter.FillZero(out, out + validRun.length);
            out += validRun.length;
            skipped += static_cast<int>(validRun.length);
            validRun.length = 0;
          }
          if (validRun.length == 0) {
            validRun = bitReader.NextRun();
          }
        }
        literalCount_ -= literalBatch;
        valuesRemaining -= literalBatch;
        valuesRead += literalBatch + skipped;
      }
    } else {
      converter.FillZero(out, out + validRun.length);
      out += validRun.length;
      valuesRead += static_cast<int>(validRun.length);
      validRun.length = 0;
    }
  }
  VELOX_DCHECK_EQ(validRun.length, 0);
  VELOX_DCHECK_EQ(valuesRemaining, 0);
  return valuesRead;
}

// Converter for GetSpaced that handles runs that get returned
// directly as output.
template <typename T>
struct PlainRleConverter {
  T kZero = {};
  inline bool IsValid(const T& values) const {
    return true;
  }
  inline bool IsValid(const T* values, int32_t length) const {
    return true;
  }
  inline void Fill(T* begin, T* end, const T& runValue) const {
    std::fill(begin, end, runValue);
  }
  inline void FillZero(T* begin, T* end) {
    std::fill(begin, end, kZero);
  }
  inline void Copy(T* out, const T* values, int length) const {
    std::memcpy(out, values, length * sizeof(T));
  }
};

template <typename T>
inline int RleDecoder::GetBatchSpaced(
    int batchSize,
    int nullCount,
    const uint8_t* validBits,
    int64_t validBitsOffset,
    T* out) {
  if (nullCount == 0) {
    return GetBatch<T>(out, batchSize);
  }

  PlainRleConverter<T> converter;
  ::arrow::internal::BitBlockCounter blockCounter(
      validBits, validBitsOffset, batchSize);

  int totalProcessed = 0;
  int processed = 0;
  ::arrow::internal::BitBlockCount block;

  do {
    block = blockCounter.NextFourWords();
    if (block.length == 0) {
      break;
    }
    if (block.AllSet()) {
      processed = GetBatch<T>(out, block.length);
    } else if (block.NoneSet()) {
      converter.FillZero(out, out + block.length);
      processed = block.length;
    } else {
      processed = GetSpaced<T, /*RunType=*/T, PlainRleConverter<T>>(
          converter,
          block.length,
          block.length - block.popcount,
          validBits,
          validBitsOffset,
          out);
    }
    totalProcessed += processed;
    out += block.length;
    validBitsOffset += block.length;
  } while (processed == block.length);
  return totalProcessed;
}

static inline bool IndexInRange(int32_t idx, int32_t dictionaryLength) {
  return idx >= 0 && idx < dictionaryLength;
}

// Converter for GetSpaced that handles runs of returned dictionary
// indices.
template <typename T>
struct DictionaryConverter {
  T kZero = {};
  const T* dictionary;
  int32_t dictionaryLength;

  inline bool IsValid(int32_t value) {
    return IndexInRange(value, dictionaryLength);
  }

  inline bool IsValid(const int32_t* values, int32_t length) const {
    using IndexType = int32_t;
    IndexType minIndex = std::numeric_limits<IndexType>::max();
    IndexType maxIndex = std::numeric_limits<IndexType>::min();
    for (int x = 0; x < length; x++) {
      minIndex = std::min(values[x], minIndex);
      maxIndex = std::max(values[x], maxIndex);
    }

    return IndexInRange(minIndex, dictionaryLength) &&
        IndexInRange(maxIndex, dictionaryLength);
  }
  inline void Fill(T* begin, T* end, const int32_t& runValue) const {
    std::fill(begin, end, dictionary[runValue]);
  }
  inline void FillZero(T* begin, T* end) {
    std::fill(begin, end, kZero);
  }

  inline void Copy(T* out, const int32_t* values, int length) const {
    for (int x = 0; x < length; x++) {
      out[x] = dictionary[values[x]];
    }
  }
};

template <typename T>
inline int RleDecoder::GetBatchWithDict(
    const T* dictionary,
    int32_t dictionaryLength,
    T* values,
    int batchSize) {
  // Per https://github.com/apache/parquet-format/blob/master/Encodings.md,
  // the maximum dictionary index width in Parquet is 32 bits.
  using IndexType = int32_t;
  DictionaryConverter<T> converter;
  converter.dictionary = dictionary;
  converter.dictionaryLength = dictionaryLength;

  VELOX_DCHECK_GE(bitWidth_, 0);
  int valuesRead = 0;

  auto* out = values;

  while (valuesRead < batchSize) {
    int remaining = batchSize - valuesRead;

    if (repeatCount_ > 0) {
      auto idx = static_cast<IndexType>(currentValue_);
      if (FOLLY_UNLIKELY(!IndexInRange(idx, dictionaryLength))) {
        return valuesRead;
      }
      T val = dictionary[idx];

      int repeatBatch = std::min(remaining, repeatCount_);
      std::fill(out, out + repeatBatch, val);

      /* Upkeep counters */
      repeatCount_ -= repeatBatch;
      valuesRead += repeatBatch;
      out += repeatBatch;
    } else if (literalCount_ > 0) {
      constexpr int kBufferSize = 1024;
      IndexType indices[kBufferSize];

      int literalBatch = std::min(remaining, literalCount_);
      literalBatch = std::min(literalBatch, kBufferSize);

      int actualRead = bitReader_.GetBatch(bitWidth_, indices, literalBatch);
      if (FOLLY_UNLIKELY(actualRead != literalBatch)) {
        return valuesRead;
      }
      if (FOLLY_UNLIKELY(
              !converter.IsValid(indices, /*length=*/literalBatch))) {
        return valuesRead;
      }
      converter.Copy(out, indices, literalBatch);

      /* Upkeep counters */
      literalCount_ -= literalBatch;
      valuesRead += literalBatch;
      out += literalBatch;
    } else {
      if (!NextCounts<IndexType>())
        return valuesRead;
    }
  }

  return valuesRead;
}

template <typename T>
inline int RleDecoder::GetBatchWithDictSpaced(
    const T* dictionary,
    int32_t dictionaryLength,
    T* out,
    int batchSize,
    int nullCount,
    const uint8_t* validBits,
    int64_t validBitsOffset) {
  if (nullCount == 0) {
    return GetBatchWithDict<T>(dictionary, dictionaryLength, out, batchSize);
  }
  ::arrow::internal::BitBlockCounter blockCounter(
      validBits, validBitsOffset, batchSize);
  using IndexType = int32_t;
  DictionaryConverter<T> converter;
  converter.dictionary = dictionary;
  converter.dictionaryLength = dictionaryLength;

  int totalProcessed = 0;
  int processed = 0;
  ::arrow::internal::BitBlockCount block;
  do {
    block = blockCounter.NextFourWords();
    if (block.length == 0) {
      break;
    }
    if (block.AllSet()) {
      processed =
          GetBatchWithDict<T>(dictionary, dictionaryLength, out, block.length);
    } else if (block.NoneSet()) {
      converter.FillZero(out, out + block.length);
      processed = block.length;
    } else {
      processed = GetSpaced<T, /*RunType=*/IndexType, DictionaryConverter<T>>(
          converter,
          block.length,
          block.length - block.popcount,
          validBits,
          validBitsOffset,
          out);
    }
    totalProcessed += processed;
    out += block.length;
    validBitsOffset += block.length;
  } while (processed == block.length);
  return totalProcessed;
}

template <typename T>
bool RleDecoder::NextCounts() {
  // Read the next run's indicator int, it could be a literal or repeated run.
  // The int is encoded as a vlq-encoded value.
  uint32_t indicatorValue = 0;
  if (!bitReader_.GetVlqInt(&indicatorValue))
    return false;

  // lsb indicates if it is a literal run or repeated run
  bool isLiteral = indicatorValue & 1;
  uint32_t count = indicatorValue >> 1;
  if (isLiteral) {
    if (FOLLY_UNLIKELY(
            count == 0 || count > static_cast<uint32_t>(INT32_MAX) / 8)) {
      return false;
    }
    literalCount_ = count * 8;
  } else {
    if (FOLLY_UNLIKELY(
            count == 0 || count > static_cast<uint32_t>(INT32_MAX))) {
      return false;
    }
    repeatCount_ = count;
    T value = {};
    if (!bitReader_.GetAligned<T>(
            static_cast<int>(::arrow::bit_util::CeilDiv(bitWidth_, 8)),
            &value)) {
      return false;
    }
    currentValue_ = static_cast<uint64_t>(value);
  }
  return true;
}

/// This function buffers input values 8 at a time.  After seeing all 8 values,
/// it decides whether they should be encoded as a literal or repeated run.
inline bool RleEncoder::Put(uint64_t value) {
  VELOX_DCHECK(bitWidth_ == 64 || value < (1ULL << bitWidth_));
  if (FOLLY_UNLIKELY(bufferFull_))
    return false;

  if (FOLLY_LIKELY(currentValue_ == value)) {
    ++repeatCount_;
    if (repeatCount_ > 8) {
      // This is just a continuation of the current run, no need to buffer the
      // values.
      // Note that this is the fast path for long repeated runs.
      return true;
    }
  } else {
    if (repeatCount_ >= 8) {
      // We had a run that was long enough but it has ended.  Flush the
      // current repeated run.
      VELOX_DCHECK_EQ(literalCount_, 0);
      FlushRepeatedRun();
    }
    repeatCount_ = 1;
    currentValue_ = value;
  }

  bufferedValues_[numBufferedValues_] = value;
  if (++numBufferedValues_ == 8) {
    VELOX_DCHECK_EQ(literalCount_ % 8, 0);
    FlushBufferedValues(false);
  }
  return true;
}

inline void RleEncoder::FlushLiteralRun(bool updateIndicatorByte) {
  if (literalIndicatorByte_ == NULL) {
    // The literal indicator byte has not been reserved yet, get one now.
    literalIndicatorByte_ = bitWriter_.GetNextBytePtr();
    VELOX_DCHECK(literalIndicatorByte_ != NULL);
  }

  // Write all the buffered values as bit packed literals
  for (int i = 0; i < numBufferedValues_; ++i) {
    VELOX_DEBUG_ONLY bool success =
        bitWriter_.PutValue(bufferedValues_[i], bitWidth_);
    VELOX_DCHECK(success, "There is a bug in using CheckBufferFull()");
  }
  numBufferedValues_ = 0;

  if (updateIndicatorByte) {
    // At this point we need to write the indicator byte for the literal run.
    // We only reserve one byte, to allow for streaming writes of literal
    // values. The logic makes sure we flush literal runs often enough to not
    // overrun the 1 byte.
    VELOX_DCHECK_EQ(literalCount_ % 8, 0);
    int numGroups = literalCount_ / 8;
    int32_t indicatorValue = (numGroups << 1) | 1;
    VELOX_DCHECK_EQ(indicatorValue & 0xFFFFFF00, 0);
    *literalIndicatorByte_ = static_cast<uint8_t>(indicatorValue);
    literalIndicatorByte_ = NULL;
    literalCount_ = 0;
    CheckBufferFull();
  }
}

inline void RleEncoder::FlushRepeatedRun() {
  VELOX_DCHECK_GT(repeatCount_, 0);
  VELOX_DEBUG_ONLY bool result = true;
  // The lsb of 0 indicates this is a repeated run
  int32_t indicatorValue = repeatCount_ << 1 | 0;
  result &= bitWriter_.PutVlqInt(static_cast<uint32_t>(indicatorValue));
  result &= bitWriter_.PutAligned(
      currentValue_,
      static_cast<int>(::arrow::bit_util::CeilDiv(bitWidth_, 8)));
  VELOX_DCHECK(result);
  numBufferedValues_ = 0;
  repeatCount_ = 0;
  CheckBufferFull();
}

/// Flush the values that have been buffered.  At this point we decide whether
/// we need to switch between the run types or continue the current one.
inline void RleEncoder::FlushBufferedValues(bool done) {
  if (repeatCount_ >= 8) {
    // Clear the buffered values.  They are part of the repeated run now and we
    // don't want to flush them out as literals.
    numBufferedValues_ = 0;
    if (literalCount_ != 0) {
      // There was a current literal run.  All the values in it have been
      // flushed but we still need to update the indicator byte.
      VELOX_DCHECK_EQ(literalCount_ % 8, 0);
      VELOX_DCHECK_EQ(repeatCount_, 8);
      FlushLiteralRun(true);
    }
    VELOX_DCHECK_EQ(literalCount_, 0);
    return;
  }

  literalCount_ += numBufferedValues_;
  VELOX_DCHECK_EQ(literalCount_ % 8, 0);
  int numGroups = literalCount_ / 8;
  if (numGroups + 1 >= (1 << 6)) {
    // We need to start a new literal run because the indicator byte we've
    // reserved cannot store more values.
    VELOX_DCHECK(literalIndicatorByte_ != NULL);
    FlushLiteralRun(true);
  } else {
    FlushLiteralRun(done);
  }
  repeatCount_ = 0;
}

inline int RleEncoder::Flush() {
  if (literalCount_ > 0 || repeatCount_ > 0 || numBufferedValues_ > 0) {
    bool allRepeat = literalCount_ == 0 &&
        (repeatCount_ == numBufferedValues_ || numBufferedValues_ == 0);
    // There is something pending, figure out if it's a repeated or literal run
    if (repeatCount_ > 0 && allRepeat) {
      FlushRepeatedRun();
    } else {
      VELOX_DCHECK_EQ(literalCount_ % 8, 0);
      // Buffer the last group of literals to 8 by padding with 0s.
      for (; numBufferedValues_ != 0 && numBufferedValues_ < 8;
           ++numBufferedValues_) {
        bufferedValues_[numBufferedValues_] = 0;
      }
      literalCount_ += numBufferedValues_;
      FlushLiteralRun(true);
      repeatCount_ = 0;
    }
  }
  bitWriter_.Flush();
  VELOX_DCHECK_EQ(numBufferedValues_, 0);
  VELOX_DCHECK_EQ(literalCount_, 0);
  VELOX_DCHECK_EQ(repeatCount_, 0);

  return bitWriter_.bytesWritten();
}

inline void RleEncoder::CheckBufferFull() {
  int bytesWritten = bitWriter_.bytesWritten();
  if (bytesWritten + maxRunByteSize_ > bitWriter_.bufferLen()) {
    bufferFull_ = true;
  }
}

inline void RleEncoder::Clear() {
  bufferFull_ = false;
  currentValue_ = 0;
  repeatCount_ = 0;
  numBufferedValues_ = 0;
  literalCount_ = 0;
  literalIndicatorByte_ = NULL;
  bitWriter_.Clear();
}

} // namespace facebook::velox::parquet
