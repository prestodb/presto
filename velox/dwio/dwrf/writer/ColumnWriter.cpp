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

#include "velox/dwio/dwrf/writer/ColumnWriter.h"
#include <velox/dwio/common/exception/Exception.h>
#include "velox/dwio/common/ChainedBuffer.h"
#include "velox/dwio/dwrf/common/EncoderUtil.h"
#include "velox/dwio/dwrf/writer/DictionaryEncodingUtils.h"
#include "velox/dwio/dwrf/writer/EntropyEncodingSelector.h"
#include "velox/dwio/dwrf/writer/FlatMapColumnWriter.h"
#include "velox/dwio/dwrf/writer/IntegerDictionaryEncoder.h"
#include "velox/dwio/dwrf/writer/StatisticsBuilderUtils.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::memory;

namespace facebook::velox::dwrf {

WriterContext::LocalDecodedVector BaseColumnWriter::decode(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  auto& selected = context_.getSharedSelectivityVector(slice->size());
  // initialize
  selected.clearAll();
  for (auto& range : ranges.getRanges()) {
    selected.setValidRange(std::get<0>(range), std::get<1>(range), true);
  }
  selected.updateBounds();
  // decode
  auto localDecoded = context_.getLocalDecodedVector();
  localDecoded.get().decode(*slice, selected);
  return localDecoded;
}

namespace {

template <typename T>
class ByteRleColumnWriter : public BaseColumnWriter {
 public:
  ByteRleColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<std::unique_ptr<ByteRleEncoder>(
          std::unique_ptr<BufferedOutputStream>)> factory,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        data_{factory(newStream(StreamKind::StreamKind_DATA))} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    data_->flush();
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    data_->recordPosition(*indexBuilder_);
  }

 private:
  std::unique_ptr<ByteRleEncoder> data_;
};

template <>
uint64_t ByteRleColumnWriter<int8_t>::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  static_assert(sizeof(int8_t) == sizeof(char), "unexpected type width");
  static_assert(NULL_SIZE == 1, "unexpected raw data size");
  if (slice->encoding() == VectorEncoding::Simple::FLAT) {
    // Optimized path for flat vectors
    auto flatVector = slice->as<FlatVector<int8_t>>();
    DWIO_ENSURE(flatVector, "unexpected vector type");
    writeNulls(slice, ranges);
    const auto* nulls = slice->rawNulls();
    const auto* vals = flatVector->template rawValues<char>();
    data_->add(vals, ranges, nulls);
    StatisticsBuilderUtils::addValues<int8_t>(
        dynamic_cast<IntegerStatisticsBuilder&>(*indexStatsBuilder_),
        slice,
        ranges);
  } else {
    // The path for non-flat vectors
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    writeNulls(decodedVector, ranges);

    std::function<bool(vector_size_t)> isNullAt = nullptr;
    if (decodedVector.mayHaveNulls()) {
      isNullAt = [&decodedVector](vector_size_t index) {
        return decodedVector.isNullAt(index);
      };
    }

    data_->add(
        [&decodedVector](vector_size_t index) {
          return decodedVector.valueAt<int8_t>(index);
        },
        ranges,
        isNullAt);

    // Updating status builder with individual boolean stream values
    // The logic is equivalent to addValues which has been used for
    // the flat version before.
    StatisticsBuilderUtils::addValues<int8_t>(
        dynamic_cast<IntegerStatisticsBuilder&>(*indexStatsBuilder_),
        decodedVector,
        ranges);
  }
  indexStatsBuilder_->increaseRawSize(ranges.size());
  return ranges.size();
}

template <>
uint64_t ByteRleColumnWriter<bool>::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  static_assert(sizeof(bool) == sizeof(char), "unexpected type width");
  static_assert(NULL_SIZE == 1, "unexpected raw data size");
  if (slice->encoding() == VectorEncoding::Simple::FLAT) {
    // Optimized path for flat vectors
    auto flatVector = slice->as<FlatVector<bool>>();
    DWIO_ENSURE(flatVector, "unexpected vector type");
    writeNulls(slice, ranges);
    const auto* nulls = slice->rawNulls();
    const auto* vals = flatVector->template rawValues<uint64_t>();
    data_->addBits(vals, ranges, nulls, false);
    StatisticsBuilderUtils::addValues(
        dynamic_cast<BooleanStatisticsBuilder&>(*indexStatsBuilder_),
        slice,
        ranges);
  } else {
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    writeNulls(decodedVector, ranges);

    std::function<bool(vector_size_t)> isNullAt = nullptr;
    if (decodedVector.mayHaveNulls()) {
      isNullAt = [&decodedVector](vector_size_t index) {
        return decodedVector.isNullAt(index);
      };
    }

    data_->addBits(
        [&decodedVector](vector_size_t index) {
          return decodedVector.valueAt<bool>(index);
        },
        ranges,
        isNullAt,
        false);

    // Updating status builder with individual boolean stream values
    // The logic is equivalent to addValues which has been used for
    // the flat version before.
    StatisticsBuilderUtils::addValues(
        dynamic_cast<BooleanStatisticsBuilder&>(*indexStatsBuilder_),
        decodedVector,
        ranges);
  }
  indexStatsBuilder_->increaseRawSize(ranges.size());
  return ranges.size();
}

// The integer writer class that tries to use dictionary encoding to write
// the given integer values. If dictionary encoding is determined to be
// inefficient for the column, we would write the column with direct encoding
// instead.
template <typename T>
class IntegerColumnWriter : public BaseColumnWriter {
 public:
  IntegerColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        data_{nullptr},
        dataDirect_{nullptr},
        inDictionary_{nullptr},
        dictEncoder_{context.getIntDictionaryEncoder<T>(
            EncodingKey{
                type.id,
                context.shareFlatMapDictionaries ? 0 : sequence},
            getMemoryPool(MemoryUsageCategory::DICTIONARY),
            getMemoryPool(MemoryUsageCategory::GENERAL))},
        rows_{getMemoryPool(MemoryUsageCategory::GENERAL)},
        dictionaryKeySizeThreshold_{
            getConfig(Config::DICTIONARY_NUMERIC_KEY_SIZE_THRESHOLD)},
        sort_{getConfig(Config::DICTIONARY_SORT_KEYS)},
        useDictionaryEncoding_{useDictionaryEncoding()},
        strideOffsets_{getMemoryPool(MemoryUsageCategory::GENERAL)} {
    DWIO_ENSURE_GE(dictionaryKeySizeThreshold_, 0.0);
    DWIO_ENSURE_LE(dictionaryKeySizeThreshold_, 1.0);
    DWIO_ENSURE(firstStripe_);
    if (!useDictionaryEncoding_) {
      // Suppress the stream used to initialize dictionary encoder.
      // TODO: passing factory method into the dict encoder also works
      // around this problem but has messier code organization.
      suppressStream(
          StreamKind::StreamKind_DICTIONARY_DATA,
          context_.shareFlatMapDictionaries ? 0 : sequence_);
      initStreamWriters(useDictionaryEncoding_);
    }
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void reset() override {
    // Lots of decisions regarding the presence of streams are made at flush
    // time. We would defer recording all stream positions till then, and
    // only record position for PRESENT stream upon construction.
    BaseColumnWriter::recordPosition();
    // Record starting position only for direct encoding because we don't know
    // until the next flush whether we need to write the IN_DICTIONARY stream.
    if (useDictionaryEncoding_) {
      // explicitly initialize first entry
      strideOffsets_.resize(0);
      strideOffsets_.append(0);
    } else {
      strideOffsets_.clear();
      recordDirectEncodingStreamPositions();
    }
  }

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    tryAbandonDictionaries(false);
    initStreamWriters(useDictionaryEncoding_);

    size_t dictEncoderSize = dictEncoder_.size();
    if (useDictionaryEncoding_) {
      finalDictionarySize_ = dictEncoder_.flush();
      populateDictionaryEncodingStreams();
      dictEncoder_.clear();
      rows_.clear();
    }

    // NOTE: Flushes index. All index entries need to have been backfilled.
    BaseColumnWriter::flush(encodingFactory, encodingOverride);

    ensureValidStreamWriters(useDictionaryEncoding_);
    // Flush in the order of the spec: IN_DICT, DICT_DATA, DATA
    if (useDictionaryEncoding_) {
      // Suppress IN_DICTIONARY stream if all values are kept in dict.
      // TODO: It would be nice if the suppression could be pushed down to
      // the index builder as well so we don't have to micromanage when and
      // whether to write specific streams.
      if (UNLIKELY(finalDictionarySize_ == dictEncoderSize)) {
        suppressStream(StreamKind::StreamKind_IN_DICTIONARY);
      } else {
        inDictionary_->flush();
      }
      data_->flush();
    } else {
      dataDirect_->flush();
    }

    // Start the new stripe.
    firstStripe_ = false;
  }

  // FIXME: call base class set encoding first to deal with sequence and
  // whatnot.
  void setEncoding(proto::ColumnEncoding& encoding) const override {
    BaseColumnWriter::setEncoding(encoding);
    if (useDictionaryEncoding_) {
      encoding.set_kind(
          proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);
      encoding.set_dictionarysize(finalDictionarySize_);
    }
  }

  void createIndexEntry() override {
    hasNull_ = hasNull_ || indexStatsBuilder_->hasNull().value();
    fileStatsBuilder_->merge(*indexStatsBuilder_);
    // Add entry with stats for either case.
    indexBuilder_->addEntry(*indexStatsBuilder_);
    indexStatsBuilder_->reset();
    BaseColumnWriter::recordPosition();
    // TODO: the only way useDictionaryEncoding_ right now is
    // through abandonDictionary, so we already have the stream initialization
    // handled. Might want to rethink stream initialization in the future when
    // we support starting with low memory mode and switching encoding after
    // first stripe.
    if (useDictionaryEncoding_) {
      // Record the stride boundaries so that we can backfill the stream
      // positions when actually writing the streams.
      strideOffsets_.append(rows_.size());
    } else {
      recordDirectEncodingStreamPositions();
    }
  }

  void recordDirectEncodingStreamPositions(int32_t strideOffset = -1) {
    dataDirect_->recordPosition(*indexBuilder_, strideOffset);
  }

  void recordDictionaryEncodingStreamPositions(
      bool writeInDictionaryStream,
      int32_t strideOffset) {
    if (writeInDictionaryStream) {
      inDictionary_->recordPosition(*indexBuilder_, strideOffset);
    }
    data_->recordPosition(*indexBuilder_, strideOffset);
  }

  // This incurs additional memory usage. A good long term adjustment but not
  // viable mitigation to memory pressure.
  bool tryAbandonDictionaries(bool force) override {
    // We can not switch encodings beyond the first stripe.
    // We won't need to do any additional checks if we are already
    // using direct encodings.
    if (!useDictionaryEncoding_ || !firstStripe_) {
      return false;
    }

    useDictionaryEncoding_ = shouldKeepDictionary() && !force;
    // If we are still using dictionary encodings, we are not
    // performing an encoding switch.
    if (useDictionaryEncoding_) {
      return false;
    }

    initStreamWriters(useDictionaryEncoding_);
    // Record direct encoding stream starting position.
    recordDirectEncodingStreamPositions(0);
    convertToDirectEncoding();
    // Suppress the stream used to initialize dictionary encoder.
    // TODO: passing factory method into the dict encoder also works
    // around this problem but has messier code organization.
    suppressStream(
        StreamKind::StreamKind_DICTIONARY_DATA,
        context_.shareFlatMapDictionaries ? 0 : sequence_);
    dictEncoder_.clear();
    rows_.clear();
    strideOffsets_.clear();
    return true;
  }

 private:
  uint64_t writeDict(
      DecodedVector& decodedVector,
      const common::Ranges& ranges);

  uint64_t writeDirect(const VectorPtr& slice, const common::Ranges& ranges);

  void ensureValidStreamWriters(bool dictEncoding) {
    // Ensure we have valid streams for exactly one encoding.
    DWIO_ENSURE(
        (dictEncoding && data_ && inDictionary_ && !dataDirect_) ||
            (!dictEncoding && !data_ && !inDictionary_ && dataDirect_),
        fmt::format(
            "dictEncoding = {}, data_ = {}, inDictionary_ = {}, dataDirect_ = {}",
            dictEncoding,
            data_ != nullptr,
            inDictionary_ != nullptr,
            dataDirect_ != nullptr));
  }

  void initStreamWriters(bool dictEncoding) {
    if (!data_ && !dataDirect_) {
      if (dictEncoding) {
        data_ = createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_DATA),
            getConfig(Config::USE_VINTS),
            sizeof(T));
        inDictionary_ = createBooleanRleEncoder(
            newStream(StreamKind::StreamKind_IN_DICTIONARY));
      } else {
        dataDirect_ = createDirectEncoder</* isSigned */ true>(
            newStream(StreamKind::StreamKind_DATA),
            getConfig(Config::USE_VINTS),
            sizeof(T));
      }
    }
    ensureValidStreamWriters(dictEncoding);
  }

  // NOTE: This should be called *before* clearing the rows_ buffer.
  bool shouldKeepDictionary() const {
    // TODO(T91508412): Move the dictionary efficiency based decision into
    // dictionary encoder.
    auto totalElementCount = dictEncoder_.getTotalCount();
    return totalElementCount != 0 &&
        // TODO: wonder if this should be final dictionary size instead. In that
        // case, might be better off passing in the dict size and row size
        // directly in order to prevent from being called before getting the
        // actual final dict size.
        (static_cast<float>(dictEncoder_.size()) / totalElementCount) <=
        dictionaryKeySizeThreshold_;
  }

  // Should be called only once per stripe for both flushing and abandoning
  // dictionary encoding.
  void populateStrides(
      std::function<void(size_t, size_t)> populateData,
      std::function<void(size_t)> populateIndex) {
    size_t numStrides = strideOffsets_.size() - 1;
    // Populate data and stream positions for completed strides.
    for (size_t i = 1; i <= numStrides; ++i) {
      const auto& start = strideOffsets_[i - 1];
      const auto& end = strideOffsets_[i];
      DWIO_ENSURE_GE(end, start);
      if (end > start) {
        populateData(start, end);
      }
      // Set up for next stride.
      if (isIndexEnabled()) {
        // Backing filling stream positions for the stride index entry.
        // Stats are already captured when creating the entry.
        populateIndex(i);
      }
    }
    size_t endOfLastStride = strideOffsets_[numStrides];
    // Populate data only for the current stride.
    // Happens only when abandoning dictionary in the incomplete (current)
    // stride.
    if (endOfLastStride < rows_.size()) {
      populateData(endOfLastStride, rows_.size());
    }
  }

  void populateDictionaryEncodingStreams();
  void convertToDirectEncoding();

  // Could be RLE encoded raw data or dictionary encoded values.
  std::unique_ptr<IntEncoder<false>> data_;
  // Direct-encoded data in case dictionary encoding is not optimal.
  std::unique_ptr<IntEncoder<true>> dataDirect_;
  // Whether data at offset is in the stripe dictionary. If not, it's either
  // written as raw value or in the stride dictionary.
  std::unique_ptr<ByteRleEncoder> inDictionary_;
  IntegerDictionaryEncoder<T>& dictEncoder_;
  // write dict writes to an in-memory vector too so that we can later write
  // the sorted order after compiling a lookup table.
  ChainedBuffer<uint32_t> rows_;
  size_t finalDictionarySize_;
  const float dictionaryKeySizeThreshold_;
  const bool sort_;
  // This value could change if we are writing with low memory mode or if we
  // determine with the first stripe that the data is not fit for dictionary
  // encoding.
  bool useDictionaryEncoding_;
  bool firstStripe_{true};
  DataBuffer<size_t> strideOffsets_;
};

template <typename T>
uint64_t IntegerColumnWriter<T>::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  if (useDictionaryEncoding_) {
    // Decode and then write
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    return writeDict(decodedVector, ranges);
  } else {
    // If the input is not a flat vector we make a complete copy and convert
    // it to flat vector
    if (slice->encoding() != VectorEncoding::Simple::FLAT) {
      auto newBase = BaseVector::create(
          CppToType<T>::create(),
          slice->size(),
          &getMemoryPool(MemoryUsageCategory::GENERAL));
      auto newVector = std::dynamic_pointer_cast<FlatVector<T>>(newBase);
      newVector->copy(slice.get(), 0, 0, slice->size());
      return writeDirect(newVector, ranges);
    } else {
      // Directly write the flat vector
      return writeDirect(slice, ranges);
    }
  }
}

template <typename T>
uint64_t IntegerColumnWriter<T>::writeDict(
    DecodedVector& decodedVector,
    const common::Ranges& ranges) {
  auto& statsBuilder =
      dynamic_cast<IntegerStatisticsBuilder&>(*indexStatsBuilder_);
  writeNulls(decodedVector, ranges);
  // make sure we have enough space
  rows_.reserve(rows_.size() + ranges.size());
  auto processRow = [&](vector_size_t pos) {
    T value = decodedVector.valueAt<T>(pos);
    rows_.unsafeAppend(dictEncoder_.addKey(value));
    statsBuilder.addValues(value);
  };

  uint64_t nullCount = 0;
  if (decodedVector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (decodedVector.isNullAt(pos)) {
        ++nullCount;
      } else {
        processRow(pos);
      }
    }
  } else {
    for (auto& pos : ranges) {
      processRow(pos);
    }
  }

  uint64_t rawSize = (ranges.size() - nullCount) * sizeof(T);
  if (nullCount > 0) {
    statsBuilder.setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  statsBuilder.increaseRawSize(rawSize);
  return rawSize;
}

template <typename T>
uint64_t IntegerColumnWriter<T>::writeDirect(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  ensureValidStreamWriters(false);
  auto flatVector = slice->asFlatVector<T>();
  DWIO_ENSURE(flatVector, "unexpected vector type");
  writeNulls(slice, ranges);
  auto nulls = slice->rawNulls();
  auto vals = flatVector->rawValues();

  auto count = dataDirect_->add(vals, ranges, nulls);
  StatisticsBuilderUtils::addValues<T>(
      dynamic_cast<IntegerStatisticsBuilder&>(*indexStatsBuilder_),
      slice,
      ranges);
  auto rawSize = count * sizeof(T) + (ranges.size() - count) * NULL_SIZE;
  indexStatsBuilder_->increaseRawSize(rawSize);
  return rawSize;
}

// TODO: T45220726 Reduce memory usage in this method by
// allocating fewer buffers.
// One way to do so is via passing a getData function into writer instead of
// writing to intermediate buffers.
template <typename T>
void IntegerColumnWriter<T>::populateDictionaryEncodingStreams() {
  ensureValidStreamWriters(true);
  MemoryPool& pool{getMemoryPool(MemoryUsageCategory::GENERAL)};

  // Allocate fixed size buffer for batch write
  DataBuffer<char> writeBuffer{pool};
  writeBuffer.reserve(64 * 1024);

  // Lookup table may contain unsigned dictionary offsets [0 - 2^31) or signed
  // raw values. So the type used by it need to cover both ranges.
  using LookupType = typename std::conditional<sizeof(T) < 4, int32_t, T>::type;

  // When all the Keys are in Dictionary, inDictionaryStream is omitted.
  bool writeInDictionaryStream = finalDictionarySize_ != dictEncoder_.size();

  // Record starting positions of the dictionary encoding streams.
  recordDictionaryEncodingStreamPositions(writeInDictionaryStream, 0);

  // Add and populate index entry for rows_[start:end).
  // TODO: T45260340 Unfortunately standard usage of the index
  // builder would need to segment the writes in the data buffer again. Should
  // we manually call add on index builder and still batch the entire write?
  auto populateDictionaryStreams = [&, this](size_t start, size_t end) {
    // Do this once per stride as opposed to per row to reduce branching
    // cost.
    if (writeInDictionaryStream) {
      bufferedWrite<char>(
          writeBuffer,
          start,
          end,
          [&](auto index) { return dictEncoder_.getInDict()[rows_[index]]; },
          [&](auto buf, auto size) {
            inDictionary_->add(buf, common::Ranges::of(0, size), nullptr);
          });
    }

    // lookupTable represents the data as signed, but each value needs to be
    // interpretted by combining the corresponding offset in the indctionary
    // data. if inDictionary is true, lookupTable contains offset into
    // dictionary(unsigned) if inDictionary is false, lookupTable contains the
    // actual value(signed).
    bufferedWrite<LookupType>(
        writeBuffer,
        start,
        end,
        [&](auto index) { return dictEncoder_.getLookupTable()[rows_[index]]; },
        [&](auto buf, auto size) {
          data_->add(buf, common::Ranges::of(0, size), nullptr);
        });
  };

  // The writer always calls createIndexEntry before flush.
  size_t lastIndexStrideOffset = strideOffsets_[strideOffsets_.size() - 1];
  DWIO_ENSURE_EQ(lastIndexStrideOffset, rows_.size());
  populateStrides(
      populateDictionaryStreams,
      std::bind(
          &IntegerColumnWriter<T>::recordDictionaryEncodingStreamPositions,
          this,
          writeInDictionaryStream,
          std::placeholders::_1));
}

template <typename T>
void IntegerColumnWriter<T>::convertToDirectEncoding() {
  ensureValidStreamWriters(false);
  auto populateDirectStream = [&, this](size_t start, size_t end) {
    bufferedWrite<T>(
        getMemoryPool(MemoryUsageCategory::GENERAL),
        64 * 1024,
        start,
        end,
        [&](auto index) { return dictEncoder_.getKey(rows_[index]); },
        [&](auto buf, auto size) {
          dataDirect_->add(buf, common::Ranges::of(0, size), nullptr);
        });
  };

  populateStrides(
      populateDirectStream,
      std::bind(
          &IntegerColumnWriter<T>::recordDirectEncodingStreamPositions,
          this,
          std::placeholders::_1));
}

class TimestampColumnWriter : public BaseColumnWriter {
 public:
  TimestampColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        seconds_{createRleEncoder</* isSigned = */ true>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_DATA),
            context.getConfig(Config::USE_VINTS),
            LONG_BYTE_SIZE)},
        nanos_{createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_NANO_DATA),
            context.getConfig(Config::USE_VINTS),
            LONG_BYTE_SIZE)} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    seconds_->flush();
    nanos_->flush();
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    seconds_->recordPosition(*indexBuilder_);
    nanos_->recordPosition(*indexBuilder_);
  }

 private:
  std::unique_ptr<IntEncoder<true>> seconds_;
  std::unique_ptr<IntEncoder<false>> nanos_;
};

namespace {
FOLLY_ALWAYS_INLINE int64_t formatTime(int64_t seconds, uint64_t nanos) {
  DWIO_ENSURE(seconds >= MIN_SECONDS);

  if (seconds < 0 && nanos != 0) {
    // Adding 1 for neagive seconds is there to imitate the Java ORC writer.
    // Consider the case where -1500 milliseconds need to be represented.
    // In java world, due to a bug (-1500/1000 will result in -1 or rounding up)
    // Due to this Java represented -1500 millis as -1 for seconds and 5*10^8
    // for nanos (Note, Epoch is ignored for simplicity sake).
    // In CPP world, it will be represented as -2 seconds and 5*10^8 nanos
    // and nanos can never be negative in CPP. (CPP has right
    // representation).
    // PS: Due to this bug, the second before UTC epoch with non zero nanos will
    // always be converted to second after UTC epoch with same nanos.
    seconds += 1;
  }

  return seconds - EPOCH_OFFSET;
}

FOLLY_ALWAYS_INLINE int64_t formatNanos(uint64_t nanos) {
  if (nanos == 0) {
    return 0;
  }
  DWIO_ENSURE(nanos <= MAX_NANOS);

  if (nanos % 100 != 0) {
    return nanos << 3;
  } else {
    nanos /= 100;
    int32_t trailingZeros = 1;
    while (nanos % 10 == 0 && trailingZeros < 7) {
      nanos /= 10;
      trailingZeros += 1;
    }
    return (nanos << 3) | trailingZeros;
  }
}
} // namespace

uint64_t TimestampColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  // Timestamp is not frequently used, always decode so we have less branches to
  // deal with.
  auto localDecoded = decode(slice, ranges);
  auto& decodedVector = localDecoded.get();
  writeNulls(decodedVector, ranges);

  size_t count = 0;
  if (decodedVector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (!decodedVector.isNullAt(pos)) {
        auto ts = decodedVector.valueAt<Timestamp>(pos);
        seconds_->writeValue(formatTime(ts.getSeconds(), ts.getNanos()));
        nanos_->writeValue(formatNanos(ts.getNanos()));
        ++count;
      }
    }
  } else {
    for (auto& pos : ranges) {
      auto ts = decodedVector.valueAt<Timestamp>(pos);
      seconds_->writeValue(formatTime(ts.getSeconds(), ts.getNanos()));
      nanos_->writeValue(formatNanos(ts.getNanos()));
      ++count;
    }
  }

  indexStatsBuilder_->increaseValueCount(count);
  if (count != ranges.size()) {
    indexStatsBuilder_->setHasNull();
  }

  // Seconds is Long, Nanos is int.
  constexpr uint32_t TimeStampSize =
      LONG_BYTE_SIZE + dwio::common::INT_BYTE_SIZE;
  auto rawSize = count * TimeStampSize + (ranges.size() - count) * NULL_SIZE;
  indexStatsBuilder_->increaseRawSize(rawSize);
  return rawSize;
}

// The string writer class that tries to use dictionary encoding to write
// the given string values. If dictionary encoding is determined to be
// inefficient for the column, we would write the column with direct encoding
// instead.
class StringColumnWriter : public BaseColumnWriter {
 public:
  StringColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        data_{nullptr},
        dataDirect_{nullptr},
        dataDirectLength_{nullptr},
        dictionaryData_{nullptr},
        dictionaryDataLength_{nullptr},
        inDictionary_{nullptr},
        strideDictionaryData_{nullptr},
        strideDictionaryDataLength_{nullptr},
        dictEncoder_{
            getMemoryPool(MemoryUsageCategory::DICTIONARY),
            getMemoryPool(MemoryUsageCategory::GENERAL)},
        rows_{getMemoryPool(MemoryUsageCategory::GENERAL)},
        encodingSelector_{
            getMemoryPool(MemoryUsageCategory::GENERAL),
            getConfig(Config::DICTIONARY_STRING_KEY_SIZE_THRESHOLD),
            getConfig(Config::ENTROPY_KEY_STRING_SIZE_THRESHOLD),
            getConfig(Config::ENTROPY_STRING_MIN_SAMPLES),
            getConfig(Config::ENTROPY_STRING_DICT_SAMPLE_FRACTION),
            getConfig(Config::ENTROPY_STRING_THRESHOLD)},
        sort_{getConfig(Config::DICTIONARY_SORT_KEYS)},
        useDictionaryEncoding_{useDictionaryEncoding()},
        strideOffsets_{getMemoryPool(MemoryUsageCategory::GENERAL)} {
    DWIO_ENSURE(firstStripe_);
    if (!useDictionaryEncoding_) {
      initStreamWriters(useDictionaryEncoding_);
    }
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void reset() override {
    // Lots of decisions regarding the presence of streams are made at flush
    // time. We would defer recording all stream positions till then, and
    // only record position for PRESENT stream upon construction.
    BaseColumnWriter::recordPosition();
    // Record starting position only for direct encoding because we don't know
    // until the next flush whether we need to write the IN_DICTIONARY stream.
    if (useDictionaryEncoding_) {
      // explicitly initialize first entry
      strideOffsets_.resize(0);
      strideOffsets_.append(0);
    } else {
      strideOffsets_.clear();
      recordDirectEncodingStreamPositions();
    }
  }

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    tryAbandonDictionaries(false);
    initStreamWriters(useDictionaryEncoding_);

    size_t dictEncoderSize = dictEncoder_.size();
    if (useDictionaryEncoding_) {
      populateDictionaryEncodingStreams();
      dictEncoder_.clear();
      rows_.clear();
    }

    // NOTE: Flushes index. All index entries need to have been backfilled.
    BaseColumnWriter::flush(encodingFactory, encodingOverride);

    ensureValidStreamWriters(useDictionaryEncoding_);
    // Flush in the order of the spec:
    // STRIDE_DICT, STRIDE_DICT_LENGTH, IN_DICT, DICT, DICT_LENGTH, DATA
    if (useDictionaryEncoding_) {
      // Suppress IN_DICTIONARY stream if all values are kept in dict.
      // TODO: T46365785 It would be nice if the suppression
      // could be pushed down to the index builder as well so we don't have to
      // micromanage when and whether to write specific streams.
      if (UNLIKELY(finalDictionarySize_ == dictEncoderSize)) {
        suppressStream(StreamKind::StreamKind_STRIDE_DICTIONARY);
        suppressStream(StreamKind::StreamKind_STRIDE_DICTIONARY_LENGTH);
        suppressStream(StreamKind::StreamKind_IN_DICTIONARY);
      } else {
        strideDictionaryData_->flush();
        strideDictionaryDataLength_->flush();
        inDictionary_->flush();
      }
      dictionaryData_->flush();
      dictionaryDataLength_->flush();
      data_->flush();
    } else {
      dataDirect_->flush();
      dataDirectLength_->flush();
    }

    // Start the new stripe.
    firstStripe_ = false;
  }

  // FIXME: call base class set encoding first to deal with sequence and
  // whatnot.
  void setEncoding(proto::ColumnEncoding& encoding) const override {
    BaseColumnWriter::setEncoding(encoding);
    if (useDictionaryEncoding_) {
      encoding.set_kind(
          proto::ColumnEncoding_Kind::ColumnEncoding_Kind_DICTIONARY);
      encoding.set_dictionarysize(finalDictionarySize_);
    }
  }

  void createIndexEntry() override {
    hasNull_ = hasNull_ || indexStatsBuilder_->hasNull().value();
    fileStatsBuilder_->merge(*indexStatsBuilder_);
    // Add entry with stats for either case.
    indexBuilder_->addEntry(*indexStatsBuilder_);
    indexStatsBuilder_->reset();
    BaseColumnWriter::recordPosition();
    // TODO: the only way useDictionaryEncoding_ right now is
    // through abandonDictionary, so we already have the stream initialization
    // handled. Might want to rethink stream initialization in the future when
    // we support starting with low memory mode and switching encoding after
    // first stripe.
    if (useDictionaryEncoding_) {
      // Record the stride boundaries so that we can backfill the stream
      // positions when actually writing the streams.
      strideOffsets_.append(rows_.size());
    } else {
      recordDirectEncodingStreamPositions();
    }
  }

  void recordDirectEncodingStreamPositions(int32_t strideOffset = -1) {
    dataDirect_->recordPosition(*indexBuilder_, strideOffset);
    dataDirectLength_->recordPosition(*indexBuilder_, strideOffset);
  }

  void recordDictionaryEncodingStreamPositions(
      bool writeInDictionaryStream,
      int32_t strideOffset,
      const DataBuffer<uint32_t>& strideDictCounts) {
    if (writeInDictionaryStream) {
      strideDictionaryData_->recordPosition(*indexBuilder_, strideOffset);
      strideDictionaryDataLength_->recordPosition(*indexBuilder_, strideOffset);
      indexBuilder_->add(strideDictCounts[strideOffset], strideOffset);
      data_->recordPosition(*indexBuilder_, strideOffset);
      inDictionary_->recordPosition(*indexBuilder_, strideOffset);
    } else {
      data_->recordPosition(*indexBuilder_, strideOffset);
    }
  }

  bool tryAbandonDictionaries(bool force) override {
    // TODO: hopefully not required in the future, but need to solve the problem
    // of replacing/deleting streams.
    if (!useDictionaryEncoding_ || !firstStripe_) {
      return false;
    }

    useDictionaryEncoding_ = shouldKeepDictionary() && !force;
    if (useDictionaryEncoding_) {
      return false;
    }

    initStreamWriters(useDictionaryEncoding_);
    // Record direct encoding stream starting position.
    recordDirectEncodingStreamPositions(0);
    convertToDirectEncoding();
    dictEncoder_.clear();
    rows_.clear();
    strideOffsets_.clear();
    return true;
  }

 protected:
  bool useDictionaryEncoding() const override {
    return (sequence_ == 0 ||
            !context_.getConfig(
                Config::MAP_FLAT_DISABLE_DICT_ENCODING_STRING)) &&
        !context_.isLowMemoryMode();
  }

 private:
  uint64_t writeDict(
      DecodedVector& decodedVector,
      const common::Ranges& ranges);

  uint64_t writeDirect(
      DecodedVector& decodedVector,
      const common::Ranges& ranges);

  void ensureValidStreamWriters(bool dictEncoding) {
    // Ensure we have exactly one valid data stream.
    DWIO_ENSURE(
        (dictEncoding && data_ && dictionaryData_ && dictionaryDataLength_ &&
         strideDictionaryData_ && strideDictionaryDataLength_ &&
         inDictionary_ && !dataDirect_ && !dataDirectLength_) ||
        (!dictEncoding && !data_ && !dictionaryData_ &&
         !dictionaryDataLength_ && !strideDictionaryData_ &&
         !strideDictionaryDataLength_ && !inDictionary_ && dataDirect_ &&
         dataDirectLength_));
  }

  void initStreamWriters(bool dictEncoding) {
    if (!data_ && !dataDirect_) {
      if (dictEncoding) {
        data_ = createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_DATA),
            getConfig(Config::USE_VINTS),
            sizeof(uint32_t));
        dictionaryData_ = std::make_unique<AppendOnlyBufferedStream>(
            newStream(StreamKind::StreamKind_DICTIONARY_DATA));
        dictionaryDataLength_ = createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_LENGTH),
            getConfig(Config::USE_VINTS),
            sizeof(uint32_t));
        inDictionary_ = createBooleanRleEncoder(
            newStream(StreamKind::StreamKind_IN_DICTIONARY));
        strideDictionaryData_ = std::make_unique<AppendOnlyBufferedStream>(
            newStream(StreamKind::StreamKind_STRIDE_DICTIONARY));
        strideDictionaryDataLength_ = createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_STRIDE_DICTIONARY_LENGTH),
            getConfig(Config::USE_VINTS),
            sizeof(uint32_t));
      } else {
        dataDirect_ = std::make_unique<AppendOnlyBufferedStream>(
            newStream(StreamKind::StreamKind_DATA));
        dataDirectLength_ = createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_LENGTH),
            getConfig(Config::USE_VINTS),
            sizeof(uint32_t));
      }
    }
    ensureValidStreamWriters(dictEncoding);
  }

  // NOTE: This should be called *before* clearing the rows_ buffer.
  bool shouldKeepDictionary() const {
    return rows_.size() != 0 &&
        encodingSelector_.useDictionary(dictEncoder_, rows_.size());
  }

  // Should be called only once per stripe for both flushing and abandoning
  // dictionary encoding.
  void populateStrides(
      std::function<void(size_t, size_t, size_t)> populateData,
      std::function<void(size_t)> populateIndex) {
    size_t numStrides = strideOffsets_.size() - 1;
    // Populate data and stream positions for completed strides.
    for (size_t i = 1; i <= numStrides; ++i) {
      const auto& start = strideOffsets_[i - 1];
      const auto& end = strideOffsets_[i];
      DWIO_ENSURE_GE(end, start);
      if (end > start) {
        populateData(start, end, i - 1);
      }

      // Set up for next stride.
      if (isIndexEnabled()) {
        // Backing filling stream positions for the stride index entry.
        // Stats are already captured when creating the entry.
        populateIndex(i);
      }
    }
    size_t endOfLastStride = strideOffsets_[numStrides];
    // Populate data only for the current stride.
    // Happens only when abandoning dictionary in the incomplete (current)
    // stride.
    if (endOfLastStride < rows_.size()) {
      populateData(endOfLastStride, rows_.size(), numStrides);
    }
  }

  void populateDictionaryEncodingStreams();
  void convertToDirectEncoding();

  // Could be RLE encoded raw data or dictionary encoded values.
  std::unique_ptr<IntEncoder<false>> data_;
  // Direct-encoded data in case dictionary encoding is not optimal.
  std::unique_ptr<AppendOnlyBufferedStream> dataDirect_;
  std::unique_ptr<IntEncoder<false>> dataDirectLength_;
  // Keys in stripe dictionary
  std::unique_ptr<AppendOnlyBufferedStream> dictionaryData_;
  std::unique_ptr<IntEncoder<false>> dictionaryDataLength_;
  // Whether data at offset is in the stripe dictionary. If not, it's either
  // written as raw value or in the stride dictionary.
  std::unique_ptr<ByteRleEncoder> inDictionary_;
  // Keys in stride dictionary
  std::unique_ptr<AppendOnlyBufferedStream> strideDictionaryData_;
  std::unique_ptr<IntEncoder<false>> strideDictionaryDataLength_;
  StringDictionaryEncoder dictEncoder_;
  // write dict writes to an in-memory vector so that we can later write
  // the sorted order after compiling a lookup table.
  ChainedBuffer<uint32_t> rows_;
  size_t finalDictionarySize_;
  EntropyEncodingSelector encodingSelector_;
  const bool sort_;
  // This value could change if we are writing with low memory mode or if we
  // determine with the first stripe that the data is not fit for dictionary
  // encoding.
  bool useDictionaryEncoding_;
  bool firstStripe_{true};
  DataBuffer<size_t> strideOffsets_;
};

uint64_t StringColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  auto localDecoded = decode(slice, ranges);
  auto& decodedVector = localDecoded.get();

  if (useDictionaryEncoding_) {
    return writeDict(decodedVector, ranges);
  } else {
    return writeDirect(decodedVector, ranges);
  }
}

uint64_t StringColumnWriter::writeDict(
    DecodedVector& decodedVector,
    const common::Ranges& ranges) {
  auto& statsBuilder =
      dynamic_cast<StringStatisticsBuilder&>(*indexStatsBuilder_);
  writeNulls(decodedVector, ranges);
  // make sure we have enough space
  rows_.reserve(rows_.size() + ranges.size());
  size_t strideIndex = strideOffsets_.size() - 1;
  uint64_t rawSize = 0;
  auto processRow = [&](size_t pos) {
    auto sp = decodedVector.valueAt<StringView>(pos);
    rows_.unsafeAppend(dictEncoder_.addKey(sp, strideIndex));
    statsBuilder.addValues(sp);
    rawSize += sp.size();
  };

  uint64_t nullCount = 0;
  if (decodedVector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (decodedVector.isNullAt(pos)) {
        ++nullCount;
      } else {
        processRow(pos);
      }
    }
  } else {
    for (auto& pos : ranges) {
      processRow(pos);
    }
  }

  if (nullCount > 0) {
    statsBuilder.setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  statsBuilder.increaseRawSize(rawSize);
  return rawSize;
}

uint64_t StringColumnWriter::writeDirect(
    DecodedVector& decodedVector,
    const common::Ranges& ranges) {
  auto& statsBuilder =
      dynamic_cast<StringStatisticsBuilder&>(*indexStatsBuilder_);
  writeNulls(decodedVector, ranges);

  // make sure we have enough space
  rows_.reserve(rows_.size() + ranges.size());
  // TODO Optimize to avoid copy
  DataBuffer<int64_t> lengths{getMemoryPool(MemoryUsageCategory::GENERAL)};
  lengths.reserve(ranges.size());

  uint64_t rawSize = 0;
  auto processRow = [&](size_t pos) {
    auto sp = decodedVector.valueAt<StringView>(pos);
    auto size = sp.size();
    dataDirect_->write(sp.data(), size);
    statsBuilder.addValues(sp);
    rawSize += size;
    lengths.unsafeAppend(size);
  };

  uint64_t nullCount = 0;
  if (decodedVector.mayHaveNulls()) {
    for (auto& pos : ranges) {
      if (decodedVector.isNullAt(pos)) {
        ++nullCount;
      } else {
        processRow(pos);
      }
    }
  } else {
    for (auto& pos : ranges) {
      processRow(pos);
    }
  }
  if (lengths.size() > 0) {
    dataDirectLength_->add(
        lengths.data(), common::Ranges::of(0, lengths.size()), nullptr);
  }

  if (nullCount > 0) {
    statsBuilder.setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  statsBuilder.increaseRawSize(rawSize);
  return rawSize;
}

// TODO: T45220726 Reduce memory usage in this method by
// allocating fewer buffers.
// One way to do so is via passing a getData function into writer instead of
// writing to intermediate buffers.

void StringColumnWriter::populateDictionaryEncodingStreams() {
  ensureValidStreamWriters(true);
  MemoryPool& pool{getMemoryPool(MemoryUsageCategory::GENERAL)};
  DataBuffer<uint32_t> lookupTable{pool};
  DataBuffer<bool> inDict{pool};
  // All elements are initialized as 0.
  DataBuffer<uint32_t> strideDictCounts{
      pool,
      strideOffsets_.size(),
  };
  finalDictionarySize_ = DictionaryEncodingUtils::getSortedIndexLookupTable(
      dictEncoder_,
      pool,
      sort_,
      DictionaryEncodingUtils::frequencyOrdering,
      true,
      lookupTable,
      inDict,
      strideDictCounts,
      [&](auto buf, auto size) { dictionaryData_->write(buf, size); },
      [&](auto buf, auto size) {
        dictionaryDataLength_->add(buf, common::Ranges::of(0, size), nullptr);
      });

  // When all the Keys are in Dictionary, inDictionaryStream is omitted.
  bool writeInDictionaryStream = finalDictionarySize_ != dictEncoder_.size();

  // Record starting positions of the dictionary encoding streams.
  recordDictionaryEncodingStreamPositions(
      writeInDictionaryStream, 0, strideDictCounts);

  // Add and populate index entry for rows_[start:end).
  // TODO: T45260340 Unfortunately standard usage of the index
  // builder would need to segment the writes in the data buffer again. Should
  // we manually call add on index builder and still batch the entire write?
  auto populateDictionaryStreams =
      [&, this](size_t start, size_t end, size_t strideIndex) {
        // Do this once per stride as opposed to per row to reduce branching
        // cost.

        if (writeInDictionaryStream) {
          auto strideDictKeyCount = strideDictCounts[strideIndex];
          DataBuffer<uint32_t> sortedStrideDictKeyIndexBuffer{pool};
          sortedStrideDictKeyIndexBuffer.reserve(strideDictKeyCount);
          {
            auto inDictWriter = createBufferedWriter<char>(
                getMemoryPool(MemoryUsageCategory::GENERAL),
                64 * 1024,
                [&](auto buf, auto size) {
                  inDictionary_->add(buf, common::Ranges::of(0, size), nullptr);
                });

            uint32_t strideDictSize = 0;
            for (size_t i = start; i != end; ++i) {
              auto origIndex = rows_[i];
              bool valInDict = inDict[origIndex];
              inDictWriter.add(valInDict ? 1 : 0);
              // TODO: optimize this branching either through restoring visitor
              // pattern, or through separating the index backfill.
              if (!valInDict) {
                auto strideDictIndex = lookupTable[origIndex];
                sortedStrideDictKeyIndexBuffer[strideDictIndex] = origIndex;
                ++strideDictSize;
              }
            }
            DWIO_ENSURE_EQ(strideDictSize, strideDictKeyCount);
          }

          // StrideDictKey can be empty, when all keys for stride are in
          // dictionary.
          if (strideDictKeyCount > 0) {
            auto strideLengthWriter = createBufferedWriter<uint32_t>(
                getMemoryPool(MemoryUsageCategory::GENERAL),
                64 * 1024,
                [&](auto buf, auto size) {
                  strideDictionaryDataLength_->add(
                      buf, common::Ranges::of(0, size), nullptr);
                });
            for (size_t i = 0; i < strideDictKeyCount; ++i) {
              auto val = dictEncoder_.getKey(sortedStrideDictKeyIndexBuffer[i]);
              strideDictionaryData_->write(val.data(), val.size());
              strideLengthWriter.add(val.size());
            }
          }
        }

        bufferedWrite<uint32_t>(
            pool,
            64 * 1024,
            start,
            end,
            [&](auto index) { return lookupTable[rows_[index]]; },
            [&](auto buf, auto size) {
              data_->add(buf, common::Ranges::of(0, size), nullptr);
            });
      };

  // The writer always calls createIndexEntry before flush.
  size_t lastIndexStrideOffset = strideOffsets_[strideOffsets_.size() - 1];
  DWIO_ENSURE_EQ(lastIndexStrideOffset, rows_.size());
  populateStrides(
      populateDictionaryStreams,
      std::bind(
          &StringColumnWriter::recordDictionaryEncodingStreamPositions,
          this,
          writeInDictionaryStream,
          std::placeholders::_1,
          std::cref(strideDictCounts)));
}

void StringColumnWriter::convertToDirectEncoding() {
  ensureValidStreamWriters(false);

  auto populateDirectStream =
      [&, this](size_t start, size_t end, size_t /* unused */) {
        auto lengthWriter = createBufferedWriter<uint32_t>(
            getMemoryPool(MemoryUsageCategory::GENERAL),
            64 * 1024,
            [&](auto buf, auto size) {
              dataDirectLength_->add(buf, common::Ranges::of(0, size), nullptr);
            });
        for (size_t i = start; i != end; ++i) {
          auto key = dictEncoder_.getKey(rows_[i]);
          dataDirect_->write(key.data(), key.size());
          lengthWriter.add(key.size());
        }
      };

  populateStrides(
      populateDirectStream,
      std::bind(
          &StringColumnWriter::recordDirectEncodingStreamPositions,
          this,
          std::placeholders::_1));
}

template <typename T>
class FloatColumnWriter : public BaseColumnWriter {
 public:
  FloatColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        data_{newStream(StreamKind::StreamKind_DATA)} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    data_.flush();
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    data_.recordPosition(*indexBuilder_);
  }

 private:
  AppendOnlyBufferedStream data_;
};

template <typename T>
uint64_t FloatColumnWriter<T>::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  static_assert(folly::kIsLittleEndian, "not supported");
  auto& statsBuilder =
      dynamic_cast<DoubleStatisticsBuilder&>(*indexStatsBuilder_);

  uint64_t nullCount = 0;
  if (slice->encoding() == VectorEncoding::Simple::FLAT) {
    auto flatVector = slice->asFlatVector<T>();
    DWIO_ENSURE(flatVector, "unexpected vector type");
    writeNulls(slice, ranges);
    auto nulls = slice->rawNulls();
    auto data = flatVector->rawValues();
    if (slice->mayHaveNulls()) {
      // higher throughput is achieved through this local buffer
      auto writer = createBufferedWriter<T>(
          getMemoryPool(MemoryUsageCategory::GENERAL),
          1024,
          [&](auto buf, auto size) {
            data_.write(reinterpret_cast<const char*>(buf), size * sizeof(T));
          });

      auto processRow = [&](size_t pos) {
        auto val = data[pos];
        writer.add(val);
        statsBuilder.addValues(val);
      };

      for (auto& pos : ranges) {
        if (bits::isBitNull(nulls, pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        statsBuilder.addValues(data[pos]);
      }
      for (const auto& [start, end] : ranges.getRanges()) {
        const char* srcPtr = reinterpret_cast<const char*>(data + start);
        size_t sz = end - start;
        data_.write(srcPtr, sz * sizeof(T));
      }
    }
  } else {
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    writeNulls(decodedVector, ranges);

    // higher throughput is achieved through this local buffer
    auto writer = createBufferedWriter<T>(
        getMemoryPool(MemoryUsageCategory::GENERAL),
        1024,
        [&](auto buf, auto size) {
          data_.write(reinterpret_cast<const char*>(buf), size * sizeof(T));
        });

    auto processRow = [&](size_t pos) {
      auto val = decodedVector.template valueAt<T>(pos);
      writer.add(val);
      statsBuilder.addValues(val);
    };

    if (decodedVector.mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (decodedVector.isNullAt(pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(pos);
      }
    }
  }

  uint64_t rawSize = (ranges.size() - nullCount) * sizeof(T);
  if (nullCount > 0) {
    statsBuilder.setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  statsBuilder.increaseRawSize(rawSize);
  return rawSize;
}

class BinaryColumnWriter : public BaseColumnWriter {
 public:
  BinaryColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        data_{newStream(StreamKind::StreamKind_DATA)},
        lengths_{createRleEncoder</* isSigned */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_LENGTH),
            context.getConfig(Config::USE_VINTS),
            dwio::common::INT_BYTE_SIZE)} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    data_.flush();
    lengths_->flush();
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    data_.recordPosition(*indexBuilder_);
    lengths_->recordPosition(*indexBuilder_);
  }

 private:
  AppendOnlyBufferedStream data_;
  std::unique_ptr<IntEncoder<false>> lengths_;
};

uint64_t BinaryColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  auto& statsBuilder =
      dynamic_cast<BinaryStatisticsBuilder&>(*indexStatsBuilder_);
  uint64_t rawSize = 0;
  uint64_t nullCount = 0;

  // TODO Optimize to avoid copy
  DataBuffer<int64_t> lengths{getMemoryPool(MemoryUsageCategory::GENERAL)};
  lengths.reserve(ranges.size());

  if (slice->encoding() == VectorEncoding::Simple::FLAT) {
    auto binarySlice = slice->asFlatVector<StringView>();
    DWIO_ENSURE(binarySlice, "unexpected vector type");
    writeNulls(slice, ranges);
    auto nulls = slice->rawNulls();
    auto data = binarySlice->rawValues();

    auto processRow = [&](size_t pos) {
      auto size = data[pos].size();
      lengths.unsafeAppend(size);
      data_.write(data[pos].data(), size);
      statsBuilder.addValues(size);
      rawSize += size;
    };

    if (slice->mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (bits::isBitNull(nulls, pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(pos);
      }
    }
  } else {
    // Decode
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    writeNulls(decodedVector, ranges);

    auto processRow = [&](size_t pos) {
      auto val = decodedVector.template valueAt<StringView>(pos);
      auto size = val.size();
      lengths.unsafeAppend(size);
      data_.write(val.data(), size);
      statsBuilder.addValues(size);
      rawSize += size;
    };

    if (decodedVector.mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (decodedVector.isNullAt(pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(pos);
      }
    }
  }

  if (lengths.size() > 0) {
    lengths_->add(
        lengths.data(), common::Ranges::of(0, lengths.size()), nullptr);
  }
  if (nullCount > 0) {
    statsBuilder.setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  statsBuilder.increaseRawSize(rawSize);
  return rawSize;
}

class StructColumnWriter : public BaseColumnWriter {
 public:
  StructColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    for (auto& c : children_) {
      c->flush(encodingFactory);
    }
  }

 private:
  uint64_t writeChildrenAndStats(
      const RowVector* rowSlice,
      const common::Ranges& ranges,
      uint64_t nullCount);
};

uint64_t StructColumnWriter::writeChildrenAndStats(
    const RowVector* rowSlice,
    const common::Ranges& ranges,
    uint64_t nullCount) {
  uint64_t rawSize = 0;
  if (ranges.size() > 0) {
    for (size_t i = 0; i < children_.size(); ++i) {
      rawSize += children_.at(i)->write(rowSlice->childAt(i), ranges);
    }
  }
  if (nullCount) {
    indexStatsBuilder_->setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  indexStatsBuilder_->increaseValueCount(ranges.size());
  indexStatsBuilder_->increaseRawSize(rawSize);
  return rawSize;
}

uint64_t StructColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  // Special case for writing the root. Root writer accepts rows, so all are
  // not null.
  if (isRoot()) {
    common::Ranges childRanges;
    const RowVector* rowSlice;
    const common::Ranges* childRangesPtr;
    if (slice->encoding() != VectorEncoding::Simple::ROW) {
      auto localDecoded = decode(slice, ranges);
      auto& decodedVector = localDecoded.get();
      rowSlice = decodedVector.base()->as<RowVector>();
      DWIO_ENSURE(rowSlice, "unexpected vector type");
      childRangesPtr = &childRanges;

      // For every index in wrapped indices we will add a 1-entry size range.
      for (auto& pos : ranges) {
        childRanges.add(decodedVector.index(pos), decodedVector.index(pos) + 1);
      }
    } else {
      rowSlice = slice->as<RowVector>();
      DWIO_ENSURE(rowSlice, "unexpected vector type");
      childRangesPtr = &ranges;
    }
    auto rawSize = writeChildrenAndStats(rowSlice, *childRangesPtr, 0);
    context_.incRowCount(ranges.size());
    return rawSize;
  }

  // General case for writing row (struct)
  uint64_t nullCount = 0;
  common::Ranges childRanges;
  const RowVector* rowSlice;
  const common::Ranges* childRangesPtr;
  if (slice->encoding() != VectorEncoding::Simple::ROW) {
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    rowSlice = decodedVector.base()->as<RowVector>();
    DWIO_ENSURE(rowSlice, "unexpected vector type");
    childRangesPtr = &childRanges;
    writeNulls(decodedVector, ranges);

    if (decodedVector.mayHaveNulls()) {
      // Compute range of slice that child writer need to write.
      // For every index in wrapped indices we will add a 1-entry size range.
      for (auto& pos : ranges) {
        if (decodedVector.isNullAt(pos)) {
          nullCount++;
        } else {
          childRanges.add(
              decodedVector.index(pos), decodedVector.index(pos) + 1);
        }
      }
    } else {
      for (auto& pos : ranges) {
        childRanges.add(decodedVector.index(pos), decodedVector.index(pos) + 1);
      }
    }
  } else {
    rowSlice = slice->as<RowVector>();
    DWIO_ENSURE(rowSlice, "unexpected vector type");
    childRangesPtr = &ranges;
    writeNulls(slice, ranges);
    auto nulls = slice->rawNulls();
    if (slice->mayHaveNulls()) {
      // Compute range of slice that child writer need to write.
      childRanges = ranges.filter(
          [&nulls](auto i) { return !bits::isBitNull(nulls, i); });
      childRangesPtr = &childRanges;
      nullCount = ranges.size() - childRanges.size();
    }
  }
  return writeChildrenAndStats(rowSlice, *childRangesPtr, nullCount);
}

class ListColumnWriter : public BaseColumnWriter {
 public:
  ListColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        lengths_{createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_LENGTH),
            context.getConfig(Config::USE_VINTS),
            dwio::common::INT_BYTE_SIZE)} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    lengths_->flush();
    children_.at(0)->flush(encodingFactory);
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    lengths_->recordPosition(*indexBuilder_);
  }

 private:
  std::unique_ptr<IntEncoder</* isSigned = */ false>> lengths_;
};

uint64_t ListColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  DataBuffer<vector_size_t> nonNullLengths{
      getMemoryPool(MemoryUsageCategory::GENERAL)};
  nonNullLengths.reserve(ranges.size());
  common::Ranges childRanges;
  uint64_t nullCount = 0;

  const ArrayVector* arraySlice;
  const vector_size_t* offsets;
  const vector_size_t* lengths;
  auto processRow = [&](size_t pos) {
    // only add if length > 0
    auto begin = offsets[pos];
    auto end = begin + lengths[pos];
    if (end > begin) {
      childRanges.add(begin, end);
    }
    nonNullLengths.unsafeAppend(end - begin);
  };

  if (slice->encoding() != VectorEncoding::Simple::ARRAY) {
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    arraySlice = decodedVector.base()->as<ArrayVector>();
    DWIO_ENSURE(arraySlice, "unexpected vector type");

    writeNulls(decodedVector, ranges);
    offsets = arraySlice->rawOffsets();
    lengths = arraySlice->rawSizes();

    if (decodedVector.mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (decodedVector.isNullAt(pos)) {
          ++nullCount;
        } else {
          processRow(decodedVector.index(pos));
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(decodedVector.index(pos));
      }
    }
  } else {
    arraySlice = slice->as<ArrayVector>();
    DWIO_ENSURE(arraySlice, "unexpected vector type");

    writeNulls(slice, ranges);
    auto nulls = slice->rawNulls();
    // Retargeting array and its offsets and lengths to be used
    // by the processRow lambda.
    offsets = arraySlice->rawOffsets();
    lengths = arraySlice->rawSizes();

    if (slice->mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (bits::isBitNull(nulls, pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(pos);
      }
    }
  }

  if (nonNullLengths.size()) {
    lengths_->add(
        nonNullLengths.data(),
        common::Ranges::of(0, nonNullLengths.size()),
        nullptr);
  }

  uint64_t rawSize = 0;
  if (childRanges.size()) {
    rawSize += children_.at(0)->write(arraySlice->elements(), childRanges);
  }

  if (nullCount > 0) {
    indexStatsBuilder_->setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  indexStatsBuilder_->increaseValueCount(ranges.size() - nullCount);
  indexStatsBuilder_->increaseRawSize(rawSize);
  return rawSize;
}

class MapColumnWriter : public BaseColumnWriter {
 public:
  MapColumnWriter(
      WriterContext& context,
      const TypeWithId& type,
      const uint32_t sequence,
      std::function<void(IndexBuilder&)> onRecordPosition)
      : BaseColumnWriter{context, type, sequence, onRecordPosition},
        lengths_{createRleEncoder</* isSigned = */ false>(
            RleVersion_1,
            newStream(StreamKind::StreamKind_LENGTH),
            context.getConfig(Config::USE_VINTS),
            dwio::common::INT_BYTE_SIZE)} {
    reset();
  }

  uint64_t write(const VectorPtr& slice, const common::Ranges& ranges) override;

  void flush(
      std::function<proto::ColumnEncoding&(uint32_t)> encodingFactory,
      std::function<void(proto::ColumnEncoding&)> encodingOverride) override {
    BaseColumnWriter::flush(encodingFactory, encodingOverride);
    lengths_->flush();
    children_.at(0)->flush(encodingFactory);
    children_.at(1)->flush(encodingFactory);
  }

  void recordPosition() override {
    BaseColumnWriter::recordPosition();
    lengths_->recordPosition(*indexBuilder_);
  }

 private:
  std::unique_ptr<IntEncoder<false>> lengths_;
};

uint64_t MapColumnWriter::write(
    const VectorPtr& slice,
    const common::Ranges& ranges) {
  DataBuffer<vector_size_t> nonNullLengths{
      getMemoryPool(MemoryUsageCategory::GENERAL)};
  nonNullLengths.reserve(ranges.size());
  common::Ranges childRanges;
  uint64_t nullCount = 0;

  const MapVector* mapSlice;
  const vector_size_t* offsets;
  const vector_size_t* lengths;
  auto processRow = [&](size_t pos) {
    // Only add if length > 0.
    auto begin = offsets[pos];
    auto end = begin + lengths[pos];
    if (end > begin) {
      childRanges.add(begin, end);
    }
    nonNullLengths.unsafeAppend(end - begin);
  };

  if (slice->encoding() != VectorEncoding::Simple::MAP) {
    auto localDecoded = decode(slice, ranges);
    auto& decodedVector = localDecoded.get();
    mapSlice = decodedVector.base()->as<MapVector>();
    DWIO_ENSURE(mapSlice, "unexpected vector type");

    writeNulls(decodedVector, ranges);
    offsets = mapSlice->rawOffsets();
    lengths = mapSlice->rawSizes();

    if (decodedVector.mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (decodedVector.isNullAt(pos)) {
          ++nullCount;
        } else {
          processRow(decodedVector.index(pos));
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(decodedVector.index(pos));
      }
    }
  } else {
    mapSlice = slice->as<MapVector>();
    DWIO_ENSURE(mapSlice, "unexpected vector type");

    writeNulls(slice, ranges);
    auto nulls = slice->rawNulls();
    offsets = mapSlice->rawOffsets();
    lengths = mapSlice->rawSizes();

    if (slice->mayHaveNulls()) {
      for (auto& pos : ranges) {
        if (bits::isBitNull(nulls, pos)) {
          ++nullCount;
        } else {
          processRow(pos);
        }
      }
    } else {
      for (auto& pos : ranges) {
        processRow(pos);
      }
    }
  }

  if (nonNullLengths.size()) {
    lengths_->add(
        nonNullLengths.data(),
        common::Ranges::of(0, nonNullLengths.size()),
        nullptr);
  }

  uint64_t rawSize = 0;
  if (childRanges.size()) {
    DWIO_ENSURE_EQ(mapSlice->mapKeys()->size(), mapSlice->mapValues()->size());
    rawSize += children_.at(0)->write(mapSlice->mapKeys(), childRanges);
    rawSize += children_.at(1)->write(mapSlice->mapValues(), childRanges);
  }

  if (nullCount > 0) {
    indexStatsBuilder_->setHasNull();
    rawSize += nullCount * NULL_SIZE;
  }
  indexStatsBuilder_->increaseValueCount(ranges.size() - nullCount);
  indexStatsBuilder_->increaseRawSize(rawSize);
  return rawSize;
}

} // namespace

std::unique_ptr<BaseColumnWriter> BaseColumnWriter::create(
    WriterContext& context,
    const TypeWithId& type,
    const uint32_t sequence,
    std::function<void(IndexBuilder&)> onRecordPosition) {
  const auto flatMapEnabled = context.getConfig(Config::FLATTEN_MAP);
  const auto& flatMapCols = context.getConfig(Config::MAP_FLAT_COLS);

  // When flat map is enabled, all columns provided in the MAP_FLAT_COLS config,
  // must be of MAP type. We only check top level columns (columns which are
  // direct children of the root node).
  if (flatMapEnabled && type.parent != nullptr && type.parent->id == 0) {
    if (type.type->kind() != TypeKind::MAP &&
        std::find(flatMapCols.begin(), flatMapCols.end(), type.column) !=
            flatMapCols.end()) {
      DWIO_RAISE(fmt::format(
          "MAP_FLAT_COLS contains column {}, but the root type of this column is {}."
          " Column root types must be of type MAP",
          type.column,
          mapTypeKindToName(type.type->kind())));
    }
    const auto structColumnKeys =
        context.getConfig(Config::MAP_FLAT_COLS_STRUCT_KEYS);
    if (!structColumnKeys.empty()) {
      DWIO_ENSURE(
          type.parent->size() == structColumnKeys.size(),
          "MAP_FLAT_COLS_STRUCT_KEYS size must match number of columns.");
      if (!structColumnKeys[type.column].empty()) {
        DWIO_ENSURE(
            std::find(flatMapCols.begin(), flatMapCols.end(), type.column) !=
                flatMapCols.end(),
            "Struct input found in MAP_FLAT_COLS_STRUCT_KEYS. Column must also be in MAP_FLAT_COLS.");
      }
    }
  }

  switch (type.type->kind()) {
    case TypeKind::BOOLEAN:
      return std::make_unique<ByteRleColumnWriter<bool>>(
          context, type, sequence, &createBooleanRleEncoder, onRecordPosition);
    case TypeKind::TINYINT:
      return std::make_unique<ByteRleColumnWriter<int8_t>>(
          context, type, sequence, &createByteRleEncoder, onRecordPosition);
    case TypeKind::SMALLINT:
      return std::make_unique<IntegerColumnWriter<int16_t>>(
          context, type, sequence, onRecordPosition);
    case TypeKind::INTEGER:
      return std::make_unique<IntegerColumnWriter<int32_t>>(
          context, type, sequence, onRecordPosition);
    case TypeKind::BIGINT:
      return std::make_unique<IntegerColumnWriter<int64_t>>(
          context, type, sequence, onRecordPosition);
    case TypeKind::REAL:
      return std::make_unique<FloatColumnWriter<float>>(
          context, type, sequence, onRecordPosition);
    case TypeKind::DOUBLE:
      return std::make_unique<FloatColumnWriter<double>>(
          context, type, sequence, onRecordPosition);
    case TypeKind::VARCHAR:
      return std::make_unique<StringColumnWriter>(
          context, type, sequence, onRecordPosition);
    case TypeKind::VARBINARY:
      return std::make_unique<BinaryColumnWriter>(
          context, type, sequence, onRecordPosition);
    case TypeKind::TIMESTAMP:
      return std::make_unique<TimestampColumnWriter>(
          context, type, sequence, onRecordPosition);
    case TypeKind::ROW: {
      auto ret = std::make_unique<StructColumnWriter>(
          context, type, sequence, onRecordPosition);
      ret->children_.reserve(type.size());
      for (int32_t i = 0; i < type.size(); ++i) {
        ret->children_.push_back(create(context, *type.childAt(i), sequence));
      }
      return ret;
    }
    case TypeKind::MAP: {
      DWIO_ENSURE_EQ(type.size(), 2, "Map should have exactly two children");

      // We only flatten maps which are direct children of the root node.
      // All other (nested) maps are treated as regular maps.
      if (type.parent != nullptr && type.parent->id == 0 &&
          context.getConfig(Config::FLATTEN_MAP) &&
          std::find(flatMapCols.begin(), flatMapCols.end(), type.column) !=
              flatMapCols.end()) {
        DWIO_ENSURE(!onRecordPosition, "unexpected flat map nesting");
        return FlatMapColumnWriter<TypeKind::INVALID>::create(
            context, type, sequence);
      }
      auto ret = std::make_unique<MapColumnWriter>(
          context, type, sequence, onRecordPosition);
      ret->children_.push_back(create(context, *type.childAt(0), sequence));
      ret->children_.push_back(create(context, *type.childAt(1), sequence));
      return ret;
    }
    case TypeKind::ARRAY: {
      DWIO_ENSURE_EQ(type.size(), 1, "Array should have exactly one child");
      auto ret = std::make_unique<ListColumnWriter>(
          context, type, sequence, onRecordPosition);
      ret->children_.push_back(create(context, *type.childAt(0), sequence));
      return ret;
    }
    default:
      DWIO_RAISE("not supported yet ", mapTypeKindToName(type.type->kind()));
  }
}

} // namespace facebook::velox::dwrf
