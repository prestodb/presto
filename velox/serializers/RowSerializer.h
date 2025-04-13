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

#include "velox/serializers/CompactRowSerializer.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer {

using TRowSize = uint32_t;

namespace detail {
struct RowGroupHeader {
  int32_t uncompressedSize;
  int32_t compressedSize;
  bool compressed;

  static RowGroupHeader read(ByteInputStream* source);

  void write(OutputStream* out);

  void write(char* out);

  static size_t size();

  std::string debugString() const;
};
} // namespace detail

template <class Serializer>
class RowSerializer : public IterativeVectorSerializer {
 public:
  RowSerializer(memory::MemoryPool* pool, const VectorSerde::Options* options)
      : pool_(pool),
        options_(options == nullptr ? VectorSerde::Options() : *options),
        codec_(common::compressionKindToCodec(options_.compressionKind)) {}

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& /*scratch*/) override {
    size_t totalSize = 0;
    const auto totalRows = std::accumulate(
        ranges.begin(),
        ranges.end(),
        0,
        [](vector_size_t sum, const auto& range) { return sum + range.size; });

    if (totalRows == 0) {
      return;
    }

    Serializer row(vector);
    std::vector<vector_size_t> rowSize(totalRows);
    if (auto fixedRowSize =
            Serializer::fixedRowSize(asRowType(vector->type()))) {
      totalSize += (fixedRowSize.value() + sizeof(TRowSize)) * totalRows;
      std::fill(rowSize.begin(), rowSize.end(), fixedRowSize.value());
    } else {
      vector_size_t index = 0;
      for (const auto& range : ranges) {
        for (auto i = 0; i < range.size; ++i, ++index) {
          rowSize[index] = row.rowSize(range.begin + i);
          totalSize += rowSize[index] + sizeof(TRowSize);
        }
      }
    }

    if (totalSize == 0) {
      return;
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_, 0);
    auto* rawBuffer = buffer->asMutable<char>();
    buffers_.push_back(std::move(buffer));

    serializeRanges(row, ranges, rawBuffer, rowSize);
  }

  void append(
      const Serializer& compactRow,
      const folly::Range<const vector_size_t*>& rows,
      const std::vector<vector_size_t>& sizes) override {
    size_t totalSize = 0;
    for (const auto row : rows) {
      totalSize += sizes[row];
    }
    if (totalSize == 0) {
      return;
    }

    BufferPtr buffer = AlignedBuffer::allocate<char>(totalSize, pool_, 0);
    auto* rawBuffer = buffer->asMutable<char>();
    buffers_.push_back(std::move(buffer));

    size_t offset = 0;
    for (auto& row : rows) {
      // Write row data.
      const TRowSize size =
          compactRow.serialize(row, rawBuffer + offset + sizeof(TRowSize));

      // Write raw size. Needs to be in big endian order.
      *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(size);
      offset += sizeof(TRowSize) + size;
    }
  }

  size_t maxSerializedSize() const override {
    const auto size = uncompressedSize();
    if (!needCompression()) {
      return detail::RowGroupHeader::size() + size;
    }
    VELOX_CHECK_LE(
        size,
        codec_->maxUncompressedLength(),
        "UncompressedSize exceeds limit");
    return detail::RowGroupHeader::size() + codec_->maxCompressedLength(size);
  }

  /// The serialization format is | uncompressedSize | compressedSize |
  /// compressed | data.
  void flush(OutputStream* stream) override {
    constexpr int32_t kMaxCompressionAttemptsToSkip = 30;
    const auto size = uncompressedSize();
    if (!needCompression()) {
      flushUncompressed(size, stream);
    } else if (numCompressionToSkip_ > 0) {
      flushUncompressed(size, stream);
      stats_.compressionSkippedBytes += size;
      --numCompressionToSkip_;
      ++stats_.numCompressionSkipped;
    } else {
      // Compress the buffer if satisfied condition.
      const auto toCompress = toIOBuf(buffers_);
      const auto compressedBuffer = codec_->compress(toCompress.get());
      const int32_t compressedSize = compressedBuffer->length();
      stats_.compressionInputBytes += size;
      stats_.compressedBytes += compressedSize;
      if (compressedSize > options_.minCompressionRatio * size) {
        // Skip this compression.
        numCompressionToSkip_ = std::min<int64_t>(
            kMaxCompressionAttemptsToSkip, 1 + stats_.numCompressionSkipped);
        flushUncompressed(size, stream);
      } else {
        // Do the compression.
        detail::RowGroupHeader header = {size, compressedSize, true};
        header.write(stream);
        for (auto range : *compressedBuffer) {
          stream->write(
              reinterpret_cast<const char*>(range.data()), range.size());
        }
      }
    }

    buffers_.clear();
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override {
    std::unordered_map<std::string, RuntimeCounter> map;
    if (stats_.compressionInputBytes != 0) {
      map.emplace(
          kCompressionInputBytes,
          RuntimeCounter(
              stats_.compressionInputBytes, RuntimeCounter::Unit::kBytes));
    }
    if (stats_.compressedBytes != 0) {
      map.emplace(
          kCompressedBytes,
          RuntimeCounter(stats_.compressedBytes, RuntimeCounter::Unit::kBytes));
    }
    if (stats_.compressionSkippedBytes != 0) {
      map.emplace(
          kCompressionSkippedBytes,
          RuntimeCounter(
              stats_.compressionSkippedBytes, RuntimeCounter::Unit::kBytes));
    }
    return map;
  }

  void clear() override {}

 protected:
  virtual void serializeRanges(
      const Serializer& rowSerializer,
      const folly::Range<const IndexRange*>& ranges,
      char* rawBuffer,
      const std::vector<vector_size_t>& /*rowSize*/) {
    size_t offset = 0;
    for (auto& range : ranges) {
      for (auto row = range.begin; row < range.begin + range.size; ++row) {
        // Write row data.
        TRowSize size =
            rowSerializer.serialize(row, rawBuffer + offset + sizeof(TRowSize));

        // Write raw size. Needs to be in big endian order.
        *(TRowSize*)(rawBuffer + offset) = folly::Endian::big(size);
        offset += sizeof(TRowSize) + size;
      }
    }
  }

  memory::MemoryPool* const pool_;
  std::vector<BufferPtr> buffers_;

 private:
  std::unique_ptr<folly::IOBuf> toIOBuf(const std::vector<BufferPtr>& buffers) {
    std::unique_ptr<folly::IOBuf> iobuf;
    for (const auto& buffer : buffers) {
      auto newBuf =
          folly::IOBuf::wrapBuffer(buffer->asMutable<char>(), buffer->size());
      if (iobuf) {
        iobuf->prev()->appendChain(std::move(newBuf));
      } else {
        iobuf = std::move(newBuf);
      }
    }
    return iobuf;
  }

  int32_t uncompressedSize() const {
    int32_t totalSize = 0;
    for (const auto& buffer : buffers_) {
      totalSize += buffer->size();
    }
    return totalSize;
  }

  bool needCompression() const {
    return codec_->type() != folly::compression::CodecType::NO_COMPRESSION;
  }

  void flushUncompressed(int32_t size, OutputStream* stream) {
    detail::RowGroupHeader header = {size, size, false};
    header.write(stream);
    for (const auto& buffer : buffers_) {
      stream->write(buffer->template asMutable<char>(), buffer->size());
    }
  }

  const VectorSerde::Options options_;
  const std::unique_ptr<folly::compression::Codec> codec_;
  // Count of forthcoming compressions to skip.
  int32_t numCompressionToSkip_{0};
  CompressionStats stats_;
};

/// RowIteratorImpl is an iterator to read deserialize rows from an uncompressed
/// input source. It provides the ability to iterate over the stream by
/// retrieving a serialized buffer containing one row in each iteration.
///
/// Usage:
/// RowIteratorImpl iterator(source, endOffset);
/// while (iterator.hasNext()) {
///   auto next = iterator.next();
///   ...Process the data in next...
/// }
class RowIteratorImpl : public velox::RowIterator {
 public:
  /// Constructs a row iterator without source ownership.
  RowIteratorImpl(ByteInputStream* source, size_t endOffset);

  /// Constructs a row iterator with source ownership.
  RowIteratorImpl(
      std::unique_ptr<ByteInputStream> source,
      std::unique_ptr<folly::IOBuf> buf,
      size_t endOffset);

  bool hasNext() const override;

  std::unique_ptr<std::string> next() override;

 private:
  TRowSize readRowSize();

  // Read from the stream until the full row is concatenated.
  static void concatenatePartialRow(
      ByteInputStream* source,
      TRowSize rowSize,
      std::string& rowBuffer);

  // Stream holder if constructed with source ownership.
  const std::unique_ptr<ByteInputStream> sourceHolder_;

  // Buffer holder if constructed with source ownership.
  const std::unique_ptr<folly::IOBuf> bufHolder_;
};

template <typename SerializeView>
class RowDeserializer {
 public:
  static void deserialize(
      ByteInputStream* source,
      std::vector<SerializeView>& serializedRows,
      std::vector<std::unique_ptr<std::string>>& serializedBuffers,
      const VectorSerde::Options* options,
      std::function<std::unique_ptr<RowIterator>(ByteInputStream*, size_t)>
          rowIteratorFactory = [](auto* source, auto endOffset) {
            return std::make_unique<RowIteratorImpl>(source, endOffset);
          }) {
    const auto compressionKind = options == nullptr
        ? VectorSerde::Options().compressionKind
        : options->compressionKind;
    while (!source->atEnd()) {
      std::unique_ptr<folly::IOBuf> uncompressedBuf;
      const auto header = detail::RowGroupHeader::read(source);
      if (header.compressed) {
        VELOX_DCHECK_NE(
            compressionKind, common::CompressionKind::CompressionKind_NONE);
        auto compressBuf = folly::IOBuf::create(header.compressedSize);
        source->readBytes(compressBuf->writableData(), header.compressedSize);
        compressBuf->append(header.compressedSize);

        // Process chained uncompressed results IOBufs.
        const auto codec = common::compressionKindToCodec(compressionKind);
        uncompressedBuf =
            codec->uncompress(compressBuf.get(), header.uncompressedSize);
      }

      std::unique_ptr<ByteInputStream> uncompressedStream;
      ByteInputStream* uncompressedSource{nullptr};
      if (uncompressedBuf == nullptr) {
        uncompressedSource = source;
      } else {
        uncompressedStream = std::make_unique<BufferInputStream>(
            byteRangesFromIOBuf(uncompressedBuf.get()));
        uncompressedSource = uncompressedStream.get();
      }
      const std::streampos initialSize = uncompressedSource->tellp();
      std::unique_ptr<velox::RowIterator> rowIterator = rowIteratorFactory(
          uncompressedSource, header.uncompressedSize + initialSize);
      while (rowIterator->hasNext()) {
        serializedBuffers.emplace_back(rowIterator->next());
        serializedRows.push_back(std::string_view(
            serializedBuffers.back()->data(),
            serializedBuffers.back()->size()));
      }
    }
  }

  /// @param maxRows Max number of rows to deserialize
  /// @param sourceRowIterator The iterator used to start deserializing. If
  /// nullptr, the method will start to deserialize from 'source'. After
  /// deserialization, the method will set the iterator ready for the next call
  /// to continue to iterate. If no more data to deserialize, it will be set to
  /// nullptr.
  static uint64_t deserialize(
      ByteInputStream* source,
      uint64_t maxRows,
      std::unique_ptr<velox::RowIterator>& sourceRowIterator,
      std::vector<SerializeView>& serializedRows,
      std::vector<std::unique_ptr<std::string>>& serializedBuffers,
      const VectorSerde::Options* options) {
    auto remainingRows = maxRows;
    if (sourceRowIterator == nullptr) {
      VELOX_CHECK(!source->atEnd());
      sourceRowIterator = createNextRowIter(source, options);
    }
    while (remainingRows > 0) {
      while (sourceRowIterator->hasNext()) {
        serializedBuffers.emplace_back(sourceRowIterator->next());
        if constexpr (std::is_same_v<SerializeView, std::string_view>) {
          serializedRows.push_back(std::string_view(
              serializedBuffers.back()->data(),
              serializedBuffers.back()->size()));
        } else {
          serializedRows.push_back(serializedBuffers.back()->data());
        }
        if (--remainingRows == 0) {
          break;
        }
      }
      if (!sourceRowIterator->hasNext()) {
        if (source->atEnd()) {
          // No more data to read.
          sourceRowIterator.reset();
          break;
        }
        sourceRowIterator = createNextRowIter(source, options);
      }
    }
    return maxRows - remainingRows;
  }

 private:
  static std::unique_ptr<velox::RowIterator> createNextRowIter(
      ByteInputStream* source,
      const VectorSerde::Options* options) {
    const auto header = detail::RowGroupHeader::read(source);
    if (!header.compressed) {
      return std::make_unique<RowIteratorImpl>(
          source, header.uncompressedSize + source->tellp());
    }

    const auto compressionKind = options == nullptr
        ? VectorSerde::Options().compressionKind
        : options->compressionKind;
    VELOX_DCHECK_NE(
        compressionKind, common::CompressionKind::CompressionKind_NONE);
    auto compressBuf = folly::IOBuf::create(header.compressedSize);
    source->readBytes(compressBuf->writableData(), header.compressedSize);
    compressBuf->append(header.compressedSize);

    // Process chained uncompressed results IOBufs.
    const auto codec = common::compressionKindToCodec(compressionKind);
    auto uncompressedBuf =
        codec->uncompress(compressBuf.get(), header.uncompressedSize);

    auto uncompressedStream = std::make_unique<BufferInputStream>(
        byteRangesFromIOBuf(uncompressedBuf.get()));
    const std::streampos initialSize = uncompressedStream->tellp();
    return std::make_unique<RowIteratorImpl>(
        std::move(uncompressedStream),
        std::move(uncompressedBuf),
        header.uncompressedSize + initialSize);
  }
};
} // namespace facebook::velox::serializer
