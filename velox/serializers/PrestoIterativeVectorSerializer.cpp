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

#include "velox/serializers/PrestoIterativeVectorSerializer.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"

namespace facebook::velox::serializer::presto::detail {
PrestoIterativeVectorSerializer::PrestoIterativeVectorSerializer(
    const RowTypePtr& rowType,
    int32_t numRows,
    StreamArena* streamArena,
    const PrestoVectorSerde::PrestoOptions& opts)
    : opts_(opts),
      streamArena_(streamArena),
      codec_(common::compressionKindToCodec(opts.compressionKind)),
      streams_(memory::StlAllocator<VectorStream>(*streamArena->pool())) {
  const auto types = rowType->children();
  const auto numTypes = types.size();
  streams_.reserve(numTypes);

  for (int i = 0; i < numTypes; ++i) {
    streams_.emplace_back(
        types[i], std::nullopt, std::nullopt, streamArena, numRows, opts);
  }
}

void PrestoIterativeVectorSerializer::append(
    const RowVectorPtr& vector,
    const folly::Range<const IndexRange*>& ranges,
    Scratch& scratch) {
  const auto numNewRows = rangesTotalSize(ranges);
  if (numNewRows == 0) {
    return;
  }
  numRows_ += numNewRows;
  for (int32_t i = 0; i < vector->childrenSize(); ++i) {
    serializeColumn(vector->childAt(i), ranges, &streams_[i], scratch);
  }
}

void PrestoIterativeVectorSerializer::append(
    const RowVectorPtr& vector,
    const folly::Range<const vector_size_t*>& rows,
    Scratch& scratch) {
  const auto numNewRows = rows.size();
  if (numNewRows == 0) {
    return;
  }
  numRows_ += numNewRows;
  for (int32_t i = 0; i < vector->childrenSize(); ++i) {
    serializeColumn(vector->childAt(i), rows, &streams_[i], scratch);
  }
}

size_t PrestoIterativeVectorSerializer::maxSerializedSize() const {
  size_t dataSize = 4; // streams_.size()
  for (auto& stream : streams_) {
    dataSize += const_cast<VectorStream&>(stream).serializedSize();
  }

  auto compressedSize = needCompression(*codec_)
      ? codec_->maxCompressedLength(dataSize)
      : dataSize;
  return kHeaderSize + compressedSize;
}

// The SerializedPage layout is:
// numRows(4) | codec(1) | uncompressedSize(4) | compressedSize(4) |
// checksum(8) | data
void PrestoIterativeVectorSerializer::flush(OutputStream* out) {
  constexpr int32_t kMaxCompressionAttemptsToSkip = 30;
  if (!needCompression(*codec_)) {
    flushStreams(
        streams_,
        numRows_,
        *streamArena_,
        *codec_,
        opts_.minCompressionRatio,
        out);
  } else {
    if (numCompressionToSkip_ > 0) {
      const auto noCompressionCodec = common::compressionKindToCodec(
          common::CompressionKind::CompressionKind_NONE);
      auto [size, ignore] = flushStreams(
          streams_, numRows_, *streamArena_, *noCompressionCodec, 1, out);
      stats_.compressionSkippedBytes += size;
      --numCompressionToSkip_;
      ++stats_.numCompressionSkipped;
    } else {
      auto [size, compressedSize] = flushStreams(
          streams_,
          numRows_,
          *streamArena_,
          *codec_,
          opts_.minCompressionRatio,
          out);
      stats_.compressionInputBytes += size;
      stats_.compressedBytes += compressedSize;
      if (compressedSize > size * opts_.minCompressionRatio) {
        numCompressionToSkip_ = std::min<int64_t>(
            kMaxCompressionAttemptsToSkip, 1 + stats_.numCompressionSkipped);
      }
    }
  }
}

std::unordered_map<std::string, RuntimeCounter>
PrestoIterativeVectorSerializer::runtimeStats() {
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

void PrestoIterativeVectorSerializer::clear() {
  numRows_ = 0;
  for (auto& stream : streams_) {
    stream.clear();
  }
}
} // namespace facebook::velox::serializer::presto::detail
