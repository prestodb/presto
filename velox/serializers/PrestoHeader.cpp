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
#include "velox/serializers/PrestoHeader.h"

#include "velox/serializers/PrestoSerializerSerializationUtils.h"

namespace facebook::velox::serializer::presto::detail {
/* static */ Expected<PrestoHeader> PrestoHeader::read(
    ByteInputStream* source) {
  if (source->remainingSize() < kHeaderSize) {
    return folly::makeUnexpected(Status::Invalid(
        fmt::format("{} bytes for header", source->remainingSize())));
  }
  PrestoHeader header;
  header.numRows = source->read<int32_t>();
  header.pageCodecMarker = source->read<int8_t>();
  header.uncompressedSize = source->read<int32_t>();
  header.compressedSize = source->read<int32_t>();
  header.checksum = source->read<int64_t>();

  if (header.numRows < 0) {
    return folly::makeUnexpected(
        Status::Invalid(fmt::format("negative numRows: {}", header.numRows)));
  }
  if (header.uncompressedSize < 0) {
    return folly::makeUnexpected(Status::Invalid(
        fmt::format("negative uncompressedSize: {}", header.uncompressedSize)));
  }
  if (header.compressedSize < 0) {
    return folly::makeUnexpected(Status::Invalid(
        fmt::format("negative compressedSize: {}", header.compressedSize)));
  }

  return header;
}

/* static */ std::optional<PrestoHeader> PrestoHeader::read(
    std::string_view* source) {
  if (source->size() < kHeaderSize) {
    return std::nullopt;
  }

  PrestoHeader header;
  header.numRows = readInt<int32_t>(source);
  header.pageCodecMarker = readInt<int8_t>(source);
  header.uncompressedSize = readInt<int32_t>(source);
  header.compressedSize = readInt<int32_t>(source);
  header.checksum = readInt<int64_t>(source);

  if (header.numRows < 0) {
    return std::nullopt;
  }
  if (header.uncompressedSize < 0) {
    return std::nullopt;
  }
  if (header.compressedSize < 0) {
    return std::nullopt;
  }

  return header;
}
} // namespace facebook::velox::serializer::presto::detail
