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

#include "velox/common/compression/Compression.h"
#include "velox/common/base/Exceptions.h"

#include <folly/Conv.h>

namespace facebook::velox::common {

std::unique_ptr<folly::io::Codec> compressionKindToCodec(CompressionKind kind) {
  switch (static_cast<int32_t>(kind)) {
    case CompressionKind_NONE:
      return getCodec(folly::io::CodecType::NO_COMPRESSION);
    case CompressionKind_ZLIB:
      return getCodec(folly::io::CodecType::ZLIB);
    case CompressionKind_SNAPPY:
      return getCodec(folly::io::CodecType::SNAPPY);
    case CompressionKind_ZSTD:
      return getCodec(folly::io::CodecType::ZSTD);
    case CompressionKind_LZ4:
      return getCodec(folly::io::CodecType::LZ4);
    case CompressionKind_GZIP:
      return getCodec(folly::io::CodecType::GZIP);
    default:
      VELOX_UNSUPPORTED(
          "Not support {} in folly", compressionKindToString(kind));
  }
}

CompressionKind codecTypeToCompressionKind(folly::io::CodecType type) {
  switch (type) {
    case folly::io::CodecType::NO_COMPRESSION:
      return CompressionKind_NONE;
    case folly::io::CodecType::ZLIB:
      return CompressionKind_ZLIB;
    case folly::io::CodecType::SNAPPY:
      return CompressionKind_SNAPPY;
    case folly::io::CodecType::ZSTD:
      return CompressionKind_ZSTD;
    case folly::io::CodecType::LZ4:
      return CompressionKind_LZ4;
    case folly::io::CodecType::GZIP:
      return CompressionKind_GZIP;
    default:
      VELOX_UNSUPPORTED(
          "Not support folly codec type {}", folly::to<std::string>(type));
  }
}

std::string compressionKindToString(CompressionKind kind) {
  switch (static_cast<int32_t>(kind)) {
    case CompressionKind_NONE:
      return "none";
    case CompressionKind_ZLIB:
      return "zlib";
    case CompressionKind_SNAPPY:
      return "snappy";
    case CompressionKind_LZO:
      return "lzo";
    case CompressionKind_ZSTD:
      return "zstd";
    case CompressionKind_LZ4:
      return "lz4";
    case CompressionKind_GZIP:
      return "gzip";
  }
  return folly::to<std::string>("unknown - ", kind);
}

CompressionKind stringToCompressionKind(const std::string& kind) {
  static const std::unordered_map<std::string, CompressionKind>
      stringToCompressionKindMap = {
          {"none", CompressionKind_NONE},
          {"zlib", CompressionKind_ZLIB},
          {"snappy", CompressionKind_SNAPPY},
          {"lzo", CompressionKind_LZO},
          {"zstd", CompressionKind_ZSTD},
          {"lz4", CompressionKind_LZ4},
          {"gzip", CompressionKind_GZIP}};
  auto iter = stringToCompressionKindMap.find(kind);
  if (iter != stringToCompressionKindMap.end()) {
    return iter->second;
  } else {
    VELOX_UNSUPPORTED("Not support compression kind {}", kind);
  }
}
} // namespace facebook::velox::common
