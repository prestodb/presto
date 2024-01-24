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

#include <fmt/format.h>
#include <folly/compression/Compression.h>
#include <string>

namespace facebook::velox::common {

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_ZSTD = 4,
  CompressionKind_LZ4 = 5,
  CompressionKind_GZIP = 6,
  CompressionKind_MAX = INT64_MAX
};

std::unique_ptr<folly::io::Codec> compressionKindToCodec(CompressionKind kind);

CompressionKind codecTypeToCompressionKind(folly::io::CodecType type);

/**
 * Get the name of the CompressionKind.
 */
std::string compressionKindToString(CompressionKind kind);

CompressionKind stringToCompressionKind(const std::string& kind);

constexpr uint64_t DEFAULT_COMPRESSION_BLOCK_SIZE = 256 * 1024;

} // namespace facebook::velox::common

template <>
struct fmt::formatter<facebook::velox::common::CompressionKind>
    : fmt::formatter<std::string> {
  auto format(
      const facebook::velox::common::CompressionKind& s,
      format_context& ctx) {
    return formatter<std::string>::format(
        facebook::velox::common::compressionKindToString(s), ctx);
  }
};
