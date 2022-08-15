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

#include "velox/dwio/dwrf/common/Common.h"

#include <folly/Conv.h>

namespace facebook::velox::dwrf {

std::string writerVersionToString(WriterVersion version) {
  switch (static_cast<int32_t>(version)) {
    case ORIGINAL:
      return "original";
    case DWRF_4_9:
      return "dwrf-4.9";
    case DWRF_5_0:
      return "dwrf-5.0";
    case DWRF_6_0:
      return "dwrf-6.0";
    case DWRF_7_0:
      return "dwrf-7.0";
  }
  return folly::to<std::string>("future - ", version);
}

std::string streamKindToString(StreamKind kind) {
  switch (static_cast<int32_t>(kind)) {
    case StreamKind_PRESENT:
      return "present";
    case StreamKind_DATA:
      return "data";
    case StreamKind_LENGTH:
      return "length";
    case StreamKind_DICTIONARY_DATA:
      return "dictionary";
    case StreamKind_DICTIONARY_COUNT:
      return "dictionary count";
    case StreamKind_NANO_DATA:
      return "nano data";
    case StreamKind_ROW_INDEX:
      return "index";
    case StreamKind_IN_DICTIONARY:
      return "in dictionary";
    case StreamKind_STRIDE_DICTIONARY:
      return "stride dictionary";
    case StreamKind_STRIDE_DICTIONARY_LENGTH:
      return "stride dictionary length";
    case StreamKind_BLOOM_FILTER_UTF8:
      return "bloom";
  }
  return folly::to<std::string>("unknown - ", kind);
}

std::string columnEncodingKindToString(ColumnEncodingKind kind) {
  switch (static_cast<int32_t>(kind)) {
    case ColumnEncodingKind_DIRECT:
      return "direct";
    case ColumnEncodingKind_DICTIONARY:
      return "dictionary";
    case ColumnEncodingKind_DIRECT_V2:
      return "direct rle2";
    case ColumnEncodingKind_DICTIONARY_V2:
      return "dictionary rle2";
  }
  return folly::to<std::string>("unknown - ", kind);
}

DwrfStreamIdentifier EncodingKey::forKind(const proto::Stream_Kind kind) const {
  return DwrfStreamIdentifier(node, sequence, 0, kind);
}

namespace {
using dwio::common::CompressionKind;

CompressionKind orcCompressionToCompressionKind(
    proto::orc::CompressionKind compression) {
  switch (compression) {
    case proto::orc::CompressionKind::NONE:
      return CompressionKind::CompressionKind_NONE;
    case proto::orc::CompressionKind::ZLIB:
      return CompressionKind::CompressionKind_ZLIB;
    case proto::orc::CompressionKind::SNAPPY:
      return CompressionKind::CompressionKind_SNAPPY;
    case proto::orc::CompressionKind::LZO:
      return CompressionKind::CompressionKind_LZO;
    case proto::orc::CompressionKind::LZ4:
      return CompressionKind::CompressionKind_ZSTD;
    case proto::orc::CompressionKind::ZSTD:
      return CompressionKind::CompressionKind_LZ4;
  }
  return CompressionKind::CompressionKind_NONE;
}
} // namespace
} // namespace facebook::velox::dwrf
