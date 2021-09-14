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

#include <string>

#include <fmt/format.h>

#include "folly/Range.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

namespace facebook::velox::dwrf {

// Writer version
constexpr folly::StringPiece WRITER_NAME_KEY{"orc.writer.name"};
constexpr folly::StringPiece WRITER_VERSION_KEY{"orc.writer.version"};
constexpr folly::StringPiece kDwioWriter{"dwio"};
constexpr folly::StringPiece kPrestoWriter{"presto"};

enum CompressionKind {
  CompressionKind_NONE = 0,
  CompressionKind_ZLIB = 1,
  CompressionKind_SNAPPY = 2,
  CompressionKind_LZO = 3,
  CompressionKind_ZSTD = 4,
  CompressionKind_LZ4 = 5,
  CompressionKind_MAX = INT64_MAX
};

/**
 * Get the name of the CompressionKind.
 */
std::string compressionKindToString(CompressionKind kind);

enum WriterVersion {
  ORIGINAL = 0, // all default versions including files written by Presto
  DWRF_4_9 = 1, // string stats collection uses text rather than string
  DWRF_5_0 = 2, // string stats saved/read as raw bytes,
  DWRF_6_0 = 3, // adding sequence to each stream
  DWRF_7_0 = 4, // flatmap integer dictionary sharing
  FUTURE = INT64_MAX
};

constexpr WriterVersion WriterVersion_CURRENT = WriterVersion::DWRF_7_0;

/**
 * Get the name of the WriterVersion.
 */
std::string writerVersionToString(WriterVersion kind);

enum StreamKind {
  StreamKind_PRESENT = 0,
  StreamKind_DATA = 1,
  StreamKind_LENGTH = 2,
  StreamKind_DICTIONARY_DATA = 3,
  StreamKind_DICTIONARY_COUNT = 4,
  StreamKind_NANO_DATA = 5,
  StreamKind_ROW_INDEX = 6,
  StreamKind_IN_DICTIONARY = 7,
  StreamKind_STRIDE_DICTIONARY = 8,
  StreamKind_STRIDE_DICTIONARY_LENGTH = 9,
  StreamKind_BLOOM_FILTER_UTF8 = 10,
  StreamKind_IN_MAP = 11
};

/**
 * Get the string representation of the StreamKind.
 */
std::string streamKindToString(StreamKind kind);

class StreamInformation {
 public:
  virtual ~StreamInformation() = default;

  virtual StreamKind getKind() const = 0;
  virtual uint32_t getNode() const = 0;
  virtual uint32_t getSequence() const = 0;
  virtual uint64_t getOffset() const = 0;
  virtual uint64_t getLength() const = 0;
  virtual bool getUseVInts() const = 0;
  virtual bool valid() const = 0;
};

enum ColumnEncodingKind {
  ColumnEncodingKind_DIRECT = 0,
  ColumnEncodingKind_DICTIONARY = 1,
  ColumnEncodingKind_DIRECT_V2 = 2,
  ColumnEncodingKind_DICTIONARY_V2 = 3
};

constexpr uint32_t MAX_UINT32 = std::numeric_limits<uint32_t>::max();

class StreamIdentifier;
class EncodingKey {
 public:
  static const EncodingKey& getInvalid() {
    static const EncodingKey INVALID;
    return INVALID;
  }

 public:
  EncodingKey() : EncodingKey(MAX_UINT32, MAX_UINT32) {}

  /* implicit */ EncodingKey(uint32_t n, uint32_t s = 0)
      : node{n}, sequence{s} {}
  uint32_t node;
  uint32_t sequence;

  bool operator==(const EncodingKey& other) const {
    return node == other.node && sequence == other.sequence;
  }

  std::size_t hash() const {
    return std::hash<uint32_t>()(node) ^ std::hash<uint32_t>()(sequence);
  }

  bool valid() const {
    return node != MAX_UINT32 && sequence >= 0;
  }

  std::string toString() const {
    return fmt::format("[node={}, sequence={}]", node, sequence);
  }

  StreamIdentifier forKind(const proto::Stream_Kind kind) const;
};

struct EncodingKeyHash {
  std::size_t operator()(const EncodingKey& ek) const {
    return ek.hash();
  }
};

class StreamIdentifier : public EncodingKey {
 public:
  static const StreamIdentifier& getInvalid() {
    static const StreamIdentifier INVALID;
    return INVALID;
  }

 public:
  StreamIdentifier()
      : EncodingKey(), column(MAX_UINT32), kind(StreamKind_DATA) {}

  /* implicit */ StreamIdentifier(const proto::Stream& stream)
      : StreamIdentifier(
            stream.node(),
            stream.has_sequence() ? stream.sequence() : 0,
            stream.has_column() ? stream.column() : MAX_UINT32,
            stream.kind()) {}

  StreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      proto::Stream_Kind pkind)
      : StreamIdentifier{
            node,
            sequence,
            column,
            static_cast<StreamKind>(pkind)} {}

  StreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      StreamKind kind)
      : EncodingKey(node, sequence), column{column}, kind(kind) {}

  ~StreamIdentifier() = default;

  bool operator==(const StreamIdentifier& other) const {
    // column == other.column may be join the expression if all files
    // share the same new version that has column field filled
    return node == other.node && sequence == other.sequence &&
        kind == other.kind;
  }

  std::size_t hash() const {
    return std::hash<uint32_t>()(node) ^ std::hash<uint32_t>()(sequence) ^
        std::hash<uint32_t>()(kind);
  }

  uint32_t column;
  StreamKind kind;

  std::string toString() const {
    return fmt::format(
        "[node={}, sequence={}, column={}, kind={}]",
        node,
        sequence,
        column,
        static_cast<uint32_t>(kind));
  }
};

struct StreamIdentifierHash {
  std::size_t operator()(const StreamIdentifier& si) const {
    return si.hash();
  }
};

std::string columnEncodingKindToString(ColumnEncodingKind kind);

class StripeInformation {
 public:
  virtual ~StripeInformation() = default;

  /**
   * Get the byte offset of the start of the stripe.
   * @return the bytes from the start of the file
   */
  virtual uint64_t getOffset() const = 0;

  /**
   * Get the total length of the stripe in bytes.
   * @return the number of bytes in the stripe
   */
  virtual uint64_t getLength() const = 0;

  /**
   * Get the length of the stripe's indexes.
   * @return the number of bytes in the index
   */
  virtual uint64_t getIndexLength() const = 0;

  /**
   * Get the length of the stripe's data.
   * @return the number of bytes in the stripe
   */
  virtual uint64_t getDataLength() const = 0;

  /**
   * Get the length of the stripe's tail section, which contains its index.
   * @return the number of bytes in the tail
   */
  virtual uint64_t getFooterLength() const = 0;

  /**
   * Get the number of rows in the stripe.
   * @return a count of the number of rows
   */
  virtual uint64_t getNumberOfRows() const = 0;
};

} // namespace facebook::velox::dwrf
