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
#include <string>

#include "folly/Range.h"
#include "velox/common/caching/ScanTracker.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/StreamIdentifier.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/dwio/dwrf/common/wrap/orc-proto-wrapper.h"

namespace facebook::velox::dwrf {

// Writer version
constexpr folly::StringPiece WRITER_NAME_KEY{"orc.writer.name"};
constexpr folly::StringPiece WRITER_VERSION_KEY{"orc.writer.version"};
constexpr folly::StringPiece WRITER_HOSTNAME_KEY{"orc.writer.host"};
constexpr folly::StringPiece kDwioWriter{"dwio"};
constexpr folly::StringPiece kPrestoWriter{"presto"};

enum class DwrfFormat : uint8_t {
  kDwrf = 0,
  kOrc = 1,
};

enum StripeCacheMode { NA = 0, INDEX = 1, FOOTER = 2, BOTH = 3 };

enum WriterVersion {
  ORIGINAL = 0, // all default versions including files written by Presto
  DWRF_4_9 = 1, // string stats collection uses text rather than string
  DWRF_5_0 = 2, // string stats saved/read as raw bytes,
  DWRF_6_0 = 3, // adding sequence to each stream
  DWRF_7_0 = 4, // flatmap integer dictionary sharing
  FUTURE = INT64_MAX
};

constexpr WriterVersion WriterVersion_CURRENT = WriterVersion::DWRF_7_0;

constexpr auto kStreamKindOrcOffset = 1000;
constexpr auto DefineStreamKindOrc(int value) {
  return value + kStreamKindOrcOffset;
}
/**
 * Get the name of the WriterVersion.
 */
std::string writerVersionToString(WriterVersion kind);

enum StreamKind {
  // DWRF
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
  StreamKind_IN_MAP = 11,

  // Offset for ORC streams `value + 1000`
  StreamKindOrc_PRESENT = DefineStreamKindOrc(0),
  StreamKindOrc_DATA = DefineStreamKindOrc(1),
  StreamKindOrc_LENGTH = DefineStreamKindOrc(2),
  StreamKindOrc_DICTIONARY_DATA = DefineStreamKindOrc(3),
  StreamKindOrc_DICTIONARY_COUNT = DefineStreamKindOrc(4),
  StreamKindOrc_SECONDARY = DefineStreamKindOrc(5),
  StreamKindOrc_ROW_INDEX = DefineStreamKindOrc(6),
  StreamKindOrc_BLOOM_FILTER = DefineStreamKindOrc(7),
  StreamKindOrc_BLOOM_FILTER_UTF8 = DefineStreamKindOrc(8),
  StreamKindOrc_ENCRYPTED_INDEX = DefineStreamKindOrc(9),
  StreamKindOrc_ENCRYPTED_DATA = DefineStreamKindOrc(10),
  StreamKindOrc_STRIPE_STATISTICS = DefineStreamKindOrc(100),
  StreamKindOrc_FILE_STATISTICS = DefineStreamKindOrc(101),
};

inline DwrfFormat toDwrfFormat(StreamKind kind) {
  return kind < kStreamKindOrcOffset ? DwrfFormat::kDwrf : DwrfFormat::kOrc;
}

inline StreamKind toStreamKind(proto::Stream_Kind kind) {
  switch (kind) {
    case proto::Stream_Kind_PRESENT: // 0
      return StreamKind::StreamKind_PRESENT;

    case proto::Stream_Kind_DATA: // 1
      return StreamKind::StreamKind_DATA;

    case proto::Stream_Kind_LENGTH: // 2
      return StreamKind::StreamKind_LENGTH;

    case proto::Stream_Kind_DICTIONARY_DATA: // 3
      return StreamKind::StreamKind_DICTIONARY_DATA;

    case proto::Stream_Kind_DICTIONARY_COUNT: // 4
      return StreamKind::StreamKind_DICTIONARY_COUNT;

    case proto::Stream_Kind_NANO_DATA: // 5
      return StreamKind::StreamKind_NANO_DATA;

    case proto::Stream_Kind_ROW_INDEX: // 6
      return StreamKind::StreamKind_ROW_INDEX;

    case proto::Stream_Kind_IN_DICTIONARY: // 7
      return StreamKind::StreamKind_IN_DICTIONARY;

    case proto::Stream_Kind_STRIDE_DICTIONARY: // 8
      return StreamKind::StreamKind_STRIDE_DICTIONARY;

    case proto::Stream_Kind_STRIDE_DICTIONARY_LENGTH: // 9
      return StreamKind::StreamKind_STRIDE_DICTIONARY_LENGTH;

    case proto::Stream_Kind_BLOOM_FILTER_UTF8: // 10
      return StreamKind::StreamKind_BLOOM_FILTER_UTF8;

    case proto::Stream_Kind_IN_MAP: // 11
      return StreamKind::StreamKind_IN_MAP;

    default:
      DWIO_RAISE("Unknown stream dwrf kind: ", kind);
  }
}

inline StreamKind toStreamKind(proto::orc::Stream_Kind kind) {
  switch (kind) {
    case proto::orc::Stream_Kind_PRESENT: // 0,
      return StreamKind::StreamKindOrc_PRESENT;

    case proto::orc::Stream_Kind_DATA: // 1,
      return StreamKind::StreamKindOrc_DATA;

    case proto::orc::Stream_Kind_LENGTH: // 2,
      return StreamKind::StreamKindOrc_LENGTH;

    case proto::orc::Stream_Kind_DICTIONARY_DATA: // 3,
      return StreamKind::StreamKindOrc_DICTIONARY_DATA;

    case proto::orc::Stream_Kind_DICTIONARY_COUNT: // 4,
      return StreamKind::StreamKindOrc_DICTIONARY_COUNT;

    case proto::orc::Stream_Kind_SECONDARY: // 5,
      return StreamKind::StreamKindOrc_SECONDARY;

    case proto::orc::Stream_Kind_ROW_INDEX: // 6,
      return StreamKind::StreamKindOrc_ROW_INDEX;

    case proto::orc::Stream_Kind_BLOOM_FILTER: // 7,
      return StreamKind::StreamKindOrc_BLOOM_FILTER;

    case proto::orc::Stream_Kind_BLOOM_FILTER_UTF8: // 8,
      return StreamKind::StreamKindOrc_BLOOM_FILTER_UTF8;

    case proto::orc::Stream_Kind_ENCRYPTED_INDEX: // 9,
      return StreamKind::StreamKindOrc_ENCRYPTED_INDEX;

    case proto::orc::Stream_Kind_ENCRYPTED_DATA: // 10,
      return StreamKind::StreamKindOrc_ENCRYPTED_DATA;

    case proto::orc::Stream_Kind_STRIPE_STATISTICS: // 100,
      return StreamKind::StreamKindOrc_STRIPE_STATISTICS;

    case proto::orc::Stream_Kind_FILE_STATISTICS: // 101
      return StreamKind::StreamKindOrc_FILE_STATISTICS;

    default:
      DWIO_RAISE("Unknown stream orc kind: ", kind);
  }
}

inline bool isIndexStream(StreamKind kind) {
  return kind == StreamKind::StreamKind_ROW_INDEX ||
      kind == StreamKind::StreamKind_BLOOM_FILTER_UTF8 ||
      kind == StreamKind::StreamKindOrc_ROW_INDEX ||
      kind == StreamKind::StreamKindOrc_BLOOM_FILTER ||
      kind == StreamKind::StreamKindOrc_BLOOM_FILTER_UTF8;
}

/**
 * Get the string representation of the StreamKind.
 */
std::string streamKindToString(StreamKind kind);

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    const StreamKind& kind) {
  os << streamKindToString(kind);
  return os;
}

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

class DwrfStreamIdentifier;
class EncodingKey {
 public:
  static const EncodingKey& getInvalid() {
    static const EncodingKey INVALID;
    return INVALID;
  }

  EncodingKey()
      : EncodingKey(dwio::common::MAX_UINT32, dwio::common::MAX_UINT32) {}

  /* implicit */ EncodingKey(uint32_t n, uint32_t s = 0)
      : node_{n}, sequence_{s} {}

  bool operator==(const EncodingKey& other) const {
    return node_ == other.node_ && sequence_ == other.sequence_;
  }

  uint32_t node() const {
    return node_;
  }

  uint32_t sequence() const {
    return sequence_;
  }

  std::size_t hash() const {
    return std::hash<uint32_t>()(node_) ^ std::hash<uint32_t>()(sequence_);
  }

  bool valid() const {
    return node_ != dwio::common::MAX_UINT32;
  }

  std::string toString() const {
    return fmt::format("[node={}, sequence={}]", node_, sequence_);
  }

  DwrfStreamIdentifier forKind(const proto::Stream_Kind kind) const;

  DwrfStreamIdentifier forKind(const proto::orc::Stream_Kind kind) const;

 private:
  uint32_t node_;
  uint32_t sequence_;
};

struct EncodingKeyHash {
  std::size_t operator()(const EncodingKey& ek) const {
    return ek.hash();
  }
};

class DwrfStreamIdentifier : public dwio::common::StreamIdentifier {
 public:
  static const DwrfStreamIdentifier& getInvalid() {
    static const DwrfStreamIdentifier kInvalidId;
    return kInvalidId;
  }

  DwrfStreamIdentifier()
      : column_(dwio::common::MAX_UINT32),
        kind_(StreamKind_DATA),
        format_(DwrfFormat::kDwrf) {}

  /* implicit */ DwrfStreamIdentifier(const proto::orc::Stream& stream)
      : DwrfStreamIdentifier(
            stream.has_column() ? stream.column() : 0,
            0,
            dwio::common::MAX_UINT32,
            stream.kind()) {}

  /* implicit */ DwrfStreamIdentifier(const proto::Stream& stream)
      : DwrfStreamIdentifier(
            stream.node(),
            stream.has_sequence() ? stream.sequence() : 0,
            stream.has_column() ? stream.column() : dwio::common::MAX_UINT32,
            stream.kind()) {}

  /// Pruned flat map keys are not enqueued thus all flatmap values on same
  /// column should have similar read percentage, so it is ok for them to
  /// share the same TrackingData.
  DwrfStreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      StreamKind kind,
      DwrfFormat format)
      : StreamIdentifier(
            velox::cache::TrackingId((node << kNodeShift) | kind).id()),
        column_{column},
        kind_(kind),
        encodingKey_{node, sequence},
        format_(format) {}

  DwrfStreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      proto::Stream_Kind pkind)
      : DwrfStreamIdentifier(
            node,
            sequence,
            column,
            toStreamKind(pkind),
            DwrfFormat::kDwrf) {}

  DwrfStreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      proto::orc::Stream_Kind pkind)
      : DwrfStreamIdentifier(
            node,
            sequence,
            column,
            toStreamKind(pkind),
            DwrfFormat::kOrc) {}

  DwrfStreamIdentifier(
      uint32_t node,
      uint32_t sequence,
      uint32_t column,
      StreamKind kind)
      : DwrfStreamIdentifier(node, sequence, column, kind, toDwrfFormat(kind)) {
  }

  ~DwrfStreamIdentifier() = default;

  bool operator==(const StreamIdentifier& other) const override {
    if (const auto* otherDwrf =
            dynamic_cast<const DwrfStreamIdentifier*>(&other)) {
      // column == other.column may be join the expression if all files
      // share the same new version that has column field filled.
      return encodingKey_ == otherDwrf->encodingKey_ &&
          kind_ == otherDwrf->kind_;
    }
    return false;
  }

  std::size_t hash() const override {
    return encodingKey_.hash() ^ std::hash<uint32_t>()(kind_);
  }

  uint32_t column() const {
    return column_;
  }

  const StreamKind& kind() const {
    return kind_;
  }

  const EncodingKey& encodingKey() const {
    return encodingKey_;
  }

  DwrfFormat format() const {
    return format_;
  }

  std::string toString() const override {
    return fmt::format(
        "[id={}, node={}, sequence={}, column={}, kind={}]",
        id_,
        encodingKey_.node(),
        encodingKey_.sequence(),
        column_,
        static_cast<uint32_t>(kind_));
  }

 private:
  static constexpr int32_t kNodeShift = 5;

  uint32_t column_;
  StreamKind kind_;
  EncodingKey encodingKey_;
  DwrfFormat format_;
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

enum RleVersion { RleVersion_1, RleVersion_2 };

constexpr int32_t RLE_MINIMUM_REPEAT = 3;
constexpr int32_t RLE_MAXIMUM_REPEAT = 127 + RLE_MINIMUM_REPEAT;
constexpr int32_t RLE_MAX_LITERAL_SIZE = 128;

} // namespace facebook::velox::dwrf

template <>
struct fmt::formatter<facebook::velox::dwrf::StreamKind>
    : fmt::formatter<std::string> {
  auto format(
      const facebook::velox::dwrf::StreamKind& kind,
      format_context& ctx) const {
    return formatter<std::string>::format(streamKindToString(kind), ctx);
  }
};
