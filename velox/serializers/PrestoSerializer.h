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

#include "velox/common/base/Crc.h"
#include "velox/common/compression/Compression.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto {

/// There are two ways to serialize data using PrestoVectorSerde:
///
/// 1. In order to append multiple RowVectors into the same serialized payload,
/// one can first create an IterativeVectorSerializer using
/// createIterativeSerializer(), then append successive RowVectors using
/// IterativeVectorSerializer::append(). In this case, since different RowVector
/// might encode columns differently, data is always flattened in the serialized
/// payload.
///
/// Note that there are two flavors of append(), one that takes a range of rows,
/// and one that takes a list of row ids. The former is useful when serializing
/// large sections of the input vector (or the full vector); the latter is
/// efficient for a selective subset, e.g. when splitting a vector to a large
/// number of output shuffle destinations.
///
/// 2. To serialize a single RowVector, one can use the BatchVectorSerializer
/// returned by createBatchSerializer(). Since it serializes a single RowVector,
/// it tries to preserve the encodings of the input data.
class PrestoVectorSerde : public VectorSerde {
 public:
  // Input options that the serializer recognizes.
  struct PrestoOptions : VectorSerde::Options {
    PrestoOptions() = default;

    PrestoOptions(
        bool _useLosslessTimestamp,
        common::CompressionKind _compressionKind,
        bool _nullsFirst = false)
        : useLosslessTimestamp(_useLosslessTimestamp),
          compressionKind(_compressionKind),
          nullsFirst(_nullsFirst) {}

    /// Currently presto only supports millisecond precision and the serializer
    /// converts velox native timestamp to that resulting in loss of precision.
    /// This option allows it to serialize with nanosecond precision and is
    /// currently used for spilling. Is false by default.
    bool useLosslessTimestamp{false};

    common::CompressionKind compressionKind{
        common::CompressionKind::CompressionKind_NONE};

    /// Serializes nulls of structs before the columns. Used to allow
    /// single pass reading of in spilling.
    ///
    /// TODO: Make Presto also serialize nulls before columns of
    /// structs.
    bool nullsFirst{false};
  };

  /// Adds the serialized sizes of the rows of 'vector' in 'ranges[i]' to
  /// '*sizes[i]'.
  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) override;

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const vector_size_t*> rows,
      vector_size_t** sizes,
      Scratch& scratch) override;

  std::unique_ptr<IterativeVectorSerializer> createIterativeSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options) override;

  /// Note that in addition to the differences highlighted in the VectorSerde
  /// interface, BatchVectorSerializer returned by this function can maintain
  /// the encodings of the input vectors recursively.
  std::unique_ptr<BatchVectorSerializer> createBatchSerializer(
      memory::MemoryPool* pool,
      const Options* options) override;

  bool supportsAppendInDeserialize() const override {
    return true;
  }

  void deserialize(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* options) override {
    return deserialize(source, pool, type, result, 0, options);
  }

  void deserialize(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      vector_size_t resultOffset,
      const Options* options) override;

  /// This function is used to deserialize a single column that is serialized in
  /// PrestoPage format. It is important to note that the PrestoPage format used
  /// here does not include the Presto page header. Therefore, the 'source'
  /// should contain uncompressed, serialized binary data, beginning at the
  /// column header.
  void deserializeSingleColumn(
      ByteInputStream* source,
      velox::memory::MemoryPool* pool,
      TypePtr type,
      VectorPtr* result,
      const Options* options);

  static void registerVectorSerde();
};

class PrestoOutputStreamListener : public OutputStreamListener {
 public:
  void onWrite(const char* s, std::streamsize count) override {
    if (not paused_) {
      crc_.process_bytes(s, count);
    }
  }

  void pause() {
    paused_ = true;
  }

  void resume() {
    paused_ = false;
  }

  auto crc() const {
    return crc_;
  }

  void reset() {
    crc_.reset();
  }

 private:
  bool paused_{false};
  bits::Crc32 crc_;
};
} // namespace facebook::velox::serializer::presto
