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
/// one can first create a VectorSerializer using createSerializer(), then
/// append successive RowVectors using VectorSerializer::append(). In this case,
/// since different RowVector might encode columns differently, data is always
/// flattened in the serialized payload.
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
        common::CompressionKind _compressionKind)
        : useLosslessTimestamp(_useLosslessTimestamp),
          compressionKind(_compressionKind) {}

    /// Currently presto only supports millisecond precision and the serializer
    /// converts velox native timestamp to that resulting in loss of precision.
    /// This option allows it to serialize with nanosecond precision and is
    /// currently used for spilling. Is false by default.
    bool useLosslessTimestamp{false};
    common::CompressionKind compressionKind{
        common::CompressionKind::CompressionKind_NONE};

    /// Specifies the encoding for each of the top-level child vector.
    std::vector<VectorEncoding::Simple> encodings;
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

  std::unique_ptr<VectorSerializer> createSerializer(
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

  /// Serializes a single RowVector with possibly encoded children, preserving
  /// their encodings. Encodings are preserved recursively for any RowVector
  /// children, but not for children of other nested vectors such as Array, Map,
  /// and Dictionary.
  ///
  /// PrestoPage does not support serialization of Dictionaries with nulls;
  /// in case dictionaries contain null they are serialized as flat buffers.
  ///
  /// In order to override the encodings of top-level columns in the RowVector,
  /// you can specifiy the encodings using PrestoOptions.encodings
  ///
  /// DEPRECATED: Use createBatchSerializer and the BatchVectorSerializer's
  /// serialize function instead.
  void deprecatedSerializeEncoded(
      const RowVectorPtr& vector,
      StreamArena* streamArena,
      const Options* options,
      OutputStream* out);

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

  static void registerVectorSerde();
};

// Testing function for nested encodings. See comments in scatterStructNulls().
void testingScatterStructNulls(
    vector_size_t size,
    vector_size_t scatterSize,
    const vector_size_t* scatter,
    const uint64_t* incomingNulls,
    RowVector& row,
    vector_size_t rowOffset);

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
