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
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto {
class PrestoVectorSerde : public VectorSerde {
 public:
  // Input options that the serializer recognizes.
  struct PrestoOptions : VectorSerde::Options {
    explicit PrestoOptions(bool useLosslessTimestamp)
        : useLosslessTimestamp(useLosslessTimestamp) {}
    // Currently presto only supports millisecond precision and the serializer
    // converts velox native timestamp to that resulting in loss of precision.
    // This option allows it to serialize with nanosecond precision and is
    // currently used for spilling. Is false by default.
    bool useLosslessTimestamp{false};
  };

  void estimateSerializedSize(
      std::shared_ptr<BaseVector> vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) override;

  std::unique_ptr<VectorSerializer> createSerializer(
      std::shared_ptr<const RowType> type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options) override;

  /// Serializes a RowVector with a constant children.
  void serializeConstants(
      const RowVectorPtr& vector,
      StreamArena* streamArena,
      const Options* options,
      OutputStream* out);

  void deserialize(
      ByteStream* source,
      velox::memory::MemoryPool* pool,
      std::shared_ptr<const RowType> type,
      std::shared_ptr<RowVector>* result,
      const Options* options) override;

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
