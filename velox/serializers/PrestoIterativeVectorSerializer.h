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

#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/VectorStream.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto::detail {

class PrestoIterativeVectorSerializer : public IterativeVectorSerializer {
 public:
  PrestoIterativeVectorSerializer(
      const RowTypePtr& rowType,
      int32_t numRows,
      StreamArena* streamArena,
      const PrestoVectorSerde::PrestoOptions& opts);

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch) override;

  void append(
      const RowVectorPtr& vector,
      const folly::Range<const vector_size_t*>& rows,
      Scratch& scratch) override;

  size_t maxSerializedSize() const override;

  // The SerializedPage layout is:
  // numRows(4) | codec(1) | uncompressedSize(4) | compressedSize(4) |
  // checksum(8) | data
  void flush(OutputStream* out) override;

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  void clear() override;

 private:
  const PrestoVectorSerde::PrestoOptions opts_;
  StreamArena* const streamArena_;
  const std::unique_ptr<folly::compression::Codec> codec_;

  int32_t numRows_{0};
  std::vector<VectorStream, memory::StlAllocator<VectorStream>> streams_;

  // Count of forthcoming compressions to skip.
  int32_t numCompressionToSkip_{0};
  CompressionStats stats_;
};
} // namespace facebook::velox::serializer::presto::detail
