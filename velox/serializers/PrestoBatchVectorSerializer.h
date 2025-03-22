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

#include <folly/ThreadLocal.h>
#include "velox/serializers/PrestoSerializer.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::presto::detail {
class PrestoBatchVectorSerializer : public BatchVectorSerializer {
 public:
  PrestoBatchVectorSerializer(
      memory::MemoryPool* pool,
      const PrestoVectorSerde::PrestoOptions& opts)
      : pool_(pool),
        codec_(common::compressionKindToCodec(opts.compressionKind)),
        opts_(opts) {}

  void serialize(
      const RowVectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      Scratch& scratch,
      OutputStream* stream) override;

  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch) override {
    estimateSerializedSizeImpl(vector, ranges, sizes, scratch);
  }

 private:
  void estimateSerializedSizeImpl(
      const VectorPtr& vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes,
      Scratch& scratch);

  memory::MemoryPool* const pool_;
  const std::unique_ptr<folly::compression::Codec> codec_;
  const PrestoVectorSerde::PrestoOptions opts_;
  // Used to protect against concurrent calls to serailize which can lead to
  // concurrency bugs.
  std::atomic_bool inUse{false};
};
} // namespace facebook::velox::serializer::presto::detail
