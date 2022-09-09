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
#include "velox/vector/ComplexVector.h"
#include "velox/vector/VectorStream.h"

namespace facebook::velox::serializer::spark {

class UnsafeRowVectorSerde : public VectorSerde {
 public:
  // We do not implement this method since it is not used in production code.
  void estimateSerializedSize(
      VectorPtr vector,
      const folly::Range<const IndexRange*>& ranges,
      vector_size_t** sizes) override;

  // This method is not used in production code. It is only used to
  // support round-trip tests for deserialization.
  std::unique_ptr<VectorSerializer> createSerializer(
      RowTypePtr type,
      int32_t numRows,
      StreamArena* streamArena,
      const Options* options) override;

  // This method is used when reading data from the exchange.
  void deserialize(
      ByteStream* source,
      velox::memory::MemoryPool* pool,
      RowTypePtr type,
      RowVectorPtr* result,
      const Options* options) override;

  static void registerVectorSerde();
};
} // namespace facebook::velox::serializer::spark
