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

#include "velox/common/memory/ByteStream.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/PrestoSerializerSerializationUtils.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::serializer::presto::detail {
inline bool isCompressedBitSet(int8_t codec) {
  return (codec & kCompressedBitMask) == kCompressedBitMask;
}

inline bool isEncryptedBitSet(int8_t codec) {
  return (codec & kEncryptedBitMask) == kEncryptedBitMask;
}

inline bool isChecksumBitSet(int8_t codec) {
  return (codec & kCheckSumBitMask) == kCheckSumBitMask;
}

void readTopColumns(
    ByteInputStream& source,
    const RowTypePtr& type,
    velox::memory::MemoryPool* pool,
    const RowVectorPtr& result,
    int32_t resultOffset,
    const PrestoVectorSerde::PrestoOptions& opts,
    bool singleColumn = false);
} // namespace facebook::velox::serializer::presto::detail
