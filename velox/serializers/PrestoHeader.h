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

#include "velox/common/base/Status.h"
#include "velox/common/memory/ByteStream.h"

namespace facebook::velox::serializer::presto::detail {

struct PrestoHeader {
  int32_t numRows;
  int8_t pageCodecMarker;
  int32_t uncompressedSize;
  int32_t compressedSize;
  int64_t checksum;

  static Expected<PrestoHeader> read(ByteInputStream* source);

  static std::optional<PrestoHeader> read(std::string_view* source);

  template <typename T>
  static T readInt(std::string_view* source) {
    assert(source->size() >= sizeof(T));
    auto value = folly::loadUnaligned<T>(source->data());
    source->remove_prefix(sizeof(T));
    return value;
  }
};
} // namespace facebook::velox::serializer::presto::detail
