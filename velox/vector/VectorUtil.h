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

#include "velox/buffer/Buffer.h"
#include "velox/common/base/Exceptions.h"
#include "velox/type/Type.h"

namespace facebook::velox {

template <typename T>
static inline BufferPtr copyToBuffer(
    const T& values,
    velox::memory::MemoryPool* pool,
    std::optional<int32_t> size = std::nullopt,
    bool returnsNullptr = false) {
  using Value = typename T::value_type;
  VELOX_CHECK(pool, "pool must be non-null");
  VELOX_CHECK(
      !size.has_value() || size.value() <= values.size(),
      "size out of bound: {}",
      size.value());
  int32_t numValues = size.has_value() ? size.value() : values.size();
  if (numValues != 0) {
    BufferPtr buffer = AlignedBuffer::allocate<Value>(numValues, pool);
    auto data = buffer->asMutable<Value>();
    memcpy(data, &values[0], buffer->size());
    return buffer;
  }
  return returnsNullptr ? nullptr : AlignedBuffer::allocate<Value>(0, pool);
}

} // namespace facebook::velox
