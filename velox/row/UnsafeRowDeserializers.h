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

#include "velox/row/UnsafeRowFast.h"

namespace facebook::velox::row {
/// UnsafeRow dynamic deserializer using TypePtr, deserializes an UnsafeRow to
/// a Vector.
/// For backward compatibility, keep this API.
struct UnsafeRowDeserializer {
  /**
   * Deserializes a complex element type to its Vector representation.
   * @param data A string_view over a given element in the UnsafeRow.
   * @param type the element type.
   * @param pool the memory pool to allocate Vectors from
   *data to a array.
   * @return a VectorPtr
   */
  static VectorPtr deserializeOne(
      std::optional<std::string_view> data,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    std::vector<std::optional<std::string_view>> vectors{data};
    return deserialize(vectors, type, pool);
  }

  /**
   * Deserializes a complex element type to its Vector representation.
   * @param data A vector of string_view over a given element in the
   *UnsafeRow.
   * @param type the element type.
   * @param pool the memory pool to allocate Vectors from
   *data to a array.
   * @return a VectorPtr
   */
  static VectorPtr deserialize(
      const std::vector<std::optional<std::string_view>>& data,
      const TypePtr& type,
      memory::MemoryPool* pool) {
    std::vector<char*> dataPtrs;
    dataPtrs.reserve(data.size());
    for (const auto& row : data) {
      const char* ptr = row.value().data();
      dataPtrs.emplace_back(const_cast<char*>(ptr));
    }
    return UnsafeRowFast::deserialize(dataPtrs, asRowType(type), pool);
  }
};
} // namespace facebook::velox::row
