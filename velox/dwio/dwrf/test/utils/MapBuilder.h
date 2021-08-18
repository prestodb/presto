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

#include "velox/type/Type.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::test {

template <typename TKEY, typename TVALUE>
class MapBuilder {
 public:
  using pair = std::pair<TKEY, std::optional<TVALUE>>;
  using row = std::vector<pair>;
  using rows = std::vector<std::optional<row>>;

  static VectorPtr create(memory::MemoryPool& pool, rows rows) {
    const std::shared_ptr<const Type> type =
        CppToType<Map<TKEY, TVALUE>>::create();
    auto items = 0;
    for (auto& row : rows) {
      if (row.has_value()) {
        items += row->size();
      }
    }

    BufferPtr nulls =
        AlignedBuffer::allocate<char>(bits::nbytes(rows.size()), &pool);
    auto* nullsPtr = nulls->asMutable<uint64_t>();
    size_t nullCount = 0;

    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), &pool);
    auto* offsetsPtr = offsets->asMutable<vector_size_t>();

    BufferPtr lengths =
        AlignedBuffer::allocate<vector_size_t>(rows.size(), &pool);
    auto* lengthsPtr = lengths->asMutable<vector_size_t>();

    BufferPtr keys = AlignedBuffer::allocate<TKEY>(items, &pool);
    auto* keysPtr = keys->asMutable<TKEY>();

    BufferPtr values = AlignedBuffer::allocate<TVALUE>(items, &pool);
    auto* valuesPtr = values->asMutable<TVALUE>();

    BufferPtr valueNulls =
        AlignedBuffer::allocate<char>(bits::nbytes(items), &pool);
    auto* valueNullsPtr = valueNulls->asMutable<uint64_t>();
    size_t valueNullCount = 0;

    auto i = 0;
    auto offset = 0;

    for (auto& row : rows) {
      offsetsPtr[i] = offset;

      if (row.has_value()) {
        bits::clearNull(nullsPtr, i);
        lengthsPtr[i] = (*row).size();

        for (auto& pair : *row) {
          keysPtr[offset] = pair.first;
          if (pair.second.has_value()) {
            valuesPtr[offset] = *pair.second;
            bits::clearNull(valueNullsPtr, offset);
          } else {
            valueNullCount++;
            bits::setNull(valueNullsPtr, offset);
          }
          ++offset;
        }
      } else {
        nullCount++;
        bits::setNull(nullsPtr, i);
        lengthsPtr[i] = 0;
      }

      ++i;
    }

    offsetsPtr[i] = offset;

    return std::make_shared<MapVector>(
        &pool,
        type,
        nulls,
        rows.size(),
        offsets,
        lengths,
        std::make_shared<FlatVector<TKEY>>(
            &pool,
            BufferPtr(nullptr),
            items /*length*/,
            keys,
            std::vector<BufferPtr>()),
        std::make_shared<FlatVector<TVALUE>>(
            &pool,
            valueNulls,
            items /*length*/,
            values,
            std::vector<BufferPtr>()),
        nullCount);
  };
};

} // namespace facebook::velox::test
