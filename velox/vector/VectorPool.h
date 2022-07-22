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

#include "velox/vector/FlatVector.h"

namespace facebook::velox {

/// A thread-level cache of pre-allocated flat vectors of different types.
/// Keeps up to 10 recyclable vectors of each of primitive types. A vector is
/// recyclable if it is flat and recursively singly-referenced.
class VectorPool {
 public:
  explicit VectorPool(memory::MemoryPool* pool) : pool_{pool} {}

  /// Gets a possibly recycled vector of 'type and 'size'. Allocates from
  /// 'pool_' if no pre-allocated vector or type is a complex type.
  VectorPtr get(const TypePtr& type, vector_size_t size);

  /// Moves vector into 'this' if it is flat, recursively singly referenced and
  /// there is space.
  bool release(VectorPtr& vector);

  size_t release(std::vector<VectorPtr>& vectors);

 private:
  static constexpr int32_t kNumCachedVectorTypes =
      static_cast<int32_t>(TypeKind::ARRAY);
  /// Max number of elements for a vector to be recyclable. The larger
  /// the batch the less the win from recycling.
  static constexpr vector_size_t kMaxRecycleSize = 64 * 1024;
  static constexpr int32_t kNumPerType = 10;

  struct TypePool {
    int32_t size{0};
    std::array<VectorPtr, kNumPerType> vectors;

    bool maybePushBack(VectorPtr& vector);

    VectorPtr pop(
        const TypePtr& type,
        vector_size_t vectorSize,
        memory::MemoryPool& pool);
  };

  memory::MemoryPool* const pool_;

  /// Caches of pre-allocated vectors indexed by typeKind.
  std::array<TypePool, kNumCachedVectorTypes> vectors_;
};

} // namespace facebook::velox
