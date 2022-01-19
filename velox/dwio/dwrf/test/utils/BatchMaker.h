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

#include <random>
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::test {

struct BatchMaker {
  static VectorPtr createBatch(
      const std::shared_ptr<const Type>& type,
      uint64_t capacity,
      memory::MemoryPool& memoryPool,
      std::mt19937& gen,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr);

  static VectorPtr createBatch(
      const std::shared_ptr<const Type>& type,
      uint64_t capacity,
      memory::MemoryPool& memoryPool,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr,
      std::mt19937::result_type seed = std::mt19937::default_seed);

  template <TypeKind KIND>
  static VectorPtr createVector(
      const std::shared_ptr<const Type>& type,
      size_t size,
      memory::MemoryPool& pool,
      std::mt19937& gen,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr);

  template <TypeKind KIND>
  static VectorPtr createVector(
      const std::shared_ptr<const Type>& type,
      size_t size,
      memory::MemoryPool& pool,
      std::function<bool(vector_size_t /*index*/)> isNullAt = nullptr,
      std::mt19937::result_type seed = std::mt19937::default_seed) {
    std::mt19937 gen{seed};
    return createVector<KIND>(type, size, pool, gen, isNullAt);
  }
};

} // namespace facebook::velox::test
