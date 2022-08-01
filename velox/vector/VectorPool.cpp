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
#include "velox/vector/VectorPool.h"

namespace facebook::velox {

inline int32_t toCacheIndex(TypeKind kind) {
  return static_cast<int32_t>(kind);
}

VectorPtr VectorPool::get(const TypePtr& type, vector_size_t size) {
  auto cacheIndex = toCacheIndex(type->kind());
  if (cacheIndex < kNumCachedVectorTypes && size <= kMaxRecycleSize) {
    return vectors_[cacheIndex].pop(type, size, *pool_);
  }
  return BaseVector::create(type, size, pool_);
}

bool VectorPool::release(VectorPtr& vector) {
  if (FOLLY_UNLIKELY(vector == nullptr)) {
    return false;
  }
  if (!vector.unique() || vector->size() > kMaxRecycleSize) {
    return false;
  }
  auto cacheIndex = toCacheIndex(vector->typeKind());
  if (cacheIndex >= kNumCachedVectorTypes) {
    return false;
  }
  return vectors_[cacheIndex].maybePushBack(vector);
}

size_t VectorPool::release(std::vector<VectorPtr>& vectors) {
  size_t numReleased = 0;
  for (auto& vector : vectors) {
    if (FOLLY_LIKELY(vector != nullptr)) {
      if (release(vector)) {
        ++numReleased;
      }
    }
  }
  return numReleased;
}

bool VectorPool::TypePool::maybePushBack(VectorPtr& vector) {
  if (!vector->isRecyclable()) {
    return false;
  }
  if (size >= kNumPerType) {
    return false;
  }

  vector->prepareForReuse();
  vectors[size++] = std::move(vector);
  return true;
}

VectorPtr VectorPool::TypePool::pop(
    const TypePtr& type,
    vector_size_t vectorSize,
    memory::MemoryPool& pool) {
  if (size) {
    auto result = std::move(vectors[--size]);
    if (UNLIKELY(result->rawNulls() != nullptr)) {
      // This is a recyclable vector, no need to check uniqueness.
      simd::memset(
          const_cast<uint64_t*>(result->rawNulls()),
          bits::kNotNullByte,
          bits::roundUp(std::min<int32_t>(vectorSize, result->size()), 64) / 8);
    }
    if (UNLIKELY(
            result->typeKind() == TypeKind::VARCHAR ||
            result->typeKind() == TypeKind::VARBINARY)) {
      simd::memset(
          const_cast<void*>(result->valuesAsVoid()),
          0,
          std::min<int32_t>(vectorSize, result->size()) * sizeof(StringView));
    }
    if (result->size() != vectorSize) {
      result->resize(vectorSize);
    }
    return result;
  }
  return BaseVector::create(type, vectorSize, &pool);
}
} // namespace facebook::velox
