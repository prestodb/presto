/*
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
#include "velox/vector/FlatVector.h"

namespace facebook::velox::dwrf::flatmap_helper {

// Reset vector with the desired size/hasNulls properties
void reset(BaseVector& vector, vector_size_t size, bool hasNulls);

// Initialize flat vector
template <typename T>
void initializeFlatVector(
    VectorPtr& vector,
    memory::MemoryPool& pool,
    vector_size_t size,
    bool hasNulls,
    std::vector<BufferPtr>&& stringBuffers = {}) {
  if (vector) {
    auto& flatVector = dynamic_cast<FlatVector<T>&>(*vector);
    reset(flatVector, size, hasNulls);
    flatVector.stringBuffers() = stringBuffers;
  } else {
    vector = std::make_shared<FlatVector<T>>(
        &pool,
        hasNulls ? AlignedBuffer::allocate<bool>(size, &pool) : nullptr,
        0 /*length*/,
        AlignedBuffer::allocate<T>(size, &pool),
        std::move(stringBuffers));
    vector->setNullCount(0);
  }
}

// Initialize string vector.
void initializeStringVector(
    VectorPtr& vector,
    memory::MemoryPool& pool,
    vector_size_t size,
    bool hasNulls,
    std::vector<BufferPtr>&& stringBuffers);

// Initialize map vector.
void initializeMapVector(
    VectorPtr& vector,
    const std::shared_ptr<const Type>& type,
    memory::MemoryPool& pool,
    const std::vector<const BaseVector*>& vectors,
    std::optional<vector_size_t> sizeOverride = std::nullopt);

// Initialize vector with a list of vectors. Make sure the initialized vector
// has the capacity to hold all data from them.
void initializeVector(
    VectorPtr& vector,
    const std::shared_ptr<const Type>& type,
    memory::MemoryPool& pool,
    const std::vector<const BaseVector*>& vectors);

// Copy one value from source vector to target.
void copyOne(
    const std::shared_ptr<const Type>& type,
    BaseVector& target,
    vector_size_t targetIndex,
    const BaseVector& source,
    vector_size_t sourceIndex);

// Copy values from source vector to target.
void copy(
    const std::shared_ptr<const Type>& type,
    BaseVector& target,
    vector_size_t targetIndex,
    const BaseVector& source,
    vector_size_t sourceIndex,
    vector_size_t count);

} // namespace facebook::velox::dwrf::flatmap_helper
