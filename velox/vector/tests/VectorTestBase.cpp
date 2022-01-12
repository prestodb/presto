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

#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::test {

// static
VectorPtr VectorTestBase::wrapInDictionary(
    BufferPtr indices,
    vector_size_t size,
    VectorPtr vector) {
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, size, vector);
}

BufferPtr VectorTestBase::makeOddIndices(vector_size_t size) {
  return makeIndices(size, [](vector_size_t i) { return 2 * i + 1; });
}

BufferPtr VectorTestBase::makeEvenIndices(vector_size_t size) {
  return makeIndices(size, [](vector_size_t i) { return 2 * i; });
}

// static
BufferPtr VectorTestBase::makeIndices(
    vector_size_t size,
    std::function<vector_size_t(vector_size_t)> indexAt) {
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool());
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < size; i++) {
    rawIndices[i] = indexAt(i);
  }

  return indices;
}

} // namespace facebook::velox::test
