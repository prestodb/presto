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

#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::test {

BufferPtr makeIndicesInReverse(vector_size_t size, memory::MemoryPool* pool) {
  auto indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();
  for (auto i = 0; i < size; i++) {
    rawIndices[i] = size - 1 - i;
  }
  return indices;
}

BufferPtr makeIndices(
    vector_size_t size,
    std::function<vector_size_t(vector_size_t)> indexAt,
    memory::MemoryPool* pool) {
  BufferPtr indices = AlignedBuffer::allocate<vector_size_t>(size, pool);
  auto rawIndices = indices->asMutable<vector_size_t>();

  for (vector_size_t i = 0; i < size; i++) {
    rawIndices[i] = indexAt(i);
  }

  return indices;
}

VectorTestBase::~VectorTestBase() {
  // Reset the executor to wait for all the async activities to finish.
  executor_.reset();
}

// static
VectorPtr VectorTestBase::wrapInDictionary(
    BufferPtr indices,
    vector_size_t size,
    VectorPtr vector) {
  return BaseVector::wrapInDictionary(
      BufferPtr(nullptr), indices, size, vector);
}

// static
VectorPtr VectorTestBase::wrapInDictionary(
    BufferPtr indices,
    VectorPtr vector) {
  return wrapInDictionary(
      indices, indices->size() / sizeof(vector_size_t), vector);
}

BufferPtr VectorTestBase::makeOddIndices(vector_size_t size) {
  return makeIndices(size, [](vector_size_t i) { return 2 * i + 1; });
}

BufferPtr VectorTestBase::makeEvenIndices(vector_size_t size) {
  return makeIndices(size, [](vector_size_t i) { return 2 * i; });
}

BufferPtr VectorTestBase::makeIndices(
    vector_size_t size,
    std::function<vector_size_t(vector_size_t)> indexAt) const {
  return test::makeIndices(size, indexAt, pool());
}

BufferPtr VectorTestBase::makeIndices(
    const std::vector<vector_size_t>& indices) const {
  auto size = indices.size();
  BufferPtr indicesBuffer =
      AlignedBuffer::allocate<vector_size_t>(size, pool());
  auto rawIndices = indicesBuffer->asMutable<vector_size_t>();

  for (int i = 0; i < size; i++) {
    rawIndices[i] = indices[i];
  }
  return indicesBuffer;
}

BufferPtr VectorTestBase::makeNulls(
    vector_size_t size,
    std::function<bool(vector_size_t /*row*/)> isNullAt) {
  auto nulls = AlignedBuffer::allocate<bool>(size, pool());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < size; i++) {
    bits::setNull(rawNulls, i, isNullAt(i));
  }
  return nulls;
}

BufferPtr VectorTestBase::makeNulls(const std::vector<bool>& values) {
  auto nulls = allocateNulls(values.size(), pool());
  auto rawNulls = nulls->asMutable<uint64_t>();
  for (auto i = 0; i < values.size(); i++) {
    bits::setNull(rawNulls, i, values[i]);
  }
  return nulls;
}

std::vector<RowVectorPtr> VectorTestBase::split(
    const RowVectorPtr& vector,
    int32_t n) {
  const auto numRows = vector->size();
  VELOX_CHECK_GE(numRows, n);

  const auto numRowsPerVector = numRows / n;
  std::vector<RowVectorPtr> vectors;

  vector_size_t offset = 0;
  for (auto i = 0; i < n - 1; ++i) {
    vectors.push_back(std::dynamic_pointer_cast<RowVector>(
        vector->slice(offset, numRowsPerVector)));
    offset += numRowsPerVector;
  }

  vectors.push_back(std::dynamic_pointer_cast<RowVector>(
      vector->slice(offset, numRows - offset)));
  return vectors;
}

VectorPtr VectorTestBase::asArray(VectorPtr elements) {
  auto* pool = elements->pool();
  auto arrayType = ARRAY(elements->type());

  BufferPtr sizes =
      AlignedBuffer::allocate<vector_size_t>(1, pool, elements->size());
  BufferPtr offsets = allocateOffsets(1, pool);
  return std::make_shared<ArrayVector>(
      pool, arrayType, nullptr, 1, offsets, sizes, std::move(elements));
}

void assertEqualVectors(const VectorPtr& expected, const VectorPtr& actual) {
  ASSERT_EQ(expected->size(), actual->size());
  ASSERT_TRUE(expected->type()->equivalent(*actual->type()))
      << "Expected " << expected->type()->toString() << ", but got "
      << actual->type()->toString();
  for (auto i = 0; i < expected->size(); i++) {
    ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
        << "at " << i << ": expected " << expected->toString(i) << ", but got "
        << actual->toString(i);
  }
}

void assertEqualVectors(
    const VectorPtr& expected,
    const VectorPtr& actual,
    const SelectivityVector& rowsToCompare) {
  ASSERT_LE(rowsToCompare.end(), actual->size())
      << "Vectors should at least have the required amount of rows that need "
         "to be verified.";
  ASSERT_TRUE(expected->type()->equivalent(*actual->type()))
      << "Expected " << expected->type()->toString() << ", but got "
      << actual->type()->toString();
  rowsToCompare.applyToSelected([&](vector_size_t i) {
    ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
        << "at " << i << ": expected " << expected->toString(i) << ", but got "
        << actual->toString(i);
  });
}

void assertCopyableVector(const VectorPtr& vector) {
  auto copy =
      BaseVector::create(vector->type(), vector->size(), vector->pool());
  copy->copy(vector.get(), 0, 0, vector->size());
}

} // namespace facebook::velox::test
