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

#include <gtest/gtest.h>
#include <velox/buffer/Buffer.h>
#include "velox/vector/ComplexVector.h"
#include "velox/vector/tests/utils/VectorMaker.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

class IsWritableVectorTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_ = memory::getDefaultScopedMemoryPool();
    vectorMaker_ = std::make_unique<VectorMaker>(pool_.get());
  }

  // We use templates here to avoid the compiler automatically creating new
  // shared_ptrs which it would do if we used VectorPtr.
  template <typename T, typename V>
  void testChildVector(
      const std::shared_ptr<T>& vector,
      const std::shared_ptr<V>& childVector) {
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_TRUE(BaseVector::isVectorWritable(vector));

    {
      // Make a copy of childVector so it's no longer writable.
      auto copy = childVector;
      ASSERT_TRUE(vector->isNullsWritable());
      ASSERT_FALSE(BaseVector::isVectorWritable(vector));
    }
  }

  // We use templates here to avoid the compiler automatically creating new
  // shared_ptrs which it would do if we used VectorPtr.
  template <typename T>
  void testBufferPtr(
      const std::shared_ptr<T>& vector,
      const BufferPtr& buffer) {
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_TRUE(BaseVector::isVectorWritable(vector));

    {
      // Make a copy of buffer so it's no longer mutable.
      auto copy = buffer;
      ASSERT_TRUE(vector->isNullsWritable());
      ASSERT_FALSE(BaseVector::isVectorWritable(vector));
    }

    // Set buffer to no longer be mutable.
    buffer->setIsMutable(false);
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_FALSE(BaseVector::isVectorWritable(vector));

    buffer->setIsMutable(true);

    // Make sure nothing gets left unwritable.
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_TRUE(BaseVector::isVectorWritable(vector));
  }

  // We use templates here to avoid the compiler automatically creating new
  // shared_ptrs which it would do if we used VectorPtr.
  template <typename T>
  void basicTest(const std::shared_ptr<T>& vector) {
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_TRUE(BaseVector::isVectorWritable(vector));

    {
      auto copy = vector;
      ASSERT_TRUE(vector->isNullsWritable());
      // The Vector is not uniquely referenced.
      ASSERT_FALSE(BaseVector::isVectorWritable(vector));
    }

    // Hack to ensure nulls are allocated.
    vector->mutableRawNulls();
    ASSERT_TRUE(vector->isNullsWritable());
    ASSERT_TRUE(BaseVector::isVectorWritable(vector));

    {
      // Make a copy of nulls so it's no longer mutable.
      auto nulls = vector->nulls();
      ASSERT_FALSE(vector->isNullsWritable());
      ASSERT_FALSE(BaseVector::isVectorWritable(vector));
    }
  }

  std::unique_ptr<memory::MemoryPool> pool_;
  std::unique_ptr<VectorMaker> vectorMaker_;
};

TEST_F(IsWritableVectorTest, flatVector) {
  auto flatVector = vectorMaker_->flatVector(std::vector<int32_t>{1, 2, 3});

  basicTest(flatVector);
  testBufferPtr(flatVector, flatVector->values());
}

TEST_F(IsWritableVectorTest, arrayVector) {
  auto arrayVector =
      vectorMaker_->arrayVector<int32_t>({{1, 2}, {3, 4}, {5, 6}});

  basicTest(arrayVector);
  testBufferPtr(arrayVector, arrayVector->offsets());
  testBufferPtr(arrayVector, arrayVector->sizes());
  testChildVector(arrayVector, arrayVector->elements());
}

TEST_F(IsWritableVectorTest, mapVector) {
  auto mapVector = vectorMaker_->mapVector(
      {0, 2, 4, 6},
      vectorMaker_->flatVector<int32_t>({1, 2, 3, 4, 5, 6}),
      vectorMaker_->flatVector<int32_t>({1, 2, 3, 4, 5, 6}));

  basicTest(mapVector);
  testBufferPtr(mapVector, mapVector->offsets());
  testBufferPtr(mapVector, mapVector->sizes());
  testChildVector(mapVector, mapVector->mapKeys());
  testChildVector(mapVector, mapVector->mapValues());
}

TEST_F(IsWritableVectorTest, rowVector) {
  auto rowVector = vectorMaker_->rowVector(
      {vectorMaker_->flatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       vectorMaker_->flatVector<int32_t>({1, 2, 3, 4, 5, 6}),
       vectorMaker_->flatVector<int32_t>({1, 2, 3, 4, 5, 6})});

  basicTest(rowVector);
  testChildVector(rowVector, rowVector->childAt(0));
  testChildVector(rowVector, rowVector->childAt(1));
  testChildVector(rowVector, rowVector->childAt(2));
}
