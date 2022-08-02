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
#include <gtest/gtest.h>
#include "velox/vector/tests/VectorTestBase.h"

namespace facebook::velox::test {

class VectorPoolTest : public testing::Test, public test::VectorTestBase {};

TEST_F(VectorPoolTest, basic) {
  VectorPool vectorPool(pool());

  // Get new vector from the pool.
  auto vector = vectorPool.get(BIGINT(), 1'000);
  ASSERT_NE(vector, nullptr);
  ASSERT_EQ(1'000, vector->size());

  // Return the vector to the pool and fetch it back.
  auto* vectorPtr = vector.get();
  ASSERT_TRUE(vectorPool.release(vector));
  ASSERT_EQ(vector, nullptr);

  auto recycledVector = vectorPool.get(BIGINT(), 1'000);
  ASSERT_NE(recycledVector, nullptr);
  ASSERT_EQ(vectorPtr, recycledVector.get());
  ASSERT_EQ(1'000, recycledVector->size());

  // Get another vector from the pool.
  auto anotherVector = vectorPool.get(BIGINT(), 1'000);
  ASSERT_NE(anotherVector, nullptr);
  ASSERT_NE(anotherVector.get(), recycledVector.get());
  ASSERT_EQ(1'000, anotherVector->size());

  // Return the vector to the pool and fetch it back using a larger size.
  auto* anotherVectorPtr = anotherVector.get();
  ASSERT_TRUE(vectorPool.release(anotherVector));
  ASSERT_EQ(anotherVector, nullptr);

  auto anotherRecycledVector = vectorPool.get(BIGINT(), 2'000);
  ASSERT_NE(anotherRecycledVector, nullptr);
  ASSERT_EQ(anotherRecycledVector.get(), anotherVectorPtr);
  ASSERT_EQ(2'000, anotherRecycledVector->size());

  // Verify that multiply-referenced vector cannot be returned to the pool.
  auto copy = anotherRecycledVector;
  ASSERT_FALSE(vectorPool.release(anotherRecycledVector));

  VectorPtr nullVector = nullptr;
  ASSERT_FALSE(vectorPool.release(nullVector));
}

TEST_F(VectorPoolTest, limit) {
  VectorPool vectorPool(pool());

  // Fill up vector pool and verify that vectors are not added above the limit.
  std::vector<VectorPtr> vectors(15);
  std::vector<BaseVector*> vectorPtrs(15);
  for (auto i = 0; i < 15; ++i) {
    vectors[i] = vectorPool.get(BIGINT(), 1'000);
    ASSERT_NE(vectors[i], nullptr);
    vectorPtrs[i] = vectors[i].get();
  }

  // Verify that we can release only 'limit' number of vectors.
  for (auto i = 0; i < 10; ++i) {
    ASSERT_TRUE(vectorPool.release(vectors[i]));
    ASSERT_EQ(vectors[i], nullptr);
  }

  for (auto i = 10; i < 15; ++i) {
    ASSERT_FALSE(vectorPool.release(vectors[i]));
    ASSERT_NE(vectors[i], nullptr);
  }

  // Fetch recycled vectors from the pool.
  for (auto i = 9; i >= 0; --i) {
    vectors[i] = vectorPool.get(BIGINT(), 1'000);
    ASSERT_NE(vectors[i], nullptr);
    ASSERT_EQ(vectors[i].get(), vectorPtrs[i]);
  }

  // Return all vectors to the pool. Verify that only 'limit' number of vectors
  // can be returned.
  ASSERT_EQ(vectorPool.release(vectors), 10);
}

TEST_F(VectorPoolTest, ScopedVectorPtr) {
  VectorPool vectorPool(pool());

  // Empty scoped vector does nothing.
  VectorPtr vectorPtr;
  { VectorRecycler vectorRecycler(vectorPtr, vectorPool); }

  // Get new vector from the pool and release it back.
  BaseVector* rawPtr;
  {
    VectorRecycler vectorRecycler(vectorPtr, vectorPool);
    vectorPtr = vectorPool.get(BIGINT(), 1'000);
    rawPtr = vectorPtr.get();
  }

  // Get new vector from the pool and hold it on scoped vector destruction.
  VectorPtr vectorHolder;
  {
    VectorRecycler vectorRecycler(vectorPtr, vectorPool);
    vectorPtr = vectorPool.get(BIGINT(), 1'000);
    ASSERT_EQ(rawPtr, vectorPtr.get());
    vectorHolder = vectorPtr;
  }
  ASSERT_NE(vectorHolder, nullptr);

  {
    VectorRecycler vectorRecycler(vectorPtr, vectorPool);
    vectorPtr = vectorPool.get(BIGINT(), 1'000);
    ASSERT_NE(rawPtr, vectorPtr.get());
  }
}
} // namespace facebook::velox::test
