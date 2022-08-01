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

#include "gtest/gtest.h"

#include "velox/expression/EvalCtx.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

class EvalCtxTest : public testing::Test, public VectorTestBase {
 protected:
  core::ExecCtx execCtx_{pool_.get(), nullptr};
};

TEST_F(EvalCtxTest, selectivityVectors) {
  EvalCtx context(&execCtx_);
  SelectivityVector all100(100, true);
  SelectivityVector none100(100, false);

  // Not initialized, initially nullptr.
  LocalSelectivityVector local1(context);
  EXPECT_TRUE(!local1.get());

  // Specify initialization in get()
  EXPECT_EQ(all100, *local1.get(100, true));

  // The get() stays the same.
  EXPECT_EQ(all100, *local1.get());

  // Initialize from other in get().
  EXPECT_EQ(none100, *local1.get(none100));

  // Init from existing
  LocalSelectivityVector local2(context, all100);
  EXPECT_EQ(all100, *local2.get());
}

TEST_F(EvalCtxTest, vectorPool) {
  EvalCtx context(&execCtx_);

  auto vector = context.getVector(BIGINT(), 1'000);
  ASSERT_NE(vector, nullptr);
  ASSERT_EQ(vector->size(), 1'000);

  auto* vectorPtr = vector.get();
  ASSERT_TRUE(context.releaseVector(vector));
  ASSERT_EQ(vector, nullptr);

  auto recycledVector = context.getVector(BIGINT(), 2'000);
  ASSERT_NE(recycledVector, nullptr);
  ASSERT_EQ(recycledVector->size(), 2'000);
  ASSERT_EQ(recycledVector.get(), vectorPtr);

  ASSERT_TRUE(context.releaseVector(recycledVector));
  ASSERT_EQ(recycledVector, nullptr);

  VectorPtr anotherVector;
  SelectivityVector rows(512);
  context.ensureWritable(rows, BIGINT(), anotherVector);
  ASSERT_NE(anotherVector, nullptr);
  ASSERT_EQ(anotherVector->size(), 512);
  ASSERT_EQ(anotherVector.get(), vectorPtr);
}

TEST_F(EvalCtxTest, ScopedVectorPtr) {
  EvalCtx context(&execCtx_);
  auto vector = context.getVector(BIGINT(), 1'00);
  auto* vectorPtr = vector.get();
  {
    ScopedVectorPtr tempVectorPtr(context);
    tempVectorPtr.ptr() = std::move(vector);
  }
  vector = context.getVector(BIGINT(), 1'00);
  ASSERT_EQ(vector.get(), vectorPtr);
  { ScopedVectorPtr emptyVectorPtr(context); }

  // Hold the allocated vector on scoped vector destruction.
  {
    ScopedVectorPtr tempVectorPtr(context);
    tempVectorPtr.ptr() = vector;
  }
  vector = context.getVector(BIGINT(), 1'00);
  ASSERT_NE(vectorPtr, vector.get());
}
