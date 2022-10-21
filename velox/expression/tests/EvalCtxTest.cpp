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

#include <exception>
#include "gtest/gtest.h"

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/expression/EvalCtx.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

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

TEST_F(EvalCtxTest, vectorRecycler) {
  EvalCtx context(&execCtx_);
  VectorPtr vector;
  BaseVector* vectorPtr;
  {
    VectorRecycler vectorRecycler(vector, context.vectorPool());
    vector = context.getVector(BIGINT(), 1'00);
    vectorPtr = vector.get();
  }
  auto newVector = context.getVector(BIGINT(), 1'00);
  ASSERT_EQ(newVector.get(), vectorPtr);
  vector.reset();
  { VectorRecycler vectorRecycler(vector, context.vectorPool()); }

  // Hold the allocated vector on scoped vector destruction.
  vector = context.getVector(BIGINT(), 1'00);
  ASSERT_NE(vector.get(), newVector.get());
  newVector = vector;
  { VectorRecycler vectorRecycler(vector, context.vectorPool()); }
  vector = context.getVector(BIGINT(), 1'00);
  ASSERT_NE(vector.get(), newVector.get());
}

TEST_F(EvalCtxTest, ensureErrorsVectorSize) {
  EvalCtx context(&execCtx_);
  context.ensureErrorsVectorSize(*context.errorsPtr(), 10);
  ASSERT_GE(context.errors()->size(), 10);
  ASSERT_EQ(BaseVector::countNulls(context.errors()->nulls(), 10), 10);

  std::exception exception;
  *context.mutableThrowOnError() = false;
  context.setError(0, std::make_exception_ptr(exception));

  ASSERT_EQ(BaseVector::countNulls(context.errors()->nulls(), 10), 9);

  context.ensureErrorsVectorSize(*context.errorsPtr(), 20);

  ASSERT_GE(context.errors()->size(), 20);
  ASSERT_EQ(BaseVector::countNulls(context.errors()->nulls(), 20), 19);
}

TEST_F(EvalCtxTest, setErrors) {
  EvalCtx context(&execCtx_);
  *context.mutableThrowOnError() = false;

  ASSERT_TRUE(context.errors() == nullptr);

  SelectivityVector rows(5);
  context.setErrors(
      rows, std::make_exception_ptr(std::invalid_argument("This is a test.")));

  auto errors = context.errors();
  ASSERT_TRUE(errors != nullptr);
  ASSERT_EQ(errors->size(), rows.size());
  std::exception_ptr firstEx;
  for (auto i = 0; i < rows.size(); ++i) {
    auto ex = std::static_pointer_cast<std::exception_ptr>(errors->valueAt(i));
    VELOX_ASSERT_THROW(std::rethrow_exception(*ex), "This is a test.");

    // Verify that a single exception is re-used for all rows vs. each row
    // storing a copy.
    if (i == 0) {
      firstEx = *ex;
    } else {
      ASSERT_EQ(*ex, firstEx);
    }
  }
}

TEST_F(EvalCtxTest, addErrorsPreserveOldErrors) {
  // Add two invalid_argument to context.errors().
  EvalCtx context(&execCtx_);
  std::invalid_argument argumentError{"invalid argument"};
  *context.mutableThrowOnError() = false;
  context.addError(
      0, std::make_exception_ptr(argumentError), *context.errorsPtr());
  context.addError(
      3, std::make_exception_ptr(argumentError), *context.errorsPtr());
  ASSERT_EQ(BaseVector::countNulls(context.errors()->nulls(), 4), 2);

  // Add two out_of_range to anotherErrors.
  EvalCtx::ErrorVectorPtr anotherErrors;
  std::out_of_range rangeError{"out of range"};
  context.addError(0, std::make_exception_ptr(rangeError), anotherErrors);
  context.addError(4, std::make_exception_ptr(rangeError), anotherErrors);
  ASSERT_EQ(BaseVector::countNulls(anotherErrors->nulls(), 5), 3);

  // Add errors in anotherErrors to context.errors() and check that the original
  // error at index 0 is preserved.
  SelectivityVector rows{5};
  context.addErrors(rows, anotherErrors, *context.errorsPtr());
  ASSERT_EQ(context.errors()->size(), 5);
  ASSERT_EQ(BaseVector::countNulls(context.errors()->nulls(), 5), 2);

  auto checkErrors = [&](vector_size_t index, const char* message) {
    ASSERT_GT(context.errors()->size(), index);
    try {
      std::static_pointer_cast<std::exception_ptr>(
          context.errors()->valueAt(index));
    } catch (const std::exception& e) {
      ASSERT_NE(std::strstr(e.what(), message), nullptr);
    }
  };

  checkErrors(0, "invalid argument");
  checkErrors(3, "invalid argument");
  checkErrors(4, "out of range");
  ASSERT_TRUE(context.errors()->isNullAt(1));
  ASSERT_TRUE(context.errors()->isNullAt(2));
}
