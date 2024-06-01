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

namespace facebook::velox::exec {
namespace {

class EvalErrorsTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }
};

TEST_F(EvalErrorsTest, noErrors) {
  EvalErrors errors(pool(), 10);

  ASSERT_EQ(errors.size(), 10);
  ASSERT_EQ(errors.countErrors(), 0);
  ASSERT_FALSE(errors.hasError());

  for (auto i = 0; i < errors.size(); ++i) {
    ASSERT_FALSE(errors.hasErrorAt(i));
    ASSERT_NO_THROW(errors.throwIfErrorAt(i));
    ASSERT_FALSE(errors.errorAt(i).has_value());
    ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
  }

  // Test 'out-of-bounds' indices.
  for (auto i = errors.size(); i < errors.size() + 5; ++i) {
    ASSERT_FALSE(errors.hasErrorAt(i));
    ASSERT_NO_THROW(errors.throwIfErrorAt(i));
    ASSERT_FALSE(errors.errorAt(i).has_value());
  }

  SelectivityVector rows(123);
  ASSERT_NO_THROW(errors.throwFirstError(rows));
}

TEST_F(EvalErrorsTest, someErrors) {
  EvalErrors errors(pool(), 10);

  errors.setError(3);

  ASSERT_EQ(errors.size(), 10);
  ASSERT_EQ(errors.countErrors(), 1);
  ASSERT_TRUE(errors.hasError());
  ASSERT_TRUE(errors.hasErrorAt(3));

  for (auto i = 0; i < errors.size(); ++i) {
    if (i != 3) {
      ASSERT_FALSE(errors.hasErrorAt(i));
      ASSERT_NO_THROW(errors.throwIfErrorAt(i));
      ASSERT_FALSE(errors.errorAt(i).has_value());
      ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
    }
  }

  // Test 'out-of-bounds' index.
  errors.setError(23);

  ASSERT_EQ(errors.size(), 24);
  ASSERT_EQ(errors.countErrors(), 2);
  ASSERT_TRUE(errors.hasError());
  ASSERT_TRUE(errors.hasErrorAt(3));
  ASSERT_TRUE(errors.hasErrorAt(23));

  for (auto i = 0; i < errors.size(); ++i) {
    if (i != 3 && i != 23) {
      ASSERT_FALSE(errors.hasErrorAt(i));
      ASSERT_NO_THROW(errors.throwIfErrorAt(i));
      ASSERT_FALSE(errors.errorAt(i).has_value());
      ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
    }
  }

  errors.clearError(3);

  ASSERT_EQ(errors.size(), 24);
  ASSERT_EQ(errors.countErrors(), 1);
  ASSERT_TRUE(errors.hasError());
  ASSERT_TRUE(errors.hasErrorAt(23));

  for (auto i = 0; i < errors.size(); ++i) {
    if (i != 23) {
      ASSERT_FALSE(errors.hasErrorAt(i));
      ASSERT_NO_THROW(errors.throwIfErrorAt(i));
      ASSERT_FALSE(errors.errorAt(i).has_value());
      ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
    }
  }

  errors.clearError(23);

  ASSERT_EQ(errors.size(), 24);
  ASSERT_EQ(errors.countErrors(), 0);
  ASSERT_FALSE(errors.hasError());

  for (auto i = 0; i < errors.size(); ++i) {
    ASSERT_FALSE(errors.hasErrorAt(i));
    ASSERT_NO_THROW(errors.throwIfErrorAt(i));
    ASSERT_FALSE(errors.errorAt(i).has_value());
    ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
  }
}

std::exception_ptr makeVeloxException(const std::string& errorMessage) {
  try {
    VELOX_USER_FAIL(errorMessage);
    return nullptr;
  } catch (...) {
    return std::current_exception();
  }
}

TEST_F(EvalErrorsTest, errorDetails) {
  EvalErrors errors(pool(), 10);

  errors.setError(0, makeVeloxException("Test error X"));
  errors.setError(3, makeVeloxException("Test error Y"));
  errors.setError(23, makeVeloxException("Test error Z"));

  ASSERT_EQ(errors.size(), 24);
  ASSERT_EQ(errors.countErrors(), 3);
  ASSERT_TRUE(errors.hasError());

  ASSERT_TRUE(errors.hasErrorAt(0));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(0), "Test error X");

  ASSERT_TRUE(errors.hasErrorAt(3));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(3), "Test error Y");

  ASSERT_TRUE(errors.hasErrorAt(23));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(23), "Test error Z");

  SelectivityVector rows(123);
  VELOX_ASSERT_THROW(errors.throwFirstError(rows), "Test error X");

  rows.setValid(0, false);
  rows.updateBounds();
  VELOX_ASSERT_THROW(errors.throwFirstError(rows), "Test error Y");

  for (auto i = 0; i < errors.size(); ++i) {
    if (i != 0 && i != 3 && i != 23) {
      ASSERT_FALSE(errors.hasErrorAt(i));
      ASSERT_NO_THROW(errors.throwIfErrorAt(i));
      ASSERT_FALSE(errors.errorAt(i).has_value());
      ASSERT_FALSE(bits::isBitSet(errors.errorFlags(), i));
    }
  }
}

TEST_F(EvalErrorsTest, copyError) {
  EvalErrors other(pool(), 10);
  for (auto i = 0; i < 20; i += 3) {
    other.setError(i);
  }

  ASSERT_EQ(other.countErrors(), 7);

  // Copy errors from 3 rows. Only one of these has an error.
  EvalErrors errors(pool(), 10);
  errors.copyError(other, 10, 0);
  errors.copyError(other, 11, 1);
  errors.copyError(other, 12, 2);

  ASSERT_EQ(errors.size(), 10);
  ASSERT_EQ(errors.countErrors(), 1);
  ASSERT_TRUE(errors.hasError());
  ASSERT_TRUE(errors.hasErrorAt(2));

  // Copy errors from all rows. Shift destination rows by 5.
  for (auto i = 0; i < other.size(); ++i) {
    errors.copyError(other, i, i + 5);
  }

  ASSERT_EQ(errors.size(), 24);
  ASSERT_EQ(errors.countErrors(), 8);
  ASSERT_TRUE(errors.hasError());
  ASSERT_TRUE(errors.hasErrorAt(2));
  ASSERT_TRUE(errors.hasErrorAt(5));
  ASSERT_TRUE(errors.hasErrorAt(8));
  ASSERT_TRUE(errors.hasErrorAt(11));
  ASSERT_TRUE(errors.hasErrorAt(14));
  ASSERT_TRUE(errors.hasErrorAt(17));
  ASSERT_TRUE(errors.hasErrorAt(20));
  ASSERT_TRUE(errors.hasErrorAt(23));
}

TEST_F(EvalErrorsTest, copyErrorWithDetails) {
  EvalErrors other(pool(), 10);
  for (auto i = 0; i < 20; i += 2) {
    other.setError(i, makeVeloxException(fmt::format("Test error at {}", i)));
  }

  ASSERT_EQ(other.countErrors(), 10);

  // Copy errors from rows 10 to 19. Shift destination row by 10 down: 0 to 9.
  EvalErrors errors(pool(), 10);
  for (auto i = 10; i < 30; ++i) {
    errors.copyError(other, i, i - 10);
  }

  ASSERT_EQ(errors.size(), 10);
  ASSERT_EQ(errors.countErrors(), 5);
  ASSERT_TRUE(errors.hasError());

  ASSERT_TRUE(errors.hasErrorAt(0));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(0), "Test error at 10");

  ASSERT_TRUE(errors.hasErrorAt(2));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(2), "Test error at 12");

  ASSERT_TRUE(errors.hasErrorAt(4));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(4), "Test error at 14");

  ASSERT_TRUE(errors.hasErrorAt(6));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(6), "Test error at 16");

  ASSERT_TRUE(errors.hasErrorAt(8));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(8), "Test error at 18");

  // Copy one more error from row 0 to row 27.
  errors.copyError(other, 0, 27);

  ASSERT_EQ(errors.size(), 28);
  ASSERT_EQ(errors.countErrors(), 6);
  ASSERT_TRUE(errors.hasError());

  ASSERT_TRUE(errors.hasErrorAt(27));
  VELOX_ASSERT_THROW(errors.throwIfErrorAt(27), "Test error at 0");
}

TEST_F(EvalErrorsTest, copyErrors) {
  EvalErrors other(pool(), 10);
  for (auto i = 0; i < 20; i += 2) {
    other.setError(i);
  }

  ASSERT_EQ(other.countErrors(), 10);

  SelectivityVector rows(123, false);
  rows.setValidRange(0, 5, true);
  rows.setValidRange(10, 100, true);
  rows.updateBounds();

  EvalErrors errors(pool(), 10);
  errors.copyErrors(rows, other);

  ASSERT_EQ(errors.size(), 19);
  ASSERT_EQ(errors.countErrors(), 8);
  ASSERT_TRUE(errors.hasError());

  ASSERT_TRUE(errors.hasErrorAt(0));
  ASSERT_TRUE(errors.hasErrorAt(2));
  ASSERT_TRUE(errors.hasErrorAt(4));
  ASSERT_TRUE(errors.hasErrorAt(10));
  ASSERT_TRUE(errors.hasErrorAt(12));
  ASSERT_TRUE(errors.hasErrorAt(14));
  ASSERT_TRUE(errors.hasErrorAt(16));
  ASSERT_TRUE(errors.hasErrorAt(18));
}

TEST_F(EvalErrorsTest, copyAllErrors) {
  EvalErrors other(pool(), 10);
  for (auto i = 0; i < 20; i += 2) {
    other.setError(i, makeVeloxException(fmt::format("Test error at {}", i)));
  }

  ASSERT_EQ(other.countErrors(), 10);

  // Copy all errors from 'other' to this. Make sure pre-existing error doesn't
  // get overwritten.
  EvalErrors errors(pool(), 10);
  errors.setError(4, makeVeloxException("Some other error"));
  errors.copyErrors(other);

  ASSERT_EQ(errors.size(), 19);
  ASSERT_EQ(errors.countErrors(), 10);
  ASSERT_TRUE(errors.hasError());

  for (auto i = 0; i < 20; i += 2) {
    ASSERT_TRUE(errors.hasErrorAt(i));
    if (i == 4) {
      VELOX_ASSERT_THROW(errors.throwIfErrorAt(i), "Some other error");
    } else {
      VELOX_ASSERT_THROW(
          errors.throwIfErrorAt(i), fmt::format("Test error at {}", i));
    }
  }

  for (auto i = 1; i < 20; i += 2) {
    ASSERT_FALSE(errors.hasErrorAt(i));
  }
}
} // namespace
} // namespace facebook::velox::exec
