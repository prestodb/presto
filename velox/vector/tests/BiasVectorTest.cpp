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

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/tests/VectorMaker.h"

namespace facebook::velox::test {

class BiasVectorTestBase : public testing::Test {
 protected:
  std::unique_ptr<velox::memory::ScopedMemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  VectorMaker vectorMaker_{pool_.get()};
};

class BiasVectorErrorTest : public BiasVectorTestBase {
 protected:
  void errorTest(std::vector<std::optional<int64_t>> values) {
    EXPECT_ANY_THROW(vectorMaker_.biasVector(values));
  }
};

TEST_F(BiasVectorErrorTest, checkRangeTooLargeError) {
  errorTest({
      std::numeric_limits<int64_t>::min(),
      std::numeric_limits<int64_t>::max(),
  });
}

TEST_F(BiasVectorErrorTest, checkNoRangeError) {
  errorTest({1});
}

template <typename T>
class BiasVectorOverflowTest : public BiasVectorTestBase {
 public:
  void runMaxOverflowTest(size_t delta) {
    const size_t size = 100;
    std::vector<std::optional<T>> input;
    input.reserve(size);
    T maxValue = std::numeric_limits<T>::max();
    for (size_t i = 0; i < size; ++i) {
      input.push_back(maxValue - i);
    }

    // add min value so get needed delta
    T minValue = maxValue - delta;
    input.push_back(minValue);

    runOverflowTest(std::move(input), true /*positiveBias*/);
  }

  void runMinOverflowTest(size_t delta) {
    const size_t size = 10;
    std::vector<std::optional<T>> input;
    input.reserve(size);
    T minValue = std::numeric_limits<T>::min();
    for (size_t i = 0; i < size; ++i) {
      input.push_back(minValue + i);
    }

    T maxValue = minValue + delta;
    input.push_back(maxValue);

    runOverflowTest(std::move(input), false /*positiveBias*/);
  }

 private:
  void runOverflowTest(std::vector<std::optional<T>> input, bool positiveBias) {
    auto biasVector = vectorMaker_.biasVector(input);

    if (positiveBias) {
      ASSERT_TRUE(biasVector->bias() > 0);
    } else {
      ASSERT_TRUE(biasVector->bias() < 0);
    }

    for (size_t i = 0; i < input.size(); ++i) {
      ASSERT_EQ(biasVector->valueAt(i), input[i]);
    }
  }
};

using inputTypes = ::testing::Types<int16_t, int32_t, int64_t>;
VELOX_TYPED_TEST_SUITE(BiasVectorOverflowTest, inputTypes);

TYPED_TEST(BiasVectorOverflowTest, maxOverflowTest_delta_int8) {
  this->runMaxOverflowTest(std::numeric_limits<int8_t>::max());
}

TYPED_TEST(BiasVectorOverflowTest, maxOverflowTest_delta_int16) {
  size_t delta = std::numeric_limits<int16_t>::max();
  if constexpr (std::is_same_v<TypeParam, int16_t>) {
    delta = std::numeric_limits<int8_t>::max();
  }
  this->runMaxOverflowTest(delta);
}

TYPED_TEST(BiasVectorOverflowTest, maxOverflowTest_delta_int32) {
  using T = TypeParam;
  size_t delta = std::numeric_limits<int32_t>::max();
  if constexpr (std::is_same_v<T, int16_t>) {
    delta = std::numeric_limits<int8_t>::max();
  }

  if constexpr (std::is_same_v<T, int32_t>) {
    delta = std::numeric_limits<int16_t>::max();
  }

  this->runMaxOverflowTest(delta);
}

TYPED_TEST(BiasVectorOverflowTest, minOverflowTest_delta_int8) {
  this->runMinOverflowTest(std::numeric_limits<int8_t>::max() + 1);
}

TYPED_TEST(BiasVectorOverflowTest, minOverflowTest_delta_int16) {
  size_t delta = std::numeric_limits<int16_t>::max();
  if constexpr (std::is_same_v<TypeParam, int16_t>) {
    delta = std::numeric_limits<int8_t>::max();
  }
  delta++;
  this->runMinOverflowTest(delta);
}

TYPED_TEST(BiasVectorOverflowTest, minOverflowTest_delta_int32) {
  using T = TypeParam;
  size_t delta = std::numeric_limits<int32_t>::max();
  if constexpr (std::is_same_v<T, int16_t>) {
    delta = std::numeric_limits<int8_t>::max();
  }
  // delta++;
  if constexpr (std::is_same_v<T, int32_t>) {
    delta = std::numeric_limits<int16_t>::max();
  }
  delta++;
  this->runMinOverflowTest(delta);
}

} // namespace facebook::velox::test
