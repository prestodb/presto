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
#include <utility>

#include "velox/type/FloatingPointUtil.h"

namespace facebook::velox {
namespace {

template <typename T>
void testFloatingPoint() {
  using namespace util::floating_point;

  static constexpr T kNaN = std::numeric_limits<T>::quiet_NaN();
  static constexpr T kSNAN = std::numeric_limits<T>::signaling_NaN();
  static constexpr T kInf = std::numeric_limits<T>::infinity();

  ASSERT_TRUE(NaNAwareEquals<T>{}(kNaN, kNaN));
  ASSERT_TRUE(NaNAwareEquals<T>{}(kNaN, kSNAN));
  ASSERT_FALSE(NaNAwareEquals<T>{}(kNaN, 0.0));

  ASSERT_FALSE(NaNAwareLessThan<T>{}(kNaN, kNaN));
  ASSERT_FALSE(NaNAwareLessThan<T>{}(kNaN, kSNAN));
  ASSERT_FALSE(NaNAwareLessThan<T>{}(kNaN, kInf));
  ASSERT_TRUE(NaNAwareLessThan<T>{}(kInf, kNaN));

  ASSERT_TRUE(NaNAwareLessThanEqual<T>{}(kNaN, kNaN));
  ASSERT_TRUE(NaNAwareLessThanEqual<T>{}(kNaN, kSNAN));
  ASSERT_TRUE(NaNAwareLessThanEqual<T>{}(kInf, kNaN));
  ASSERT_TRUE(NaNAwareLessThanEqual<T>{}(0.0, kInf));
  ASSERT_FALSE(NaNAwareLessThanEqual<T>{}(kNaN, kInf));
  ASSERT_FALSE(NaNAwareLessThanEqual<T>{}(kNaN, 0.0));

  ASSERT_FALSE(NaNAwareGreaterThan<T>{}(kNaN, kNaN));
  ASSERT_FALSE(NaNAwareGreaterThan<T>{}(kNaN, kSNAN));
  ASSERT_FALSE(NaNAwareGreaterThan<T>{}(kInf, kNaN));
  ASSERT_TRUE(NaNAwareGreaterThan<T>{}(kNaN, kInf));

  ASSERT_TRUE(NaNAwareGreaterThanEqual<T>{}(kNaN, kNaN));
  ASSERT_TRUE(NaNAwareGreaterThanEqual<T>{}(kNaN, kSNAN));
  ASSERT_FALSE(NaNAwareGreaterThanEqual<T>{}(kInf, kNaN));
  ASSERT_FALSE(NaNAwareGreaterThanEqual<T>{}(0.0, kInf));
  ASSERT_TRUE(NaNAwareGreaterThanEqual<T>{}(kNaN, kInf));
  ASSERT_TRUE(NaNAwareGreaterThanEqual<T>{}(kNaN, 0.0));

  ASSERT_EQ(NaNAwareHash<T>{}(kNaN), NaNAwareHash<T>{}(kSNAN));
  ASSERT_EQ(NaNAwareHash<T>{}(kNaN), NaNAwareHash<T>{}(kNaN));
  ASSERT_EQ(NaNAwareHash<T>{}(0.0), NaNAwareHash<T>{}(0.0));
  ASSERT_EQ(NaNAwareHash<T>{}(kInf), NaNAwareHash<T>{}(kInf));
  ASSERT_NE(NaNAwareHash<T>{}(kNaN), NaNAwareHash<T>{}(kInf));
  ASSERT_NE(NaNAwareHash<T>{}(kNaN), NaNAwareHash<T>{}(0.0));

  HashSetNaNAware<T> set;
  set.insert(kNaN);
  set.insert(kSNAN);
  ASSERT_EQ(set.size(), 1);
  ASSERT_TRUE(NaNAwareEquals<T>{}(*set.begin(), kSNAN));
  ASSERT_TRUE(NaNAwareEquals<T>{}(*set.begin(), kNaN));
  ASSERT_NE(*set.begin(), kNaN);
  ASSERT_NE(*set.begin(), kSNAN);

  typename HashMapNaNAwareTypeTraits<
      T,
      int32_t,
      folly::f14::DefaultAlloc<std::pair<const T, int32_t>>>::Type map;
  map[kNaN] = 1;
  map[kSNAN] = 2;
  ASSERT_EQ(map.size(), 1);
  ASSERT_EQ(map[kNaN], 2);
  ASSERT_EQ(map[kSNAN], 2);
}
} // namespace

TEST(FloatingPointUtilTest, basic) {
  testFloatingPoint<float>();
  testFloatingPoint<double>();
}
} // namespace facebook::velox
