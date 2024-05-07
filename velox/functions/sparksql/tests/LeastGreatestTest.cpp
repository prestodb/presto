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

#include "velox/functions/sparksql/tests/SparkFunctionBaseTest.h"

namespace facebook::velox::functions::sparksql::test {
namespace {

class LeastTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> least(
      std::optional<T> arg0,
      std::optional<T> arg1,
      std::optional<T> arg2,
      const TypePtr& type = CppToType<T>::create()) {
    return evaluateOnce<T>(
        "least(c0, c1, c2)", {type, type, type}, arg0, arg1, arg2);
  }

  template <typename T>
  std::optional<T> least(
      std::optional<T> arg0,
      std::optional<T> arg1,
      std::optional<T> arg2,
      std::optional<T> arg3,
      const TypePtr& type = CppToType<T>::create()) {
    return evaluateOnce<T>(
        "least(c0, c1, c2, c3)",
        {type, type, type, type},
        arg0,
        arg1,
        arg2,
        arg3);
  }

  template <typename T>
  void flat(const TypePtr& type = CppToType<T>::create()) {
    vector_size_t size = 20;

    // {0, null, null, 3, null, null, 6, null, null, ...}.
    auto first = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row; },
        [](vector_size_t row) { return row % 3 != 0; },
        type);

    // {0, 10, null, 30, 40, null, 60, 70, null, ...}.
    auto second = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row * 10; },
        nullEvery(3, 2),
        type);

    // {0, 100, 200, 300, 400, 500, 600, 700, 800, ...}.
    auto third = makeFlatVector<T>(
        size, [](vector_size_t row) { return row * 100; }, nullptr, type);

    auto data = makeRowVector({first, second, third});

    // Expect {0, 10, 200, 3, 40, 500, 6, 70, 800, ...}.
    auto expected = [](vector_size_t row) {
      return std::min<T>(
          {row % 3 == 0 ? T(row) : std::numeric_limits<T>::max(),
           row % 3 == 2 ? std::numeric_limits<T>::max() : T(10 * row),
           T(100 * row)});
    };

    auto result = evaluate<FlatVector<T>>("least(c0, c1, c2)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }

    result = evaluate<FlatVector<T>>("least(c2, c1, c0)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }

    result = evaluate<FlatVector<T>>("least(c1, c0, c2)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }
  }

  template <typename T>
  void constant(const TypePtr& type = CppToType<T>::create()) {
    vector_size_t size = 20;

    // {0, null, null, 3, null, null, 6, null, null, ...}.
    auto first = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row; },
        [](vector_size_t row) { return row % 3 != 0; },
        type);

    // {9, 9, 9, ...}.
    auto second = makeConstant<T>(9, size, type);

    auto data = makeRowVector({first, second});

    // Expect {0, 9, 9, 3, 9, 9, 6, 9, 9, ...}.
    auto expected = [](vector_size_t row) {
      return std::min<T>(9, row % 3 == 0 ? row : std::numeric_limits<T>::max());
    };

    auto result = evaluate<FlatVector<T>>("least(c0, c1)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }
  }
};

TEST_F(LeastTest, integral) {
  flat<int16_t>();
  constant<int16_t>();

  flat<int32_t>();
  constant<int32_t>();

  flat<int64_t>();
  constant<int64_t>();

  flat<int32_t>(DATE());
  constant<int32_t>(DATE());
}

TEST_F(LeastTest, floating) {
  flat<float>();
  constant<float>();

  flat<double>();
  constant<double>();
}

TEST_F(LeastTest, edgecases) {
  // All inputs are null.
  std::optional<int32_t> null;
  EXPECT_EQ(least<int32_t>(null, null, null), null);

  // IEEE-floating point exceptions.
  constexpr float inf = std::numeric_limits<float>::infinity();
  constexpr float nan = std::numeric_limits<float>::quiet_NaN();

  EXPECT_EQ(least<float>(inf, nan, -inf, 0.f), -inf);
  EXPECT_EQ(least<float>(nan, inf, 0.f), 0.f);
  EXPECT_EQ(least<float>(nan, nan, inf), inf);
  EXPECT_TRUE(std::isnan(least<float>(nan, nan, nan).value()));
}

TEST_F(LeastTest, boolean) {
  EXPECT_EQ(least<bool>(true, false, false), false);
}

TEST_F(LeastTest, string) {
  EXPECT_EQ(least<std::string>("b", "abcde", "abcdefg"), "abcde");
}

TEST_F(LeastTest, timestamp) {
  EXPECT_EQ(
      least<Timestamp>(
          Timestamp(1569, 25), Timestamp(4859, 482), Timestamp(581, 1651)),
      Timestamp(581, 1651));
}

TEST_F(LeastTest, date) {
  EXPECT_EQ(least<int32_t>(100, 1000, 10000, DATE()), 100);
}

class GreatestTest : public SparkFunctionBaseTest {
 protected:
  template <typename T>
  std::optional<T> greatest(
      std::optional<T> arg0,
      std::optional<T> arg1,
      std::optional<T> arg2,
      const TypePtr& type = CppToType<T>::create()) {
    return evaluateOnce<T>(
        "greatest(c0, c1, c2)", {type, type, type}, arg0, arg1, arg2);
  }

  template <typename T>
  std::optional<T> greatest(
      std::optional<T> arg0,
      std::optional<T> arg1,
      std::optional<T> arg2,
      std::optional<T> arg3,
      const TypePtr& type = CppToType<T>::create()) {
    return evaluateOnce<T>(
        "greatest(c0, c1, c2, c3)",
        {type, type, type, type},
        arg0,
        arg1,
        arg2,
        arg3);
  }

  template <typename T>
  void flat(const TypePtr& type = CppToType<T>::create()) {
    vector_size_t size = 20;

    // {0, null, null, 300, null, null, 600, null, null, ...}.
    auto first = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row * 100; },
        [](vector_size_t row) { return row % 3 != 0; },
        type);

    // {0, 10, null, 30, 40, null, 60, 70, null, ...}.
    auto second = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row * 10; },
        nullEvery(3, 2),
        type);

    // {0, 1, 2, 3, 4, 5, 6, 7, 8, ...}.
    auto third = makeFlatVector<T>(
        size, [](vector_size_t row) { return row; }, nullptr, type);

    auto data = makeRowVector({first, second, third});

    // Expect {0, 10, 2, 300, 40, 5, 600, 70, 8, ...}.
    auto expected = [](vector_size_t row) {
      return std::max<T>(
          {row % 3 == 0 ? T(100 * row) : std::numeric_limits<T>::min(),
           row % 3 == 2 ? std::numeric_limits<T>::min() : T(10 * row),
           T(row)});
    };

    auto result = evaluate<FlatVector<T>>("greatest(c0, c1, c2)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }

    result = evaluate<FlatVector<T>>("greatest(c2, c1, c0)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }

    result = evaluate<FlatVector<T>>("greatest(c1, c0, c2)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }
  }

  template <typename T>
  void constant(const TypePtr& type = CppToType<T>::create()) {
    vector_size_t size = 20;

    // {0, null, null, 3, null, null, 6, null, null, ...}.
    auto first = makeFlatVector<T>(
        size,
        [](vector_size_t row) { return row; },
        [](vector_size_t row) { return row % 3 != 0; },
        type);

    // {9, 9, 9, ...}.
    auto second = makeConstant<T>(9, size, type);

    auto data = makeRowVector({first, second});

    // Expect {9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 9, 12, 9, 9, 15...}.
    auto expected = [](vector_size_t row) {
      return std::max(
          T(9), row % 3 == 0 ? T(row) : std::numeric_limits<T>::min());
    };

    auto result = evaluate<FlatVector<T>>("greatest(c0, c1)", data);
    for (vector_size_t i = 0; i < size; i++) {
      EXPECT_EQ(result->valueAt(i), expected(i)) << "at " << i;
    }
  }
};

TEST_F(GreatestTest, integral) {
  flat<int16_t>();
  constant<int16_t>();

  flat<int32_t>();
  constant<int32_t>();

  flat<int64_t>();
  constant<int64_t>();

  flat<int32_t>(DATE());
  constant<int32_t>(DATE());
}

TEST_F(GreatestTest, floating) {
  flat<float>();
  constant<float>();

  flat<double>();
  constant<double>();
}

TEST_F(GreatestTest, edgecases) {
  // All inputs are null.
  std::optional<int32_t> null;
  EXPECT_EQ(greatest<int32_t>(null, null, null), null);

  // IEEE-floating point exceptions.
  constexpr float inf = std::numeric_limits<float>::infinity();
  constexpr float nan = std::numeric_limits<float>::quiet_NaN();

  EXPECT_TRUE(std::isnan(greatest<float>(inf, nan, -inf, 0.f).value()));
  EXPECT_EQ(greatest<float>(0.f, -inf, inf), inf);
  EXPECT_EQ(greatest<float>(-inf, -inf, 0.f), 0.f);
  EXPECT_EQ(greatest<float>(-inf, -inf, -inf), -inf);
}

TEST_F(GreatestTest, boolean) {
  EXPECT_EQ(greatest<bool>(true, false, false), true);
}

TEST_F(GreatestTest, string) {
  EXPECT_EQ(greatest<std::string>("b", "abcde", "abcdefg"), "b");
}

TEST_F(GreatestTest, timestamp) {
  EXPECT_EQ(
      greatest<Timestamp>(
          Timestamp(1569, 25), Timestamp(4859, 482), Timestamp(581, 1651)),
      Timestamp(4859, 482));
}

TEST_F(GreatestTest, date) {
  EXPECT_EQ(greatest<int32_t>(100, 1000, 10000, DATE()), 10000);
}

} // namespace
} // namespace facebook::velox::functions::sparksql::test
