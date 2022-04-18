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

#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

// This files contains e2e tests the simple functions resolution based on
// priority which is determined based on the generality.
namespace facebook::velox::exec {
namespace {

class SimpleFunctionResolutionTest : public functions::test::FunctionBaseTest {
 protected:
  void checkResults(const std::string& func, int32_t expected) {
    auto results = evaluateOnce<int32_t, int32_t, int32_t>(
        fmt::format("{}(c0, c1)", func), 1, 2);
    ASSERT_EQ(results.value(), expected);
  }
};

// Several `call` functions all that accepts two integers. With different
// runtime representations.
template <typename T>
struct TestFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  bool call(int32_t& out, int32_t, int32_t) {
    out = 1;
    return true;
  }

  bool call(int32_t& out, const arg_type<Variadic<int32_t>>&) {
    out = 2;
    return true;
  }

  bool call(
      int32_t& out,
      const arg_type<Generic<T1>>&,
      const arg_type<Generic<T1>>&) {
    out = 3;
    return true;
  }

  bool call(int32_t& out, const arg_type<Variadic<Any>>&) {
    out = 4;
    return true;
  }

  bool call(int32_t& out, const int32_t&, const arg_type<Variadic<Any>>&) {
    out = 5;
    return true;
  }
};

TEST_F(SimpleFunctionResolutionTest, rank1Picked) {
  registerFunction<TestFunction, int32_t, int32_t, int32_t>({"f1"});
  registerFunction<TestFunction, int32_t, Variadic<int32_t>>({"f1"});
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f1"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f1"});

  checkResults("f1", 1);
}

TEST_F(SimpleFunctionResolutionTest, rank2Picked) {
  registerFunction<TestFunction, int32_t, Variadic<int32_t>>({"f2"});
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f2"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f2"});
  checkResults("f2", 2);
}

TEST_F(SimpleFunctionResolutionTest, rank3Picked) {
  registerFunction<TestFunction, int32_t, Generic<T1>, Generic<T1>>({"f3"});
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f3"});
  checkResults("f3", 3);
}

TEST_F(SimpleFunctionResolutionTest, rank4Picked) {
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f4"});
  checkResults("f4", 4);
}

// Test when two functions have the same rank.
TEST_F(SimpleFunctionResolutionTest, sameRank) {
  registerFunction<TestFunction, int32_t, Variadic<Any>>({"f5"});
  registerFunction<TestFunction, int32_t, int32_t, Variadic<Any>>({"f5"});
  checkResults("f5", 5);
}

} // namespace
} // namespace facebook::velox::exec
