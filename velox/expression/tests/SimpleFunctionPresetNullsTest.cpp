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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <type_traits>

#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/type/StringView.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/SelectivityVector.h"

// This file contains tests that ensure that simple function adapter properly
// sets the nullity of the output when the input vector nullity is already set
// to null for some indices. It was added after we discovered that simple
// function adapter assumes results vector nullity to be intitially all not
// null.

namespace facebook::velox {

using namespace facebook::velox::test;

namespace {

class SimpleFunctionPresetNullsTest : public functions::test::FunctionBaseTest {
  // Helper class to create the test function for type T.
  template <typename T>
  struct TestFunction {
    template <typename ExecEnv>
    struct udf {
      VELOX_DEFINE_FUNCTION_TYPES(ExecEnv);
      bool call(out_type<T>& out, const arg_type<T>& in) {
        if constexpr (std::is_same_v<T, Varchar>) {
          out.setEmpty();
        } else if constexpr (std::is_same_v<T, Array<int64_t>>) {
          out.push_back(1);
        } else {
          out = in;
        }
        return true;
      }
    };

    static void registerUdf() {
      registerFunction<udf, T, T>({"preset_nulls_test_func"});
    }
  };

 protected:
  // Create vector of type T as input with arbitrary data.
  template <typename T>
  RowVectorPtr createInput(vector_size_t size) {
    if constexpr (std::is_same_v<T, Varchar>) {
      auto flatInput = makeFlatVector<StringView>(size);

      for (auto i = 0; i < flatInput->size(); i++) {
        flatInput->set(i, "test"_sv);
      }
      return makeRowVector({flatInput});
    } else if constexpr (std::is_same_v<T, Array<int64_t>>) {
      std::vector<std::vector<int64_t>> arrayData{(unsigned int)size, {1}};
      auto input = vectorMaker_.arrayVector(arrayData);
      return makeRowVector({input});
    } else {
      auto flatInput = makeFlatVector<T>(size);
      for (auto i = 0; i < flatInput->size(); i++) {
        flatInput->set(i, T());
      }
      return makeRowVector({flatInput});
    }
  }

  // Create result vector for type T, and preset nulls to true.
  template <typename T>
  VectorPtr createResults(vector_size_t size) {
    VectorPtr result;
    if constexpr (std::is_same_v<T, Varchar>) {
      result = makeFlatVector<StringView>(size);
    } else if constexpr (std::is_same_v<T, Array<int64_t>>) {
      std::vector<std::vector<int64_t>> arrayData{(unsigned int)size, {1}};
      result = vectorMaker_.arrayVector(arrayData);
    } else {
      result = makeFlatVector<T>(size);
    }

    for (auto i = 0; i < result->size(); i++) {
      result->setNull(i, true);
    }
    return result;
  }

  template <typename T>
  void test(vector_size_t size = 10) {
    TestFunction<T>::registerUdf();
    SelectivityVector rows;
    rows.resize(size);
    rows.setAll();

    auto input = createInput<T>(size);
    auto result = createResults<T>(size);
    evaluate<SimpleVector<T>>(
        "preset_nulls_test_func(c0)", input, rows, result);
    auto expectedResult = evaluate("preset_nulls_test_func(c0)", input);

    assertEqualVectors(result, expectedResult);
  }
};

TEST_F(SimpleFunctionPresetNullsTest, primitivesOutput) {
  test<int64_t>();
  test<double>();
  test<int32_t>();
  test<float>();
}

TEST_F(SimpleFunctionPresetNullsTest, boolOutput) {
  test<bool>();
}

TEST_F(SimpleFunctionPresetNullsTest, stringOutput) {
  test<Varchar>();
}

TEST_F(SimpleFunctionPresetNullsTest, arrayOutput) {
  test<Array<int64_t>>();
}
} // namespace
} // namespace facebook::velox
