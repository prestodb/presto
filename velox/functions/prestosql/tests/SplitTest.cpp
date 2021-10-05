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
#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"
#include "velox/parse/Expressions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;
using namespace facebook::velox::test;

/// Class to test 'split' vector function.
class SplitTest : public FunctionBaseTest {
 protected:
  /// Method runs the given split function, f.e. split(C0, C1), where C0 is the
  /// input column and the C1 is delimiter column.
  /// Encoding arguments control what kind of vectors we should create for the
  /// function arguments.
  /// limit should be set to the corresponding limit, if query contains limit
  /// argument (C2), or to zero to indicate 'no limit'.
  std::vector<std::vector<std::string>> run(
      const std::vector<std::string>& inputStrings,
      const std::string& delim,
      const char* query,
      int32_t limit = 0,
      VectorEncoding::Simple encodingStrings = VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple encodingDelims = VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple encodingLimit = VectorEncoding::Simple::CONSTANT) {
    std::shared_ptr<BaseVector> strings, delims, limits;
    const vector_size_t numRows = inputStrings.size();

    // Functors to create flat vectors, used as is and for lazy vector.
    auto funcCreateFlatStrings = [&](RowSet /*rows*/) {
      auto stringsFlat = std::dynamic_pointer_cast<FlatVector<StringView>>(
          BaseVector::create(VARCHAR(), numRows, execCtx_.pool()));
      for (auto i = 0; i < numRows; ++i) {
        stringsFlat->set(i, StringView(inputStrings[i]));
      }
      return stringsFlat;
    };

    auto funcCreateFlatDelims = [&](RowSet /*rows*/) {
      auto delimsFlat = std::dynamic_pointer_cast<FlatVector<StringView>>(
          BaseVector::create(VARCHAR(), numRows, execCtx_.pool()));
      for (auto i = 0; i < numRows; ++i) {
        delimsFlat->set(i, StringView(delim));
      }
      return delimsFlat;
    };

    auto funcCreateFlatLimits = [&](RowSet /*rows*/) {
      auto limitsFlat = std::dynamic_pointer_cast<FlatVector<int32_t>>(
          BaseVector::create(INTEGER(), numRows, execCtx_.pool()));
      for (auto i = 0; i < numRows; ++i) {
        limitsFlat->set(i, limit);
      }
      return limitsFlat;
    };

    // Generate strings vector
    if (isFlat(encodingStrings)) {
      strings = funcCreateFlatStrings({});
    } else if (isConstant(encodingStrings)) {
      strings = BaseVector::createConstant(
          inputStrings[0].c_str(), numRows, execCtx_.pool());
    } else if (isLazy(encodingStrings)) {
      strings = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<StringView>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(funcCreateFlatStrings));
    } else if (isDictionary(encodingStrings)) {
      strings = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          makeIndices(numRows, [](vector_size_t row) { return row; }),
          numRows,
          funcCreateFlatStrings({}));
    }

    // Generate delimiters vector
    if (isFlat(encodingDelims)) {
      delims = funcCreateFlatDelims({});
    } else if (isConstant(encodingDelims)) {
      delims =
          BaseVector::createConstant(delim.c_str(), numRows, execCtx_.pool());
    } else if (isLazy(encodingDelims)) {
      delims = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<StringView>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(funcCreateFlatDelims));
    } else if (isDictionary(encodingDelims)) {
      delims = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          makeIndices(numRows, [](vector_size_t row) { return row; }),
          numRows,
          funcCreateFlatDelims({}));
    }

    // Generate limits vector
    if (isFlat(encodingLimit)) {
      limits = funcCreateFlatLimits({});
    } else if (isConstant(encodingLimit)) {
      limits = BaseVector::createConstant(limit, numRows, execCtx_.pool());
    } else if (isLazy(encodingLimit)) {
      limits = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<int32_t>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(funcCreateFlatLimits));
    } else if (isDictionary(encodingLimit)) {
      limits = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          makeIndices(numRows, [](vector_size_t row) { return row; }),
          numRows,
          funcCreateFlatLimits({}));
    }

    VectorPtr result = (limit == 0)
        ? evaluate<BaseVector>(query, makeRowVector({strings, delims}))
        : evaluate<BaseVector>(query, makeRowVector({strings, delims, limits}));

    // We can have the array vector returned wrapped in const or dictionary
    // vector, so take care of that.
    std::shared_ptr<ArrayVector> array = arrayFromResult(result);

    // Convert the result array vector into the return std::vector, so we can
    // compare that with the expected vector outside of this method, which
    // provides better error message.
    const VectorPtr& resultElements = array->elements();
    auto* flatResultElements = resultElements->asFlatVector<StringView>();
    const auto* rawSizes = array->rawSizes();
    const auto* rawOffsets = array->rawOffsets();
    std::vector<std::vector<std::string>> actualArrays(numRows);
    for (auto rowIndex = 0; rowIndex < numRows; ++rowIndex) {
      for (auto elemIndex = 0; elemIndex < rawSizes[rowIndex]; ++elemIndex) {
        actualArrays[rowIndex].emplace_back(
            flatResultElements->valueAt(elemIndex + rawOffsets[rowIndex]));
      }
    }
    return actualArrays;
  }

  /// We can have the array vector returned wrapped in const or dictionary
  /// vector, so take care of that.
  static std::shared_ptr<ArrayVector> arrayFromResult(const VectorPtr& result) {
    std::shared_ptr<ArrayVector> array =
        std::dynamic_pointer_cast<ArrayVector>(result);
    if (array == nullptr) {
      // See if there is a constant vector wrapping the type we want.
      // We will return it, if that's the case.
      if (auto constVector =
              std::dynamic_pointer_cast<ConstantVector<ComplexType>>(result)) {
        array =
            std::dynamic_pointer_cast<ArrayVector>(constVector->valueVector());
      }
      // Likewise, if there is a dictionary wrapping around our result vector,
      // we return the underlying one.
      if (auto dictVector =
              std::dynamic_pointer_cast<DictionaryVector<ComplexType>>(
                  result)) {
        array =
            std::dynamic_pointer_cast<ArrayVector>(dictVector->valueVector());
      }
    }
    return array;
  }

  /// For expected result vectors, for some combinations of input encodings, we
  /// need to massage the expected vector.
  static std::vector<std::vector<std::string>> prepare(
      const std::vector<std::vector<std::string>>& arrays,
      VectorEncoding::Simple stringEncoding,
      VectorEncoding::Simple delimEncoding,
      VectorEncoding::Simple limitEncoding,
      int32_t limit) {
    // All input vectors are constant - will have a const vector returned with
    // only the 1st row valid.
    // Note, when limit is zero, it is not used, so we consider only const-ness
    // of the two first arguments.
    if (isConstant(stringEncoding) and isConstant(delimEncoding) and
        (isConstant(limitEncoding) or (limit == 0))) {
      std::vector<std::vector<std::string>> ret(arrays.size());
      ret[0] = arrays[0];
      return ret;
    }

    // Constant string and non-constant others. We will have all rows as the
    // 1st one.
    if (isConstant(stringEncoding)) {
      return std::vector<std::vector<std::string>>(arrays.size(), arrays[0]);
    }

    // Non-const string. Unchanged.
    return arrays;
  }
};

/**
 * Test split vector function
 */
TEST_F(SplitTest, split) {
  std::vector<std::string> inputStrings;
  std::string delim;
  std::vector<std::vector<std::string>> actualArrays;
  std::vector<std::vector<std::string>> expectedArrays;
  std::vector<std::vector<std::string>> expectedArrays3;
  std::vector<std::vector<std::string>> expectedArrays1;

  // We want to check these encodings for the vectors.
  std::vector<VectorEncoding::Simple> encodings{
      VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple::LAZY,
      VectorEncoding::Simple::DICTIONARY,
  };

  // Ascii, flat strings, flat delimiter, no limit.
  delim = ",";
  inputStrings = std::vector<std::string>{
      {"I,he,she,they"}, // Simple
      {"one,,,four,"}, // Empty strings
      {""}, // The whole string is empty
  };
  expectedArrays = std::vector<std::vector<std::string>>{
      {"I", "he", "she", "they"},
      {"one", "", "", "four", ""},
      {""},
  };
  expectedArrays3 = std::vector<std::vector<std::string>>{
      {"I", "he", "she,they"},
      {"one", "", ",four,"},
      {""},
  };
  expectedArrays1 = std::vector<std::vector<std::string>>{
      {inputStrings[0]},
      {inputStrings[1]},
      {inputStrings[2]},
  };

  // Mix and match encodings.
  for (const auto& sEn : encodings) {
    for (const auto& dEn : encodings) {
      for (const auto& lEn : encodings) {
        // Cover 'no limit', 'high limit', 'small limit', 'limit 1'.
        actualArrays =
            run(inputStrings, delim, "split(C0, C1)", 0, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays, sEn, dEn, lEn, 0), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays, sEn, dEn, lEn, 10), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays3, sEn, dEn, lEn, 3), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays1, sEn, dEn, lEn, 1), actualArrays);
      }
    }
  }

  // Non-ascii, flat strings, flat delimiter, no limit.
  delim = "లేదా";
  inputStrings = std::vector<std::string>{
      {"синяя сливаలేదా赤いトマトలేదా黃苹果లేదాbrown pear"}, // Simple
      {"зелёное небоలేదాలేదాలేదా緑の空లేదా"}, // Empty strings
      {""}, // The whole string is empty
  };
  expectedArrays = std::vector<std::vector<std::string>>{
      {"синяя слива", "赤いトマト", "黃苹果", "brown pear"},
      {"зелёное небо", "", "", "緑の空", ""},
      {""},
  };
  expectedArrays3 = std::vector<std::vector<std::string>>{
      {"синяя слива", "赤いトマト", "黃苹果లేదాbrown pear"},
      {"зелёное небо", "", "లేదా緑の空లేదా"},
      {""},
  };
  expectedArrays1 = std::vector<std::vector<std::string>>{
      {inputStrings[0]},
      {inputStrings[1]},
      {inputStrings[2]},
  };
  // Mix and match encodings.
  for (const auto& sEn : encodings) {
    for (const auto& dEn : encodings) {
      for (const auto& lEn : encodings) {
        // Cover 'no limit', 'high limit', 'small limit', 'limit 1'.
        actualArrays =
            run(inputStrings, delim, "split(C0, C1)", 0, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays, sEn, dEn, lEn, 0), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays, sEn, dEn, lEn, 10), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays3, sEn, dEn, lEn, 3), actualArrays);
        actualArrays =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        ASSERT_EQ(prepare(expectedArrays1, sEn, dEn, lEn, 1), actualArrays);
      }
    }
  }
}
