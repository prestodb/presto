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
#include "velox/expression/Expr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
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
  /// argument (C2), or to -1 to indicate 'no limit'.
  VectorPtr run(
      const std::vector<std::string>& input,
      const std::string& delim,
      const char* query,
      int32_t limit = -1,
      VectorEncoding::Simple encodingStrings = VectorEncoding::Simple::FLAT,
      VectorEncoding::Simple encodingDelims = VectorEncoding::Simple::CONSTANT,
      VectorEncoding::Simple encodingLimit = VectorEncoding::Simple::CONSTANT) {
    std::shared_ptr<BaseVector> strings, delims, limits;
    const vector_size_t numRows = input.size();

    // Functors to create flat vectors, used as is and for lazy vector.
    auto funcCreateFlatStrings = [&](RowSet /*rows*/) {
      return makeFlatVector<StringView>(
          numRows, [&](vector_size_t row) { return StringView{input[row]}; });
    };

    auto funcCreateFlatDelims = [&](RowSet /*rows*/) {
      return makeFlatVector<StringView>(
          numRows, [&](vector_size_t row) { return StringView{delim}; });
    };

    auto funcCreateFlatLimits = [&](RowSet /*rows*/) {
      return makeFlatVector<int32_t>(
          numRows, [&](vector_size_t row) { return limit; });
    };

    auto funcReverseIndices = [&](vector_size_t row) {
      return numRows - 1 - row;
    };

    // Generate strings vector
    if (isFlat(encodingStrings)) {
      strings = funcCreateFlatStrings({});
    } else if (isConstant(encodingStrings)) {
      strings =
          BaseVector::wrapInConstant(numRows, 0, funcCreateFlatStrings({}));
    } else if (isLazy(encodingStrings)) {
      strings = std::make_shared<LazyVector>(
          execCtx_.pool(),
          CppToType<StringView>::create(),
          numRows,
          std::make_unique<SimpleVectorLoader>(funcCreateFlatStrings));
    } else if (isDictionary(encodingStrings)) {
      strings = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          makeIndices(numRows, funcReverseIndices),
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
          makeIndices(numRows, funcReverseIndices),
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
          makeIndices(numRows, funcReverseIndices),
          numRows,
          funcCreateFlatLimits({}));
    }

    VectorPtr result = (limit < 0)
        ? evaluate<BaseVector>(query, makeRowVector({strings, delims}))
        : evaluate<BaseVector>(query, makeRowVector({strings, delims, limits}));

    return VectorMaker::flatten(result);
  }

  /// For expected result vectors, for some combinations of input encodings, we
  /// need to massage the expected vector.
  /// Const we wrap in const, dictionary we wrap in dictionary and the reast
  /// leave 'as is'. In the end we flatten.
  VectorPtr prepare(
      const std::vector<std::vector<std::string>>& arrays,
      VectorEncoding::Simple stringEncoding) {
    auto arrayVector = toArrayVector(arrays);

    // Constant: we will have all rows as the 1st one.
    if (isConstant(stringEncoding)) {
      auto constVector =
          BaseVector::wrapInConstant(arrayVector->size(), 0, arrayVector);
      return VectorMaker::flatten(constVector);
    }

    // Dictionary: we will have reversed rows, because we use reverse index
    // functor to generate indices when wrapping in dictionary.
    if (isDictionary(stringEncoding)) {
      auto funcReverseIndices = [&](vector_size_t row) {
        return arrayVector->size() - 1 - row;
      };

      auto dictVector = BaseVector::wrapInDictionary(
          BufferPtr(nullptr),
          makeIndices(arrayVector->size(), funcReverseIndices),
          arrayVector->size(),
          arrayVector);
      return VectorMaker::flatten(dictVector);
    }

    // Non-const string. Unchanged.
    return arrayVector;
  }

  // Creates array vector (we use it to create expected result).
  VectorPtr toArrayVector(const std::vector<std::vector<std::string>>& data) {
    auto fSizeAt = [&](vector_size_t row) { return data[row].size(); };
    auto fValueAt = [&](vector_size_t row, vector_size_t idx) {
      return StringView{data[row][idx]};
    };

    return makeArrayVector<StringView>(data.size(), fSizeAt, fValueAt);
  }
};

/**
 * Test split vector function on vectors with different encodings.
 */
TEST_F(SplitTest, split) {
  std::vector<std::string> inputStrings;
  std::string delim;
  std::vector<std::vector<std::string>> actualArrays;
  VectorPtr actual;
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
  // Base expected data.
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
        actual = run(inputStrings, delim, "split(C0, C1)", -1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays3, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
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
  // Base expected data.
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
        actual = run(inputStrings, delim, "split(C0, C1)", -1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 10, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 3, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays3, sEn), actual);
        actual =
            run(inputStrings, delim, "split(C0, C1, C2)", 1, sEn, dEn, lEn);
        assertEqualVectors(prepare(expectedArrays1, sEn), actual);
      }
    }
  }
}

/// Test split vector function with errors.
TEST_F(SplitTest, splitError) {
  const std::string delim = ",";
  const std::vector<std::string> inputStrings{{"I,he,she,they"}};

// To make expects below one-liners.
#define RUN(_sql_, _limit_)             \
  run(inputStrings,                     \
      delim,                            \
      _sql_,                            \
      _limit_,                          \
      VectorEncoding::Simple::FLAT,     \
      VectorEncoding::Simple::CONSTANT, \
      VectorEncoding::Simple::CONSTANT)

  // Limit should be positive.
  VELOX_ASSERT_THROW(RUN("split(C0, C1, C2)", 0), "Limit must be positive");
}
