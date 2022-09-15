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

#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::test;
using namespace facebook::velox::functions::test;

namespace {

class SliceTest : public FunctionBaseTest {
 protected:
  static const vector_size_t kVectorSize{1000};

  void testSlice(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      const ArrayVectorPtr& expectedArrayVector) {
    auto result = evaluate<ArrayVector>(expression, makeRowVector(parameters));
    assertEqualVectors(expectedArrayVector, result);
  }
};
} // namespace

TEST_F(SliceTest, prestoTestCases) {
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4, 5}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    testSlice("slice(C0, 1, 4)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{1, 2}});
    testSlice("slice(C0, 1, 4)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4, 5}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{3, 4}});
    testSlice("slice(C0, 3, 2)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{3, 4}});
    testSlice("slice(C0, 3, 3)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{2, 3, 4}});
    testSlice("slice(C0, -3, 3)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{2, 3, 4}});
    testSlice("slice(C0, -3, 5)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, 1, 0)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, -2, 0)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, -2, 0)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, -5, 5)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, -6, 5)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    testSlice("slice(C0, -6, 5)", {arrayVector}, expectedArrayVector);
  }
  {
    // The implementation is zero-copy, so floating point number comparison
    // won't have issue here since numerical representation remains the same.
    auto arrayVector = makeArrayVector<double>({{2.3, 2.3, 2.2}});
    auto expectedArrayVector = makeArrayVector<double>({{2.3, 2.2}});
    testSlice("slice(C0, 2, 3)", {arrayVector}, expectedArrayVector);
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    assertUserInvalidArgument(
        [&]() {
          testSlice(
              "slice(C0, 1, -1)",
              {arrayVector, arrayVector, expectedArrayVector},
              expectedArrayVector);
        },
        "The value of length argument of slice() function should not be negative");
  }
  {
    auto arrayVector = makeArrayVector<int64_t>({{1, 2, 3, 4}});
    auto expectedArrayVector = makeArrayVector<int64_t>({{}});
    assertUserInvalidArgument(
        [&]() {
          testSlice(
              "slice(C0, 0, 1)",
              {arrayVector, arrayVector, expectedArrayVector},
              expectedArrayVector);
        },
        "SQL array indices start at 1");
  }
}

TEST_F(SliceTest, constantInputArray) {
  auto arrayVector = makeArrayVector<int64_t>({
      {1, 2, 3, 4, 5, 6, 7},
      {1, 2, 7},
      {1, 2, 3, 5, 6, 7},
  });

  // Happy test 1.
  {
    auto expectedArrayVector =
        makeArrayVector<int64_t>({{2, 3}, {2, 7}, {2, 3}});
    testSlice("slice(C0, 2, 2)", {arrayVector}, expectedArrayVector);
  }

  // Happy test 2.
  {
    auto expectedArrayVector = makeArrayVector<int64_t>({{3}, {7}, {3}});

    testSlice("slice(C0, 3, 1)", {arrayVector}, expectedArrayVector);
  }

  // Allow negative start index.
  {
    auto expectedArrayVector =
        makeArrayVector<int64_t>({{6, 7}, {2, 7}, {6, 7}});

    testSlice("slice(C0, -2, 2)", {arrayVector}, expectedArrayVector);
  }

  // Allow length extends beyond boundary.
  {
    auto expectedArrayVector = makeArrayVector<int64_t>(
        {{1, 2, 3, 4, 5, 6, 7}, {1, 2, 7}, {1, 2, 3, 5, 6, 7}});
    testSlice("slice(C0, 1, 7)", {arrayVector}, expectedArrayVector);
  }

  // Throw invalid argument when start index = 0.
  {
    auto expectedArrayVector = makeArrayVector<int64_t>(
        {{1, 2, 3, 4, 5, 6, 7}, {1, 2, 7}, {1, 2, 3, 5, 6, 7}});
    assertUserInvalidArgument(
        [&]() {
          testSlice("slice(C0, 0, 1)", {arrayVector}, expectedArrayVector);
        },
        "SQL array indices start at 1");
  }
}

TEST_F(SliceTest, variableInputArray) {
  {
    auto startsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
    auto lengthsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t /*row*/) { return 1; });
    auto sizeAt = [](vector_size_t /*row*/) { return 7; };
    auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
      return 1 + idx;
    };
    auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

    auto expectedSizeAt = [](vector_size_t /*row*/) { return 1; };
    auto expectedValueAt = [](vector_size_t row, vector_size_t /*idx*/) {
      return 1 + row % 7;
    };
    auto expectedArrayVector =
        makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
    testSlice(
        "slice(C0, C1, C2)",
        {arrayVector, startsVector, lengthsVector},
        expectedArrayVector);
  }

  {
    auto startsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
    auto lengthsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t /*row*/) { return 2; });
    auto sizeAt = [](vector_size_t /*row*/) { return 7; };
    auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
      return 1 + idx;
    };
    auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

    auto expectedSizeAt = [](vector_size_t row) {
      return (row + 1) % 7 == 0 ? 1 : 2;
    };
    auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
      return 1 + row % 7 + idx;
    };
    auto expectedArrayVector =
        makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
    testSlice(
        "slice(C0, C1, C2)",
        {arrayVector, startsVector, lengthsVector},
        expectedArrayVector);
  }

  {
    auto startsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
    auto lengthsVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t /*row*/) { return 7; });
    auto sizeAt = [](vector_size_t /*row*/) { return 7; };
    auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
      return 1 + idx;
    };
    auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

    auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
    auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
      return 1 + row % 7 + idx;
    };
    auto expectedArrayVector =
        makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
    testSlice(
        "slice(C0, C1, C2)",
        {arrayVector, startsVector, lengthsVector},
        expectedArrayVector);
  }
}

TEST_F(SliceTest, varcharVariableInput) {
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 7; });

  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return StringView(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
  auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
    return StringView(folly::to<std::string>(1 + row % 7 + idx));
  };
  auto expectedArrayVector =
      makeArrayVector<StringView>(kVectorSize, expectedSizeAt, expectedValueAt);

  testSlice(
      "slice(C0, C1, C2)",
      {arrayVector, startsVector, lengthsVector},
      expectedArrayVector);
}

TEST_F(SliceTest, allIndicesGreaterThanArraySize) {
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 8; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 7; });
  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return 1 + idx;
  };
  auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t /*row*/) { return 0; };
  // Not going to be used, created to satisfy function signature.
  auto expectedValueAt = [](vector_size_t row) { return 1 + row % 7; };
  auto expectedArrayVector =
      makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
  testSlice(
      "slice(C0, C1, C2)",
      {arrayVector, startsVector, lengthsVector},
      expectedArrayVector);
}

TEST_F(SliceTest, errorStatesArray) {
  auto startsVector =
      makeFlatVector<int64_t>(kVectorSize, [](vector_size_t row) {
        // Zero out index -> should throw an error.
        if (row == 40) {
          return 0;
        }
        return 1 + row % 7;
      });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 7; });

  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return StringView(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
  auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
    return StringView(folly::to<std::string>(1 + row % 7 + idx));
  };
  auto expectedArrayVector =
      makeArrayVector<StringView>(kVectorSize, expectedSizeAt, expectedValueAt);

  assertUserInvalidArgument(
      [&]() {
        testSlice(
            "slice(C0, C1, C2)",
            {arrayVector, startsVector, lengthsVector},
            expectedArrayVector);
      },
      "SQL array indices start at 1");
}

TEST_F(SliceTest, zeroSliceLength) {
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 0; });

  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return StringView(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t /*row*/) { return 0; };
  auto expectedValueAt = [](vector_size_t /*row*/, vector_size_t /*idx*/) {
    return StringView(folly::to<std::string>());
  };
  auto expectedArrayVector =
      makeArrayVector<StringView>(kVectorSize, expectedSizeAt, expectedValueAt);

  testSlice(
      "slice(C0, C1, C2)",
      {arrayVector, startsVector, lengthsVector},
      expectedArrayVector);
}

TEST_F(SliceTest, negativeSliceLength) {
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return -1; });
  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return 1 + idx;
  };
  auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

  // Not actually used.
  auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
  auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
    return 1 + row % 7 + idx;
  };
  auto expectedArrayVector =
      makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
  assertUserInvalidArgument(
      [&]() {
        testSlice(
            "slice(C0, C1, C2)",
            {arrayVector, startsVector, lengthsVector},
            expectedArrayVector);
      },
      "The value of length argument of slice() function should not be negative");
}
