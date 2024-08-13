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
#include "velox/functions/lib/tests/SliceTestBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

namespace facebook::velox::functions::prestosql {
namespace {

class SliceTest : public test::SliceTestBase {
 protected:
  static const vector_size_t kVectorSize{1000};
  static void SetUpTestCase() {
    SliceTestBase::SetUpTestCase();
    registerAllScalarFunctions();
  }
};

TEST_F(SliceTest, basic) {
  basicTestCases();
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
    VELOX_ASSERT_THROW(
        testSlice("slice(C0, 0, 1)", {arrayVector}, expectedArrayVector),
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
    return StringView::makeInline(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
  auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
    return StringView::makeInline(folly::to<std::string>(1 + row % 7 + idx));
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
    return StringView::makeInline(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t row) { return 7 - row % 7; };
  auto expectedValueAt = [](vector_size_t row, vector_size_t idx) {
    return StringView::makeInline(folly::to<std::string>(1 + row % 7 + idx));
  };
  auto expectedArrayVector =
      makeArrayVector<StringView>(kVectorSize, expectedSizeAt, expectedValueAt);

  VELOX_ASSERT_THROW(
      testSlice(
          "slice(C0, C1, C2)",
          {arrayVector, startsVector, lengthsVector},
          expectedArrayVector),
      "SQL array indices start at 1");
}

TEST_F(SliceTest, zeroSliceLength) {
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t row) { return 1 + row % 7; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 0; });

  auto sizeAt = [](vector_size_t /*row*/) { return 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return StringView::makeInline(folly::to<std::string>(idx + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  auto expectedSizeAt = [](vector_size_t /*row*/) { return 0; };
  auto expectedValueAt = [](vector_size_t /*row*/, vector_size_t /*idx*/) {
    return StringView::makeInline(folly::to<std::string>());
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
  VELOX_ASSERT_THROW(
      testSlice(
          "slice(C0, C1, C2)",
          {arrayVector, startsVector, lengthsVector},
          expectedArrayVector),
      "The value of length argument of slice() function should not be negative");
}

TEST_F(SliceTest, constantArrayNonConstantLength) {
  // Tests constant arrays and non-constant starts and lengths. Ensure they
  // don't create overlapping ranges in the output ArrayVector.
  auto startsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 2; });
  auto lengthsVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 2; });
  auto arrayVector = makeConstantArray<int64_t>(kVectorSize, {99, 100, 101});

  auto expectedSizeAt = [](vector_size_t /*row*/) { return 2; };
  auto expectedValueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return idx == 0 ? 100 : 101;
  };
  auto expectedArrayVector =
      makeArrayVector<int64_t>(kVectorSize, expectedSizeAt, expectedValueAt);
  testSlice(
      "slice(C0, C1, C2)",
      {arrayVector, startsVector, lengthsVector},
      expectedArrayVector);
}

} // namespace
} // namespace facebook::velox::functions::prestosql
