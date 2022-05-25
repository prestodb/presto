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

#include <optional>
#include "velox/functions/lib/SubscriptUtil.h"
#include "velox/functions/prestosql/tests/FunctionBaseTest.h"

using namespace facebook::velox;
using namespace facebook::velox::functions::test;
using facebook::velox::functions::SubscriptImpl;

namespace {

class ElementAtTest : public FunctionBaseTest {
 protected:
  static const vector_size_t kVectorSize{1'000};

  template <typename T>
  void testElementAt(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters,
      std::function<T(vector_size_t /* row */)> expectedValueAt,
      std::function<bool(vector_size_t /* row */)> expectedNullAt = nullptr) {
    auto result =
        evaluate<SimpleVector<T>>(expression, makeRowVector(parameters));
    for (vector_size_t i = 0; i < parameters[0]->size(); ++i) {
      auto isNull =
          expectedNullAt ? expectedNullAt(i) : parameters[0]->isNullAt(i);
      EXPECT_EQ(isNull, result->isNullAt(i)) << "at " << i;
      if (!isNull) {
        EXPECT_EQ(expectedValueAt(i), result->valueAt(i)) << "at " << i;
      }
    }
  }

  template <typename T>
  void testVariableInputMap() {
    auto indicesVector = makeFlatVector<T>(
        kVectorSize, [](vector_size_t row) { return row % 7; });

    auto sizeAt = [](vector_size_t row) { return 7; };
    auto keyAt = [](vector_size_t idx) { return idx % 7; };
    auto valueAt = [](vector_size_t idx) { return idx / 7; };
    auto expectedValueAt = [](vector_size_t row) { return row; };
    auto mapVector = makeMapVector<T, T>(kVectorSize, sizeAt, keyAt, valueAt);
    testElementAt<T>(
        "element_at(C0, C1)", {mapVector, indicesVector}, expectedValueAt);
  }

  template <typename T = int64_t>
  std::optional<T> elementAtSimple(
      const std::string& expression,
      const std::vector<VectorPtr>& parameters) {
    auto result =
        evaluate<SimpleVector<T>>(expression, makeRowVector(parameters));
    if (result->size() != 1) {
      throw std::invalid_argument(
          "elementAtSimple expects a single output row.");
    }
    if (result->isNullAt(0)) {
      return std::nullopt;
    }
    return result->valueAt(0);
  }

  // Create a simple vector containing a single map ([10=>10, 11=>11, 12=>12]).
  MapVectorPtr getSimpleMapVector() {
    auto keyAt = [](auto idx) { return idx + 10; };
    auto sizeAt = [](auto) { return 3; };
    auto mapValueAt = [](auto idx) { return idx + 10; };
    return makeMapVector<int64_t, int64_t>(1, sizeAt, keyAt, mapValueAt);
  }
};

template <>
void ElementAtTest::testVariableInputMap<StringView>() {
  auto toStr = [](size_t input) {
    return StringView(folly::sformat("str{}", input));
  };

  auto indicesVector = makeFlatVector<StringView>(
      kVectorSize, [&](vector_size_t row) { return toStr(row % 7); });

  auto sizeAt = [](vector_size_t row) { return 7; };
  auto keyAt = [&](vector_size_t idx) { return toStr(idx % 7); };
  auto valueAt = [&](vector_size_t idx) { return toStr(idx / 7); };
  auto expectedValueAt = [&](vector_size_t row) { return toStr(row); };
  auto mapVector = makeMapVector<StringView, StringView>(
      kVectorSize, sizeAt, keyAt, valueAt);
  testElementAt<StringView>(
      "element_at(C0, C1)", {mapVector, indicesVector}, expectedValueAt);
}

} // namespace

TEST_F(ElementAtTest, constantInputArray) {
  {
    auto arrayVector = makeArrayVector<int64_t>({
        {1, 2, 3, 4, 5, 6, 7},
        {1, 2, 7},
        {1, 2, 3, 5, 6, 7},
    });
    auto expectedValueAt = [](vector_size_t row) { return 2; };

    testElementAt<int64_t>("element_at(C0, 2)", {arrayVector}, expectedValueAt);
    testElementAt<int64_t>("C0[2]", {arrayVector}, expectedValueAt);

    // negative index
    auto expectedValueAt2 = [](vector_size_t row) { return 7; };
    testElementAt<int64_t>(
        "element_at(C0, -1)", {arrayVector}, expectedValueAt2);
  }

  {
    // test variable size arrays + negative index
    // the last element in each array is always "row number".
    auto arrayVector = makeArrayVector<int64_t>({
        {1, 2, 6, 0},
        {1},
        {9, 100, 10043, 44, 22, 2},
        {-1, -10, 3},
    });
    auto expectedValueAt = [](vector_size_t row) { return row; };
    testElementAt<int64_t>(
        "element_at(C0, -1)", {arrayVector}, expectedValueAt);
  }
}

// First flavor - the regular element_at() behavior:
// #1 - start indices at 1.
// #2 - allow out of bounds access for array and map.
// #3 - allow negative indices.
TEST_F(ElementAtTest, allFlavors1) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 11, 12}});
  auto mapVector = getSimpleMapVector();

  // #1
  assertUserInvalidArgument(
      [&]() { elementAtSimple("element_at(C0, 0)", {arrayVector}); },
      "SQL array indices start at 1");

  EXPECT_EQ(
      elementAtSimple("try(element_at(C0, 0))", {arrayVector}), std::nullopt);

  EXPECT_EQ(elementAtSimple("element_at(C0, 1)", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("element_at(C0, 2)", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("element_at(C0, 3)", {arrayVector}), 12);

  // #2
  EXPECT_EQ(elementAtSimple("element_at(C0, 4)", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("element_at(C0, 5)", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("element_at(C0, 1001)", {mapVector}), std::nullopt);

  // #3
  EXPECT_EQ(elementAtSimple("element_at(C0, -1)", {arrayVector}), 12);
  EXPECT_EQ(elementAtSimple("element_at(C0, -2)", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("element_at(C0, -3)", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("element_at(C0, -4)", {arrayVector}), std::nullopt);
}

// Second flavor - the regular subscript ("a[1]") behavior:
// #1 - start indices at 1.
// #2 - do not allow out of bounds access for arrays.
// #3 - do not allow negative indices.
TEST_F(ElementAtTest, allFlavors2) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 11, 12}});
  auto mapVector = getSimpleMapVector();

  // #1
  assertUserInvalidArgument(
      [&]() { elementAtSimple("C0[0]", {arrayVector}); },
      "SQL array indices start at 1");

  EXPECT_EQ(elementAtSimple("try(C0[0])", {arrayVector}), std::nullopt);

  EXPECT_EQ(elementAtSimple("C0[1]", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("C0[2]", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("C0[3]", {arrayVector}), 12);

  // #2
  assertUserInvalidArgument(
      [&]() { elementAtSimple("C0[4]", {arrayVector}); },
      "Array subscript out of bounds.");
  assertUserInvalidArgument(
      [&]() { elementAtSimple("C0[5]", {arrayVector}); },
      "Array subscript out of bounds.");

  EXPECT_EQ(elementAtSimple("try(C0[4])", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("try(C0[5])", {arrayVector}), std::nullopt);

  // Maps are ok.
  EXPECT_EQ(elementAtSimple("C0[1001]", {mapVector}), std::nullopt);

  // #3
  assertUserInvalidArgument(
      [&]() { elementAtSimple("C0[-1]", {arrayVector}); },
      "Array subscript is negative.");
  assertUserInvalidArgument(
      [&]() { elementAtSimple("C0[-4]", {arrayVector}); },
      "Array subscript is negative.");

  EXPECT_EQ(elementAtSimple("try(C0[-1])", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("try(C0[-4])", {arrayVector}), std::nullopt);
}

// Third flavor:
// #1 - indices start at 0.
// #2 - do not allow out of bound access for arrays (throw).
// #3 - null on negative indices.
TEST_F(ElementAtTest, allFlavors3) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 11, 12}});
  auto mapVector = getSimpleMapVector();

  exec::registerVectorFunction(
      "__f2",
      SubscriptImpl<false, true, false, false>::signatures(),
      std::make_unique<SubscriptImpl<false, true, false, false>>());

  // #1
  EXPECT_EQ(elementAtSimple("__f2(C0, 0)", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("__f2(C0, 1)", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("__f2(C0, 2)", {arrayVector}), 12);

  // #2
  assertUserInvalidArgument(
      [&]() { elementAtSimple("__f2(C0, 3)", {arrayVector}); },
      "Array subscript out of bounds.");
  assertUserInvalidArgument(
      [&]() { elementAtSimple("__f2(C0, 4)", {arrayVector}); },
      "Array subscript out of bounds.");
  assertUserInvalidArgument(
      [&]() { elementAtSimple("__f2(C0, 100)", {arrayVector}); },
      "Array subscript out of bounds.");

  EXPECT_EQ(elementAtSimple("try(__f2(C0, 3))", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("try(__f2(C0, 4))", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("try(__f2(C0, 100))", {arrayVector}), std::nullopt);

  // We still allow non-existent map keys, even if out of bounds is disabled for
  // arrays.
  EXPECT_EQ(elementAtSimple("__f2(C0, 1001)", {mapVector}), std::nullopt);

  // #3
  EXPECT_EQ(elementAtSimple("__f2(C0, -1)", {mapVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("__f2(C0, -5)", {mapVector}), std::nullopt);
}

// Fourth flavor:
// #1 - indices start at 0.
// #2 - allow out of bound access (throw).
// #3 - allow negative indices.
TEST_F(ElementAtTest, allFlavors4) {
  auto arrayVector = makeArrayVector<int64_t>({{10, 11, 12}});
  auto mapVector = getSimpleMapVector();

  exec::registerVectorFunction(
      "__f3",
      SubscriptImpl<true, false, true, false>::signatures(),
      std::make_unique<SubscriptImpl<true, false, true, false>>());

  EXPECT_EQ(elementAtSimple("__f3(C0, 0)", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("__f3(C0, 1)", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("__f3(C0, 2)", {arrayVector}), 12);
  EXPECT_EQ(elementAtSimple("__f3(C0, 3)", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("__f3(C0, 4)", {arrayVector}), std::nullopt);
  EXPECT_EQ(elementAtSimple("__f3(C0, -1)", {arrayVector}), 12);
  EXPECT_EQ(elementAtSimple("__f3(C0, -2)", {arrayVector}), 11);
  EXPECT_EQ(elementAtSimple("__f3(C0, -3)", {arrayVector}), 10);
  EXPECT_EQ(elementAtSimple("__f3(C0, -4)", {arrayVector}), std::nullopt);

  EXPECT_EQ(elementAtSimple("__f3(C0, 1001)", {mapVector}), std::nullopt);
}

TEST_F(ElementAtTest, constantInputMap) {
  {
    // Matches a key on every single row.
    auto sizeAt = [](vector_size_t /* row */) { return 5; };
    auto keyAt = [](vector_size_t idx) { return idx % 5; };
    auto valueAt = [](vector_size_t idx) { return (idx % 5) + 10; };
    auto expectedValueAt = [](vector_size_t /* row */) { return 13; };
    auto mapVector =
        makeMapVector<int64_t, int64_t>(kVectorSize, sizeAt, keyAt, valueAt);
    testElementAt<int64_t>("element_at(C0, 3)", {mapVector}, expectedValueAt);
    testElementAt<int64_t>("C0[3]", {mapVector}, expectedValueAt);
  }

  {
    // Match only on row 1, everything else is null.
    auto sizeAt = [](vector_size_t /* row */) { return 5; };
    auto keyAt = [](vector_size_t idx) { return idx + 1; };
    auto valueAt = [](vector_size_t idx) { return idx + 1; };
    auto expectedValueAt = [](vector_size_t /* row */) { return 7; };
    auto expectedNullAt = [](vector_size_t row) { return row != 1; };
    auto mapVector =
        makeMapVector<int64_t, int64_t>(kVectorSize, sizeAt, keyAt, valueAt);
    testElementAt<int64_t>(
        "element_at(C0, 7)", {mapVector}, expectedValueAt, expectedNullAt);

    // Matches nothing.
    auto expectedNullAt2 = [](vector_size_t /* row */) { return true; };
    testElementAt<int64_t>(
        "element_at(C0, 10000)", {mapVector}, expectedValueAt, expectedNullAt2);
    testElementAt<int64_t>(
        "element_at(C0, -1)", {mapVector}, expectedValueAt, expectedNullAt2);
  }

  {
    // Test with IF filtering first.
    auto sizeAt = [](vector_size_t /* row */) { return 5; };
    auto keyAt = [](vector_size_t idx) { return idx % 5; };
    auto valueAt = [](vector_size_t idx) { return (idx % 5) + 10; };
    auto expectedValueAt = [](vector_size_t /* row */) { return 13; };
    auto mapVector =
        makeMapVector<int64_t, int64_t>(kVectorSize, sizeAt, keyAt, valueAt);

    // Boolean columns used for filtering.
    auto boolVector = makeFlatVector<bool>(
        kVectorSize, [](vector_size_t row) { return (row % 3) == 0; });
    auto expectedNullAt = [](vector_size_t row) { return (row % 3) != 0; };

    testElementAt<int64_t>(
        "if(C1, element_at(C0, 3), element_at(C0, 10000))",
        {mapVector, boolVector},
        expectedValueAt,
        expectedNullAt);
  }

  {
    // Vary map value types.
    auto sizeAt = [](vector_size_t /* row */) { return 5; };
    auto keyAt = [](vector_size_t idx) { return idx % 5; };
    auto valueAt = [](vector_size_t idx) { return (idx % 5) + 10; };
    auto expectedValueAt = [](vector_size_t /* row */) { return 13; };
    auto mapVector =
        makeMapVector<int64_t, int16_t>(kVectorSize, sizeAt, keyAt, valueAt);
    testElementAt<int16_t>("element_at(C0, 3)", {mapVector}, expectedValueAt);

    // String map value type.
    auto valueAt2 = [](vector_size_t idx) {
      return StringView(folly::sformat("str{}", idx % 5));
    };
    auto expectedValueAt2 = [](vector_size_t /* row */) {
      return StringView("str3");
    };
    auto mapVector2 = makeMapVector<int64_t, StringView>(
        kVectorSize, sizeAt, keyAt, valueAt2);
    testElementAt<StringView>(
        "element_at(C0, 3)", {mapVector2}, expectedValueAt2);
  }
}

TEST_F(ElementAtTest, variableInputMap) {
  testVariableInputMap<int64_t>(); // BIGINT
  testVariableInputMap<int32_t>(); // INTEGER
  testVariableInputMap<int16_t>(); // SMALLINT
  testVariableInputMap<int8_t>(); // TINYINT

  testVariableInputMap<StringView>(); // VARCHAR
}

TEST_F(ElementAtTest, variableInputArray) {
  {
    auto indicesVector = makeFlatVector<int64_t>(
        kVectorSize, [](vector_size_t row) { return 1 + row % 7; });

    auto sizeAt = [](vector_size_t row) { return 7; };
    auto valueAt = [](vector_size_t row, vector_size_t idx) { return 1 + idx; };
    auto expectedValueAt = [](vector_size_t row) { return 1 + row % 7; };
    auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);
    testElementAt<int64_t>(
        "element_at(C0, C1)", {arrayVector, indicesVector}, expectedValueAt);
  }

  {
    auto indicesVector =
        makeFlatVector<int64_t>(kVectorSize, [](vector_size_t row) {
          if (row % 7 == 0) {
            // every first row should be null
            return 8;
          }
          if (row % 7 == 4) {
            // every fourth out of seven rows should be a negative access
            return -3;
          }
          return 1 + row % 7; // access the last element
        });

    auto sizeAt = [](vector_size_t row) { return row % 7 + 1; };
    auto valueAt = [](vector_size_t row, vector_size_t idx) { return idx; };
    auto expectedValueAt = [](vector_size_t row) {
      if (row % 7 == 4) {
        return row % 7 - 2; // negative access
      }
      return row % 7;
    };
    auto expectedNullAt = [](vector_size_t row) { return row % 7 == 0; };
    auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

    testElementAt<int64_t>(
        "element_at(C0, C1)",
        {arrayVector, indicesVector},
        expectedValueAt,
        expectedNullAt);
  }
}

TEST_F(ElementAtTest, varcharVariableInput) {
  auto indicesVector = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t row) { return 1 + row % 7; });

  auto sizeAt = [](vector_size_t row) { return 7; };
  auto valueAt = [](vector_size_t row, vector_size_t idx) {
    return StringView(folly::to<std::string>(idx + 1));
  };
  auto expectedValueAt = [](vector_size_t row) {
    return StringView(folly::to<std::string>(row % 7 + 1));
  };
  auto arrayVector = makeArrayVector<StringView>(kVectorSize, sizeAt, valueAt);

  testElementAt<StringView>(
      "element_at(C0, C1)", {arrayVector, indicesVector}, expectedValueAt);
}

TEST_F(ElementAtTest, allIndicesGreaterThanArraySize) {
  auto indices = makeFlatVector<int64_t>(
      kVectorSize, [](vector_size_t /*row*/) { return 2; });

  auto sizeAt = [](vector_size_t row) { return 1; };
  auto expectedValueAt = [](vector_size_t row) { return 1; };
  auto valueAt = [](vector_size_t row, vector_size_t idx) { return 1; };
  auto expectedNullAt = [](vector_size_t row) { return true; };

  auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

  testElementAt<int64_t>(
      "element_at(C0, C1)",
      {arrayVector, indices},
      expectedValueAt,
      expectedNullAt);
}

TEST_F(ElementAtTest, errorStatesArray) {
  auto indicesVector =
      makeFlatVector<int64_t>(kVectorSize, [](vector_size_t row) {
        // zero out index -> should throw an error
        if (row == 40) {
          return 0;
        }
        return 1 + row % 7;
      });

  // Make arrays of 1-based sequences of variying lengths: [1, 2], [1, 2, 3],
  // [1, 2, 3, 4], etc.
  auto sizeAt = [](vector_size_t row) { return 2 + row % 7; };
  auto expectedValueAt = [](vector_size_t row) { return 1 + row % 7; };
  auto valueAt = [](vector_size_t /*row*/, vector_size_t idx) {
    return 1 + idx;
  };
  auto arrayVector = makeArrayVector<int64_t>(kVectorSize, sizeAt, valueAt);

  // Trying to use index = 0.
  assertUserInvalidArgument(
      [&]() {
        testElementAt<int64_t>(
            "element_at(C0, C1)",
            {arrayVector, indicesVector},
            expectedValueAt);
      },
      "SQL array indices start at 1");

  testElementAt<int64_t>(
      "try(element_at(C0, C1))",
      {arrayVector, indicesVector},
      expectedValueAt,
      [](auto row) { return row == 40; });
}
