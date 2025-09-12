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
#include <cstdint>
#include <optional>

#include "velox/common/base/tests/GTestUtils.h"
#include "velox/core/Expressions.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/lib/MakeRowFromMap.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"
#include "velox/vector/SelectivityVector.h"
#include "velox/vector/SimpleVector.h"
#include "velox/vector/TypeAliases.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

namespace facebook::velox::functions {
namespace {

// For testing Opaque types.
struct NonPOD {
  static int alive;

  int x;
  NonPOD(int x = 123) : x(x) {
    ++alive;
  }
  ~NonPOD() {
    --alive;
  }
  bool operator==(const NonPOD& other) const {
    return x == other.x;
  }
};

int NonPOD::alive = 0;

class MakeRowFroMapTest : public testing::Test, public test::VectorTestBase {
 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance(memory::MemoryManager::Options{});
  }

  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();
  }

  template <TypeKind Kind>
  VectorPtr toRowVector(
      const BaseVector& map,
      const MakeRowFromMapOptions& options,
      const SelectivityVector& rows,
      exec::EvalCtx* evalCtx = nullptr) {
    return MakeRowFromMap<Kind>(options).apply(map, rows, evalCtx);
  }

  // Generates a set of test cases for toRowVector. Each test case
  // is a MapVector with the same keys but different value types.
  std::vector<MapVectorPtr> makeTestCases() {
    std::vector<MapVectorPtr> testCases;
    auto map = makeMapVectorFromJson<int64_t, int64_t>(
        {"{1:1, 2:2, 3:3}", // All valid values
         "{2:20, 4:40, 7:3}", // Only one projected key
         "{1:100, 2:null, 6:600}", // Null Value
         "{4:4000, 6:6000}", // No projected keys
         "{1:10000}", // single element, projected key
         "{}", // empty map
         "null"}); // null map
    auto keyType = map->type()->asMap().keyType();
    auto intValues = map->mapValues();
    VELOX_CHECK_EQ(intValues->size(), 12);
    testCases.push_back(map);

    auto makeCopyWithValues = [&](VectorPtr& values) {
      return std::make_shared<MapVector>(
          pool(),
          MAP(keyType, values->type()),
          map->nulls(),
          map->size(),
          map->offsets(),
          map->sizes(),
          map->mapKeys(),
          values);
    };

    // With String Values.
    VectorFuzzer::Options opts{.nullRatio = 0};
    VectorFuzzer fuzzer(opts, pool());
    auto stringValues = fuzzer.fuzzFlat(VARCHAR(), intValues->size());
    stringValues->setNulls(intValues->nulls());
    testCases.push_back(makeCopyWithValues(stringValues));

    // With Array Values.
    auto arrayValues = fuzzer.fuzzFlat(ARRAY(BIGINT()), intValues->size());
    arrayValues->setNulls(intValues->nulls());
    testCases.push_back(makeCopyWithValues(arrayValues));

    // With Row Values.
    auto rowValues = fuzzer.fuzzFlat(ROW({BIGINT()}), intValues->size());
    rowValues->setNulls(intValues->nulls());
    testCases.push_back(makeCopyWithValues(rowValues));

    // With Map Values.
    auto mapValues =
        fuzzer.fuzzFlat(MAP(BIGINT(), BIGINT()), intValues->size());
    mapValues->setNulls(intValues->nulls());
    testCases.push_back(makeCopyWithValues(mapValues));

    auto opaqueValues =
        BaseVector::create(OPAQUE<int>(), intValues->size(), pool());
    auto flat = opaqueValues->asFlatVector<std::shared_ptr<void>>();
    for (int i = 0; i < intValues->size(); i++) {
      flat->set(i, std::make_shared<NonPOD>(i));
    }
    testCases.push_back(makeCopyWithValues(opaqueValues));

    return testCases;
  }

  template <TypeKind ValueKind>
  void verifyDefaultValue(VectorPtr& vector, vector_size_t row) {
    if constexpr (
        (TypeTraits<ValueKind>::isPrimitiveType ||
         ValueKind == TypeKind::OPAQUE) &&
        ValueKind != TypeKind::UNKNOWN) {
      using NativeType = typename TypeTraits<ValueKind>::NativeType;
      static NativeType defaultValue;
      ASSERT_EQ(vector->asFlatVector<NativeType>()->valueAt(row), defaultValue);
    } else if constexpr (
        ValueKind == TypeKind::ARRAY || ValueKind == TypeKind::MAP) {
      auto arrayBaseVector = vector->asChecked<ArrayVectorBase>();
      ASSERT_NE(arrayBaseVector, nullptr);
      ASSERT_EQ(arrayBaseVector->offsetAt(row), 0);
      ASSERT_EQ(arrayBaseVector->sizeAt(row), 0);
    } else {
      VELOX_USER_FAIL(
          "Unsupported type for replacing nulls: {}",
          TypeTraits<ValueKind>::TypeTraits::name);
    }
  }

  // For the case where 'replaceNulls' and 'allowTopLevelNulls' is false, we
  // use this helper function to verify the result by evaluating an expression
  // that mimics the map to row projection.
  void verifyResultUsingExprEval(
      const VectorPtr& result,
      const VectorPtr& input,
      SelectivityVector& rows) {
    auto inputRow = makeRowVector({input});
    // Evaluate an expression the mimics the map to row projection.
    // eg: cast(row_constructor(c0[1], c0[2]) as row(key1 bigint, key2 bigint))
    // Extract keys and wrap it in a struct
    auto typedExpr = core::Expressions::inferTypes(
        parse::parseExpr("row_constructor(c0[1], c0[2])", {}),
        inputRow->type(),
        execCtx_->pool());
    // Wrap the struct in a cast to get the correct field names
    typedExpr =
        std::make_shared<core::CastTypedExpr>(result->type(), typedExpr, false);
    exec::ExprSet exprSet({typedExpr}, execCtx_.get());
    std::vector<VectorPtr> output;
    exec::EvalCtx context(execCtx_.get(), &exprSet, inputRow.get());
    exprSet.eval(rows, context, output);
    test::assertEqualVectors(result, output[0], rows);
  }

  // Helper function that verifies the output of toRowVector. For
  // each row in the result, it retrieves the corresponding value from the
  // original MapVector by looking up the key for that row, if present. If the
  // key is not found, it checks that the value is set to null or to the default
  // value, depending on whether replaceNulls is enabled. As a sanity check, for
  // cases that can be represented as valid expressions, the verification is
  // also performed using expression evaluation to ensure the logic is correct.
  void verifyResult(
      const VectorPtr& result,
      const VectorPtr& input,
      SelectivityVector& rows,
      MakeRowFromMapOptions& options) {
    ASSERT_TRUE(result != nullptr);
    // Assert expected types and field names
    ASSERT_EQ(result->type()->asRow().children().size(), 2);
    ASSERT_EQ(result->type()->asRow().nameOf(0), "key1");
    ASSERT_EQ(result->type()->asRow().nameOf(1), "key2");
    ASSERT_EQ(
        result->type()->asRow().childAt(0), input->type()->asMap().valueType());
    ASSERT_EQ(
        result->type()->asRow().childAt(1), input->type()->asMap().valueType());

    // Assert expected number of rows
    ASSERT_EQ(result->size(), rows.end());

    if (!options.replaceNulls && !options.allowTopLevelNulls) {
      verifyResultUsingExprEval(result, input, rows);
    }

    DecodedVector decodedInput(*input);
    auto map = decodedInput.base()->as<MapVector>();
    auto mapKeys = map->mapKeys()->as<SimpleVector<int64_t>>();
    auto mapValues = map->mapValues();
    auto rawOffsets = map->rawOffsets();
    auto rawSizes = map->rawSizes();

    auto findValueIdx = [&](vector_size_t row,
                            int64_t key) -> std::optional<vector_size_t> {
      if (decodedInput.isNullAt(row)) {
        return std::nullopt;
      }
      auto offset = rawOffsets[decodedInput.index(row)];
      auto size = rawSizes[decodedInput.index(row)];
      for (auto i = offset; i < offset + size; ++i) {
        if (mapKeys->valueAt(i) == key) {
          return i;
        }
      }
      return std::nullopt;
    };
    // Verify expected values
    rows.applyToSelected([&](auto row) {
      // Check for top-level nulls if allowed
      if (options.allowTopLevelNulls && !options.replaceNulls &&
          decodedInput.isNullAt(row)) {
        ASSERT_TRUE(result->isNullAt(row)) << " row idx: " << row;
        return;
      }
      // For every projected key, find the corresponding value in the original
      // map and verify its correctly set in the result.
      for (int64_t key : {1, 2}) {
        auto valueIdx = findValueIdx(row, key);
        auto child = result->as<RowVector>()->childAt(key - 1);
        if (valueIdx.has_value() && !mapValues->isNullAt(valueIdx.value())) {
          ASSERT_TRUE(!child->isNullAt(row));
          if (child->type()->kind() == TypeKind::OPAQUE) {
            auto opaqueValue =
                child->asFlatVector<std::shared_ptr<void>>()->valueAt(row);
            auto expectedOpaqueValue =
                mapValues->as<SimpleVector<std::shared_ptr<void>>>()->valueAt(
                    valueIdx.value());
            EXPECT_EQ(
                *std::static_pointer_cast<NonPOD>(opaqueValue),
                *std::static_pointer_cast<NonPOD>(expectedOpaqueValue));
          } else {
            child->compare(mapValues.get(), row, valueIdx.value());
          }
        } else if (options.replaceNulls) {
          VELOX_DYNAMIC_TYPE_DISPATCH_ALL(
              verifyDefaultValue, child->type()->kind(), child, row);
        } else {
          ASSERT_TRUE(child->isNullAt(row));
        }
      }
    });
  }

  // Helper to create SelectivityVector for different selected patterns
  SelectivityVector createSelectivityVector(
      vector_size_t size,
      const std::string& pattern) {
    SelectivityVector rows(size);
    if (pattern == "all") {
      // All rows selected (default)
    } else if (pattern == "start") {
      // First half selected
      rows.setValidRange(size / 2, size, false);
    } else if (pattern == "end") {
      // Second half selected
      rows.setValidRange(0, size / 2, false);
    } else if (pattern == "mid") {
      // Middle quarter selected
      auto start = size / 4;
      auto end = 3 * size / 4;
      rows.setValidRange(0, start, false);
      rows.setValidRange(end, size, false);
    } else if (pattern == "scattered") {
      // Every 3rd row selected
      rows.clearAll();
      for (auto i = 0; i < size; i += 3) {
        rows.setValid(i, true);
      }
    }
    rows.updateBounds();
    return rows;
  }

  // Helper functions to execute a single test case with different parameters
  // like selected rows and replaceNulls verify the result.
  void executeTestCase(MapVectorPtr& testCase) {
    auto keysToProject = makeFlatVector<int64_t>({1, 2});
    auto outputFieldNames = std::vector<std::string>{"key1", "key2"};
    MakeRowFromMapOptions options{
        .keysToProject = keysToProject,
        .outputFieldNames = outputFieldNames,
        .replaceNulls = false,
        .allowTopLevelNulls = false,
        .throwOnDuplicateKeys = false};
    for (bool replaceNulls : {false, true}) {
      for (bool allowTopLevelNulls : {false, true}) {
        for (bool useEvalCtx : {false, true}) {
          for (std::string selectedRowsStr :
               {"all", "start", "end", "mid", "scattered"}) {
            if (replaceNulls &&
                testCase->type()->asMap().valueType()->kind() ==
                    TypeKind::ROW) {
              continue;
            }
            SCOPED_TRACE(
                "Selected Rows: " + selectedRowsStr +
                " replaceNulls: " + std::to_string(replaceNulls) +
                " allowTopLevelNulls: " + std::to_string(allowTopLevelNulls) +
                " useEvalCtx: " + std::to_string(useEvalCtx) +
                " MapType: " + testCase->type()->toString());
            auto rows =
                createSelectivityVector(testCase->size(), selectedRowsStr);
            options.replaceNulls = replaceNulls;
            options.allowTopLevelNulls = allowTopLevelNulls;
            std::optional<exec::EvalCtx> evalCtx;
            if (useEvalCtx) {
              exec::EvalCtx evalCtx(
                  execCtx_.get(), &dummyExprSet, dummyRowVector.get());
              auto result = toRowVector<TypeKind::BIGINT>(
                  *testCase, options, rows, &evalCtx);
              verifyResult(result, testCase, rows, options);
            } else {
              auto result =
                  toRowVector<TypeKind::BIGINT>(*testCase, options, rows);
              verifyResult(result, testCase, rows, options);
            }
          }
        }
      }
    }
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{velox::core::QueryCtx::create()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  // Dummy EvalCtx and related objects to pass to MakeRowFroMap when
  // testing the code path that uses EvalCtx.
  exec::ExprSet dummyExprSet{{}, execCtx_.get()};
  RowVectorPtr dummyRowVector = makeRowVector({});
};

TEST_F(MakeRowFroMapTest, basic) {
  EXPECT_EQ(NonPOD::alive, 0);
  auto testCases = makeTestCases();
  for (auto& testCase : testCases) {
    executeTestCase(testCase);
    break;
  }
  testCases.clear();
  EXPECT_EQ(NonPOD::alive, 0);
}

TEST_F(MakeRowFroMapTest, unknownTypeValues) {
  auto testCase = makeMapVectorFromJson<int64_t, int64_t>(
      {"{1:1, 2:2, 3:3}", // All valid values
       "{2:20, 4:40, 7:3}", // Only one projected key
       "{1:100, 2:null, 6:600}", // Null Value
       "{4:4000, 6:6000}", // No projected keys
       "{1:10000}", // single element, projected key
       "{}", // empty map
       "null"}); // null map
  auto unknownTypeValues =
      BaseVector::create(UNKNOWN(), testCase->mapValues()->size(), pool());

  testCase = std::make_shared<MapVector>(
      pool(),
      MAP(BIGINT(), UNKNOWN()),
      testCase->nulls(),
      testCase->size(),
      testCase->offsets(),
      testCase->sizes(),
      testCase->mapKeys(),
      unknownTypeValues);

  SelectivityVector rows(testCase->size());
  auto keysToProject = makeFlatVector<int64_t>({1, 2});
  auto outputFieldNames = std::vector<std::string>{"key1", "key2"};
  MakeRowFromMapOptions options{
      .keysToProject = keysToProject,
      .outputFieldNames = outputFieldNames,
      .replaceNulls = false,
      .allowTopLevelNulls = false,
      .throwOnDuplicateKeys = false};
  auto result = toRowVector<TypeKind::BIGINT>(*testCase, options, rows);

  auto expectedType = ROW(outputFieldNames, {UNKNOWN(), UNKNOWN()});
  ASSERT_EQ(*result->type(), *expectedType);
  ASSERT_EQ(result->size(), testCase->size());

  options.replaceNulls = true;
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*testCase, options, rows),
      "Unsupported type for replacing nulls: UNKNOWN");
}

TEST_F(MakeRowFroMapTest, encoded) {
  EXPECT_EQ(NonPOD::alive, 0);
  auto testCases = makeTestCases();
  auto reverseIndices = makeIndicesInReverse(testCases[0]->size());
  auto reverseElementIndices =
      makeIndicesInReverse(testCases[0]->mapKeys()->size());

  // Wrap the top level map with a dictionary and a constant encoding.
  for (auto& testCase : testCases) {
    auto dictOnMap = BaseVector::wrapInDictionary(
        nullptr, reverseIndices, testCase->size(), testCase);
    executeTestCase(testCase);

    auto constOnMap =
        BaseVector::wrapInConstant(testCase->size(), 0, dictOnMap);
    executeTestCase(testCase);
  }

  // Wrap the keys and values with a dictionary encoding.
  for (auto& testCase : testCases) {
    auto elementsSizes = testCase->mapValues()->size();
    auto dictKeys = BaseVector::wrapInDictionary(
        nullptr, reverseElementIndices, elementsSizes, testCase->mapKeys());
    auto dictValues = BaseVector::wrapInDictionary(
        nullptr, reverseElementIndices, elementsSizes, testCase->mapValues());
    testCase->setKeysAndValues(dictKeys, dictValues);
    executeTestCase(testCase);
  }
  testCases.clear();
  EXPECT_EQ(NonPOD::alive, 0);
}

TEST_F(MakeRowFroMapTest, errorCases) {
  SelectivityVector rows(1);
  auto validMap = makeMapVectorFromJson<int64_t, int64_t>({"{1:1, 2:2, 3:3}"});

  // Input of typeKind other than Map used
  MakeRowFromMapOptions options{
      .keysToProject = makeFlatVector<int64_t>({1, 2}),
      .outputFieldNames = {"1", "2"},
      .replaceNulls = false,
      .allowTopLevelNulls = false,
      .throwOnDuplicateKeys = false};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*(validMap->mapKeys()), options, rows),
      "Input must be of MAP typeKind");

  // Mismatched Type between keysToProject and map keys
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(
          *makeMapVectorFromJson<int32_t, int64_t>({"{1:1}"}), options, rows),
      "(INTEGER vs. BIGINT) Map key type and the type of keys to project are not the same");

  // Mismatched Type between keysToProject and template type of toRowVector
  options.keysToProject = makeFlatVector<int32_t>({1, 2});
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Unexptected key TypeKind");

  // Invalid key type
  options.keysToProject = makeFlatVector<int32_t>({1, 2});
  options.keysToProject->setType(DATE());
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::INTEGER>(*validMap, options, rows),
      "Only SMALLINT, INTEGER, BIGINT keys are currently supported, instead got DATE");

  // Keys to project are not empty
  options.keysToProject->resize(0);
  options.outputFieldNames = {"key1", "key2"};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Keys to project cannot be empty");

  // Mismatched size between keysToProject and outputFieldNames
  options.keysToProject = makeFlatVector<int64_t>({1, 2});
  options.outputFieldNames = {"key1"};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Number of keys to project and output field names must be the same");

  // One of the outputFieldNames is empty.
  options.keysToProject = makeFlatVector<int64_t>({1, 2});
  options.outputFieldNames = {"", "key2"};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Field name cannot be empty");

  // One of the outputFieldNames is duplicate.
  options.keysToProject = makeFlatVector<int64_t>({1, 2});
  options.outputFieldNames = {"key1", "key1"};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Duplicate field names are not allowed: key1");

  // Keys to project cannot contain null
  options.keysToProject = makeNullableFlatVector<int64_t>({1, std::nullopt});
  options.outputFieldNames = {"key1", "key2"};
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Keys to project cannot contain null");

  // Duplicate keys not allowed
  options.keysToProject = makeFlatVector<int64_t>({1, 1});
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*validMap, options, rows),
      "Duplicate keys cannot be projected");

  // Replace nulls is true, but map values are of ROW type
  auto rowValues = makeRowVector({makeFlatVector<int64_t>({1, 2, 3})});
  options.keysToProject = makeFlatVector<int64_t>({1, 2});
  options.replaceNulls = true;
  auto mapWithRowValues = std::make_shared<MapVector>(
      pool(),
      MAP(validMap->mapKeys()->type(), rowValues->type()),
      validMap->nulls(),
      validMap->size(),
      validMap->offsets(),
      validMap->sizes(),
      validMap->mapKeys(),
      rowValues);
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*mapWithRowValues, options, rows),
      "Unsupported type for replacing nulls: ROW");
}

TEST_F(MakeRowFroMapTest, duplicateKey) {
  auto testCase = makeMapVector<int64_t, int64_t>({
      {{1, 1}, {2, 2}, {2, 3}},
      {{1, 10}, {2, 20}, {3, 30}},
  });

  MakeRowFromMapOptions options{
      .keysToProject = makeFlatVector<int64_t>({1, 2}),
      .outputFieldNames = std::vector<std::string>{"key1", "key2"},
      .replaceNulls = false,
      .allowTopLevelNulls = false,
      .throwOnDuplicateKeys = true};
  SelectivityVector rows(testCase->size());
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*testCase, options, rows),
      "Duplicate keys not allowed");
  exec::EvalCtx evalCtx(execCtx_.get(), &dummyExprSet, dummyRowVector.get());
  VELOX_ASSERT_THROW(
      toRowVector<TypeKind::BIGINT>(*testCase, options, rows, &evalCtx),
      "Duplicate keys not allowed");

  // Verify that the error is thrown only for the first duplicate key and the
  // rest of the rows are evaluated.
  exec::EvalCtx contextNoThrow(
      execCtx_.get(), &dummyExprSet, dummyRowVector.get());
  *contextNoThrow.mutableThrowOnError() = false;
  auto result =
      toRowVector<TypeKind::BIGINT>(*testCase, options, rows, &contextNoThrow);
  auto expected = makeRowVector(
      options.outputFieldNames,
      {makeNullableFlatVector<int64_t>({std::nullopt, 10}),
       makeNullableFlatVector<int64_t>({std::nullopt, 20})});
  ASSERT_TRUE(contextNoThrow.errors()->hasErrorAt(0));
  SelectivityVector expectedRows(result->size());
  contextNoThrow.deselectErrors(expectedRows);
  test::assertEqualVectors(expected, result, expectedRows);

  // Verify that the value from the last encountered duplicate key is used when
  // duplicate keys are allowed.
  options.throwOnDuplicateKeys = false;
  result = toRowVector<TypeKind::BIGINT>(*testCase, options, rows);
  expected = makeRowVector(
      options.outputFieldNames,
      {makeFlatVector<int64_t>({1, 10}), makeFlatVector<int64_t>({3, 20})});
  test::assertEqualVectors(expected, result);

  // Ensure no error is thrown if the row with the duplicate key is not
  // selected and duplicate keys are not allowed.
  rows.setValid(0, false);
  rows.updateBounds();
  options.throwOnDuplicateKeys = true;
  result = toRowVector<TypeKind::BIGINT>(*testCase, options, rows);
  test::assertEqualVectors(expected, result, rows);
}
} // namespace
} // namespace facebook::velox::functions
