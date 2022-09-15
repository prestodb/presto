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

#include "gtest/gtest.h"

#include "velox/common/base/Exceptions.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorEncoding.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;

namespace facebook::velox::test {

struct OpaqueState;

namespace {
// Specifies an encoding for generating test data. Multiple encodings can be
// nested. The first element of a list of  EncodingOptions gives the base
// encoding, either FLAT or CONSTANT. Subsequent elements add a wrapper, e.g.
// DICTIONARY, SEQUENCE or CONSTANT.
struct EncodingOptions {
  const VectorEncoding::Simple encoding;

  // Specifies the count of values for a FLAT, DICTIONARY or SEQUENCE.
  const int32_t cardinality;

  // Specifies the frequency of nulls added by a DICTIONARY wrapper. 0 means
  // no nulls are added, n means positions divisible by n have a null.
  const int32_t nullFrequency = 0;

  // Allows making two dictionaries with the same indices array.
  const BufferPtr indices = nullptr;

  // If wrapping vector inside a CONSTANT, specifies the element of the
  // wrapped vector which gives the constant value.
  const int32_t constantIndex = 0;

  static EncodingOptions flat(int32_t cardinality) {
    return {VectorEncoding::Simple::FLAT, cardinality};
  }

  static EncodingOptions dictionary(
      int32_t cardinality,
      int32_t nullFrequency) {
    return {VectorEncoding::Simple::DICTIONARY, cardinality, nullFrequency};
  }

  static EncodingOptions dictionary(int32_t cardinality, BufferPtr indices) {
    return {
        VectorEncoding::Simple::DICTIONARY,
        cardinality,
        -1,
        std::move(indices)};
  }

  static EncodingOptions sequence(int32_t runLength) {
    return {VectorEncoding::Simple::SEQUENCE, runLength, -1};
  }

  static EncodingOptions constant(int32_t cardinality, int32_t index) {
    return {VectorEncoding::Simple::CONSTANT, cardinality, -1, nullptr, index};
  }

  std::string toString() const {
    std::stringstream out;
    out << mapSimpleToName(encoding) << ", " << cardinality << " rows";

    switch (encoding) {
      case VectorEncoding::Simple::FLAT:
        break;
      case VectorEncoding::Simple::DICTIONARY:
        if (indices) {
          out << " shared indices";
        } else {
          out << " null every " << nullFrequency;
        }
        break;
      case VectorEncoding::Simple::CONSTANT:
        out << " base index " << constantIndex;
        break;
      default:;
    }

    return out.str();
  }
};

std::string toString(const std::vector<EncodingOptions>& options) {
  std::stringstream out;

  for (auto i = 0; i < options.size(); ++i) {
    if (i > 0) {
      out << ", ";
    }
    out << options[i].toString();
  }
  return out.str();
}

template <typename T>
struct VectorAndReference {
  std::shared_ptr<SimpleVector<T>> vector;
  std::vector<std::optional<T>> reference;
};

struct TestData {
  VectorAndReference<int64_t> bigint1;
  VectorAndReference<int64_t> bigint2;
};
} // namespace

class ExprEncodingsTest
    : public testing::TestWithParam<std::tuple<int, int, bool>>,
      public VectorTestBase {
 protected:
  void SetUp() override {
    // This test throws a lot of exceptions, so turn off stack trace capturing.
    FLAGS_velox_exception_user_stacktrace_enabled = false;

    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();

    testDataType_ = ROW({"bigint1", "bigint2"}, {BIGINT(), BIGINT()});
    // A set of dictionary indices from >= 0 < 100 with many repeats and a few
    // values that occur only at one end.
    constexpr int32_t kTestSize = 10'000;
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(kTestSize, execCtx_->pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (int32_t i = 0; i < kTestSize; ++i) {
      if (i < 1000) {
        rawIndices[i] = i % 20;
      } else if (i > 9000) {
        rawIndices[i] = 80 + (i % 20);
      } else {
        rawIndices[i] = 10 + (i % 80);
      }
    }
    testEncodings_.push_back({EncodingOptions::flat(kTestSize)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::dictionary(kTestSize, indices)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::dictionary(kTestSize, 0)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100), EncodingOptions::constant(kTestSize, 3)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100), EncodingOptions::constant(kTestSize, 7)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::sequence(10),
         EncodingOptions::dictionary(kTestSize, 6)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::dictionary(kTestSize, 6)});
    // A dictionary that masks everything as null.
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::dictionary(kTestSize, 1)});
    testEncodings_.push_back(
        {EncodingOptions::flat(100),
         EncodingOptions::dictionary(1000, 0),
         EncodingOptions::sequence(10)});
  }

  std::shared_ptr<const core::ITypedExpr> parseExpression(
      const std::string& text,
      const RowTypePtr& rowType) {
    auto untyped = parse::parseExpr(text, options_);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_->pool());
  }

  template <typename T>
  void fillVectorAndReference(
      const std::vector<EncodingOptions>& options,
      std::function<std::optional<T>(vector_size_t)> generator,
      VectorAndReference<T>* result,
      bool makeLazyVector) {
    auto& reference = result->reference;
    VectorPtr current;
    for (auto& option : options) {
      int32_t cardinality = option.cardinality;
      switch (option.encoding) {
        case VectorEncoding::Simple::FLAT: {
          VELOX_CHECK(!current, "A flat vector must be in a leaf position");
          reference.resize(cardinality);
          for (int32_t index = 0; index < cardinality; ++index) {
            reference[index] = generator(index);
          }

          auto flatVector = makeNullableFlatVector(reference);

          if (makeLazyVector) {
            current = wrapInLazyDictionary(flatVector);
          } else {
            current = flatVector;
          }
          break;
        }
        case VectorEncoding::Simple::DICTIONARY: {
          VELOX_CHECK(current, "Dictionary must be non-leaf");
          BufferPtr indices;
          BufferPtr nulls;
          std::vector<std::optional<T>> newReference(cardinality);
          if (option.indices) {
            indices = option.indices;
            auto rawIndices = indices->as<vector_size_t>();
            for (auto index = 0; index < cardinality; index++) {
              newReference[index] = reference[rawIndices[index]];
            }
          } else {
            indices = AlignedBuffer::allocate<vector_size_t>(
                cardinality, execCtx_->pool());
            auto rawIndices = indices->asMutable<vector_size_t>();
            uint64_t* rawNulls = nullptr;

            auto nullFrequency = option.nullFrequency;
            if (nullFrequency && !nulls) {
              nulls = AlignedBuffer::allocate<bool>(
                  cardinality, execCtx_->pool(), bits::kNotNull);
              rawNulls = nulls->asMutable<uint64_t>();
            }
            auto baseSize = current->size();
            for (auto index = 0; index < cardinality; index++) {
              rawIndices[index] = index % baseSize;
              if (nullFrequency && index % nullFrequency == 0) {
                bits::setNull(rawNulls, index);
                // Set the index for a position where the dictionary
                // adds a null to be out of range.
                rawIndices[index] =
                    static_cast<vector_size_t>(80'000'000L * index);
                newReference[index] = std::nullopt;
              } else {
                newReference[index] = reference[rawIndices[index]];
              }
            }
          }
          reference = newReference;
          current = BaseVector::wrapInDictionary(
              nulls, indices, cardinality, std::move(current));
          break;
        }
        case VectorEncoding::Simple::CONSTANT: {
          VELOX_CHECK(current, "Constant must be non-leaf");
          auto constantIndex = option.constantIndex;
          current =
              BaseVector::wrapInConstant(cardinality, constantIndex, current);
          std::vector<std::optional<T>> newReference(cardinality);
          for (int32_t i = 0; i < cardinality; ++i) {
            newReference[i] = reference[constantIndex];
          }
          reference = newReference;
          break;
        }
        case VectorEncoding::Simple::SEQUENCE: {
          VELOX_CHECK(current, "Sequence must be non-leaf");
          BufferPtr sizes;
          int runLength = cardinality;
          int currentSize = current->size();
          std::vector<std::optional<T>> newReference(runLength * currentSize);
          sizes = AlignedBuffer::allocate<vector_size_t>(
              currentSize, execCtx_->pool());
          auto rawSizes = sizes->asMutable<vector_size_t>();
          for (auto index = 0; index < currentSize; index++) {
            rawSizes[index] = runLength;
            for (int i = 0; i < runLength; ++i) {
              newReference[index * runLength + i] = reference[index];
            }
          }
          reference = newReference;
          current = BaseVector::wrapInSequence(
              sizes, runLength * currentSize, std::move(current));
          break;
        }
        default:; // nothing to do
      }
    }
    result->vector = std::static_pointer_cast<SimpleVector<T>>(current);

    if (!makeLazyVector) {
      // Test that the reference and the vector with possible wrappers
      // are consistent.
      SelectivityVector rows(current->size());
      auto expected = makeNullableFlatVector<T>(reference);
      assertEqualRows(expected, current, rows);
    }
  }

  void assertEqualRows(
      const VectorPtr& expected,
      const VectorPtr& actual,
      const SelectivityVector& rows) {
    ASSERT_GE(actual->size(), rows.end());
    ASSERT_EQ(expected->typeKind(), actual->typeKind());
    rows.applyToSelected([&](vector_size_t row) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), row, row))
          << "at " << row << ": expected " << expected->toString(row)
          << ", but got " << actual->toString(row);
    });
  }

  template <typename T>
  static void addField(
      const VectorAndReference<T>& field,
      std::vector<VectorPtr>& fields,
      vector_size_t& size) {
    if (field.vector) {
      if (size == -1) {
        size = field.vector->size();
      } else {
        size = std::max<int32_t>(size, field.vector->size());
      }
      fields.push_back(field.vector);
    } else {
      fields.push_back(nullptr);
    }
  }

  std::shared_ptr<RowVector> testDataRow() {
    std::vector<VectorPtr> fields;
    vector_size_t size = -1;
    addField(testData_.bigint1, fields, size);
    addField(testData_.bigint2, fields, size);

    // Keep only non-null fields.
    std::vector<std::string> names;
    std::vector<TypePtr> types;
    std::vector<VectorPtr> nonNullFields;
    for (auto i = 0; i < fields.size(); ++i) {
      if (fields[i]) {
        names.push_back(testDataType_->nameOf(i));
        types.push_back(testDataType_->childAt(i));
        nonNullFields.push_back(fields[i]);
      }
    }

    return std::make_shared<RowVector>(
        execCtx_->pool(),
        ROW(std::move(names), std::move(types)),
        nullptr,
        size,
        std::move(nonNullFields));
  }

  static std::string describeEncoding(const BaseVector* const vector) {
    std::stringstream out;
    auto currentVector = vector;
    for (;;) {
      auto encoding = currentVector->encoding();
      out << encoding;
      if (encoding != VectorEncoding::Simple::DICTIONARY &&
          encoding != VectorEncoding::Simple::SEQUENCE &&
          encoding != VectorEncoding::Simple::CONSTANT) {
        break;
      }
      auto inner = currentVector->valueVector().get();
      if (inner == currentVector || !inner) {
        break;
      }
      out << " over ";
      currentVector = inner;
    }
    return out.str();
  }

  /// Evaluates 'text' expression on 'testDataRow()' twice. First, evaluates the
  /// expression on the first 2/3 of the rows. Then, evaluates the expression on
  /// the last 1/3 of the rows.
  template <typename T>
  void run(
      const std::string& text,
      std::function<std::optional<T>(int32_t)> reference) {
    auto source = {parseExpression(text, testDataType_)};
    auto exprSet = std::make_unique<exec::ExprSet>(source, execCtx_.get());
    auto row = testDataRow();
    exec::EvalCtx context(execCtx_.get(), exprSet.get(), row.get());
    auto size = row->size();

    auto expectedResult = makeFlatVector<T>(
        size,
        [&](auto row) {
          auto v = reference(row);
          return v.has_value() ? v.value() : T();
        },
        [&](auto row) { return !reference(row).has_value(); });

    SelectivityVector allRows(size);
    *context.mutableIsFinalSelection() = false;
    *context.mutableFinalSelection() = &allRows;

    {
      vector_size_t begin = 0;
      vector_size_t end = size / 3 * 2;
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      exprSet->eval(rows, context, result);

      SCOPED_TRACE(text);
      SCOPED_TRACE(fmt::format("[{} - {})", begin, end));
      assertEqualRows(expectedResult, result[0], rows);
    }

    {
      vector_size_t begin = size / 3;
      vector_size_t end = size;
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      exprSet->eval(0, 1, false, rows, context, result);

      SCOPED_TRACE(text);
      SCOPED_TRACE(fmt::format("[{} - {})", begin, end));
      assertEqualRows(expectedResult, result[0], rows);
    }
  }

  static SelectivityVector selectRange(vector_size_t begin, vector_size_t end) {
    SelectivityVector rows(end, false);
    rows.setValidRange(begin, end, true);
    rows.updateBounds();
    return rows;
  }

  void runWithError(const std::string& text) {
    exec::ExprSet exprs({parseExpression(text, testDataType_)}, execCtx_.get());
    auto row = testDataRow();
    exec::EvalCtx context(execCtx_.get(), &exprs, row.get());
    auto size = row->size();

    vector_size_t begin = 0;
    vector_size_t end = size / 3 * 2;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);

      SCOPED_TRACE(text);
      SCOPED_TRACE(fmt::format("[{} - {})", begin, end));
      ASSERT_THROW(exprs.eval(rows, context, result), VeloxException);
    }

    begin = size / 3;
    end = size;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);

      SCOPED_TRACE(text);
      SCOPED_TRACE(fmt::format("[{} - {})", begin, end));
      ASSERT_THROW(
          exprs.eval(0, 1, false, rows, context, result), VeloxException);
    }
  }

  static bool isAllNulls(const VectorPtr& vector) {
    if (!vector->loadedVector()->mayHaveNulls()) {
      return false;
    }
    for (auto i = 0; i < vector->size(); ++i) {
      if (!vector->isNullAt(i)) {
        return false;
      }
    }
    return true;
  }

  VectorPtr wrapInLazyDictionary(VectorPtr vector) {
    return std::make_shared<LazyVector>(
        execCtx_->pool(),
        vector->type(),
        vector->size(),
        std::make_unique<SimpleVectorLoader>([=](RowSet /*rows*/) {
          auto indices =
              makeIndices(vector->size(), [](auto row) { return row; });
          return wrapInDictionary(indices, vector->size(), vector);
        }));
  }

  void prepareTestData() {
    auto& encoding1 = testEncodings_[std::get<0>(GetParam())];
    auto& encoding2 = testEncodings_[std::get<1>(GetParam())];
    bool makeLazyVector = std::get<2>(GetParam());

    fillVectorAndReference<int64_t>(
        encoding1,
        [](int32_t row) {
          return row % 7 == 0 ? std::nullopt
                              : std::optional(static_cast<int64_t>(row));
        },
        &testData_.bigint1,
        makeLazyVector);

    fillVectorAndReference<int64_t>(
        encoding2,
        [](int32_t row) {
          return (row % 11 == 0) ? std::nullopt
                                 : std::optional(static_cast<int64_t>(row));
        },
        &testData_.bigint2,
        makeLazyVector);
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  TestData testData_;
  RowTypePtr testDataType_;
  std::vector<std::vector<EncodingOptions>> testEncodings_;
  parse::ParseOptions options_;
};

#define IS_BIGINT1 testData_.bigint1.reference[row].has_value()
#define IS_BIGINT2 testData_.bigint2.reference[row].has_value()
#define BIGINT1 testData_.bigint1.reference[row].value()
#define BIGINT2 testData_.bigint2.reference[row].value()
#define INT64V(v) std::optional<int64_t>(v)
#define INT64N std::optional<int64_t>()

TEST_P(ExprEncodingsTest, basic) {
  prepareTestData();

  run<int64_t>("2 * bigint1 + 3 * bigint2", [&](int32_t row) {
    if (IS_BIGINT1 && IS_BIGINT2) {
      return INT64V(2 * BIGINT1 + 3 * BIGINT2);
    }
    return INT64N;
  });
}

TEST_P(ExprEncodingsTest, conditional) {
  prepareTestData();

  run<int64_t>(
      "if(bigint1 % 2 = 0, 2 * bigint1 + 10, 3 * bigint2) + 11",
      [&](int32_t row) {
        auto temp = (IS_BIGINT1 && BIGINT1 % 2 == 0) ? INT64V(2 * BIGINT1 + 10)
            : IS_BIGINT2                             ? INT64V(3 * BIGINT2)
                                                     : INT64N;
        return temp.has_value() ? INT64V(temp.value() + 11) : temp;
      });
}

TEST_P(ExprEncodingsTest, moreConditional) {
  prepareTestData();

  run<int64_t>(
      "if(bigint1 % 2 = 0 and bigint2 < 1000 and bigint1 + bigint2 > 0,"
      "  bigint1, bigint2) + 11",
      [&](int32_t row) {
        if ((IS_BIGINT1 && BIGINT1 % 2 == 0) &&
            (IS_BIGINT2 && BIGINT2 < 1000) &&
            (IS_BIGINT1 && IS_BIGINT2 && BIGINT1 + BIGINT2 > 0)) {
          return IS_BIGINT1 ? INT64V(BIGINT1 + 11) : INT64N;
        }
        return IS_BIGINT2 ? INT64V(BIGINT2 + 11) : INT64N;
      });
}

TEST_P(ExprEncodingsTest, errors) {
  prepareTestData();

  if (isAllNulls(testData_.bigint2.vector)) {
    LOG(WARNING)
        << "This test requires input that is not all-null. Skipping it.";
  } else {
    runWithError("bigint2 % 0");
  }
}

TEST_P(ExprEncodingsTest, maskedErrors) {
  prepareTestData();

  // Produce an error if bigint1 is a multiple of 3 or bigint2 is a multiple
  // of 13. Then mask this error by a false. Return 1 for true and 0 for
  // false.
  run<int64_t>(
      "if ((if (bigint1 % 3 = 0, bigint1 % 0 > 1, bigint1 >= 0)"
      "and if(bigint2 % 13 = 0, bigint2 % 0 > 1, bigint2 > 0)"
      "and (bigint1 % 3 > 0) and (bigint2 % 13 > 0)), 1, 0)",
      [&](int32_t row) {
        if (IS_BIGINT1 && BIGINT1 % 3 > 0 && IS_BIGINT2 && BIGINT2 % 13 > 0)
          return 1;
        return 0;
      });
}

TEST_P(ExprEncodingsTest, commonSubExpressions) {
  prepareTestData();

  // Test common subexpressions at top level and inside conditionals.
  run<int64_t>(
      "if(bigint1 % 2 = 0, 2 * (bigint1 + bigint2),"
      "   3 * (bigint1 + bigint2)) + "
      "4 * (bigint1 + bigint2)",
      [&](int32_t row) -> std::optional<int64_t> {
        if (!IS_BIGINT1 || !IS_BIGINT2) {
          return std::nullopt;
        } else {
          auto sum = BIGINT1 + BIGINT2;
          return (BIGINT1 % 2 == 0 ? 2 * sum : 3 * sum) + 4 * sum;
        }
      });
}

INSTANTIATE_TEST_SUITE_P(
    ExprEncodingsTest,
    ExprEncodingsTest,
    testing::Combine(
        testing::Range(0, 9),
        testing::Range(0, 9),
        testing::Bool()));
} // namespace facebook::velox::test
