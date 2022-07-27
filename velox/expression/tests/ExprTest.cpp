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

#include "glog/logging.h"
#include "gtest/gtest.h"

#include "velox/common/base/Exceptions.h"
#include "velox/expression/ConjunctExpr.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/functions/Udf.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorEncoding.h"
#include "velox/vector/tests/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::test;

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
};

template <typename T>
struct VectorAndReference {
  std::shared_ptr<SimpleVector<T>> vector;
  std::vector<std::optional<T>> reference;
};

struct TestData {
  VectorAndReference<int8_t> tinyint1;
  VectorAndReference<int16_t> smallint1;
  VectorAndReference<int32_t> integer1;
  VectorAndReference<int64_t> bigint1;
  VectorAndReference<int64_t> bigint2;
  VectorAndReference<StringView> string1;
  VectorAndReference<bool> bool1;
  VectorAndReference<std::shared_ptr<void>> opaquestate1;
};

} // namespace

class ExprTest : public testing::Test, public VectorTestBase {
 protected:
  void SetUp() override {
    functions::prestosql::registerAllScalarFunctions();
    parse::registerTypeResolver();

    testDataType_ =
        ROW({"tinyint1",
             "smallint1",
             "integer1",
             "bigint1",
             "bigint2",
             "string1",
             "bool1",
             "opaquestate1"},
            {TINYINT(),
             SMALLINT(),
             INTEGER(),
             BIGINT(),
             BIGINT(),
             VARCHAR(),
             BOOLEAN(),
             OPAQUE<OpaqueState>()});
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
      const RowTypePtr& rowType = nullptr) {
    auto untyped = parse::parseExpr(text);
    return core::Expressions::inferTypes(
        untyped, rowType ? rowType : testDataType_, execCtx_->pool());
  }

  std::unique_ptr<exec::ExprSet> compileExpression(
      const std::string& expr,
      const RowTypePtr& rowType) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> expressions = {
        parseExpression(expr, rowType)};
    return std::make_unique<exec::ExprSet>(
        std::move(expressions), execCtx_.get());
  }

  std::vector<VectorPtr> evaluateMultiple(
      const std::vector<std::string>& texts,
      const RowVectorPtr& input) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(input->type());
    std::vector<std::shared_ptr<const core::ITypedExpr>> expressions;
    expressions.reserve(texts.size());
    for (const auto& text : texts) {
      expressions.emplace_back(parseExpression(text, rowType));
    }
    auto exprSet =
        std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_.get());

    exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(expressions.size());
    exprSet->eval(rows, &context, &result);
    return result;
  }

  VectorPtr evaluate(const std::string& text, const RowVectorPtr& input) {
    return evaluateMultiple({text}, input)[0];
  }

  VectorPtr evaluate(exec::ExprSet* exprSet, const RowVectorPtr& input) {
    exec::EvalCtx context(execCtx_.get(), exprSet, input.get());

    SelectivityVector rows(input->size());
    std::vector<VectorPtr> result(1);
    exprSet->eval(rows, &context, &result);
    return result[0];
  }

  template <typename T>
  void fillVectorAndReference(
      const std::vector<EncodingOptions>& options,
      std::function<std::optional<T>(vector_size_t)> generator,
      VectorAndReference<T>* result,
      bool makeLazyVector = false) {
    auto& reference = result->reference;
    VectorPtr current;
    for (auto& option : options) {
      int32_t cardinality = option.cardinality;
      switch (option.encoding) {
        case VectorEncoding::Simple::FLAT: {
          VELOX_CHECK(!current, "A flat vector must be in a leaf position");
          auto flatVector =
              std::dynamic_pointer_cast<FlatVector<T>>(BaseVector::create(
                  CppToType<T>::create(), cardinality, execCtx_->pool()));
          reference.resize(cardinality);
          for (int32_t index = 0; index < cardinality; ++index) {
            reference[index] = generator(index);
            if (reference[index].has_value()) {
              flatVector->set(index, reference[index].value());
            } else {
              flatVector->setNull(index, true);
            }
          };

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
      DecodedVector decoded(*current, rows);
      compare<T>(
          rows, decoded, [&](int32_t row) { return reference[row]; }, "");
    }
  }

  template <typename T>
  void compare(
      const SelectivityVector& rows,
      DecodedVector& decoded,
      std::function<std::optional<T>(int32_t)> reference,
      const std::string& errorPrefix) {
    auto base = decoded.base()->as<SimpleVector<T>>();
    auto indices = decoded.indices();
    auto nulls = decoded.nulls();
    auto nullIndices = decoded.nullIndices();
    rows.applyToSelected([&](int32_t row) {
      auto value = reference(row);
      auto baseRow = indices[row];
      auto nullRow = nullIndices ? nullIndices[row] : row;
      if (value.has_value()) {
        if (nulls && bits::isBitNull(nulls, nullRow)) {
          FAIL() << errorPrefix << ": expected non-null at " << row;
          return;
        }
        if (value != base->valueAt(baseRow)) {
          EXPECT_EQ(value.value(), base->valueAt(baseRow))
              << errorPrefix << ": at " << row;
        }
      } else if (!(nulls && bits::isBitNull(nulls, nullRow))) {
        FAIL() << errorPrefix << ": reference is null and tests is not null at "
               << row;
      }
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
    addField(testData_.tinyint1, fields, size);
    addField(testData_.smallint1, fields, size);
    addField(testData_.integer1, fields, size);
    addField(testData_.bigint1, fields, size);
    addField(testData_.bigint2, fields, size);
    addField(testData_.string1, fields, size);
    addField(testData_.bool1, fields, size);
    addField(testData_.opaquestate1, fields, size);

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

  static std::string makeErrorPrefix(
      const std::string& text,
      const RowVectorPtr& row,
      vector_size_t begin,
      vector_size_t end) {
    std::ostringstream message;
    message << "expression: " << text << ", encodings: ";
    int32_t nonNullChildCount = 0;
    for (auto& child : row->children()) {
      if (child) {
        message << (nonNullChildCount > 0 ? ", " : "")
                << describeEncoding(child.get());
        nonNullChildCount++;
      }
    }
    message << ", begin=" << begin << ", end=" << end << std::endl;
    return message.str();
  }

  template <typename T>
  void runAll(
      const std::string& text,
      std::function<std::optional<T>(int32_t)> reference) {
    auto source = {parseExpression(text)};
    exprs_ = std::make_unique<exec::ExprSet>(std::move(source), execCtx_.get());
    auto row = testDataRow();
    exec::EvalCtx context(execCtx_.get(), exprs_.get(), row.get());
    auto size = row->size();

    auto rows = selectRange(0, size);
    std::vector<VectorPtr> result(1);
    exprs_->eval(rows, &context, &result);
    DecodedVector decoded(*result[0], rows);
    compare(rows, decoded, reference, makeErrorPrefix(text, row, 0, size));

    // reset the caches
    exprs_.reset();
  }

  /// Evaluates 'text' expression on 'testDataRow()' twice. First, evaluates the
  /// expression on the first 2/3 of the rows. Then, evaluates the expression on
  /// the last 1/3 of the rows.
  template <typename T>
  void run(
      const std::string& text,
      std::function<std::optional<T>(int32_t)> reference) {
    auto source = {parseExpression(text)};
    exprs_ = std::make_unique<exec::ExprSet>(source, execCtx_.get());
    auto row = testDataRow();
    exec::EvalCtx context(execCtx_.get(), exprs_.get(), row.get());
    auto size = row->size();

    SelectivityVector allRows(size);
    *context.mutableIsFinalSelection() = false;
    *context.mutableFinalSelection() = &allRows;

    vector_size_t begin = 0;
    vector_size_t end = size / 3 * 2;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      exprs_->eval(rows, &context, &result);
      DecodedVector decoded(*result[0], rows);
      compare(rows, decoded, reference, makeErrorPrefix(text, row, begin, end));
    }

    begin = size / 3;
    end = size;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      exprs_->eval(0, 1, false, rows, &context, &result);
      DecodedVector decoded(*result[0], rows);
      compare(rows, decoded, reference, makeErrorPrefix(text, row, begin, end));
    }
  }

  static SelectivityVector selectRange(vector_size_t begin, vector_size_t end) {
    SelectivityVector rows(end, false);
    rows.setValidRange(begin, end, true);
    rows.updateBounds();
    return rows;
  }

  void runWithError(const std::string& text) {
    exec::ExprSet exprs({parseExpression(text)}, execCtx_.get());
    auto row = testDataRow();
    exec::EvalCtx context(execCtx_.get(), &exprs, row.get());
    auto size = row->size();

    vector_size_t begin = 0;
    vector_size_t end = size / 3 * 2;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      ASSERT_THROW(exprs.eval(rows, &context, &result), VeloxException)
          << makeErrorPrefix(text, row, begin, end);
    }

    begin = size / 3;
    end = size;
    {
      auto rows = selectRange(begin, end);
      std::vector<VectorPtr> result(1);
      ASSERT_THROW(
          exprs.eval(0, 1, false, rows, &context, &result), VeloxException)
          << makeErrorPrefix(text, row, begin, end);
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

  exec::Expr* compileExpression(const std::string& text) {
    std::vector<std::shared_ptr<const core::ITypedExpr>> source = {
        parseExpression(text)};
    exprs_ = std::make_unique<exec::ExprSet>(std::move(source), execCtx_.get());
    return exprs_->expr(0).get();
  }

  template <typename T = ComplexType>
  std::shared_ptr<core::ConstantTypedExpr> makeConstantExpr(
      const VectorPtr& base,
      vector_size_t index) {
    return std::make_shared<core::ConstantTypedExpr>(
        std::make_shared<ConstantVector<T>>(execCtx_->pool(), 1, index, base));
  }

  // Create LazyVector that produces a flat vector and asserts that is is being
  // loaded for a specific set of rows.
  template <typename T>
  std::shared_ptr<LazyVector> makeLazyFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt,
      vector_size_t expectedSize,
      const std::function<vector_size_t(vector_size_t /*index*/)>&
          expectedRowAt) {
    return std::make_shared<LazyVector>(
        execCtx_->pool(),
        CppToType<T>::create(),
        size,
        std::make_unique<SimpleVectorLoader>([=](RowSet rows) {
          VELOX_CHECK_EQ(rows.size(), expectedSize);
          for (auto i = 0; i < rows.size(); i++) {
            VELOX_CHECK_EQ(rows[i], expectedRowAt(i));
          }
          return vectorMaker_.flatVector<T>(size, valueAt, isNullAt);
        }));
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

  void assertError(
      const std::string& expression,
      const VectorPtr& input,
      const std::string& context,
      const std::string& topLevelContext,
      const std::string& message) {
    try {
      evaluate(expression, makeRowVector({input}));
      ASSERT_TRUE(false) << "Expected an error";
    } catch (VeloxException& e) {
      ASSERT_EQ(context, e.context());
      ASSERT_EQ(topLevelContext, e.topLevelContext());
      ASSERT_EQ(message, e.message());
    }
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::createForTest()};
  std::unique_ptr<core::ExecCtx> execCtx_{
      std::make_unique<core::ExecCtx>(pool_.get(), queryCtx_.get())};
  TestData testData_;
  RowTypePtr testDataType_;
  std::unique_ptr<exec::ExprSet> exprs_;
  std::vector<std::vector<EncodingOptions>> testEncodings_;
};

#define IS_BIGINT1 testData_.bigint1.reference[row].has_value()
#define IS_BIGINT2 testData_.bigint2.reference[row].has_value()
#define BIGINT1 testData_.bigint1.reference[row].value()
#define BIGINT2 testData_.bigint2.reference[row].value()
#define INT64V(v) std::optional<int64_t>(v)
#define INT64N std::optional<int64_t>()

TEST_F(ExprTest, encodings) {
  // This test throws a lot of exceptions, so turn off stack trace capturing.
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  int32_t counter = 0;
  for (auto& encoding1 : testEncodings_) {
    fillVectorAndReference<int64_t>(
        encoding1,
        [](int32_t row) {
          return row % 7 == 0 ? std::nullopt
                              : std::optional(static_cast<int64_t>(row));
        },
        &testData_.bigint1);
    for (auto& encoding2 : testEncodings_) {
      fillVectorAndReference<int64_t>(
          encoding2,
          [](int32_t row) {
            return (row % 11 == 0) ? std::nullopt
                                   : std::optional(static_cast<int64_t>(row));
          },
          &testData_.bigint2);
      ++counter;

      run<int64_t>("2 * bigint1 + 3 * bigint2", [&](int32_t row) {
        if (IS_BIGINT1 && IS_BIGINT2) {
          return INT64V(2 * BIGINT1 + 3 * BIGINT2);
        }
        return INT64N;
      });

      run<int64_t>(
          "if(bigint1 % 2 = 0, 2 * bigint1 + 10, 3 * bigint2) + 11",
          [&](int32_t row) {
            auto temp = (IS_BIGINT1 && BIGINT1 % 2 == 0)
                ? INT64V(2 * BIGINT1 + 10)
                : IS_BIGINT2 ? INT64V(3 * BIGINT2)
                             : INT64N;
            return temp.has_value() ? INT64V(temp.value() + 11) : temp;
          });

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

      if (!isAllNulls(testData_.bigint2.vector)) {
        runWithError("bigint2 % 0");
      }

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
  }
}

TEST_F(ExprTest, encodingsOverLazy) {
  // This test throws a lot of exceptions, so turn off stack trace capturing.
  FLAGS_velox_exception_user_stacktrace_enabled = false;
  int32_t counter = 0;
  for (auto& encoding1 : testEncodings_) {
    fillVectorAndReference<int64_t>(
        encoding1,
        [](int32_t row) {
          return row % 7 == 0 ? std::nullopt
                              : std::optional(static_cast<int64_t>(row));
        },
        &testData_.bigint1,
        true);
    for (auto& encoding2 : testEncodings_) {
      fillVectorAndReference<int64_t>(
          encoding2,
          [](int32_t row) {
            return (row % 11 == 0) ? std::nullopt
                                   : std::optional(static_cast<int64_t>(row));
          },
          &testData_.bigint2,
          true);
      ++counter;

      run<int64_t>("2 * bigint1 + 3 * bigint2", [&](int32_t row) {
        if (IS_BIGINT1 && IS_BIGINT2) {
          return INT64V(2 * BIGINT1 + 3 * BIGINT2);
        }
        return INT64N;
      });

      run<int64_t>(
          "if(bigint1 % 2 = 0, 2 * bigint1 + 10, 3 * bigint2) + 11",
          [&](int32_t row) {
            auto temp = (IS_BIGINT1 && BIGINT1 % 2 == 0)
                ? INT64V(2 * BIGINT1 + 10)
                : IS_BIGINT2 ? INT64V(3 * BIGINT2)
                             : INT64N;
            return temp.has_value() ? INT64V(temp.value() + 11) : temp;
          });

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

      if (!isAllNulls(testData_.bigint2.vector)) {
        runWithError("bigint2 % 0");
      }

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
  }
}

TEST_F(ExprTest, moreEncodings) {
  const vector_size_t size = 1'000;
  std::vector<std::string> fruits = {"apple", "pear", "grapes", "pineapple"};
  VectorPtr a = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  VectorPtr b = vectorMaker_.flatVector(fruits);

  // Wrap b in a dictionary.
  auto indices =
      makeIndices(size, [&fruits](auto row) { return row % fruits.size(); });
  b = wrapInDictionary(indices, size, b);

  // Wrap both a and b in another dictionary.
  auto evenIndices = makeIndices(size / 2, [](auto row) { return row * 2; });

  a = wrapInDictionary(evenIndices, size / 2, a);
  b = wrapInDictionary(evenIndices, size / 2, b);

  auto result =
      evaluate("if(c1 = 'grapes', c0 + 10, c0)", makeRowVector({a, b}));
  ASSERT_EQ(VectorEncoding::Simple::DICTIONARY, result->encoding());
  ASSERT_EQ(size / 2, result->size());

  auto expected = makeFlatVector<int64_t>(size / 2, [&fruits](auto row) {
    return (fruits[row * 2 % 4] == "grapes") ? row * 2 + 10 : row * 2;
  });
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, reorder) {
  constexpr int32_t kTestSize = 20000;
  std::vector<EncodingOptions> encoding = {EncodingOptions::flat(kTestSize)};
  fillVectorAndReference<int64_t>(
      encoding,
      [](int32_t row) { return std::optional(static_cast<int64_t>(row)); },
      &testData_.bigint1);
  run<int64_t>(
      "if (bigint1 % 409 < 300 and bigint1 %103 < 30, 1, 2)", [&](int32_t row) {
        return BIGINT1 % 409 < 300 && BIGINT1 % 103 < 30 ? INT64V(1)
                                                         : INT64V(2);
      });
  auto expr = exprs_->expr(0);
  auto condition =
      std::dynamic_pointer_cast<exec::ConjunctExpr>(expr->inputs()[0]);
  EXPECT_TRUE(condition != nullptr);
  // We check that the more efficient filter is first.
  for (auto i = 1; i < condition->inputs().size(); ++i) {
    EXPECT_LE(
        condition->selectivityAt(i - 1).timeToDropValue(),
        condition->selectivityAt(i).timeToDropValue());
  }
}

TEST_F(ExprTest, constant) {
  auto expr = compileExpression("1 + 2 + 3 + 4");
  auto constExpr = dynamic_cast<exec::ConstantExpr*>(expr);
  ASSERT_NE(constExpr, nullptr);
  auto constant = constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  EXPECT_EQ(10, constant);

  expr = compileExpression("bigint1 * (1 + 2 + 3)");
  ASSERT_EQ(2, expr->inputs().size());
  constExpr = dynamic_cast<exec::ConstantExpr*>(expr->inputs()[1].get());
  ASSERT_NE(constExpr, nullptr);
  constant = constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  EXPECT_EQ(6, constant);
}

// Tests that the eval does the right thing when it receives a NULL
// ConstantVector.
TEST_F(ExprTest, constantNull) {
  // Need to manually build the expression since our eval doesn't support type
  // promotion, to upgrade the UNKOWN type generated by the NULL constant.
  auto inputExpr =
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0");
  auto nullConstant = std::make_shared<core::ConstantTypedExpr>(
      variant::null(TypeKind::INTEGER));

  // Builds the following expression: "plus(c0, plus(c0, null))"
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(),
      std::vector<core::TypedExprPtr>{
          inputExpr,
          std::make_shared<core::CallTypedExpr>(
              INTEGER(),
              std::vector<core::TypedExprPtr>{inputExpr, nullConstant},
              "plus"),
      },
      "plus");

  // Execute it and check it returns all null results.
  auto vector = vectorMaker_.flatVectorNullable<int32_t>({1, std::nullopt, 3});
  auto rowVector = makeRowVector({vector});
  SelectivityVector rows(rowVector->size());
  std::vector<VectorPtr> result(1);

  exec::ExprSet exprSet({expression}, execCtx_.get());
  exec::EvalCtx context(execCtx_.get(), &exprSet, rowVector.get());
  exprSet.eval(rows, &context, &result);

  auto expected = vectorMaker_.flatVectorNullable<int32_t>(
      {std::nullopt, std::nullopt, std::nullopt});
  assertEqualVectors(expected, result.front());
}

// Tests that exprCompiler throws if there's a return type mismatch between what
// the user specific in ConstantTypedExpr, and the available signatures.
TEST_F(ExprTest, validateReturnType) {
  auto inputExpr =
      std::make_shared<core::FieldAccessTypedExpr>(INTEGER(), "c0");

  // Builds a "eq(c0, c0)" expression.
  auto expression = std::make_shared<core::CallTypedExpr>(
      INTEGER(), std::vector<core::TypedExprPtr>{inputExpr, inputExpr}, "eq");

  // Execute it and check it returns all null results.
  auto vector = vectorMaker_.flatVectorNullable<int32_t>({1, 2, 3});
  auto rowVector = makeRowVector({vector});
  SelectivityVector rows(rowVector->size());
  std::vector<VectorPtr> result(1);

  EXPECT_THROW(
      {
        exec::ExprSet exprSet({expression}, execCtx_.get());
        exec::EvalCtx context(execCtx_.get(), &exprSet, rowVector.get());
        exprSet.eval(rows, &context, &result);
      },
      VeloxUserError);
}

TEST_F(ExprTest, constantFolding) {
  auto typedExpr = parseExpression("1 + 2");

  auto extractConstant = [](exec::Expr* expr) {
    auto constExpr = dynamic_cast<exec::ConstantExpr*>(expr);
    return constExpr->value()->as<ConstantVector<int64_t>>()->valueAt(0);
  };

  // Check that the constants have been folded.
  {
    exec::ExprSet exprSetFolded({typedExpr}, execCtx_.get(), true);

    auto expr = exprSetFolded.exprs().front();
    auto constExpr = dynamic_cast<exec::ConstantExpr*>(expr.get());

    ASSERT_TRUE(constExpr != nullptr);
    EXPECT_TRUE(constExpr->inputs().empty());
    EXPECT_EQ(3, extractConstant(expr.get()));
  }

  // Check that the constants have NOT been folded.
  {
    exec::ExprSet exprSetUnfolded({typedExpr}, execCtx_.get(), false);
    auto expr = exprSetUnfolded.exprs().front();

    ASSERT_EQ(2, expr->inputs().size());
    EXPECT_EQ(1, extractConstant(expr->inputs()[0].get()));
    EXPECT_EQ(2, extractConstant(expr->inputs()[1].get()));
  }

  {
    // codepoint() takes a single character, so this expression
    // deterministically throws; however, we should never throw at constant
    // folding time. Ensure compiling this expression does not throw..
    auto typedExpr = parseExpression("codepoint('abcdef')");
    EXPECT_NO_THROW(exec::ExprSet exprSet({typedExpr}, execCtx_.get(), true));
  }
}

TEST_F(ExprTest, constantArray) {
  auto a = makeArrayVector<int32_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });
  auto b = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 7; }, [](auto row) { return row; });

  std::vector<std::shared_ptr<const core::ITypedExpr>> expressions = {
      makeConstantExpr(a, 3), makeConstantExpr(b, 5)};

  auto exprSet =
      std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_.get());

  const vector_size_t size = 1'000;
  auto input = vectorMaker_.rowVector(ROW({}), size);
  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  SelectivityVector rows(input->size());
  std::vector<VectorPtr> result(2);
  exprSet->eval(rows, &context, &result);

  ASSERT_TRUE(a->equalValueAt(result[0].get(), 3, 0));
  ASSERT_TRUE(b->equalValueAt(result[1].get(), 5, 0));
}

TEST_F(ExprTest, constantComplexNull) {
  std::vector<core::TypedExprPtr> expressions = {
      std::make_shared<const core::ConstantTypedExpr>(
          ARRAY(BIGINT()), variant::null(TypeKind::ARRAY)),
      std::make_shared<const core::ConstantTypedExpr>(
          MAP(VARCHAR(), BIGINT()), variant::null(TypeKind::MAP)),
      std::make_shared<const core::ConstantTypedExpr>(
          ROW({SMALLINT(), BIGINT()}), variant::null(TypeKind::ROW))};
  auto exprSet =
      std::make_unique<exec::ExprSet>(std::move(expressions), execCtx_.get());

  const vector_size_t size = 10;
  auto input = makeRowVector(ROW({}), size);
  exec::EvalCtx context(execCtx_.get(), exprSet.get(), input.get());

  SelectivityVector rows(size);
  std::vector<VectorPtr> result(3);
  exprSet->eval(rows, &context, &result);

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[0]->encoding());
  ASSERT_EQ(TypeKind::ARRAY, result[0]->typeKind());
  ASSERT_TRUE(result[0]->as<ConstantVector<ComplexType>>()->isNullAt(0));

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[1]->encoding());
  ASSERT_EQ(TypeKind::MAP, result[1]->typeKind());
  ASSERT_TRUE(result[1]->as<ConstantVector<ComplexType>>()->isNullAt(0));

  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, result[2]->encoding());
  ASSERT_EQ(TypeKind::ROW, result[2]->typeKind());
  ASSERT_TRUE(result[2]->as<ConstantVector<ComplexType>>()->isNullAt(0));
}

TEST_F(ExprTest, constantScalarEquals) {
  auto a = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto b = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto c = makeFlatVector<int64_t>(10, [](auto row) { return row; });

  ASSERT_EQ(*makeConstantExpr<int32_t>(a, 3), *makeConstantExpr<int32_t>(b, 3));
  // The types differ, so not equal
  ASSERT_FALSE(
      *makeConstantExpr<int32_t>(a, 3) == *makeConstantExpr<int64_t>(c, 3));
  // The values differ, so not equal
  ASSERT_FALSE(
      *makeConstantExpr<int32_t>(a, 3) == *makeConstantExpr<int32_t>(b, 4));
}

TEST_F(ExprTest, constantComplexEquals) {
  auto testConstantEquals =
      // a and b should be equal but distinct vectors.
      // a and c should be vectors with equal values but different types (e.g.
      // int32_t and int64_t).
      [&](const VectorPtr& a, const VectorPtr& b, const VectorPtr& c) {
        ASSERT_EQ(*makeConstantExpr(a, 3), *makeConstantExpr(b, 3));
        // The types differ, so not equal
        ASSERT_FALSE(*makeConstantExpr(a, 3) == *makeConstantExpr(c, 3));
        // The values differ, so not equal
        ASSERT_FALSE(*makeConstantExpr(a, 3) == *makeConstantExpr(b, 4));
      };

  testConstantEquals(
      makeArrayVector<int32_t>(
          10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; }),
      makeArrayVector<int32_t>(
          10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; }),
      makeArrayVector<int64_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row * 3; }));

  testConstantEquals(
      makeMapVector<int32_t, int32_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }),
      makeMapVector<int32_t, int32_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }),
      makeMapVector<int32_t, int64_t>(
          10,
          [](auto /*row*/) { return 5; },
          [](auto row) { return row; },
          [](auto row) { return row * 3; }));

  auto a = makeFlatVector<int32_t>(10, [](auto row) { return row; });
  auto b = makeFlatVector<int64_t>(10, [](auto row) { return row; });

  testConstantEquals(
      makeRowVector({a}), makeRowVector({a}), makeRowVector({b}));
}

namespace {
class PlusConstantFunction : public exec::VectorFunction {
 public:
  explicit PlusConstantFunction(int32_t addition) : addition_(addition) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);

    auto& arg = args[0];

    // The argument may be flat or constant.
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());

    BaseVector::ensureWritable(rows, INTEGER(), context->pool(), result);

    auto flatResult = (*result)->asFlatVector<int32_t>();
    auto rawResult = flatResult->mutableRawValues();

    flatResult->clearNulls(rows);

    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<int32_t>>()->valueAt(0);
      rows.applyToSelected(
          [&](auto row) { rawResult[row] = value + addition_; });
    } else {
      auto* rawInput = arg->as<FlatVector<int32_t>>()->rawValues();

      rows.applyToSelected(
          [&](auto row) { rawResult[row] = rawInput[row] + addition_; });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // integer -> integer
    return {exec::FunctionSignatureBuilder()
                .returnType("integer")
                .argumentType("integer")
                .build()};
  }

 private:
  const int32_t addition_;
};

} // namespace

TEST_F(ExprTest, dictionaryAndConstantOverLazy) {
  exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5));

  const vector_size_t size = 1'000;

  // Make LazyVector with nulls.
  auto valueAt = [](vector_size_t row) { return row; };
  auto isNullAt = [](vector_size_t row) { return row % 5 == 0; };

  const auto lazyVector =
      vectorMaker_.lazyFlatVector<int32_t>(size, valueAt, isNullAt);
  auto row = makeRowVector({lazyVector});
  auto result = evaluate("plus5(c0)", row);

  auto expected = makeFlatVector<int32_t>(
      size, [](auto row) { return row + 5; }, isNullAt);
  assertEqualVectors(expected, result);

  // Wrap LazyVector in a dictionary (select only even rows).
  auto evenIndices = makeIndices(size / 2, [](auto row) { return row * 2; });

  auto vector = wrapInDictionary(evenIndices, size / 2, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeFlatVector<int32_t>(
      size / 2, [](auto row) { return row * 2 + 5; }, isNullAt);
  assertEqualVectors(expected, result);

  // non-null constant
  vector = BaseVector::wrapInConstant(size, 3, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeFlatVector<int32_t>(size, [](auto /*row*/) { return 3 + 5; });
  assertEqualVectors(expected, result);

  // null constant
  vector = BaseVector::wrapInConstant(size, 5, lazyVector);
  row = makeRowVector({vector});
  result = evaluate("plus5(c0)", row);

  expected = makeAllNullFlatVector<int32_t>(size);
  assertEqualVectors(expected, result);
}

// Test evaluating single-argument vector function on a non-zero row of
// constant vector.
TEST_F(ExprTest, vectorFunctionOnConstantInput) {
  exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5));
  const vector_size_t size = 1'000;

  auto row = makeRowVector(
      {makeFlatVector<int64_t>(size, [](auto row) { return row; }),
       makeConstant(3, size)});

  VectorPtr expected = makeFlatVector<int32_t>(
      size, [](auto row) { return row > 5 ? 3 + 5 : 0; });
  auto result = evaluate("if (c0 > 5, plus5(c1), cast(0 as integer))", row);
  assertEqualVectors(expected, result);

  result = evaluate("is_null(c1)", row);
  expected = makeConstant(false, size);
  assertEqualVectors(expected, result);
}

namespace {
// f(n) = n + rand() - non-deterministict function with a single argument
class PlusRandomIntegerFunction : public exec::VectorFunction {
 public:
  bool isDeterministic() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), 1);
    VELOX_CHECK_EQ(args[0]->typeKind(), facebook::velox::TypeKind::INTEGER);

    BaseVector::ensureWritable(rows, INTEGER(), context->pool(), result);
    auto flatResult = (*result)->asFlatVector<int32_t>();

    DecodedVector decoded(*args[0], rows);
    std::srand(1);
    rows.applyToSelected([&](auto row) {
      if (decoded.isNullAt(row)) {
        flatResult->setNull(row, true);
      } else {
        flatResult->set(row, decoded.valueAt<int32_t>(row) + std::rand());
      }
      return true;
    });
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // integer -> integer
    return {exec::FunctionSignatureBuilder()
                .returnType("integer")
                .argumentType("integer")
                .build()};
  }
};
} // namespace

// Test evaluating single-argument non-deterministic vector function on
// constant vector. The function must be called on each row, not just one.
TEST_F(ExprTest, nonDeterministicVectorFunctionOnConstantInput) {
  exec::registerVectorFunction(
      "plus_random",
      PlusRandomIntegerFunction::signatures(),
      std::make_unique<PlusRandomIntegerFunction>());

  const vector_size_t size = 1'000;
  auto row = makeRowVector({makeConstant(10, size)});

  auto result = evaluate("plus_random(c0)", row);

  std::srand(1);
  auto expected = makeFlatVector<int32_t>(
      size, [](auto /*row*/) { return 10 + std::rand(); });
  assertEqualVectors(expected, result);
}

// Verify constant folding doesn't apply to non-deterministic functions.
TEST_F(ExprTest, nonDeterministicConstantFolding) {
  exec::registerVectorFunction(
      "plus_random",
      PlusRandomIntegerFunction::signatures(),
      std::make_unique<PlusRandomIntegerFunction>());

  const vector_size_t size = 1'000;
  auto emptyRow = vectorMaker_.rowVector(ROW({}, {}), size);

  auto result = evaluate("plus_random(cast(23 as integer))", emptyRow);

  std::srand(1);
  auto expected = makeFlatVector<int32_t>(
      size, [](auto /*row*/) { return 23 + std::rand(); });
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, shortCircuit) {
  vector_size_t size = 4;

  auto a = makeConstant(10, size);
  auto b = makeFlatVector<int32_t>({-1, -2, -3, -4});

  auto result = evaluate("c0 > 0 OR c1 > 0", makeRowVector({a, b}));
  auto expectedResult = makeConstant(true, size);

  assertEqualVectors(expectedResult, result);

  result = evaluate("c0 < 0 AND c1 < 0", makeRowVector({a, b}));
  expectedResult = makeConstant(false, size);

  assertEqualVectors(expectedResult, result);
}

// Test common sub-expression (CSE) optimization with encodings.
// CSE evaluation may happen in different contexts, e.g. original input rows
// on first evaluation and base vectors uncovered through peeling of encodings
// on second. In this case, the row numbers from first evaluation and row
// numbers in the second evaluation are non-comparable.
//
// Consider two projections:
//  if (a > 0 AND c = 'apple')
//  if (b > 0 AND c = 'apple')
//
// c = 'apple' is CSE. Let a be flat vector, b and c be dictionaries with
// shared indices. On first evaluation, 'a' and 'c' don't share any encodings,
// no peeling happens and context contains the original vectors and rows. On
// second evaluation, 'b' and 'c' share dictionary encoding and it gets
// peeled. Context now contains base vectors and inner rows.
//
// Currently, this case doesn't work properly, hence, peeling disables CSE
// optimizations.
TEST_F(ExprTest, cseEncodings) {
  auto a = makeFlatVector<int32_t>({1, 2, 3, 4, 5});

  auto indices = makeIndices({0, 0, 0, 1, 1});
  auto b = wrapInDictionary(indices, 5, makeFlatVector<int32_t>({11, 15}));
  auto c = wrapInDictionary(
      indices, 5, makeFlatVector<std::string>({"apple", "banana"}));

  auto results = evaluateMultiple(
      {"if (c0 > 0 AND c2 = 'apple', 10, 3)",
       "if (c1 > 0 AND c2 = 'apple', 20, 5)"},
      makeRowVector({a, b, c}));

  auto expected = makeFlatVector<int64_t>({10, 10, 10, 3, 3});
  assertEqualVectors(expected, results[0]);

  expected = makeFlatVector<int64_t>({20, 20, 20, 5, 5});
  assertEqualVectors(expected, results[1]);
}

namespace {
class AddSuffixFunction : public exec::VectorFunction {
 public:
  explicit AddSuffixFunction(const std::string& suffix) : suffix_{suffix} {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto input = args[0]->asFlatVector<StringView>();
    auto localResult = std::dynamic_pointer_cast<FlatVector<StringView>>(
        BaseVector::create(VARCHAR(), rows.end(), context->pool()));
    rows.applyToSelected([&](auto row) {
      auto value = fmt::format("{}{}", input->valueAt(row).str(), suffix_);
      localResult->set(row, StringView(value));
      return true;
    });

    context->moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("varchar")
                .build()};
  }

 private:
  const std::string suffix_;
};
} // namespace

// Test CSE evaluation where first evaluation applies to fewer rows then
// second. Make sure values calculated on first evaluation are preserved when
// calculating additional rows on second evaluation. This could happen if CSE
// is a function that uses EvalCtx::moveOrCopyResult which relies on
// isFinalSelection flag.
TEST_F(ExprTest, csePartialEvaluation) {
  exec::registerVectorFunction(
      "add_suffix",
      AddSuffixFunction::signatures(),
      std::make_unique<AddSuffixFunction>("_xx"));

  auto a = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto b = makeFlatVector<std::string>({"a", "b", "c", "d", "e"});

  auto results = evaluateMultiple(
      {"if (c0 >= 3, add_suffix(c1), 'n/a')", "add_suffix(c1)"},
      makeRowVector({a, b}));

  auto expected =
      makeFlatVector<std::string>({"n/a", "n/a", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[0]);

  expected =
      makeFlatVector<std::string>({"a_xx", "b_xx", "c_xx", "d_xx", "e_xx"});
  assertEqualVectors(expected, results[1]);
}

// Checks that vector function registry overwrites if multiple registry
// attempts are made for the same functions.
TEST_F(ExprTest, overwriteInRegistry) {
  auto inserted = exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(500),
      true);
  ASSERT_TRUE(inserted);

  auto vectorFunction = exec::getVectorFunction("plus5", {}, {});
  ASSERT_TRUE(vectorFunction != nullptr);

  inserted = exec::registerVectorFunction(
      "plus5",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(5),
      true);
  ASSERT_TRUE(inserted);

  auto vectorFunction2 = exec::getVectorFunction("plus5", {}, {});

  ASSERT_TRUE(vectorFunction2 != nullptr);
  ASSERT_TRUE(vectorFunction != vectorFunction2);

  ASSERT_TRUE(inserted);
}

// Check non overwriting path in the function registry

TEST_F(ExprTest, keepInRegistry) {
  // Adding a new function, overwrite = false;

  bool inserted = exec::registerVectorFunction(
      "NonExistingFunction",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(500),
      false);

  ASSERT_TRUE(inserted);

  auto vectorFunction = exec::getVectorFunction("NonExistingFunction", {}, {});

  inserted = exec::registerVectorFunction(
      "NonExistingFunction",
      PlusConstantFunction::signatures(),
      std::make_unique<PlusConstantFunction>(400),
      false);
  ASSERT_FALSE(inserted);
  ASSERT_EQ(
      vectorFunction, exec::getVectorFunction("NonExistingFunction", {}, {}));
}

TEST_F(ExprTest, lazyVectors) {
  vector_size_t size = 1'000;

  // Make LazyVector with no nulls
  auto valueAt = [](auto row) { return row; };
  auto vector = vectorMaker_.lazyFlatVector<int64_t>(size, valueAt);
  auto row = makeRowVector({vector});

  auto result = evaluate("c0 + coalesce(c0, 1)", row);

  auto expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row * 2; }, nullptr);
  assertEqualVectors(expected, result);

  // Make LazyVector with nulls
  auto isNullAt = [](auto row) { return row % 5 == 0; };
  vector = vectorMaker_.lazyFlatVector<int64_t>(size, valueAt, isNullAt);
  row = makeRowVector({vector});

  result = evaluate("c0 + coalesce(c0, 1)", row);

  expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row * 2; }, isNullAt);
  assertEqualVectors(expected, result);
}

// Tests that lazy vectors are not loaded unnecessarily.
TEST_F(ExprTest, lazyLoading) {
  const vector_size_t size = 1'000;
  VectorPtr vector =
      vectorMaker_.flatVector<int64_t>(size, [](auto row) { return row % 5; });
  VectorPtr lazyVector = std::make_shared<LazyVector>(
      execCtx_->pool(),
      BIGINT(),
      size,
      std::make_unique<test::SimpleVectorLoader>([&](RowSet /*rows*/) {
        VELOX_FAIL("This lazy vector is not expected to be loaded");
        return nullptr;
      }));

  auto result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  auto expected =
      makeFlatVector<int64_t>(size, [](auto row) { return row % 5 - 5; });
  assertEqualVectors(expected, result);

  vector = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 5; }, nullEvery(7));

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 5 - 5; }, nullEvery(7));
  assertEqualVectors(expected, result);

  // Wrap non-lazy vector in a dictionary (repeat each row twice).
  auto evenIndices = makeIndices(size, [](auto row) { return row / 2; });
  vector = wrapInDictionary(evenIndices, size, vector);

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  expected = vectorMaker_.flatVector<int64_t>(
      size,
      [](auto row) { return (row / 2) % 5 - 5; },
      [](auto row) { return (row / 2) % 7 == 0; });
  assertEqualVectors(expected, result);

  // Wrap both vectors in the same dictionary.
  lazyVector = wrapInDictionary(evenIndices, size, lazyVector);

  result = evaluate(
      "if(c0 = 10, c1 + 5, c0 - 5)", makeRowVector({vector, lazyVector}));
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, selectiveLazyLoadingAnd) {
  const vector_size_t size = 1'000;

  // Evaluate AND expression on 3 lazy vectors and verify that each
  // subsequent vector is loaded for fewer rows than the one before.
  // Create 3 identical vectors with values set to row numbers. Use conditions
  // that pass on half of the rows for the first vector, a third for the
  // second, and a fifth for the third: a % 2 = 0 AND b % 3 = 0 AND c % 5 = 0.
  // Verify that all rows are loaded for the first vector, half for the second
  // and only 1/6 for the third.
  auto valueAt = [](auto row) { return row; };
  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, ceil(size / 2.0), [](auto row) {
        return row * 2;
      });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, ceil(size / 2.0 / 3.0), [](auto row) {
        return row * 2 * 3;
      });

  auto result = evaluate(
      "c0 % 2 = 0 AND c1 % 3 = 0 AND c2 % 5 = 0", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<bool>(
      size, [](auto row) { return row % (2 * 3 * 5) == 0; });
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, selectiveLazyLoadingOr) {
  const vector_size_t size = 1'000;

  // Evaluate OR expression. Columns under OR must be loaded for "all" rows
  // because the engine currently doesn't know whether a column is used
  // elsewhere or not.
  auto valueAt = [](auto row) { return row; };
  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });

  auto result = evaluate(
      "c0 % 2 <> 0 OR c1 % 4 <> 0 OR c2 % 8 <> 0", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<bool>(size, [](auto row) {
    return row % 2 != 0 || row % 4 != 0 || row % 8 != 0;
  });
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, selectiveLazyLoadingIf) {
  const vector_size_t size = 1'000;

  // Evaluate IF expression. Columns under IF must be loaded for "all" rows
  // because the engine currently doesn't know whether a column is used in a
  // single branch (then or else) or in both.
  auto valueAt = [](auto row) { return row; };

  auto a = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto b = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });
  auto c = makeLazyFlatVector<int64_t>(
      size, valueAt, nullptr, size, [](auto row) { return row; });

  auto result =
      evaluate("if (c0 % 2 = 0, c1 + c2, c2 / 3)", makeRowVector({a, b, c}));
  auto expected = makeFlatVector<int64_t>(
      size, [](auto row) { return row % 2 == 0 ? row + row : row / 3; });
  assertEqualVectors(expected, result);
}

namespace {
class StatefulVectorFunction : public exec::VectorFunction {
 public:
  explicit StatefulVectorFunction(
      const std::string& /*name*/,
      const std::vector<exec::VectorFunctionArg>& inputs)
      : numInputs_(inputs.size()) {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK_EQ(args.size(), numInputs_);
    auto numInputs =
        BaseVector::createConstant(numInputs_, rows.size(), context->pool());
    if (!result) {
      *result = numInputs;
    } else {
      BaseVector::ensureWritable(rows, INTEGER(), context->pool(), result);
      (*result)->copy(numInputs.get(), rows, nullptr);
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T... -> integer
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("integer")
                .argumentType("T")
                .variableArity()
                .build()};
  }

 private:
  const int32_t numInputs_;
};
} // namespace

TEST_F(ExprTest, statefulVectorFunctions) {
  exec::registerStatefulVectorFunction(
      "test_function",
      StatefulVectorFunction::signatures(),
      exec::makeVectorFunctionFactory<StatefulVectorFunction>());

  vector_size_t size = 1'000;

  auto a = makeFlatVector<int64_t>(size, [](auto row) { return row; });
  auto b = makeFlatVector<int64_t>(size, [](auto row) { return row * 2; });
  auto row = makeRowVector({a, b});

  {
    auto result = evaluate("test_function(c0)", row);

    auto expected =
        makeFlatVector<int32_t>(size, [](auto /*row*/) { return 1; });
    assertEqualVectors(expected, result);
  }

  {
    auto result = evaluate("test_function(c0, c1)", row);

    auto expected =
        makeFlatVector<int32_t>(size, [](auto /*row*/) { return 2; });
    assertEqualVectors(expected, result);
  }
}

struct OpaqueState {
  static int constructed;
  static int destructed;

  static void clearStats() {
    constructed = 0;
    destructed = 0;
  }

  explicit OpaqueState(int x) : x(x) {
    ++constructed;
  }

  ~OpaqueState() {
    ++destructed;
  }

  int x;
};

int OpaqueState::constructed = 0;
int OpaqueState::destructed = 0;

template <typename T>
struct TestOpaqueCreateFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<std::shared_ptr<OpaqueState>>& out,
      const arg_type<int64_t>& x) {
    out = std::make_shared<OpaqueState>(x);
    return true;
  }
};

template <typename T>
struct TestOpaqueAddFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& out,
      const arg_type<std::shared_ptr<OpaqueState>>& state,
      const arg_type<int64_t>& y) {
    out = state->x + y;
    return true;
  }
};

bool registerTestUDFs() {
  static bool once = [] {
    registerFunction<
        TestOpaqueCreateFunction,
        std::shared_ptr<OpaqueState>,
        int64_t>({"test_opaque_create"});
    registerFunction<
        TestOpaqueAddFunction,
        int64_t,
        std::shared_ptr<OpaqueState>,
        int64_t>({"test_opaque_add"});
    return true;
  }();
  return once;
}

TEST_F(ExprTest, opaque) {
  registerTestUDFs();

  const int kRows = 100;

  OpaqueState::clearStats();

  fillVectorAndReference<int64_t>(
      {EncodingOptions::flat(kRows)},
      [](int32_t row) {
        return row % 7 == 0 ? std::nullopt
                            : std::optional(static_cast<int64_t>(row));
      },
      &testData_.bigint1);
  fillVectorAndReference<int64_t>(
      {EncodingOptions::flat(kRows)},
      [](int32_t row) {
        return (row % 11 == 0) ? std::nullopt
                               : std::optional(static_cast<int64_t>(row * 2));
      },
      &testData_.bigint2);
  fillVectorAndReference<std::shared_ptr<void>>(
      {EncodingOptions::flat(1), EncodingOptions::constant(kRows, 0)},
      [](int32_t) {
        return std::static_pointer_cast<void>(
            std::make_shared<OpaqueState>(123));
      },
      &testData_.opaquestate1);
  EXPECT_EQ(1, OpaqueState::constructed);

  int nonNulls = 0;
  for (int i = 0; i < kRows; ++i) {
    nonNulls += testData_.bigint1.reference[i].has_value() &&
            testData_.bigint2.reference[i].has_value()
        ? 1
        : 0;
  }

  // opaque created each time
  OpaqueState::clearStats();
  runAll<int64_t>(
      "test_opaque_add(test_opaque_create(bigint1), bigint2)",
      [&](int32_t row) {
        if (IS_BIGINT1 && IS_BIGINT2) {
          return INT64V(BIGINT1 + BIGINT2);
        }
        return INT64N;
      });
  EXPECT_EQ(OpaqueState::constructed, nonNulls);
  EXPECT_EQ(OpaqueState::destructed, nonNulls);

  // opaque passed in as a constant explicitly
  OpaqueState::clearStats();
  runAll<int64_t>("test_opaque_add(opaquestate1, bigint2)", [&](int32_t row) {
    if (IS_BIGINT2) {
      return INT64V(123 + BIGINT2);
    }
    return INT64N;
  });
  // nothing got created!
  EXPECT_EQ(OpaqueState::constructed, 0);
  EXPECT_EQ(OpaqueState::destructed, 0);

  // opaque created by a function taking a literal and should be constant
  // folded
  OpaqueState::clearStats();
  runAll<int64_t>(
      "test_opaque_add(test_opaque_create(123), bigint2)", [&](int32_t row) {
        if (IS_BIGINT2) {
          return INT64V(123 + BIGINT2);
        }
        return INT64N;
      });
  EXPECT_EQ(OpaqueState::constructed, 1);
  EXPECT_EQ(OpaqueState::destructed, 1);
}

TEST_F(ExprTest, switchExpr) {
  vector_size_t size = 1'000;
  auto vector = makeRowVector(
      {makeFlatVector<int32_t>(size, [](auto row) { return row; }),
       makeFlatVector<int32_t>(
           size, [](auto row) { return row; }, nullEvery(5)),
       makeConstant<int32_t>(0, size)});

  auto result =
      evaluate("case c0 when 7 then 1 when 11 then 2 else 0 end", vector);
  std::function<int32_t(vector_size_t)> expectedValueAt = [](auto row) {
    switch (row) {
      case 7:
        return 1;
      case 11:
        return 2;
      default:
        return 0;
    }
  };
  auto expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  // c1 has nulls
  result = evaluate("case c1 when 7 then 1 when 11 then 2 else 0 end", vector);
  assertEqualVectors(expected, result);

  // no "else" clause
  result = evaluate("case c0 when 7 then 1 when 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(
      size, expectedValueAt, [](auto row) { return row != 7 && row != 11; });
  assertEqualVectors(expected, result);

  result = evaluate("case c1 when 7 then 1 when 11 then 2 end", vector);
  assertEqualVectors(expected, result);

  // No "else" clause and no match.
  result = evaluate("case 0 when 100 then 1 when 200 then 2 end", vector);
  expected = makeAllNullFlatVector<int64_t>(size);
  assertEqualVectors(expected, result);

  result = evaluate("case c2 when 100 then 1 when 200 then 2 end", vector);
  assertEqualVectors(expected, result);

  // non-equality case expression
  result = evaluate(
      "case when c0 < 7 then 1 when c0 < 11 then 2 else 0 end", vector);
  expectedValueAt = [](auto row) {
    if (row < 7) {
      return 1;
    }
    if (row < 11) {
      return 2;
    }
    return 0;
  };
  expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  result = evaluate(
      "case when c1 < 7 then 1 when c1 < 11 then 2 else 0 end", vector);
  expected = makeFlatVector<int64_t>(size, [](auto row) {
    if (row % 5 == 0) {
      return 0;
    }
    if (row < 7) {
      return 1;
    }
    if (row < 11) {
      return 2;
    }
    return 0;
  });
  assertEqualVectors(expected, result);

  // non-equality case expression, no else clause
  result = evaluate("case when c0 < 7 then 1 when c0 < 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(
      size, expectedValueAt, [](auto row) { return row >= 11; });
  assertEqualVectors(expected, result);

  result = evaluate("case when c1 < 7 then 1 when c1 < 11 then 2 end", vector);
  expected = makeFlatVector<int64_t>(size, expectedValueAt, [](auto row) {
    return row >= 11 || row % 5 == 0;
  });
  assertEqualVectors(expected, result);

  // non-constant then expression
  result = evaluate(
      "case when c0 < 7 then c0 + 5 when c0 < 11 then c0 - 11 "
      "else c0::BIGINT end",
      vector);
  expectedValueAt = [](auto row) {
    if (row < 7) {
      return row + 5;
    }
    if (row < 11) {
      return row - 11;
    }
    return row;
  };
  expected = makeFlatVector<int64_t>(size, expectedValueAt);
  assertEqualVectors(expected, result);

  result = evaluate(
      "case when c1 < 7 then c1 + 5 when c1 < 11 then c1 - 11 "
      "else c1::BIGINT end",
      vector);
  expected = makeFlatVector<int64_t>(size, expectedValueAt, nullEvery(5));
  assertEqualVectors(expected, result);
}

TEST_F(ExprTest, ifWithConstant) {
  vector_size_t size = 4;

  auto a = makeFlatVector<int32_t>({-1, -2, -3, -4});
  auto b = makeConstant(variant(TypeKind::INTEGER), size); // 4 nulls
  auto result = evaluate("is_null(if(c0 > 0, c0, c1))", makeRowVector({a, b}));
  EXPECT_EQ(VectorEncoding::Simple::CONSTANT, result->encoding());
  EXPECT_EQ(true, result->as<ConstantVector<bool>>()->valueAt(0));
}

namespace {
// Testing functions for generating intermediate results in different
// encodings. The test case passes vectors to these and these
// functions make constant/dictionary/sequence vectors from their arguments

// Returns the first value of the argument vector wrapped as a constant.
class TestingConstantFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* /*context*/,
      VectorPtr* result) const override {
    VELOX_CHECK(rows.isAllSelected());
    *result = BaseVector::wrapInConstant(rows.size(), 0, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

// Returns a dictionary vector with values from the first argument
// vector and indices from the second.
class TestingDictionaryFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* /*context*/,
      VectorPtr* result) const override {
    VELOX_CHECK(rows.isAllSelected());
    auto indices = args[1]->as<FlatVector<int32_t>>()->values();
    *result = BaseVector::wrapInDictionary(
        BufferPtr(nullptr), indices, rows.size(), args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T, integer -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .argumentType("integer")
                .build()};
  }
};

// Takes a vector  of values and a vector of integer run lengths.
// Wraps the values in a SequenceVector with the run lengths.

class TestingSequenceFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(rows.isAllSelected());
    auto lengths = args[1]->as<FlatVector<int32_t>>()->values();
    *result = BaseVector::wrapInSequence(lengths, rows.size(), args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T, integer -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .argumentType("integer")
                .build()};
  }
};

// Single-argument deterministic functions always receive their argument
// vector as flat or constant.
class TestingSingleArgDeterministicFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    auto& arg = args[0];
    VELOX_CHECK(arg->isFlatEncoding() || arg->isConstantEncoding());
    BaseVector::ensureWritable(rows, outputType, context->pool(), result);
    (*result)->copy(arg.get(), rows, nullptr);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T -> T
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("T")
                .argumentType("T")
                .build()};
  }
};

} // namespace
VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_constant,
    TestingConstantFunction::signatures(),
    std::make_unique<TestingConstantFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_dictionary,
    TestingDictionaryFunction::signatures(),
    std::make_unique<TestingDictionaryFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_sequence,
    TestingSequenceFunction::signatures(),
    std::make_unique<TestingSequenceFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_testing_single_arg_deterministic,
    TestingSingleArgDeterministicFunction::signatures(),
    std::make_unique<TestingSingleArgDeterministicFunction>());

TEST_F(ExprTest, peelArgs) {
  constexpr int32_t kSize = 100;
  constexpr int32_t kDistinct = 10;
  VELOX_REGISTER_VECTOR_FUNCTION(udf_testing_constant, "testing_constant");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_testing_dictionary, "testing_dictionary");
  VELOX_REGISTER_VECTOR_FUNCTION(udf_testing_sequence, "testing_sequence");
  VELOX_REGISTER_VECTOR_FUNCTION(
      udf_testing_single_arg_deterministic, "testing_single_arg_deterministic");

  std::vector<int32_t> onesSource(kSize, 1);
  std::vector<int32_t> distinctSource(kDistinct);
  std::iota(distinctSource.begin(), distinctSource.end(), 11);
  std::vector<vector_size_t> indicesSource(kSize);
  for (auto i = 0; i < indicesSource.size(); ++i) {
    indicesSource[i] = i % kDistinct;
  }
  std::vector lengthSource(kDistinct, kSize / kDistinct);
  auto allOnes = makeFlatVector<int32_t>(onesSource);

  // constant
  auto result = evaluate("1 + testing_constant(c0)", makeRowVector({allOnes}));
  auto expected64 =
      makeFlatVector<int64_t>(kSize, [](int32_t /*i*/) { return 2; });
  assertEqualVectors(expected64, result);
  result = evaluate(
      "testing_constant(c0) + testing_constant(c1)",
      makeRowVector({allOnes, allOnes}));
  auto expected32 =
      makeFlatVector<int32_t>(kSize, [](int32_t /*i*/) { return 2; });
  assertEqualVectors(expected32, result);

  // Constant and dictionary
  auto distincts = makeFlatVector<int32_t>(distinctSource);
  auto indices = makeFlatVector<int32_t>(indicesSource);
  result = evaluate(
      "testing_constant(c0) + testing_dictionary(c1, c2)",
      makeRowVector({allOnes, distincts, indices}));
  expected32 = makeFlatVector<int32_t>(kSize, [&](int32_t row) {
    return 1 + distinctSource[indicesSource[row]];
  });
  assertEqualVectors(expected32, result);

  auto lengths = makeFlatVector<int32_t>(lengthSource);
  result = evaluate(
      "testing_constant(c0) + testing_sequence(c1, c2)",
      makeRowVector({allOnes, distincts, lengths}));
  expected32 = makeFlatVector<int32_t>(kSize, [&](int32_t row) {
    return 1 + distinctSource[row / (kSize / kDistinct)];
  });

  assertEqualVectors(expected32, result);

  // dictionary and single-argument deterministic
  indices = makeFlatVector<int32_t>(kSize, [](auto) {
    // having all indices to be the same makes DictionaryVector::isConstant()
    // returns true
    return 0;
  });
  result = evaluate(
      "testing_single_arg_deterministic(testing_dictionary(c1, c0))",
      makeRowVector({indices, distincts}));
  expected32 = makeFlatVector<int32_t>(kSize, [](int32_t /*i*/) { return 11; });
  assertEqualVectors(expected32, result);
}

class NullArrayFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    // This function returns a vector of all nulls
    BaseVector::ensureWritable(rows, ARRAY(VARCHAR()), context->pool(), result);
    (*result)->addNulls(nullptr, rows);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // T... -> array(varchar)
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .returnType("array(varchar)")
                .argumentType("T")
                .variableArity()
                .build()};
  }
};

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_null_array,
    NullArrayFunction::signatures(),
    std::make_unique<NullArrayFunction>());

TEST_F(ExprTest, complexNullOutput) {
  VELOX_REGISTER_VECTOR_FUNCTION(udf_null_array, "null_array");

  auto row = makeRowVector({makeAllNullFlatVector<int64_t>(1)});

  auto expectedResults =
      BaseVector::createNullConstant(ARRAY(VARCHAR()), 1, execCtx_->pool());
  auto resultForNulls = evaluate("null_array(NULL, NULL)", row);

  // Making sure the output of the function is the same when returning all
  // null or called on NULL constants
  assertEqualVectors(expectedResults, resultForNulls);
}

TEST_F(ExprTest, rewriteInputs) {
  // rewrite one field
  {
    auto expr = parseExpression(
        "(a + b) * 2.1", ROW({"a", "b"}, {INTEGER(), DOUBLE()}));
    expr = expr->rewriteInputNames({{"a", "alpha"}});

    auto expectedExpr = parseExpression(
        "(alpha + b) * 2.1", ROW({"alpha", "b"}, {INTEGER(), DOUBLE()}));
    ASSERT_EQ(*expectedExpr, *expr);
  }

  // rewrite 2 fields
  {
    auto expr = parseExpression(
        "a + b * c", ROW({"a", "b", "c"}, {INTEGER(), DOUBLE(), DOUBLE()}));
    expr = expr->rewriteInputNames({{"a", "alpha"}, {"b", "beta"}});

    auto expectedExpr = parseExpression(
        "alpha + beta * c",
        ROW({"alpha", "beta", "c"}, {INTEGER(), DOUBLE(), DOUBLE()}));
    ASSERT_EQ(*expectedExpr, *expr);
  }
}

TEST_F(ExprTest, memo) {
  auto base = makeArrayVector<int64_t>(
      1'000,
      [](auto row) { return row % 5 + 1; },
      [](auto row, auto index) { return (row % 3) + index; });

  auto evenIndices = makeIndices(100, [](auto row) { return 8 + row * 2; });
  auto oddIndices = makeIndices(100, [](auto row) { return 9 + row * 2; });

  auto rowType = ROW({"c0"}, {base->type()});
  auto exprSet = compileExpression("c0[1] = 1", rowType);

  auto result = evaluate(
      exprSet.get(), makeRowVector({wrapInDictionary(evenIndices, 100, base)}));
  auto expectedResult = makeFlatVector<bool>(
      100, [](auto row) { return (8 + row * 2) % 3 == 1; });
  assertEqualVectors(expectedResult, result);

  result = evaluate(
      exprSet.get(), makeRowVector({wrapInDictionary(oddIndices, 100, base)}));
  expectedResult = makeFlatVector<bool>(
      100, [](auto row) { return (9 + row * 2) % 3 == 1; });
  assertEqualVectors(expectedResult, result);

  auto everyFifth = makeIndices(100, [](auto row) { return row * 5; });
  result = evaluate(
      exprSet.get(), makeRowVector({wrapInDictionary(everyFifth, 100, base)}));
  expectedResult =
      makeFlatVector<bool>(100, [](auto row) { return (row * 5) % 3 == 1; });
  assertEqualVectors(expectedResult, result);
}

// This test triggers the situation when peelEncodings() produces an empty
// selectivity vector, which if passed to evalWithMemo() causes the latter to
// produce null Expr::dictionaryCache_, which leads to a crash in evaluation
// of subsequent rows. We have fixed that issue with condition and this test
// is for that.
TEST_F(ExprTest, memoNulls) {
  // Generate 5 rows with null string and 5 with a string.
  auto base = makeFlatVector<StringView>(
      10, [](vector_size_t /*row*/) { return StringView("abcdefg"); });

  // Two batches by 5 rows each.
  auto first5Indices = makeIndices(5, [](auto row) { return row; });
  auto last5Indices = makeIndices(5, [](auto row) { return row + 5; });
  // Nulls for the 1st batch.
  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(5, execCtx_->pool(), bits::kNull);

  auto rowType = ROW({"c0"}, {base->type()});
  auto exprSet = compileExpression("STRPOS(c0, 'abc') >= 0", rowType);

  auto result = evaluate(
      exprSet.get(),
      makeRowVector(
          {BaseVector::wrapInDictionary(nulls, first5Indices, 5, base)}));
  // Expecting 5 nulls.
  auto expectedResult =
      BaseVector::createNullConstant(BOOLEAN(), 5, execCtx_->pool());
  assertEqualVectors(expectedResult, result);

  result = evaluate(
      exprSet.get(), makeRowVector({wrapInDictionary(last5Indices, 5, base)}));
  // Expecting 5 trues.
  expectedResult = BaseVector::createConstant(true, 5, execCtx_->pool());
  assertEqualVectors(expectedResult, result);
}

// This test is carefully constructed to exercise calling
// applyFunctionWithPeeling in a situation where inputValues_ can be peeled
// and applyRows and rows are distinct SelectivityVectors.  This test ensures
// we're using applyRows and rows in the right places, if not we should see a
// SIGSEGV.
TEST_F(ExprTest, peelNulls) {
  // Generate 5 distinct values for the c0 column.
  auto c0 = makeFlatVector<StringView>(5, [](vector_size_t row) {
    std::string val = "abcdefg";
    val.append(2, 'a' + row);
    return StringView(val);
  });
  // Generate 5 values for the c1 column.
  auto c1 = makeFlatVector<StringView>(
      5, [](vector_size_t /*row*/) { return StringView("xyz"); });

  // One batch of 5 rows.
  auto c0Indices = makeIndices(5, [](auto row) { return row; });
  auto c1Indices = makeIndices(5, [](auto row) { return row; });

  auto rowType = ROW({"c0", "c1"}, {c0->type(), c1->type()});
  // This expression is very deliberately written this way.
  // REGEXP_EXTRACT will return null for all but row 2, it is important we
  // get nulls and non-nulls so applyRows and rows will be distinct.
  // The result of REVERSE will be collapsed into a constant vector, which
  // is necessary so that the inputValues_ can be peeled.
  // REGEXP_LIKE is the function for which applyFunctionWithPeeling will be
  // called.
  auto exprSet = compileExpression(
      "REGEXP_LIKE(REGEXP_EXTRACT(c0, 'cc'), REVERSE(c1))", rowType);

  // It is important that both columns be wrapped in DictionaryVectors so
  // that they are not peeled until REGEXP_LIKE's children have been
  // evaluated.
  auto result = evaluate(
      exprSet.get(),
      makeRowVector(
          {BaseVector::wrapInDictionary(nullptr, c0Indices, 5, c0),
           BaseVector::wrapInDictionary(nullptr, c1Indices, 5, c1)}));

  // Since c0 only has 'cc' as a substring in row 2, all other rows should be
  // null.
  auto expectedResult = makeFlatVector<bool>(
      5,
      [](vector_size_t /*row*/) { return false; },
      [](vector_size_t row) { return row != 2; });
  assertEqualVectors(expectedResult, result);
}

TEST_F(ExprTest, peelLazyDictionaryOverConstant) {
  auto c0 = makeFlatVector<int64_t>(5, [](vector_size_t row) { return row; });
  auto c0Indices = makeIndices(5, [](auto row) { return row; });
  auto c1 = makeFlatVector<int64_t>(5, [](auto row) { return row; });

  auto result = evaluate(
      "if (not(is_null(if (c0 >= 0, c1, null))), coalesce(c0, 22), null)",
      makeRowVector(
          {BaseVector::wrapInDictionary(
               nullptr, c0Indices, 5, wrapInLazyDictionary(c0)),
           BaseVector::wrapInDictionary(
               nullptr, c0Indices, 5, wrapInLazyDictionary(c1))}));
  assertEqualVectors(c0, result);
}

TEST_F(ExprTest, accessNested) {
  // Construct Row(Row(Row(int))) vector
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({level1});
  auto level3 = makeRowVector({level2});

  auto level1Type = ROW({"c0"}, {base->type()});
  auto level2Type = ROW({"c0"}, {level1Type});
  auto level3Type = ROW({"c0"}, {level2Type});

  // Access level3->level2->level1->base
  // TODO: Expression "c0.c0.c0" currently not supported by DuckDB
  // So we wrap with parentheses to force parsing as struct extract
  // Track https://github.com/duckdb/duckdb/issues/2568
  auto exprSet = compileExpression("(c0).c0.c0", level3Type);

  auto result = evaluate(exprSet.get(), level3);

  assertEqualVectors(base, result);
}

TEST_F(ExprTest, accessNestedNull) {
  // Construct Row(Row(Row(int))) vector
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  auto level1 = makeRowVector({base});

  auto level1Type = ROW({"c0"}, {base->type()});
  auto level2Type = ROW({"c0"}, {level1Type});
  auto level3Type = ROW({"c0"}, {level2Type});

  BufferPtr nulls =
      AlignedBuffer::allocate<bool>(5, execCtx_->pool(), bits::kNull);
  // Construct level 2 row with nulls
  auto level2 = std::make_shared<RowVector>(
      execCtx_->pool(),
      level2Type,
      nulls,
      level1->size(),
      std::vector<VectorPtr>{level1});
  auto level3 = makeRowVector({level2});

  auto exprSet = compileExpression("(c0).c0.c0", level3Type);

  auto result = evaluate(exprSet.get(), level3);

  auto nullVector = makeAllNullFlatVector<int32_t>(5);

  assertEqualVectors(nullVector, result);
}

TEST_F(ExprTest, accessNestedDictionaryEncoding) {
  // Construct Row(Row(Row(int))) vector
  auto base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});

  // Reverse order in dictionary encoding
  auto indices = makeIndices(5, [](auto row) { return 4 - row; });

  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({wrapInDictionary(indices, 5, level1)});
  auto level3 = makeRowVector({level2});

  auto level1Type = ROW({"c0"}, {base->type()});
  auto level2Type = ROW({"c0"}, {level1Type});
  auto level3Type = ROW({"c0"}, {level2Type});

  auto exprSet = compileExpression("(c0).c0.c0", level3Type);

  auto result = evaluate(exprSet.get(), level3);

  assertEqualVectors(makeFlatVector<int32_t>({5, 4, 3, 2, 1}), result);
}

TEST_F(ExprTest, accessNestedConstantEncoding) {
  // Construct Row(Row(Row(int))) vector
  VectorPtr base = makeFlatVector<int32_t>({1, 2, 3, 4, 5});
  // Wrap base in constant
  base = BaseVector::wrapInConstant(5, 2, base);

  auto level1 = makeRowVector({base});
  auto level2 = makeRowVector({level1});
  auto level3 = makeRowVector({level2});

  auto level1Type = ROW({"c0"}, {base->type()});
  auto level2Type = ROW({"c0"}, {level1Type});
  auto level3Type = ROW({"c0"}, {level2Type});

  auto exprSet = compileExpression("(c0).c0.c0", level3Type);

  auto result = evaluate(exprSet.get(), level3);

  assertEqualVectors(makeConstant(3, 5), result);
}

TEST_F(ExprTest, testEmptyVectors) {
  auto a = makeFlatVector<int32_t>({});
  auto result = evaluate("c0 + c0", makeRowVector({a, a}));
  assertEqualVectors(a, result);
}

TEST_F(ExprTest, subsetOfDictOverLazy) {
  // We have dictionaries over LazyVector. We load for some indices in
  // the top dictionary. The intermediate dictionaries refer to
  // non-loaded items in the base of the LazyVector, including indices
  // past its end. We check that we end up with one level of
  // dictionary and no dictionaries that are invalid by through
  // referring to uninitialized/nonexistent positions.
  auto base = makeFlatVector<int32_t>(100, [](auto row) { return row; });
  auto lazy = std::make_shared<LazyVector>(
      execCtx_->pool(),
      INTEGER(),
      1000,
      std::make_unique<test::SimpleVectorLoader>(
          [base](auto /*size*/) { return base; }));
  auto row = makeRowVector({BaseVector::wrapInDictionary(
      nullptr,
      makeIndices(100, [](auto row) { return row; }),
      100,

      BaseVector::wrapInDictionary(
          nullptr,
          makeIndices(1000, [](auto row) { return row; }),
          1000,
          lazy))});

  // We expect a single level of dictionary.
  auto result = evaluate("c0", row);
  EXPECT_EQ(result->encoding(), VectorEncoding::Simple::DICTIONARY);
  EXPECT_EQ(result->valueVector()->encoding(), VectorEncoding::Simple::FLAT);
  assertEqualVectors(result, base);
}

TEST_F(ExprTest, peeledConstant) {
  constexpr int32_t kSubsetSize = 80;
  constexpr int32_t kBaseSize = 160;
  auto indices = makeIndices(kSubsetSize, [](auto row) { return row * 2; });
  auto numbers =
      makeFlatVector<int32_t>(kBaseSize, [](auto row) { return row; });
  auto row = makeRowVector(
      {BaseVector::wrapInDictionary(nullptr, indices, kSubsetSize, numbers),
       BaseVector::createConstant("Hans Pfaal", kBaseSize, execCtx_->pool())});
  auto result = std::dynamic_pointer_cast<SimpleVector<StringView>>(
      evaluate("if (c0 % 4 = 0, c1, null)", row));
  EXPECT_EQ(kSubsetSize, result->size());
  for (auto i = 0; i < kSubsetSize; ++i) {
    if (result->isNullAt(i)) {
      continue;
    }
    EXPECT_LE(1, result->valueAt(i).size());
    // Check that the data is readable.
    EXPECT_NO_THROW(result->toString(i));
  }
}

TEST_F(ExprTest, exceptionContext) {
  auto data = makeRowVector({
      makeFlatVector<int32_t>({1, 2, 3}),
      makeFlatVector<int32_t>({1, 2, 3}),
  });

  try {
    evaluate("c0 + (c0 + c1) % 0", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "plus(cast((c0) as BIGINT), mod(cast((plus(c0, c1)) as BIGINT), 0:BIGINT))",
        e.topLevelContext());
  }

  try {
    evaluate("c0 + (c1 % 0)", data);
    FAIL() << "Expected an exception";
  } catch (const VeloxException& e) {
    ASSERT_EQ("mod(cast((c1) as BIGINT), 0:BIGINT)", e.context());
    ASSERT_EQ(
        "plus(cast((c0) as BIGINT), mod(cast((c1) as BIGINT), 0:BIGINT))",
        e.topLevelContext());
  }
}

/// Verify the output of ConstantExpr::toString().
TEST_F(ExprTest, constantToString) {
  auto arrayVector = vectorMaker_.arrayVectorNullable<float>(
      {{{1.2, 3.4, std::nullopt, 5.6}}});

  exec::ExprSet exprSet(
      {std::make_shared<core::ConstantTypedExpr>(23),
       std::make_shared<core::ConstantTypedExpr>(
           DOUBLE(), variant::null(TypeKind::DOUBLE)),
       makeConstantExpr(arrayVector, 0)},
      execCtx_.get());

  ASSERT_EQ("23:INTEGER", exprSet.exprs()[0]->toString());
  ASSERT_EQ("null:DOUBLE", exprSet.exprs()[1]->toString());
  ASSERT_EQ(
      "4 elements starting at 0 {1.2000000476837158, 3.4000000953674316, null, 5.599999904632568}:ARRAY<REAL>",
      exprSet.exprs()[2]->toString());
}

namespace {
// A naive function that wraps the input in a dictionary vector resized to
// rows.end() - 1.  It assumes all selected values are non-null.
class TestingShrinkingDictionary : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(rows.end(), context->pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    rows.applyToSelected([&](int row) { rawIndices[row] = row; });

    *result =
        BaseVector::wrapInDictionary(nullptr, indices, rows.end() - 1, args[0]);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {exec::FunctionSignatureBuilder()
                .returnType("bigint")
                .argumentType("bigint")
                .build()};
  }
};
} // namespace

TEST_F(ExprTest, specialFormPropagateNulls) {
  exec::registerVectorFunction(
      "test_shrinking_dictionary",
      TestingShrinkingDictionary::signatures(),
      std::make_unique<TestingShrinkingDictionary>());

  // This test verifies an edge case where applyFunctionWithPeeling may produce
  // a result vector which is dictionary encoded and has fewer values than
  // are rows.
  // This can happen when the last value in a column used in an expression is
  // null which causes removeSureNulls to move the end of the SelectivityVector
  // forward.  When we incorrectly use rows.end() as the size of the
  // dictionary when rewrapping the results.
  // Normally this is masked when this vector is used in a function call which
  // produces a new output vector.  However, in SpecialForm expressions, we may
  // return the output untouched, and when we try to add back in the nulls, we
  // get an exception trying to resize the DictionaryVector.
  // This is difficult to reproduce, so this test artificially triggers the
  // issue by using a UDF that returns a dictionary one smaller than rows.end().

  // Making the last row NULL, so we call addNulls in eval.
  auto c0 = makeFlatVector<int64_t>(
      5,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row == 4; });

  auto rowVector = makeRowVector({c0});
  auto evalResult = evaluate("test_shrinking_dictionary(\"c0\")", rowVector);

  auto expectedResult = makeFlatVector<int64_t>(
      5,
      [](vector_size_t row) { return row; },
      [](vector_size_t row) { return row == 4; });
  assertEqualVectors(expectedResult, evalResult);
}

namespace {
template <typename T>
struct AlwaysThrowsFunction {
  template <typename TResult, typename TInput>
  FOLLY_ALWAYS_INLINE void call(TResult&, const TInput&) {
    VELOX_CHECK(false);
  }
};
} // namespace

TEST_F(ExprTest, tryWithConstantFailure) {
  // This test verifies the behavior of constant peeling on a function wrapped
  // in a TRY.  Specifically the case when the UDF executed on the peeled
  // vector throws an exception on the constant value.

  // When wrapping a peeled ConstantVector, the result is wrapped in a
  // ConstantVector.  ConstantVector has special handling logic to copy the
  // underlying string when the type is Varchar.  When an exception is thrown
  // and the StringView isn't initialized, without special handling logic in
  // EvalCtx this results in reading uninitialized memory triggering ASAN
  // errors.
  registerFunction<AlwaysThrowsFunction, Varchar, Varchar>({"always_throws"});
  auto c0 = makeConstant("test", 5);
  auto c1 = makeFlatVector<int64_t>(5, [](vector_size_t row) { return row; });
  auto rowVector = makeRowVector({c0, c1});

  // We use strpos and c1 to ensure that the constant is peeled before calling
  // always_throws, not before the try.
  auto evalResult =
      evaluate("try(strpos(always_throws(\"c0\"), 't', c1))", rowVector);

  auto expectedResult = makeFlatVector<int64_t>(
      5, [](vector_size_t) { return 0; }, [](vector_size_t) { return true; });
  assertEqualVectors(expectedResult, evalResult);
}

TEST_F(ExprTest, castExceptionContext) {
  assertError(
      "cast(c0 as bigint)",
      makeFlatVector<std::string>({"1a"}),
      "cast((c0) as BIGINT)",
      "Same as context.",
      "Failed to cast from VARCHAR to BIGINT: 1a. Non-whitespace character found after end of conversion: \"a\"");

  assertError(
      "cast(c0 as timestamp)",
      makeFlatVector(std::vector<int8_t>{1}),
      "cast((c0) as TIMESTAMP)",
      "Same as context.",
      "Failed to cast from TINYINT to TIMESTAMP: 1. ");
}

TEST_F(ExprTest, switchExceptionContext) {
  assertError(
      "case c0 when 7 then c0 / 0 else 0 end",
      makeFlatVector(std::vector<int64_t>{7}),
      "divide(c0, 0:BIGINT)",
      "switch(eq(c0, 7:BIGINT), divide(c0, 0:BIGINT), 0:BIGINT)",
      "division by zero");
}

TEST_F(ExprTest, conjunctExceptionContext) {
  fillVectorAndReference<int64_t>(
      {EncodingOptions::flat(20)},
      [](int32_t row) { return std::optional(static_cast<int64_t>(row)); },
      &testData_.bigint1);

  try {
    run<int64_t>(
        "if (bigint1 % 409 < 300 and bigint1 / 0 < 30, 1, 2)",
        [&](int32_t /*row*/) { return 0; });
    ASSERT_TRUE(false) << "Expected an error";
  } catch (VeloxException& e) {
    ASSERT_EQ("divide(bigint1, 0:BIGINT)", e.context());
    ASSERT_EQ(
        "switch(and(lt(mod(bigint1, 409:BIGINT), 300:BIGINT), lt(divide(bigint1, 0:BIGINT), 30:BIGINT)), 1:BIGINT, 2:BIGINT)",
        e.topLevelContext());
    ASSERT_EQ("division by zero", e.message());
  }
}

TEST_F(ExprTest, lambdaExceptionContext) {
  auto array = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });
  core::Expressions::registerLambda(
      "lambda1",
      ROW({"x"}, {BIGINT()}),
      ROW({ARRAY(BIGINT())}),
      parse::parseExpr("x / 0 > 1"),
      execCtx_->pool());
  assertError(
      "filter(c0, function('lambda1'))",
      array,
      "divide(x, 0:BIGINT)",
      "filter(c0, (x) -> gt(divide(x, 0:BIGINT), 1:BIGINT))",
      "division by zero");

  core::Expressions::registerLambda(
      "lambda2",
      ROW({"x"}, {BIGINT()}),
      ROW({"c1"}, {INTEGER()}),
      parse::parseExpr("x / c1 > 1"),
      execCtx_->pool());
  assertError(
      "filter(c0, function('lambda2'))",
      array,
      "filter(c0, (x, c1) -> gt(divide(x, cast((c1) as BIGINT)), 1:BIGINT))",
      "Same as context.",
      "Field not found: c1. Available fields are: c0.");
}

/// Verify that null inputs result in exceptions, not crashes.
TEST_F(ExprTest, invalidInputs) {
  auto rowType = ROW({"a"}, {BIGINT()});
  auto exprSet = compileExpression("a + 5", rowType);

  // Try null top-level vector.
  RowVectorPtr input;
  ASSERT_THROW(
      exec::EvalCtx(execCtx_.get(), exprSet.get(), input.get()),
      VeloxRuntimeError);

  // Try non-null vector with null children.
  input = std::make_shared<RowVector>(
      pool_.get(), rowType, nullptr, 1024, std::vector<VectorPtr>{nullptr});
  ASSERT_THROW(
      exec::EvalCtx(execCtx_.get(), exprSet.get(), input.get()),
      VeloxRuntimeError);
}

TEST_F(ExprTest, lambdaWithRowField) {
  auto array = makeArrayVector<int64_t>(
      10, [](auto /*row*/) { return 5; }, [](auto row) { return row * 3; });
  auto row = vectorMaker_.rowVector(
      {"val"},
      {makeFlatVector<int64_t>(10, [](vector_size_t row) { return row; })});
  core::Expressions::registerLambda(
      "lambda1",
      ROW({"x"}, {BIGINT()}),
      ROW({"c0", "c1"}, {ROW({"val"}, {BIGINT()}), ARRAY(BIGINT())}),
      parse::parseExpr("x + c0.val >= 0"),
      execCtx_->pool());

  auto rowVector = vectorMaker_.rowVector({"c0", "c1"}, {row, array});

  // We use strpos and c1 to ensure that the constant is peeled before calling
  // always_throws, not before the try.
  auto evalResult = evaluate("filter(c1, function('lambda1'))", rowVector);

  assertEqualVectors(array, evalResult);
}

TEST_F(ExprTest, flatNoNullsFastPath) {
  auto data = makeRowVector(
      {"a", "b", "c", "d"},
      {
          makeFlatVector<int32_t>({1, 2, 3}),
          makeFlatVector<int32_t>({10, 20, 30}),
          makeFlatVector<float>({0.1, 0.2, 0.3}),
          makeFlatVector<float>({-1.2, 0.0, 10.67}),
      });
  auto rowType = asRowType(data->type());

  // Basic math expressions.

  auto exprSet = compileExpression("a + b", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  auto expectedResult = makeFlatVector<int32_t>({11, 22, 33});
  auto result = evaluate(exprSet.get(), data);
  assertEqualVectors(expectedResult, result);

  exprSet = compileExpression("a + b * 5::integer", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression("floor(c * 1.34::real) / d", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Switch expressions.

  exprSet = compileExpression("if (a > 10::integer, 0, b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // If statement with 'then' or 'else' branch that can return null does not
  // support fast path.
  exprSet = compileExpression("if (a > 10::integer, 0, null)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression(
      "case when a > 10::integer then 1 when b > 10::integer then 2 else 3 end",
      rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Switch without an else clause doesn't support fast path.
  exprSet = compileExpression(
      "case when a > 10::integer then 1 when b > 10::integer then 2 end",
      rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // AND / OR expressions.

  exprSet = compileExpression("a > 10::integer AND b < 0::integer", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  exprSet = compileExpression(
      "a > 10::integer OR (b % 7::integer == 4::integer)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Coalesce expression.

  exprSet = compileExpression("coalesce(a, b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_TRUE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Multiplying an integer by a double requires a cast, but cast doesn't
  // support fast path.
  exprSet = compileExpression("a * 0.1 + b", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();

  // Try expression doesn't support fast path.
  exprSet = compileExpression("try(a / b)", rowType);
  ASSERT_EQ(1, exprSet->exprs().size());
  ASSERT_FALSE(exprSet->exprs()[0]->supportsFlatNoNullsFastPath())
      << exprSet->toString();
}
