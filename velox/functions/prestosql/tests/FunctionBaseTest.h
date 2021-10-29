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
#pragma once

#include <gtest/gtest.h>
#include "velox/expression/Expr.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/type/Type.h"
#include "velox/vector/tests/VectorMaker.h"

namespace facebook::velox::functions::test {

class FunctionBaseTest : public testing::Test {
 public:
  // This class generates test name suffixes based on the type.
  // We use the type's toString() return value as the test name.
  // Used as the third argument for GTest TYPED_TEST_SUITE.
  class TypeNames {
   public:
    template <typename T>
    static std::string GetName(int) {
      T type;
      return type.toString();
    }
  };

  using IntegralTypes =
      ::testing::Types<TinyintType, SmallintType, IntegerType, BigintType>;

  using FloatingPointTypes = ::testing::Types<DoubleType, RealType>;

 protected:
  static void SetUpTestCase();

  template <typename T>
  using EvalType = typename CppToType<T>::NativeType;

  static std::shared_ptr<const RowType> makeRowType(
      std::vector<std::shared_ptr<const Type>>&& types) {
    return velox::test::VectorMaker::rowType(
        std::forward<std::vector<std::shared_ptr<const Type>>&&>(types));
  }

  void setNulls(
      const VectorPtr& vector,
      std::function<bool(vector_size_t /*row*/)> isNullAt) {
    for (vector_size_t i = 0; i < vector->size(); i++) {
      if (isNullAt(i)) {
        vector->setNull(i, true);
      }
    }
  }

  RowVectorPtr makeRowVector(
      const std::vector<std::string>& childNames,
      const std::vector<VectorPtr>& children,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    auto rowVector = vectorMaker_.rowVector(childNames, children);
    if (isNullAt) {
      setNulls(rowVector, isNullAt);
    }
    return rowVector;
  }

  RowVectorPtr makeRowVector(
      const std::vector<VectorPtr>& children,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    auto rowVector = vectorMaker_.rowVector(children);
    if (isNullAt) {
      setNulls(rowVector, isNullAt);
    }
    return rowVector;
  }

  RowVectorPtr makeRowVector(
      const std::shared_ptr<const RowType>& rowType,
      vector_size_t size) {
    return vectorMaker_.rowVector(rowType, size);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(
      vector_size_t size,
      std::function<T(vector_size_t /*row*/)> valueAt,
      std::function<bool(vector_size_t /*row*/)> isNullAt = nullptr) {
    return vectorMaker_.flatVector<T>(size, valueAt, isNullAt);
  }

  template <typename T>
  FlatVectorPtr<EvalType<T>> makeFlatVector(const std::vector<T>& data) {
    return vectorMaker_.flatVector<T>(data);
  }

  template <typename T>
  FlatVectorPtr<EvalType<T>> makeNullableFlatVector(
      const std::vector<std::optional<T>>& data,
      const TypePtr& type = CppToType<T>::create()) {
    return vectorMaker_.flatVectorNullable(data, type);
  }

  template <typename T, int TupleIndex, typename TupleType>
  FlatVectorPtr<T> makeFlatVector(const std::vector<TupleType>& data) {
    return vectorMaker_.flatVector<T, TupleIndex, TupleType>(data);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(size_t size) {
    return vectorMaker_.flatVector<T>(size);
  }

  template <typename T>
  FlatVectorPtr<T> makeFlatVector(size_t size, const TypePtr& type) {
    return vectorMaker_.flatVector<T>(size, type);
  }

  // Helper function for comparing vector results
  template <typename T1, typename T2>
  bool
  compareValues(const T1& a, const std::optional<T2>& b, std::string& error) {
    bool result = (a == b.value());
    if (!result) {
      error = " " + std::to_string(a) + " vs " + std::to_string(b.value());
    } else {
      error = "";
    }
    return result;
  }

  bool compareValues(
      const StringView& a,
      const std::optional<std::string>& b,
      std::string& error) {
    bool result = (a.getString() == b.value());
    if (!result) {
      error = " " + a.getString() + " vs " + b.value();
    } else {
      error = "";
    }
    return result;
  }

  static std::function<bool(vector_size_t /*row*/)> nullEvery(
      int n,
      int startingFrom = 0) {
    return velox::test::VectorMaker::nullEvery(n, startingFrom);
  }

  static std::function<vector_size_t(vector_size_t row)> modN(int n) {
    return [n](vector_size_t row) { return row % n; };
  }

  static RowTypePtr rowType(const std::string& name, const TypePtr& type) {
    return ROW({name}, {type});
  }

  static RowTypePtr rowType(
      const std::string& name,
      const TypePtr& type,
      const std::string& name2,
      const TypePtr& type2) {
    return ROW({name, name2}, {type, type2});
  }

  std::shared_ptr<const core::ITypedExpr> makeTypedExpr(
      const std::string& text,
      const std::shared_ptr<const RowType>& rowType) {
    auto untyped = parse::parseExpr(text);
    return core::Expressions::inferTypes(untyped, rowType, execCtx_.pool());
  }

  // Convenience function to create arrayVectors (vector of arrays) based on
  // input values from nested std::vectors. The underlying elements are
  // non-nullable.
  //
  // Example:
  //   auto arrayVector = makeArrayVector<int64_t>({
  //       {1, 2, 3, 4, 5},
  //       {},
  //       {1, 2, 3},
  //   });
  //   EXPECT_EQ(3, arrayVector->size());
  template <typename T>
  ArrayVectorPtr makeArrayVector(const std::vector<std::vector<T>>& data) {
    return vectorMaker_.arrayVector<T>(data);
  }

  // Create an ArrayVector<ROW> from nested std::vectors of variants.
  // Example:
  //   auto arrayVector = makeArrayOfRowVector({
  //       {variant::row({1, "red"}), variant::row({1, "blue"})},
  //       {},
  //       {variant::row({3, "green"})},
  //   });
  //   EXPECT_EQ(3, arrayVector->size());
  //
  // Use variant(TypeKind::ROW) to specify a null array element.
  ArrayVectorPtr makeArrayOfRowVector(
      const RowTypePtr& rowType,
      const std::vector<std::vector<variant>>& data) {
    return vectorMaker_.arrayOfRowVector(rowType, data);
  }

  // Convenience function to create arrayVectors (vector of arrays) based on
  // input values from nested std::vectors. The underlying array elements are
  // nullable.
  //
  // Example:
  //   auto arrayVector = makeNullableArrayVector<int64_t>({
  //       {1, 2, std::nullopt, 4},
  //       {},
  //       {std::nullopt},
  //   });
  //   EXPECT_EQ(3, arrayVector->size());
  template <typename T>
  ArrayVectorPtr makeNullableArrayVector(
      const std::vector<std::vector<std::optional<T>>>& data) {
    std::vector<std::optional<std::vector<std::optional<T>>>> convData;
    convData.reserve(data.size());
    for (auto& array : data) {
      convData.push_back(array);
    }
    return vectorMaker_.arrayVectorNullable<T>(convData);
  }

  template <typename T>
  ArrayVectorPtr makeVectorWithNullArrays(
      const std::vector<std::optional<std::vector<std::optional<T>>>>& data) {
    return vectorMaker_.arrayVectorNullable<T>(data);
  }

  template <typename T>
  ArrayVectorPtr makeArrayVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* idx */)> valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr) {
    return vectorMaker_.arrayVector<T>(size, sizeAt, valueAt, isNullAt);
  }

  template <typename T>
  ArrayVectorPtr makeArrayVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<T(vector_size_t /* row */, vector_size_t /* idx */)>
          valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr) {
    return vectorMaker_.arrayVector<T>(size, sizeAt, valueAt, isNullAt);
  }

  template <typename TKey, typename TValue>
  MapVectorPtr makeMapVector(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t /* row */)> sizeAt,
      std::function<TKey(vector_size_t /* idx */)> keyAt,
      std::function<TValue(vector_size_t /* idx */)> valueAt,
      std::function<bool(vector_size_t /*row */)> isNullAt = nullptr,
      std::function<bool(vector_size_t /*row */)> valueIsNullAt = nullptr) {
    return vectorMaker_.mapVector<TKey, TValue>(
        size, sizeAt, keyAt, valueAt, isNullAt, valueIsNullAt);
  }

  template <typename T>
  VectorPtr makeConstant(T value, vector_size_t size) {
    return BaseVector::createConstant(value, size, execCtx_.pool());
  }

  template <typename T>
  VectorPtr makeConstant(const std::optional<T>& value, vector_size_t size) {
    return std::make_shared<ConstantVector<EvalType<T>>>(
        execCtx_.pool(),
        size,
        /*isNull=*/!value.has_value(),
        CppToType<T>::create(),
        value ? EvalType<T>(*value) : EvalType<T>(),
        cdvi::EMPTY_METADATA,
        sizeof(EvalType<T>));
  }

  /// Create constant vector of type ROW from a Variant.
  VectorPtr makeConstantRow(
      const RowTypePtr& rowType,
      variant value,
      vector_size_t size) {
    return vectorMaker_.constantRow(rowType, value, size);
  }

  VectorPtr makeNullConstant(TypeKind typeKind, vector_size_t size) {
    return BaseVector::createConstant(variant(typeKind), size, execCtx_.pool());
  }

  BufferPtr makeIndices(
      vector_size_t size,
      std::function<vector_size_t(vector_size_t)> indexAt);

  BufferPtr makeOddIndices(vector_size_t size);

  BufferPtr makeEvenIndices(vector_size_t size);

  BufferPtr makeIndicesInReverse(vector_size_t size) {
    BufferPtr indices =
        AlignedBuffer::allocate<vector_size_t>(size, execCtx_.pool());
    auto rawIndices = indices->asMutable<vector_size_t>();
    for (auto i = 0; i < size; ++i) {
      rawIndices[i] = size - 1 - i;
    }
    return indices;
  }

  static VectorPtr
  wrapInDictionary(BufferPtr indices, vector_size_t size, VectorPtr vector);

  static VectorPtr flatten(const VectorPtr& vector) {
    return velox::test::VectorMaker::flatten(vector);
  }

  // Returns a one element ArrayVector with 'vector' as elements of array at 0.
  VectorPtr asArray(VectorPtr vector) {
    BufferPtr sizes = AlignedBuffer::allocate<vector_size_t>(
        1, vector->pool(), vector->size());
    BufferPtr offsets =
        AlignedBuffer::allocate<vector_size_t>(1, vector->pool(), 0);
    return std::make_shared<ArrayVector>(
        vector->pool(),
        ARRAY(vector->type()),
        BufferPtr(nullptr),
        1,
        offsets,
        sizes,
        vector,
        0);
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      const RowVectorPtr& data) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    exec::ExprSet exprSet({makeTypedExpr(expression, rowType)}, &execCtx_);

    auto rows = std::make_unique<SelectivityVector>(data->size());
    exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results(1);
    exprSet.eval(*rows, &evalCtx, &results);

    auto result = results[0];
    VELOX_CHECK(result, "Expression evaluation result is null: {}", expression);

    auto castedResult = std::dynamic_pointer_cast<T>(result);
    VELOX_CHECK(
        castedResult,
        "Expression evaluation result is not of expected type: {} -> {}",
        expression,
        result->type()->toString());
    return castedResult;
  }

  template <typename T>
  std::shared_ptr<T> evaluate(
      const std::string& expression,
      RowVectorPtr data,
      const SelectivityVector& rows,
      VectorPtr& result) {
    auto rowType = std::dynamic_pointer_cast<const RowType>(data->type());
    facebook::velox::exec::ExprSet exprSet(
        {makeTypedExpr(expression, rowType)}, &execCtx_);

    facebook::velox::exec::EvalCtx evalCtx(&execCtx_, &exprSet, data.get());
    std::vector<VectorPtr> results{result};
    exprSet.eval(rows, &evalCtx, &results);
    return std::dynamic_pointer_cast<T>(results[0]);
  }

  // Evaluate the given expression once, returning the result as a std::optional
  // C++ value. Arguments should be referenced using c0, c1, .. cn.  Supports
  // integers, floats, booleans, and strings.
  //
  // Example:
  //   std::optional<double> exp(std::optional<double> a) {
  //     return evaluateOnce<double>("exp(c0)", a);
  //   }
  //   EXPECT_EQ(1, exp(0));
  //   EXPECT_EQ(std::nullopt, exp(std::nullopt));
  template <typename ReturnType, typename... Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const std::optional<Args>&... args) {
    return evaluateOnce<ReturnType>(
        expr,
        makeRowVector(std::vector<VectorPtr>{
            makeNullableFlatVector(std::vector{args})...}));
  }

  template <typename ReturnType, typename Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const std::vector<std::optional<Args>>& args,
      const std::vector<TypePtr>& types) {
    std::vector<VectorPtr> flatVectors;
    for (vector_size_t i = 0; i < args.size(); ++i) {
      flatVectors.emplace_back(makeNullableFlatVector(
          std::vector<std::optional<Args>>{args[i]}, types[i]));
    }
    auto rowVectorPtr = makeRowVector(flatVectors);
    return evaluateOnce<ReturnType>(expr, rowVectorPtr);
  }

  template <typename ReturnType, typename... Args>
  std::optional<ReturnType> evaluateOnce(
      const std::string& expr,
      const RowVectorPtr rowVectorPtr) {
    auto result =
        evaluate<SimpleVector<EvalType<ReturnType>>>(expr, rowVectorPtr);
    return result->isNullAt(0) ? std::optional<ReturnType>{}
                               : ReturnType(result->valueAt(0));
  }

  // TODO: Enable ASSERT_EQ for vectors
  void assertEqualVectors(
      const VectorPtr& expected,
      const VectorPtr& actual,
      const std::string& additionalContext = "") {
    ASSERT_EQ(expected->size(), actual->size());

    for (auto i = 0; i < expected->size(); i++) {
      ASSERT_TRUE(expected->equalValueAt(actual.get(), i, i))
          << "at " << i << ": " << expected->toString(i) << " vs. "
          << actual->toString(i) << additionalContext;
    }
  }

  // Asserts that `func` throws `VeloxUserError`. Optionally, checks if
  // `expectedErrorMessage` is a substr of the exception message thrown.
  template <typename TFunc>
  static void assertUserInvalidArgument(
      TFunc&& func,
      const std::string& expectedErrorMessage = "") {
    FunctionBaseTest::assertThrow<TFunc, VeloxUserError>(
        std::forward<TFunc>(func), expectedErrorMessage);
  }

  template <typename TFunc>
  static void assertUserError(
      TFunc&& func,
      const std::string& expectedErrorMessage = "") {
    FunctionBaseTest::assertThrow<TFunc, VeloxUserError>(
        std::forward<TFunc>(func), expectedErrorMessage);
  }

  template <typename TFunc, typename TException>
  static void assertThrow(
      TFunc&& func,
      const std::string& expectedErrorMessage) {
    try {
      func();
      ASSERT_FALSE(true) << "Expected an exception";
    } catch (const TException& e) {
      std::string message = e.what();
      ASSERT_TRUE(message.find(expectedErrorMessage) != message.npos)
          << message;
    }
  }

  void registerLambda(
      const std::string& name,
      const std::shared_ptr<const RowType>& signature,
      TypePtr rowType,
      const std::string& body) {
    core::Expressions::registerLambda(
        name, signature, rowType, parse::parseExpr(body), execCtx_.pool());
  }

  memory::MemoryPool* pool() const {
    return pool_.get();
  }

  std::shared_ptr<core::QueryCtx> queryCtx_{core::QueryCtx::create()};
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx_{pool_.get(), queryCtx_.get()};
  velox::test::VectorMaker vectorMaker_{pool_.get()};
};

} // namespace facebook::velox::functions::test
