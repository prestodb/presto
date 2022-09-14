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

#include "velox/functions/prestosql/Comparisons.h"
#include <velox/common/base/Exceptions.h>
#include "velox/functions/Udf.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::functions {

namespace {

/// This class implements comparison for vectors of primitive types using SIMD.
/// Currently this only supports fixed length primitive types (except Boolean).
/// It also requires the vectors to have a flat encoding.
/// If the vector encoding is not flat, we revert to non simd approach.
template <typename ComparisonOp, typename Arch = xsimd::default_arch>
struct SimdComparator {
  template <typename T, bool isConstant>
  inline auto loadSimdData(const T* rawData, vector_size_t offset) {
    using d_type = xsimd::batch<T>;
    if constexpr (isConstant) {
      return xsimd::broadcast<T>(rawData[0]);
    }
    return d_type::load_unaligned(rawData + offset);
  }

  template <typename T, bool isLeftConstant, bool isRightConstant>
  void applySimdComparison(
      const vector_size_t begin,
      const vector_size_t end,
      const T* rawLhs,
      const T* rawRhs,
      uint8_t* rawResult) {
    using d_type = xsimd::batch<T>;
    constexpr auto numScalarElements = d_type::size;
    const auto vectorEnd = (end - begin) - (end - begin) % numScalarElements;

    if constexpr (numScalarElements == 2 || numScalarElements == 4) {
      for (auto i = begin; i < vectorEnd; i += 8) {
        rawResult[i / 8] = 0;
        for (auto j = 0; j < 8 && j < vectorEnd; j += numScalarElements) {
          auto left = loadSimdData<T, isLeftConstant>(rawLhs, i + j);
          auto right = loadSimdData<T, isRightConstant>(rawRhs, i + j);

          uint8_t res = simd::toBitMask(ComparisonOp()(left, right));
          rawResult[i / 8] |= res << j;
        }
      }
    } else {
      for (auto i = begin; i < vectorEnd; i += numScalarElements) {
        auto left = loadSimdData<T, isLeftConstant>(rawLhs, i);
        auto right = loadSimdData<T, isRightConstant>(rawRhs, i);

        auto res = simd::toBitMask(ComparisonOp()(left, right));
        if constexpr (numScalarElements == 8) {
          rawResult[i / 8] = res;
        } else if constexpr (numScalarElements == 16) {
          uint16_t* addr = reinterpret_cast<uint16_t*>(rawResult + i / 8);
          *addr = res;
        } else if constexpr (numScalarElements == 32) {
          uint32_t* addr = reinterpret_cast<uint32_t*>(rawResult + i / 8);
          *addr = res;
        } else {
          VELOX_FAIL("Unsupported number of scalar elements");
        }
      }
    }

    // Evaluate remaining values.
    for (auto i = vectorEnd; i < end; i++) {
      if constexpr (isRightConstant && isLeftConstant) {
        bits::setBit(rawResult, i, ComparisonOp()(rawLhs[0], rawRhs[0]));
      } else if constexpr (isRightConstant) {
        bits::setBit(rawResult, i, ComparisonOp()(rawLhs[i], rawRhs[0]));
      } else if constexpr (isLeftConstant) {
        bits::setBit(rawResult, i, ComparisonOp()(rawLhs[0], rawRhs[i]));
      } else {
        bits::setBit(rawResult, i, ComparisonOp()(rawLhs[i], rawRhs[i]));
      }
    }
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          xsimd::has_simd_register<
              typename TypeTraits<kind>::NativeType>::value &&
              kind != TypeKind::BOOLEAN,
          int> = 0>
  void applyComparison(
      const SelectivityVector& rows,
      BaseVector& lhs,
      BaseVector& rhs,
      exec::EvalCtx& context,
      VectorPtr& result) {
    using T = typename TypeTraits<kind>::NativeType;

    auto resultVector = result->asUnchecked<FlatVector<bool>>();
    auto rawResult = resultVector->mutableRawValues<uint8_t>();

    auto isSimdizable = (lhs.isConstantEncoding() || lhs.isFlatEncoding()) &&
        (rhs.isConstantEncoding() || rhs.isFlatEncoding()) &&
        rows.isAllSelected();

    if (!isSimdizable) {
      exec::LocalDecodedVector lhsDecoded(context, lhs, rows);
      exec::LocalDecodedVector rhsDecoded(context, rhs, rows);

      context.template applyToSelectedNoThrow(rows, [&](auto row) {
        auto l = lhsDecoded->template valueAt<T>(row);
        auto r = rhsDecoded->template valueAt<T>(row);
        auto filtered = ComparisonOp()(l, r);
        resultVector->set(row, filtered);
      });
      return;
    }

    if (lhs.isConstantEncoding() && rhs.isConstantEncoding()) {
      auto l = lhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
      auto r = rhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
      applySimdComparison<T, true, true>(
          rows.begin(), rows.end(), &l, &r, rawResult);
    } else if (lhs.isConstantEncoding()) {
      auto l = lhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
      auto rawRhs = rhs.asUnchecked<FlatVector<T>>()->rawValues();
      applySimdComparison<T, true, false>(
          rows.begin(), rows.end(), &l, rawRhs, rawResult);
    } else if (rhs.isConstantEncoding()) {
      auto rawLhs = lhs.asUnchecked<FlatVector<T>>()->rawValues();
      auto r = rhs.asUnchecked<ConstantVector<T>>()->valueAt(0);
      applySimdComparison<T, false, true>(
          rows.begin(), rows.end(), rawLhs, &r, rawResult);
    } else {
      auto rawLhs = lhs.asUnchecked<FlatVector<T>>()->rawValues();
      auto rawRhs = rhs.asUnchecked<FlatVector<T>>()->rawValues();
      applySimdComparison<T, false, false>(
          rows.begin(), rows.end(), rawLhs, rawRhs, rawResult);
    }

    resultVector->clearNulls(rows);
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          !xsimd::has_simd_register<
              typename TypeTraits<kind>::NativeType>::value ||
              kind == TypeKind::BOOLEAN,
          int> = 0>
  void applyComparison(
      const SelectivityVector& /* rows */,
      BaseVector& /* lhs */,
      BaseVector& /* rhs */,
      exec::EvalCtx& /* context */,
      VectorPtr& /* result */) {
    VELOX_UNSUPPORTED("Unsupported type for SIMD comparison");
  }
};

template <typename ComparisonOp, typename Arch = xsimd::default_arch>
class ComparisonSimdFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 2, "Comparison requires two arguments");
    VELOX_CHECK_EQ(args[0]->typeKind(), args[1]->typeKind());
    VELOX_USER_CHECK_EQ(outputType->kind(), TypeKind::BOOLEAN);

    context.ensureWritable(rows, outputType, result);
    auto comparator = SimdComparator<ComparisonOp>{};

    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        comparator.template applyComparison,
        args[0]->typeKind(),
        rows,
        *args[0],
        *args[1],
        context,
        result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    std::vector<std::shared_ptr<exec::FunctionSignature>> signatures;

    for (const auto& inputType :
         {"tinyint", "smallint", "integer", "bigint", "real", "double"}) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .returnType("boolean")
                               .argumentType(inputType)
                               .argumentType(inputType)
                               .build());
    }

    return signatures;
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_eq,
    (ComparisonSimdFunction<std::equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::equal_to<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_neq,
    (ComparisonSimdFunction<std::not_equal_to<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::not_equal_to<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lt,
    (ComparisonSimdFunction<std::less<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gt,
    (ComparisonSimdFunction<std::greater<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lte,
    (ComparisonSimdFunction<std::less_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::less_equal<>>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gte,
    (ComparisonSimdFunction<std::greater_equal<>>::signatures()),
    (std::make_unique<ComparisonSimdFunction<std::greater_equal<>>>()));

} // namespace facebook::velox::functions
