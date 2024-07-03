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

using Eq = std::equal_to<>;
using Neq = std::not_equal_to<>;
using Lt = std::less<>;
using Lte = std::less_equal<>;
using Gt = std::greater<>;
using Gte = std::greater_equal<>;

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
        for (auto j = 0; j < 8 && (i + j) < vectorEnd; j += numScalarElements) {
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

  template <typename T>
  inline bool compare(T& l, T& r) const {
    if constexpr (std::is_floating_point_v<T>) {
      bool filtered = false;
      if constexpr (std::is_same_v<ComparisonOp, Eq>) {
        filtered = util::floating_point::NaNAwareEquals<T>{}(l, r);
      } else if constexpr (std::is_same_v<ComparisonOp, Neq>) {
        filtered = !util::floating_point::NaNAwareEquals<T>{}(l, r);
      } else if constexpr (std::is_same_v<ComparisonOp, Lt>) {
        filtered = util::floating_point::NaNAwareLessThan<T>{}(l, r);
      } else if constexpr (std::is_same_v<ComparisonOp, Lte>) {
        filtered = util::floating_point::NaNAwareLessThanEqual<T>{}(l, r);
      } else if constexpr (std::is_same_v<ComparisonOp, Gt>) {
        filtered = util::floating_point::NaNAwareGreaterThan<T>{}(l, r);
      } else if constexpr (std::is_same_v<ComparisonOp, Gte>) {
        filtered = util::floating_point::NaNAwareGreaterThanEqual<T>{}(l, r);
      }
      return filtered;
    } else {
      return ComparisonOp()(l, r);
    }
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          (xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value &&
           kind != TypeKind::BOOLEAN) ||
              kind == TypeKind::HUGEINT,
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

    bool isSimdizable = (lhs.isConstantEncoding() || lhs.isFlatEncoding()) &&
        (rhs.isConstantEncoding() || rhs.isFlatEncoding()) &&
        rows.isAllSelected();

    static const bool isTypeNotSupported =
        std::is_same_v<T, int128_t> || std::is_floating_point_v<T>;

    if (!isSimdizable || isTypeNotSupported) {
      exec::LocalDecodedVector lhsDecoded(context, lhs, rows);
      exec::LocalDecodedVector rhsDecoded(context, rhs, rows);

      context.template applyToSelectedNoThrow(rows, [&](auto row) {
        auto l = lhsDecoded->template valueAt<T>(row);
        auto r = rhsDecoded->template valueAt<T>(row);
        auto filtered = compare(l, r);
        resultVector->set(row, filtered);
      });
      return;
    }

    if constexpr (!isTypeNotSupported) {
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
  }

  template <
      TypeKind kind,
      typename std::enable_if_t<
          (!xsimd::has_simd_register<
               typename TypeTraits<kind>::NativeType>::value ||
           kind == TypeKind::BOOLEAN) &&
              kind != TypeKind::HUGEINT,
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

    if (args[0]->type()->isLongDecimal()) {
      comparator.template applyComparison<TypeKind::HUGEINT>(
          rows, *args[0], *args[1], context, result);
      return;
    }

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

    for (const auto& inputType : {
             "tinyint",
             "smallint",
             "integer",
             "bigint",
             "real",
             "double",
             "date",
             "interval day to second",
             "interval year to month",
         }) {
      signatures.push_back(exec::FunctionSignatureBuilder()
                               .returnType("boolean")
                               .argumentType(inputType)
                               .argumentType(inputType)
                               .build());
    }
    signatures.push_back(exec::FunctionSignatureBuilder()
                             .integerVariable("a_precision")
                             .integerVariable("a_scale")
                             .returnType("boolean")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .argumentType("DECIMAL(a_precision, a_scale)")
                             .build());
    return signatures;
  }

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  exec::FunctionCanonicalName getCanonicalName() const override {
    return std::is_same_v<ComparisonOp, Lt>
        ? exec::FunctionCanonicalName::kLt
        : exec::FunctionCanonicalName::kUnknown;
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_eq,
    (ComparisonSimdFunction<Eq>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Eq>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_neq,
    (ComparisonSimdFunction<Neq>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Neq>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lt,
    (ComparisonSimdFunction<Lt>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Lt>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gt,
    (ComparisonSimdFunction<Gt>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Gt>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_lte,
    (ComparisonSimdFunction<Lte>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Lte>>()));

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_simd_comparison_gte,
    (ComparisonSimdFunction<Gte>::signatures()),
    (std::make_unique<ComparisonSimdFunction<Gte>>()));

} // namespace facebook::velox::functions
