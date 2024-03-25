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
#include "velox/functions/sparksql/LeastGreatest.h"

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/functions/sparksql/Comparisons.h"
#include "velox/type/Type.h"

namespace facebook::velox::functions::sparksql {
namespace {

template <typename Cmp, TypeKind kind>
class ComparisonFunction final : public exec::VectorFunction {
  using T = typename TypeTraits<kind>::NativeType;

  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    const Cmp cmp;

    if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto rawA = args[0]->asUnchecked<FlatVector<T>>()->mutableRawValues();
      auto rawB = args[1]->asUnchecked<FlatVector<T>>()->mutableRawValues();
      rows.applyToSelected(
          [&](vector_size_t i) { flatResult->set(i, cmp(rawA[i], rawB[i])); });
    } else if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<ConstantVector<T>>()->valueAt(0);
      auto rawValues =
          args[1]->asUnchecked<FlatVector<T>>()->mutableRawValues();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(constant, rawValues[i]));
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto rawValues =
          args[0]->asUnchecked<FlatVector<T>>()->mutableRawValues();
      auto constant = args[1]->asUnchecked<ConstantVector<T>>()->valueAt(0);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(rawValues[i], constant));
      });
    } else {
      // Path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto decodedA = decodedArgs.at(0);
      auto decodedB = decodedArgs.at(1);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(decodedA->valueAt<T>(i), decodedB->valueAt<T>(i)));
      });
    }
  }
};

// ComparisonFunction instance for bool as it uses compact representation
template <typename Cmp>
class BoolComparisonFunction final : public exec::VectorFunction {
  bool supportsFlatNoNullsFastPath() const override {
    return true;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    const Cmp cmp;

    if (args[0]->isFlatEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (flat, flat).
      auto rawA = args[0]
                      ->asUnchecked<FlatVector<bool>>()
                      ->template mutableRawValues<uint64_t>();
      auto rawB = args[1]
                      ->asUnchecked<FlatVector<bool>>()
                      ->template mutableRawValues<uint64_t>();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(bits::isBitSet(rawA, i), bits::isBitSet(rawB, i)));
      });
    } else if (args[0]->isConstantEncoding() && args[1]->isFlatEncoding()) {
      // Fast path for (const, flat).
      auto constant = args[0]->asUnchecked<ConstantVector<bool>>()->valueAt(0);
      auto rawValues = args[1]
                           ->asUnchecked<FlatVector<bool>>()
                           ->template mutableRawValues<uint64_t>();
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(constant, bits::isBitSet(rawValues, i)));
      });
    } else if (args[0]->isFlatEncoding() && args[1]->isConstantEncoding()) {
      // Fast path for (flat, const).
      auto rawValues = args[0]
                           ->asUnchecked<FlatVector<bool>>()
                           ->template mutableRawValues<uint64_t>();
      auto constant = args[1]->asUnchecked<ConstantVector<bool>>()->valueAt(0);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(i, cmp(bits::isBitSet(rawValues, i), constant));
      });
    } else {
      // Fast path if one or more arguments are encoded.
      exec::DecodedArgs decodedArgs(rows, args, context);
      auto decodedA = decodedArgs.at(0);
      auto decodedB = decodedArgs.at(1);
      rows.applyToSelected([&](vector_size_t i) {
        flatResult->set(
            i, cmp(decodedA->valueAt<bool>(i), decodedB->valueAt<bool>(i)));
      });
    }
  }
};

template <template <typename> class Cmp>
std::shared_ptr<exec::VectorFunction> makeImpl(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args) {
  VELOX_CHECK_EQ(args.size(), 2);
  for (size_t i = 1; i < args.size(); i++) {
    VELOX_CHECK(*args[i].type == *args[0].type);
  }
  switch (args[0].type->kind()) {
#define SCALAR_CASE(kind)                            \
  case TypeKind::kind:                               \
    return std::make_shared<ComparisonFunction<      \
        Cmp<TypeTraits<TypeKind::kind>::NativeType>, \
        TypeKind::kind>>();
    SCALAR_CASE(TINYINT)
    SCALAR_CASE(SMALLINT)
    SCALAR_CASE(INTEGER)
    SCALAR_CASE(BIGINT)
    SCALAR_CASE(HUGEINT)
    SCALAR_CASE(REAL)
    SCALAR_CASE(DOUBLE)
    SCALAR_CASE(VARCHAR)
    SCALAR_CASE(VARBINARY)
    SCALAR_CASE(TIMESTAMP)
#undef SCALAR_CASE
    case TypeKind::BOOLEAN:
      return std::make_shared<BoolComparisonFunction<
          Cmp<TypeTraits<TypeKind::BOOLEAN>::NativeType>>>();
    default:
      VELOX_NYI(
          "{} does not support arguments of type {}",
          functionName,
          args[0].type->kind());
  }
}

template <TypeKind kind>
void applyTyped(
    const SelectivityVector& rows,
    std::vector<VectorPtr>& args,
    DecodedVector* decodedLhs,
    DecodedVector* decodedRhs,
    exec::EvalCtx& context,
    FlatVector<bool>* flatResult) {
  using T = typename TypeTraits<kind>::NativeType;

  Equal<T> equal;
  if (!args[0]->mayHaveNulls() && !args[1]->mayHaveNulls()) {
    // When there is no nulls, it reduces to normal equality function
    rows.applyToSelected([&](vector_size_t i) {
      flatResult->set(
          i, equal(decodedLhs->valueAt<T>(i), decodedRhs->valueAt<T>(i)));
    });
  } else {
    // (isnull(a) AND isnull(b)) || (a == b)
    // When DecodedVector::nulls() is null it means there are no nulls.
    auto* rawNulls0 = decodedLhs->nulls(&rows);
    auto* rawNulls1 = decodedRhs->nulls(&rows);
    rows.applyToSelected([&](vector_size_t i) {
      auto isNull0 = rawNulls0 && bits::isBitNull(rawNulls0, i);
      auto isNull1 = rawNulls1 && bits::isBitNull(rawNulls1, i);
      flatResult->set(
          i,
          (isNull0 || isNull1)
              ? isNull0 && isNull1
              : equal(decodedLhs->valueAt<T>(i), decodedRhs->valueAt<T>(i)));
    });
  }
}

class EqualtoNullSafe final : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::DecodedArgs decodedArgs(rows, args, context);

    DecodedVector* decodedLhs = decodedArgs.at(0);
    DecodedVector* decodedRhs = decodedArgs.at(1);
    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asUnchecked<FlatVector<bool>>();
    flatResult->mutableRawValues<int64_t>();

    VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
        applyTyped,
        args[0]->typeKind(),
        rows,
        args,
        decodedLhs,
        decodedRhs,
        context,
        flatResult);
  }
};

} // namespace

std::shared_ptr<exec::VectorFunction> makeEqualTo(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Equal>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeLessThan(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Less>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeGreaterThan(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<Greater>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeLessThanOrEqual(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<LessOrEqual>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeGreaterThanOrEqual(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args,
    const core::QueryConfig& /*config*/) {
  return makeImpl<GreaterOrEqual>(functionName, args);
}

std::shared_ptr<exec::VectorFunction> makeEqualToNullSafe(
    const std::string& name,
    const std::vector<exec::VectorFunctionArg>& inputArgs,
    const core::QueryConfig& /*config*/) {
  static const auto kEqualtoNullSafeFunction =
      std::make_shared<EqualtoNullSafe>();
  return kEqualtoNullSafeFunction;
}
} // namespace facebook::velox::functions::sparksql
