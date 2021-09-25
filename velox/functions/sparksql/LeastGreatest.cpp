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

#include "velox/expression/EvalCtx.h"
#include "velox/expression/Expr.h"
#include "velox/expression/VectorFunction.h"
#include "velox/type/Type.h"

#include "velox/functions/sparksql/LeastGreatest.h"

namespace facebook::velox::functions::sparksql {
namespace {

enum CompareType { MIN, MAX };

template <CompareType type>
class LeastGreatestFunction : public exec::VectorFunction {
 public:
  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const override {
    VELOX_CHECK(args.size() >= 2);
    for (size_t i = 1; i < args.size(); i++) {
      VELOX_CHECK(args[i]->typeKind() == args[0]->typeKind());
    }

    switch (args[0]->typeKind()) {
      case TypeKind::TINYINT:
        applyTyped<int8_t>(rows, args, caller, context, result);
        break;
      case TypeKind::SMALLINT:
        applyTyped<int16_t>(rows, args, caller, context, result);
        break;
      case TypeKind::INTEGER:
        applyTyped<int32_t>(rows, args, caller, context, result);
        break;
      case TypeKind::BIGINT:
        applyTyped<int64_t>(rows, args, caller, context, result);
        break;
      case TypeKind::REAL:
        applyTyped<float>(rows, args, caller, context, result);
        break;
      case TypeKind::DOUBLE:
        applyTyped<double>(rows, args, caller, context, result);
        break;
      case TypeKind::BOOLEAN:
        applyTyped<bool>(rows, args, caller, context, result);
        break;
      case TypeKind::VARCHAR:
        applyTyped<StringView>(rows, args, caller, context, result);
        break;
      case TypeKind::TIMESTAMP:
        applyTyped<Timestamp>(rows, args, caller, context, result);
        break;
      default:
        VELOX_CHECK(false, "Bad type for least/greatest");
    }
  }

 private:
  template <typename T>
  void applyTyped(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      exec::Expr* caller,
      exec::EvalCtx* context,
      VectorPtr* result) const {
    auto isFlatVector = [](const VectorPtr vp) -> bool {
      return vp->encoding() == VectorEncoding::Simple::FLAT;
    };

    const size_t nargs = args.size();
    const auto nrows = rows.end();

    // Setup result vector.
    BaseVector::ensureWritable(rows, caller->type(), context->pool(), result);
    FlatVector<T>& flatResult = *(*result)->as<FlatVector<T>>();

    // NULL all elements.
    rows.applyToSelected(
        [&](vector_size_t row) { flatResult.setNull(row, true); });

    exec::LocalSelectivityVector cmpRows(context, nrows);
    exec::LocalDecodedVector decodedVectorHolder(context);
    // Column-wise process: one argument at a time.
    for (size_t i = 0; i < nargs; i++) {
      // Only compare with non-null elements of each argument
      *cmpRows = rows;
      if (auto* rawNulls = args[i]->flatRawNulls(rows)) {
        cmpRows->deselectNulls(rawNulls, 0, nrows);
      }

      if (isFlatVector(args[i])) {
        // Fast path: this argument is a FlatVector
        cmpAndReplace(flatResult, *args[i]->as<FlatVector<T>>(), *cmpRows);
      } else {
        // Slow path: decode this argument
        decodedVectorHolder.get()->decode(*args[i], rows);
        cmpAndReplace(flatResult, *decodedVectorHolder, *cmpRows);
      }
    }
  }

  template <typename T>
  inline T getValue(const FlatVector<T>& v, vector_size_t i) const {
    return v.valueAt(i);
  }

  template <typename T>
  inline T getValue(const DecodedVector& v, vector_size_t i) const {
    return v.valueAt<T>(i);
  }

  template <typename T>
  static bool isNaN(const T& value) {
    if constexpr (std::is_floating_point<T>::value) {
      // In C++ NaN is not comparable (i.e. all comparisons return false)
      // to any value (including itself).
      return std::isnan(value);
    } else {
      return false;
    }
  }

  template <typename T, typename VecType>
  void cmpAndReplace(
      FlatVector<T>& dst,
      const VecType& src,
      SelectivityVector& rows) const {
    rows.applyToSelected([&](vector_size_t row) {
      const auto srcVal = getValue<T>(src, row);
      const auto dstVal = getValue<T>(dst, row);

      if (dst.isNullAt(row)) {
        dst.set(row, srcVal);
      } else {
        // NaN is treated as the largest number
        if constexpr (type == CompareType::MIN) {
          if (isNaN(dstVal)) {
            dst.set(row, srcVal);
          } else if (!isNaN(srcVal)) {
            dst.set(row, std::min(dstVal, srcVal));
          }
        } else {
          if (isNaN(srcVal)) {
            dst.set(row, srcVal);
          } else if (!isNaN(dstVal)) {
            dst.set(row, std::max(dstVal, srcVal));
          }
        }
      }
    });
  }
};
} // namespace

std::shared_ptr<exec::VectorFunction> makeLeast(
    const std::string& /**/,
    const std::vector<exec::VectorFunctionArg>& /**/) {
  return std::make_shared<LeastGreatestFunction<CompareType::MIN>>();
}

std::vector<std::shared_ptr<exec::FunctionSignature>> leastSignatures() {
  // T, T... -> T
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("T")
              .argumentType("T")
              .argumentType("T")
              .variableArity()
              .build()};
}

std::shared_ptr<exec::VectorFunction> makeGreatest(
    const std::string& /**/,
    const std::vector<exec::VectorFunctionArg>& /**/) {
  return std::make_shared<LeastGreatestFunction<CompareType::MAX>>();
}

std::vector<std::shared_ptr<exec::FunctionSignature>> greatestSignatures() {
  // T, T... -> T
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("T")
              .argumentType("T")
              .argumentType("T")
              .variableArity()
              .build()};
}

} // namespace facebook::velox::functions::sparksql
