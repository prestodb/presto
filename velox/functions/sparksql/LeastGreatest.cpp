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
class LeastGreatestFunction final : public exec::VectorFunction {
  using T = typename TypeTraits<kind>::NativeType;

  bool isDefaultNullBehavior() const override {
    return false;
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const size_t nargs = args.size();
    const auto nrows = rows.end();

    // Setup result vector.
    context.ensureWritable(rows, outputType, result);
    FlatVector<T>& flatResult = *result->as<FlatVector<T>>();

    // NULL all elements.
    rows.applyToSelected(
        [&](vector_size_t row) { flatResult.setNull(row, true); });

    exec::LocalSelectivityVector cmpRows(context, nrows);
    exec::LocalDecodedVector decodedVectorHolder(context);
    // Column-wise process: one argument at a time.
    for (size_t i = 0; i < nargs; i++) {
      decodedVectorHolder.get()->decode(*args[i], rows);

      // Only compare with non-null elements of each argument
      *cmpRows = rows;
      if (auto* rawNulls = decodedVectorHolder->nulls()) {
        cmpRows->deselectNulls(rawNulls, 0, nrows);
      }

      if (decodedVectorHolder->isIdentityMapping()) {
        // Fast path: this argument is a FlatVector
        cmpAndReplace(flatResult, *args[i]->as<FlatVector<T>>(), *cmpRows);
      } else {
        // Slow path: decode this argument
        cmpAndReplace(flatResult, *decodedVectorHolder, *cmpRows);
      }
    }
  }

  inline T getValue(const FlatVector<T>& v, vector_size_t i) const {
    return v.valueAt(i);
  }

  inline T getValue(const DecodedVector& v, vector_size_t i) const {
    return v.valueAt<T>(i);
  }

  template <typename VecType>
  void cmpAndReplace(
      FlatVector<T>& dst,
      const VecType& src,
      SelectivityVector& rows) const {
    const Cmp cmp;
    rows.applyToSelected([&](vector_size_t row) {
      const auto srcVal = getValue(src, row);
      if (dst.isNullAt(row) || cmp(srcVal, getValue(dst, row))) {
        dst.set(row, srcVal);
      }
    });
  }
};

template <template <typename> class Cmp>
std::shared_ptr<exec::VectorFunction> makeImpl(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args) {
  VELOX_CHECK_GE(args.size(), 2);
  for (size_t i = 1; i < args.size(); i++) {
    VELOX_CHECK(*args[i].type == *args[0].type);
  }

  switch (args[0].type->kind()) {
#define SCALAR_CASE(kind)                            \
  case TypeKind::kind:                               \
    return std::make_shared<LeastGreatestFunction<   \
        Cmp<TypeTraits<TypeKind::kind>::NativeType>, \
        TypeKind::kind>>();
    SCALAR_CASE(BOOLEAN);
    SCALAR_CASE(TINYINT);
    SCALAR_CASE(SMALLINT);
    SCALAR_CASE(INTEGER);
    SCALAR_CASE(BIGINT);
    SCALAR_CASE(REAL);
    SCALAR_CASE(DOUBLE);
    SCALAR_CASE(VARCHAR);
    SCALAR_CASE(VARBINARY);
    SCALAR_CASE(TIMESTAMP);
#undef SCALAR_CASE
    default:
      VELOX_NYI(
          "{} does not support arguments of type {}",
          functionName,
          args[0].type->kind());
  }
}

} // namespace

std::shared_ptr<exec::VectorFunction> makeLeast(
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args) {
  return makeImpl<Less>(functionName, args);
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
    const std::string& functionName,
    const std::vector<exec::VectorFunctionArg>& args) {
  return makeImpl<Greater>(functionName, args);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> greatestSignatures() {
  return leastSignatures();
}

} // namespace facebook::velox::functions::sparksql
