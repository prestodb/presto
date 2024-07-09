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
#include "velox/common/base/RuntimeMetrics.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"

namespace facebook::velox::functions {
namespace {

// Throws if any array in any of 'rows' has more than 10K elements.
// Evaluating 'reduce' lambda function on very large arrays is too slow.
void checkArraySizes(
    const SelectivityVector& rows,
    DecodedVector& decodedArray,
    exec::EvalCtx& context) {
  const auto* indices = decodedArray.indices();
  const auto* rawSizes = decodedArray.base()->as<ArrayVector>()->rawSizes();

  static const vector_size_t kMaxArraySize = 10'000;

  rows.applyToSelected([&](auto row) {
    if (decodedArray.isNullAt(row)) {
      return;
    }
    const auto size = rawSizes[indices[row]];
    try {
      VELOX_USER_CHECK_LT(
          size,
          kMaxArraySize,
          "reduce lambda function doesn't support arrays with more than {} elements",
          kMaxArraySize);
    } catch (VeloxUserError&) {
      context.setError(row, std::current_exception());
    }
  });
}

/// Populates indices of the n-th elements of the arrays.
/// Selects 'row' in 'arrayRows' if corresponding array has an n-th element.
/// Sets elementIndices[row] to the index of the n-th element in the 'elements'
/// vector.
/// Returns true if at least one array has n-th element.
bool toNthElementRows(
    const ArrayVectorPtr& arrayVector,
    const SelectivityVector& rows,
    vector_size_t n,
    SelectivityVector& arrayRows,
    BufferPtr& elementIndices) {
  auto* rawSizes = arrayVector->rawSizes();
  auto* rawOffsets = arrayVector->rawOffsets();
  auto* rawNulls = arrayVector->rawNulls();

  auto* rawElementIndices = elementIndices->asMutable<vector_size_t>();

  arrayRows.clearAll();
  memset(rawElementIndices, 0, elementIndices->size());

  rows.applyToSelected([&](auto row) {
    if (!rawNulls || !bits::isBitNull(rawNulls, row)) {
      if (n < rawSizes[row]) {
        arrayRows.setValid(row, true);
        rawElementIndices[row] = rawOffsets[row] + n;
      }
    }
  });
  arrayRows.updateBounds();

  return arrayRows.hasSelections();
}

/// See documentation at
/// https://prestodb.io/docs/current/functions/array.html#reduce
class ReduceFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK_EQ(args.size(), 4);
    // Flatten input array.
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    checkArraySizes(rows, decodedArray, context);

    exec::LocalSelectivityVector remainingRows(context, rows);
    context.deselectErrors(*remainingRows);

    doApply(*remainingRows, args, decodedArray, outputType, context, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // array(T), S, function(S, T, S), function(S, R) -> R
    return {exec::FunctionSignatureBuilder()
                .typeVariable("T")
                .typeVariable("S")
                .typeVariable("R")
                .returnType("R")
                .argumentType("array(T)")
                .argumentType("S")
                .argumentType("function(S,T,S)")
                .argumentType("function(S,R)")
                .build()};
  }

 private:
  void doApply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      DecodedVector& decodedArray,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto flatArray = flattenArray(rows, args[0], decodedArray);
    // Identify the rows need to be computed.
    exec::LocalSelectivityVector nonNullRowsHolder(*context.execCtx());
    const SelectivityVector* nonNullRows = &rows;
    if (flatArray->mayHaveNulls()) {
      nonNullRowsHolder.get(rows);
      nonNullRowsHolder->deselectNulls(
          flatArray->rawNulls(), rows.begin(), rows.end());
      nonNullRows = nonNullRowsHolder.get();
    }
    const auto& initialState = args[1];
    auto partialResult =
        BaseVector::create(initialState->type(), rows.end(), context.pool());
    // Process empty arrays.
    auto* rawSizes = flatArray->rawSizes();
    nonNullRows->applyToSelected([&](auto row) {
      if (rawSizes[row] == 0) {
        partialResult->copy(initialState.get(), row, row, 1);
      }
    });

    // Make sure already populated entries in 'partialResult' do not get
    // overwritten if 'arrayRows' shrinks in subsequent iterations.
    const SelectivityVector& validRowsInReusedResult = *nonNullRows;

    // Loop over lambda functions and apply these to elements of the base array.
    // In most cases there will be only one function and the loop will run once.
    auto inputFuncIt =
        args[2]->asUnchecked<FunctionVector>()->iterator(nonNullRows);

    BufferPtr elementIndices =
        allocateIndices(flatArray->size(), context.pool());
    SelectivityVector arrayRows(flatArray->size(), false);

    // Iteratively apply input function to array elements.
    // First, apply input function to first elements of all arrays.
    // Then, apply input function to second elements of all arrays.
    // And so on until all elements of all arrays have been processed.
    // At each step the number of arrays being processed will get smaller as
    // some arrays will run out of elements.
    while (auto entry = inputFuncIt.next()) {
      VectorPtr state = initialState;

      vector_size_t n = 0;
      while (true) {
        // 'state' might use the 'elementIndices', in that case we need to
        // reallocate them to avoid overwriting.
        if (not elementIndices->unique()) {
          elementIndices = allocateIndices(flatArray->size(), context.pool());
        }

        // Sets arrayRows[row] to true if array at that row has n-th element, to
        // false otherwise.
        // Set elementIndices[row] to the index of the n-th element in the
        // array's elements vector.
        if (!toNthElementRows(
                flatArray, *entry.rows, n, arrayRows, elementIndices)) {
          break; // Ran out of elements in all arrays.
        }

        // Create dictionary row -> element in array's elements vector.
        auto dictNthElements = BaseVector::wrapInDictionary(
            BufferPtr(nullptr),
            elementIndices,
            flatArray->size(),
            flatArray->elements());

        // Run input lambda on our dictionary - adding n-th element to the
        // initial state for every row.
        std::vector<VectorPtr> lambdaArgs = {state, dictNthElements};
        entry.callable->apply(
            arrayRows,
            &validRowsInReusedResult,
            nullptr,
            &context,
            lambdaArgs,
            nullptr,
            &partialResult);
        state = partialResult;
        n++;
      }
    }

    // Apply output function.
    VectorPtr localResult;
    auto outputFuncIt =
        args[3]->asUnchecked<FunctionVector>()->iterator(nonNullRows);
    while (auto entry = outputFuncIt.next()) {
      std::vector<VectorPtr> lambdaArgs = {partialResult};
      entry.callable->apply(
          *entry.rows,
          &validRowsInReusedResult,
          nullptr,
          &context,
          lambdaArgs,
          nullptr,
          &localResult);
    }
    if (flatArray->rawNulls()) {
      exec::EvalCtx::addNulls(
          rows, flatArray->rawNulls(), context, outputType, localResult);
    }
    context.moveOrCopyResult(localResult, rows, result);
  }
};

bool isVariableReference(
    const core::TypedExprPtr& expr,
    const std::string& var) {
  auto* fieldAccess =
      dynamic_cast<const core::FieldAccessTypedExpr*>(expr.get());
  return fieldAccess && fieldAccess->isInputColumn() &&
      fieldAccess->name() == var;
}

core::TypedExprPtr extractFromAddition(
    const std::string& prefix,
    const core::TypedExprPtr& expr,
    const std::string& s) {
  auto* plus = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (plus && plus->name() == prefix + "plus") {
    if (!isVariableReference(plus->inputs()[0], s)) {
      return nullptr;
    }
    return plus->inputs()[1];
  }
  if (!isVariableReference(expr, s)) {
    return nullptr;
  }
  variant zero;
  switch (expr->type()->kind()) {
    case TypeKind::TINYINT:
      zero = variant::create<int8_t>(0);
      break;
    case TypeKind::SMALLINT:
      zero = variant::create<int16_t>(0);
      break;
    case TypeKind::INTEGER:
      zero = variant::create<int32_t>(0);
      break;
    case TypeKind::BIGINT:
      zero = variant::create<int64_t>(0);
      break;
    case TypeKind::REAL:
      zero = variant::create<float>(0);
      break;
    case TypeKind::DOUBLE:
      zero = variant::create<double>(0);
      break;
    default:
      return nullptr;
  }
  return std::make_shared<core::ConstantTypedExpr>(expr->type(), zero);
}

bool containsVariableReference(
    const core::TypedExprPtr& expr,
    const std::string& var) {
  if (isVariableReference(expr, var)) {
    return true;
  }
  if (auto* lambda = dynamic_cast<const core::LambdaTypedExpr*>(expr.get())) {
    return !lambda->signature()->containsChild(var) &&
        containsVariableReference(lambda->body(), var);
  }
  for (auto& input : expr->inputs()) {
    if (containsVariableReference(input, var)) {
      return true;
    }
  }
  return false;
}

core::TypedExprPtr toArraySum(
    const std::string& prefix,
    const core::CallTypedExpr& reduce,
    const RowTypePtr& inputArgs,
    const core::TypedExprPtr& expr) {
  if (containsVariableReference(expr, inputArgs->nameOf(0))) {
    return nullptr;
  }
  auto& initial = reduce.inputs()[1];
  TypePtr sumType;
  switch (initial->type()->kind()) {
    case TypeKind::TINYINT:
    case TypeKind::SMALLINT:
    case TypeKind::INTEGER:
    case TypeKind::BIGINT:
      sumType = BIGINT();
      break;
    case TypeKind::REAL:
    case TypeKind::DOUBLE:
      sumType = DOUBLE();
      break;
    default:
      return nullptr;
  }
  auto lambda = std::make_shared<core::LambdaTypedExpr>(
      ROW({inputArgs->nameOf(1)}, {inputArgs->childAt(1)}), expr);
  auto transform = std::make_shared<core::CallTypedExpr>(
      ARRAY(expr->type()),
      std::vector<core::TypedExprPtr>({reduce.inputs()[0], lambda}),
      prefix + "transform");
  auto arraySum = std::make_shared<core::CallTypedExpr>(
      sumType,
      std::vector<core::TypedExprPtr>({transform}),
      prefix + "array_sum_propagate_element_null");
  auto cast =
      std::make_shared<core::CastTypedExpr>(initial->type(), arraySum, false);
  auto plus = std::make_shared<core::CallTypedExpr>(
      initial->type(),
      std::vector<core::TypedExprPtr>({initial, cast}),
      prefix + "plus");
  VLOG(1) << "Rewrite expression: " << reduce.toString() << " => "
          << plus->toString();
  addThreadLocalRuntimeStat("numReduceRewrite", RuntimeCounter(1));
  return plus;
}

core::TypedExprPtr rewriteReduce(
    const std::string& prefix,
    const core::TypedExprPtr& expr) {
  auto* reduce = dynamic_cast<const core::CallTypedExpr*>(expr.get());
  if (!reduce || reduce->name() != prefix + "reduce" ||
      reduce->inputs().size() != 4) {
    return nullptr;
  }
  auto* outputFunction =
      dynamic_cast<const core::LambdaTypedExpr*>(reduce->inputs()[3].get());
  if (!outputFunction) {
    return nullptr;
  }
  auto& outputArgs = outputFunction->signature();
  if (outputArgs->size() != 1) {
    return nullptr;
  }
  if (!isVariableReference(outputFunction->body(), outputArgs->nameOf(0))) {
    return nullptr;
  }
  auto* inputFunction =
      dynamic_cast<const core::LambdaTypedExpr*>(reduce->inputs()[2].get());
  if (!inputFunction) {
    return nullptr;
  }
  auto& inputArgs = inputFunction->signature();
  if (inputArgs->size() != 2) {
    return nullptr;
  }
  auto& s = inputArgs->nameOf(0);
  auto* inputBody =
      dynamic_cast<const core::CallTypedExpr*>(inputFunction->body().get());
  if (!inputBody) {
    return nullptr;
  }
  if (inputBody->name() == prefix + "plus") {
    // s + f(x) => array_sum(transform(array, x -> f(x)))
    auto fx = extractFromAddition(prefix, inputFunction->body(), s);
    if (!fx) {
      return nullptr;
    }
    return toArraySum(prefix, *reduce, inputArgs, fx);
  } else if (inputBody->name() == prefix + "minus") {
    // (s + f(x)) - g(x) => array_sum(transform(array, x -> f(x) - g(x)))
    auto fx = extractFromAddition(prefix, inputBody->inputs()[0], s);
    if (!fx) {
      return nullptr;
    }
    auto minus = std::make_shared<core::CallTypedExpr>(
        fx->type(),
        std::vector<core::TypedExprPtr>({fx, inputBody->inputs()[1]}),
        prefix + "minus");
    return toArraySum(prefix, *reduce, inputArgs, minus);
  } else if (inputBody->name() == "if" && inputBody->inputs().size() == 3) {
    // if(h(x), s + f(x), s + g(x)) =>
    // array_sum(transform(array, x -> if(h(x), f(x), g(x))))
    auto fx = extractFromAddition(prefix, inputBody->inputs()[1], s);
    if (!fx) {
      return nullptr;
    }
    auto gx = extractFromAddition(prefix, inputBody->inputs()[2], s);
    if (!gx) {
      return nullptr;
    }
    auto ifExpr = std::make_shared<core::CallTypedExpr>(
        fx->type(),
        std::vector<core::TypedExprPtr>({inputBody->inputs()[0], fx, gx}),
        "if");
    return toArraySum(prefix, *reduce, inputArgs, ifExpr);
  }
  return nullptr;
}

} // namespace

/// reduce is null preserving for the array. But since an
/// expr tree with a lambda depends on all named fields, including
/// captures, a null in a capture does not automatically make a
/// null result.

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_reduce,
    ReduceFunction::signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<ReduceFunction>());

void registerReduceRewrites(const std::string& prefix) {
  exec::registerExpressionRewrite(
      [prefix](const auto& expr) { return rewriteReduce(prefix, expr); });
}

} // namespace facebook::velox::functions
