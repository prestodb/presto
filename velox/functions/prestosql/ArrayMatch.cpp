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
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::functions {
namespace {

template <bool initialValue, bool earlyReturn>
class MatchFunction : public exec::VectorFunction {
 private:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector arrayDecoder(context, *args[0], rows);
    auto& decodedArray = *arrayDecoder.get();

    auto flatArray = flattenArray(rows, args[0], decodedArray);
    auto offsets = flatArray->rawOffsets();
    auto sizes = flatArray->rawSizes();

    std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    auto numElements = flatArray->elements()->size();

    SelectivityVector finalSelection;
    if (!context.isFinalSelection()) {
      finalSelection = toElementRows<ArrayVector>(
          numElements, *context.finalSelection(), flatArray.get());
    }

    VectorPtr matchBits;
    auto elementToTopLevelRows = getElementToTopLevelRows(
        numElements, rows, flatArray.get(), context.pool());

    // Loop over lambda functions and apply these to elements of the base array,
    // in most cases there will be only one function and the loop will run once.
    context.ensureWritable(rows, BOOLEAN(), result);
    auto flatResult = result->asFlatVector<bool>();
    exec::LocalDecodedVector bitsDecoder(context);
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);

    while (auto entry = it.next()) {
      ErrorVectorPtr elementErrors;
      auto elementRows =
          toElementRows<ArrayVector>(numElements, *entry.rows, flatArray.get());
      auto wrapCapture = toWrapCapture<ArrayVector>(
          numElements, entry.callable, *entry.rows, flatArray);
      entry.callable->applyNoThrow(
          elementRows,
          finalSelection,
          wrapCapture,
          &context,
          lambdaArgs,
          elementErrors,
          &matchBits);

      bitsDecoder.get()->decode(*matchBits, elementRows);
      entry.rows->applyToSelected([&](vector_size_t row) {
        applyInternal(
            flatResult,
            context,
            row,
            offsets,
            sizes,
            elementErrors,
            bitsDecoder);
      });
    }
  }

  static FOLLY_ALWAYS_INLINE bool hasError(
      const ErrorVectorPtr& errors,
      int idx) {
    return errors && idx < errors->size() && !errors->isNullAt(idx);
  }

  void applyInternal(
      FlatVector<bool>* flatResult,
      exec::EvalCtx& context,
      vector_size_t row,
      const vector_size_t* offsets,
      const vector_size_t* sizes,
      const ErrorVectorPtr& elementErrors,
      const exec::LocalDecodedVector& bitsDecoder) const {
    auto size = sizes[row];
    auto offset = offsets[row];

    // all_match, none_match and any_match need to loop over predicate results
    // for element arrays and check for results, nulls and errors.
    // These loops can be generalized using two booleans.
    //
    // Here is what the individual loops look like.
    //
    //---- kAll ----
    // bool allMatch = true
    //
    // loop:
    //   if not match:
    //    allMatch = false;
    //    break;
    //
    // if (!allMatch) -> false
    // else if hasError -> error
    // else if hasNull -> null
    // else -> true
    //
    //---- kAny ----
    //
    // bool anyMatch = false
    //
    // loop:
    //   if match:
    //    anyMatch = true;
    //    break;
    //
    // if (anyMatch) -> true
    // else if hasError -> error
    // else if hasNull -> null
    // else -> false
    //
    //---- kNone ----
    //
    // bool noneMatch = true;
    //
    // loop:
    //   if match:
    //    noneMatch = false;
    //    break;
    //
    // if (!noneMatch) -> false
    // else if hasError -> error
    // else if hasNull -> null
    // else -> true
    //
    // To generalize these loops, we use initialValue and earlyReturn booleans
    // like so:
    //
    //--- generic loop ---
    //
    // bool result = initialValue
    //
    // loop:
    //   if match == earlyReturn:
    //    result = false;
    //    break;
    //
    // if (result != initialValue) -> result
    // else if hasError -> error
    // else if hasNull -> null
    // else -> result
    bool result = initialValue;
    auto hasNull = false;
    std::exception_ptr errorPtr{nullptr};
    for (auto i = 0; i < size; ++i) {
      auto idx = offset + i;
      if (hasError(elementErrors, idx)) {
        errorPtr = *std::static_pointer_cast<std::exception_ptr>(
            elementErrors->valueAt(idx));
        continue;
      }

      if (bitsDecoder->isNullAt(idx)) {
        hasNull = true;
      } else if (bitsDecoder->valueAt<bool>(idx) == earlyReturn) {
        result = !result;
        break;
      }
    }

    if (result != initialValue) {
      flatResult->set(row, result);
    } else if (errorPtr) {
      context.setError(row, errorPtr);
    } else if (hasNull) {
      flatResult->setNull(row, true);
    } else {
      flatResult->set(row, result);
    }
  }
};

class AllMatchFunction : public MatchFunction<true, false> {};
class AnyMatchFunction : public MatchFunction<false, true> {};
class NoneMatchFunction : public MatchFunction<true, true> {};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T), function(T) -> boolean
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("boolean")
              .argumentType("array(T)")
              .argumentType("function(T, boolean)")
              .build()};
}

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_all_match,
    signatures(),
    std::make_unique<AllMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_any_match,
    signatures(),
    std::make_unique<AnyMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_none_match,
    signatures(),
    std::make_unique<NoneMatchFunction>());

} // namespace facebook::velox::functions
