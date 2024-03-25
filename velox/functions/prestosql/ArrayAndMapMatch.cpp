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

// @tparam TContainer Either ArrayVector or MapVector.
template <typename TContainer, bool initialValue, bool earlyReturn>
class MatchFunction : public exec::VectorFunction {
 protected:
  virtual std::shared_ptr<TContainer> flattenContainer(
      const SelectivityVector& rows,
      const VectorPtr& input,
      DecodedVector& decodedInput) const = 0;

  virtual const VectorPtr& lambdaArgument(
      const std::shared_ptr<TContainer>& flatContainer) const = 0;

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector decoder(context, *args[0], rows);
    auto& decodedContainer = *decoder.get();

    auto flatContainer = flattenContainer(rows, args[0], decodedContainer);
    auto* rawOffsets = flatContainer->rawOffsets();
    auto* rawSizes = flatContainer->rawSizes();

    std::vector<VectorPtr> lambdaArgs = {lambdaArgument(flatContainer)};
    const auto numElements = lambdaArgs[0]->size();

    context.ensureWritable(rows, BOOLEAN(), result);
    auto* flatResult = result->asFlatVector<bool>();

    VectorPtr matchBits;
    exec::LocalDecodedVector bitsDecoder(context);

    // Loop over lambda functions and apply these to elements of the base array,
    // in most cases there will be only one function and the loop will run once.
    auto it = args[1]->asUnchecked<FunctionVector>()->iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows = toElementRows<TContainer>(
          numElements, *entry.rows, flatContainer.get());
      auto wrapCapture = toWrapCapture<TContainer>(
          numElements, entry.callable, *entry.rows, flatContainer);

      ErrorVectorPtr elementErrors;
      entry.callable->applyNoThrow(
          elementRows,
          nullptr, // No need to preserve any values in 'matchBits'.
          wrapCapture,
          &context,
          lambdaArgs,
          elementErrors,
          &matchBits);

      bitsDecoder.get()->decode(*matchBits, elementRows);
      entry.rows->applyToSelected([&](vector_size_t row) {
        if (flatContainer->isNullAt(row)) {
          flatResult->setNull(row, true);
        } else {
          applyInternal(
              *flatResult,
              context,
              row,
              rawOffsets[row],
              rawSizes[row],
              elementErrors,
              bitsDecoder);
        }
      });
    }
  }

  static FOLLY_ALWAYS_INLINE bool hasError(
      const ErrorVectorPtr& errors,
      vector_size_t row) {
    return errors && row < errors->size() && !errors->isNullAt(row);
  }

  void applyInternal(
      FlatVector<bool>& flatResult,
      exec::EvalCtx& context,
      vector_size_t arrayRow,
      vector_size_t offset,
      vector_size_t size,
      const ErrorVectorPtr& elementErrors,
      const exec::LocalDecodedVector& bitsDecoder) const {
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
      flatResult.set(arrayRow, result);
    } else if (errorPtr) {
      context.setError(arrayRow, errorPtr);
    } else if (hasNull) {
      flatResult.setNull(arrayRow, true);
    } else {
      flatResult.set(arrayRow, result);
    }
  }
};

template <bool initialValue, bool earlyReturn>
class ArrayMatchFunction
    : public MatchFunction<ArrayVector, initialValue, earlyReturn> {
 protected:
  ArrayVectorPtr flattenContainer(
      const SelectivityVector& rows,
      const VectorPtr& input,
      DecodedVector& decodedInput) const override {
    return flattenArray(rows, input, decodedInput);
  }

  const VectorPtr& lambdaArgument(
      const ArrayVectorPtr& flatContainer) const override {
    return flatContainer->elements();
  }
};

class AllMatchFunction : public ArrayMatchFunction<true, false> {};
class AnyMatchFunction : public ArrayMatchFunction<false, true> {};
class NoneMatchFunction : public ArrayMatchFunction<true, true> {};

template <bool initialValue, bool earlyReturn>
class MapKeysMatchFunction
    : public MatchFunction<MapVector, initialValue, earlyReturn> {
 protected:
  MapVectorPtr flattenContainer(
      const SelectivityVector& rows,
      const VectorPtr& input,
      DecodedVector& decodedInput) const override {
    return flattenMap(rows, input, decodedInput);
  }

  const VectorPtr& lambdaArgument(
      const MapVectorPtr& flatContainer) const override {
    return flatContainer->mapKeys();
  }
};

class AllKeysMatchFunction : public MapKeysMatchFunction<true, false> {};
class AnyKeysMatchFunction : public MapKeysMatchFunction<false, true> {};
class NoKeysMatchFunction : public MapKeysMatchFunction<true, true> {};

template <bool initialValue, bool earlyReturn>
class MapValuesMatchFunction
    : public MatchFunction<MapVector, initialValue, earlyReturn> {
 protected:
  MapVectorPtr flattenContainer(
      const SelectivityVector& rows,
      const VectorPtr& input,
      DecodedVector& decodedInput) const override {
    return flattenMap(rows, input, decodedInput);
  }

  const VectorPtr& lambdaArgument(
      const MapVectorPtr& flatContainer) const override {
    return flatContainer->mapValues();
  }
};

class AnyValuesMatchFunction : public MapValuesMatchFunction<false, true> {};
class NoValuesMatchFunction : public MapValuesMatchFunction<true, true> {};

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // array(T), function(T) -> boolean
  return {exec::FunctionSignatureBuilder()
              .typeVariable("T")
              .returnType("boolean")
              .argumentType("array(T)")
              .argumentType("function(T, boolean)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> keysSignatures() {
  // map(K, V), function(K) -> boolean
  return {exec::FunctionSignatureBuilder()
              .typeVariable("K")
              .typeVariable("V")
              .returnType("boolean")
              .argumentType("map(K,V)")
              .argumentType("function(K, boolean)")
              .build()};
}

std::vector<std::shared_ptr<exec::FunctionSignature>> valuesSignatures() {
  // map(K, V), function(V) -> boolean
  return {exec::FunctionSignatureBuilder()
              .typeVariable("K")
              .typeVariable("V")
              .returnType("boolean")
              .argumentType("map(K,V)")
              .argumentType("function(V, boolean)")
              .build()};
}

} // namespace

/// Match functions are null preserving for the array/map argument, but
/// predicate expression may use other fields and may not preserve nulls in
/// these.
///
/// For example, all_match(array[1, 2, 3], x -> x > coalesce(a, 0)) should
/// not return null when 'a' is null.

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_all_match,
    signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<AllMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_any_match,
    signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<AnyMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_none_match,
    signatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<NoneMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_all_keys_match,
    keysSignatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<AllKeysMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_any_keys_match,
    keysSignatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<AnyKeysMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_no_keys_match,
    keysSignatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<NoKeysMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_any_values_match,
    valuesSignatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<AnyValuesMatchFunction>());

VELOX_DECLARE_VECTOR_FUNCTION_WITH_METADATA(
    udf_no_values_match,
    valuesSignatures(),
    exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
    std::make_unique<NoValuesMatchFunction>());

} // namespace facebook::velox::functions
