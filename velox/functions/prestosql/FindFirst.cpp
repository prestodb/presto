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
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/LambdaFunctionUtil.h"
#include "velox/functions/lib/RowsTranslationUtil.h"

namespace facebook::velox::functions {
namespace {

void recordInvalidStartIndex(vector_size_t row, exec::EvalCtx& context) {
  try {
    VELOX_USER_FAIL("SQL array indices start at 1. Got 0.");
  } catch (const VeloxUserError&) {
    context.setVeloxExceptionError(row, std::current_exception());
  }
}

class FindFirstFunctionBase : public exec::VectorFunction {
 protected:
  ArrayVectorPtr prepareInputArray(
      const VectorPtr& input,
      const SelectivityVector& rows,
      exec::EvalCtx& context) const {
    exec::LocalDecodedVector arrayDecoder(context, *input, rows);
    auto& decodedArray = *arrayDecoder.get();

    return flattenArray(rows, input, decodedArray);
  }

  // Evaluates predicate on all elements of the array and identifies the first
  // matching element.
  //
  // @tparam THit Called when first matching element is found. Receives array
  // index in the flatArray vector and an index of the first matching element in
  // flatArray->elements() vector.
  // @tparam TMiss Called when no matching element is found. Receives array
  // index in the flatArray vector.
  //
  // @param rows Rows in 'flatArray' vector to process.
  // @param flatArray Flat array vector with possibly encoded elements.
  // @param startIndex Optional vector of starting indices. These are 1-based
  // and if negative indicate that the search for first matching element should
  // proceed in reverse. This vector may not be flat.
  // @param predicates FunctionVector holding the predicate expressions.
  template <typename THit, typename TMiss>
  void doApply(
      const SelectivityVector& rows,
      const ArrayVectorPtr& flatArray,
      const VectorPtr& startIndex,
      FunctionVector& predicates,
      exec::EvalCtx& context,
      THit onHit,
      TMiss onMiss) const {
    const auto* rawNulls = flatArray->rawNulls();
    const auto* rawOffsets = flatArray->rawOffsets();
    const auto* rawSizes = flatArray->rawSizes();

    const std::vector<VectorPtr> lambdaArgs = {flatArray->elements()};
    const auto numElements = flatArray->elements()->size();

    VectorPtr matchBits;
    exec::LocalDecodedVector bitsDecoder(context);

    // Adjust offsets and sizes according to specified start indices.
    StartIndexProcessor startIndexProcessor;
    if (startIndex != nullptr) {
      startIndexProcessor.process(
          startIndex, rows, rawNulls, rawOffsets, rawSizes, context);
      rawOffsets = startIndexProcessor.adjustedOffsets->as<vector_size_t>();
      rawSizes = startIndexProcessor.adjustedSizes->as<vector_size_t>();
    }

    // Loop over lambda functions and apply these to elements of the base array,
    // in most cases there will be only one function and the loop will run once.
    auto it = predicates.iterator(&rows);
    while (auto entry = it.next()) {
      auto elementRows = toElementRows(
          numElements, *entry.rows, rawNulls, rawOffsets, rawSizes);
      if (!elementRows.hasSelections()) {
        // All arrays are NULL or empty.
        entry.rows->applyToSelected([&](vector_size_t row) { onMiss(row); });
        continue;
      }

      auto wrapCapture = toWrapCapture<ArrayVector>(
          numElements, entry.callable, *entry.rows, flatArray);

      exec::EvalErrorsPtr elementErrors;
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
        if (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) {
          onMiss(row);
        } else if (
            auto firstMatchingIndex = findFirstMatch(
                context,
                row,
                rawOffsets[row],
                rawSizes[row],
                elementErrors,
                bitsDecoder)) {
          onHit(row, firstMatchingIndex.value());
        } else {
          onMiss(row);
        }
      });
    }
  }

 private:
  // Adjusts offset and sizes to take into account custom start index.
  // For example, given an array with offset 10, size 20 and start index 3 the
  // new offset and size are 12 and 18. Given the same array and start index
  // -5, the new offset and size are 25, -16. The negative size will be used
  // later to loop over array in reverse.
  struct StartIndexProcessor {
    BufferPtr adjustedOffsets;
    BufferPtr adjustedSizes;

    void process(
        const VectorPtr& startIndex,
        const SelectivityVector& rows,
        const uint64_t* rawNulls,
        const vector_size_t* rawOffsets,
        const vector_size_t* rawSizes,
        exec::EvalCtx& context) {
      VELOX_CHECK_NOT_NULL(startIndex);

      exec::LocalDecodedVector startIndexDecoder(context, *startIndex, rows);

      adjustedOffsets = allocateIndices(rows.end(), context.pool());
      auto* rawAdjustedOffsets = adjustedOffsets->asMutable<vector_size_t>();

      adjustedSizes = allocateIndices(rows.end(), context.pool());
      auto* rawAdjustedSizes = adjustedSizes->asMutable<vector_size_t>();

      rows.applyToSelected([&](auto row) {
        if (rawNulls != nullptr && bits::isBitNull(rawNulls, row)) {
          rawAdjustedOffsets[row] = 0;
          rawAdjustedSizes[row] = 0;
        } else if (startIndexDecoder->isNullAt(row)) {
          rawAdjustedOffsets[row] = 0;
          rawAdjustedSizes[row] = 0;
        } else {
          const auto offset = rawOffsets[row];
          const auto size = rawSizes[row];
          const auto start = startIndexDecoder->valueAt<int64_t>(row);
          if (start > size || start < -size) {
            rawAdjustedOffsets[row] = 0;
            rawAdjustedSizes[row] = 0;
          } else if (start == 0) {
            rawAdjustedOffsets[row] = 0;
            rawAdjustedSizes[row] = 0;

            recordInvalidStartIndex(row, context);
          } else if (start > 0) {
            rawAdjustedOffsets[row] = offset + (start - 1);
            rawAdjustedSizes[row] = size - (start - 1);
          } else {
            // start is negative.
            rawAdjustedOffsets[row] = offset + size + start;
            rawAdjustedSizes[row] = -(size + start + 1);
          }
        }
      });
    }
  };

  static SelectivityVector toElementRows(
      vector_size_t numElements,
      const SelectivityVector& arrayRows,
      const uint64_t* rawNulls,
      const vector_size_t* rawOffsets,
      const vector_size_t* rawSizes) {
    SelectivityVector elementRows(numElements, false);

    arrayRows.applyToSelected([&](auto arrayRow) {
      if (rawNulls != nullptr && bits::isBitNull(rawNulls, arrayRow)) {
        return;
      }

      const auto offset = rawOffsets[arrayRow];
      const auto size = rawSizes[arrayRow];

      if (size > 0) {
        elementRows.setValidRange(offset, offset + size, true);
      } else {
        elementRows.setValidRange(offset + 1 + size, offset + 1, true);
      }
    });
    elementRows.updateBounds();

    return elementRows;
  }

  // Returns an index of the first matching element or std::nullopt if no
  // element matches or there was an error evaluating the predicate.
  //
  // @param size Number of elements to check starting with 'offset'. If 'size'
  // is negative, elements are processed backwards: offset, offset - 1, etc.
  std::optional<vector_size_t> findFirstMatch(
      exec::EvalCtx& context,
      vector_size_t arrayRow,
      vector_size_t offset,
      vector_size_t size,
      const exec::EvalErrorsPtr& elementErrors,
      const exec::LocalDecodedVector& matchDecoder) const {
    const auto step = size > 0 ? 1 : -1;
    for (auto index = offset; index != offset + size; index += step) {
      if (elementErrors) {
        if (auto error = elementErrors->errorAt(index)) {
          // Report first error to match Presto's implementation.
          context.setError(arrayRow, *error.value());
          return std::nullopt;
        }
      }

      if (!matchDecoder->isNullAt(index) &&
          matchDecoder->valueAt<bool>(index)) {
        return index;
      }
    }

    return std::nullopt;
  }
};

class FindFirstFunction : public FindFirstFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatArray = prepareInputArray(args[0], rows, context);
    auto startIndexVector = (args.size() == 3 ? args[1] : nullptr);

    if (flatArray->elements()->size() == 0) {
      // All arrays are NULL or empty.

      if (startIndexVector != nullptr) {
        // Check start indices for zeros.

        exec::LocalDecodedVector startIndexDecoder(
            context, *startIndexVector, rows);
        rows.applyToSelected([&](auto row) {
          if (flatArray->isNullAt(row) || startIndexDecoder->isNullAt(row)) {
            return;
          }
          const auto start = startIndexDecoder->valueAt<int64_t>(row);
          if (start == 0) {
            recordInvalidStartIndex(row, context);
          }
        });
      }

      auto localResult = BaseVector::createNullConstant(
          outputType, rows.end(), context.pool());
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }

    auto* predicateVector = args.back()->asUnchecked<FunctionVector>();

    // Collect indices of the first matching elements or NULLs if no match or
    // error.
    BufferPtr resultIndices = allocateIndices(rows.end(), context.pool());
    auto* rawResultIndices = resultIndices->asMutable<vector_size_t>();

    BufferPtr resultNulls = nullptr;
    uint64_t* rawResultNulls = nullptr;

    doApply(
        rows,
        flatArray,
        startIndexVector,
        *predicateVector,
        context,
        [&](vector_size_t row, vector_size_t firstMatchingIndex) {
          if (flatArray->elements()->isNullAt(firstMatchingIndex)) {
            try {
              VELOX_USER_FAIL("find_first found NULL as the first match");
            } catch (const VeloxUserError&) {
              context.setVeloxExceptionError(row, std::current_exception());
            }
          } else {
            rawResultIndices[row] = firstMatchingIndex;
          }
        },
        [&](vector_size_t row) {
          if (rawResultNulls == nullptr) {
            resultNulls = allocateNulls(rows.end(), context.pool());
            rawResultNulls = resultNulls->asMutable<uint64_t>();
          }
          bits::setNull(rawResultNulls, row);
        });

    auto localResult = BaseVector::wrapInDictionary(
        resultNulls, resultIndices, rows.end(), flatArray->elements());
    context.moveOrCopyResult(localResult, rows, result);
  }
};

class FindFirstIndexFunction : public FindFirstFunctionBase {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto flatArray = prepareInputArray(args[0], rows, context);
    const auto* rawOffsets = flatArray->rawOffsets();

    auto* predicateVector = args.back()->asUnchecked<FunctionVector>();
    auto startIndexVector = (args.size() == 3 ? args[1] : nullptr);

    context.ensureWritable(rows, BIGINT(), result);
    auto flatResult = result->asFlatVector<int64_t>();

    doApply(
        rows,
        flatArray,
        startIndexVector,
        *predicateVector,
        context,
        [&](vector_size_t row, vector_size_t firstMatchingIndex) {
          // Convert zero-based index of a row in the elements vector into a
          // 1-based index of the element in the array.
          flatResult->set(row, 1 + firstMatchingIndex - rawOffsets[row]);
        },
        [&](vector_size_t row) { flatResult->setNull(row, true); });
  }
};

std::vector<std::shared_ptr<exec::FunctionSignature>> valueSignatures() {
  return {
      // array(T), function(T, boolean) -> T
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("array(T)")
          .argumentType("function(T, boolean)")
          .build(),
      // array(T), bigint, function(T, boolean) -> T
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("T")
          .argumentType("array(T)")
          .argumentType("bigint")
          .argumentType("function(T, boolean)")
          .build(),
  };
}

std::vector<std::shared_ptr<exec::FunctionSignature>> indexSignatures() {
  return {
      // array(T), function(T, boolean) -> bigint
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .argumentType("array(T)")
          .argumentType("function(T, boolean)")
          .build(),
      // array(T), bigint, function(T, boolean) -> bigint
      exec::FunctionSignatureBuilder()
          .typeVariable("T")
          .returnType("bigint")
          .argumentType("array(T)")
          .argumentType("bigint")
          .argumentType("function(T, boolean)")
          .build(),
  };
}

} // namespace

/// find_first function is null preserving for the array argument, but
/// predicate expression may use other fields and may not preserve nulls in
/// these.
/// For example: find_first(array[1, 2, 3], x -> x > coalesce(a, 0)) should
/// not return null when 'a' is null.

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_find_first,
    valueSignatures(),
    std::make_unique<FindFirstFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_find_first_index,
    indexSignatures(),
    std::make_unique<FindFirstIndexFunction>());

} // namespace facebook::velox::functions
