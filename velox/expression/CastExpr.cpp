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

#include "velox/expression/CastExpr.h"
#include <stdexcept>
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/VectorUdfTypeSystem.h"
#include "velox/external/date/tz.h"
#include "velox/vector/FunctionVector.h"

namespace facebook::velox::exec {

namespace {

/// The per-row level Kernel
/// @tparam To The cast target type
/// @tparam From The expression type
/// @param row The index of the current row
/// @param input The input vector (of type From)
/// @param resultFlatVector The output vector (of type To)
/// @return False if the result is null
template <typename To, typename From, bool Truncate>
void applyCastKernel(
    vector_size_t row,
    const DecodedVector& input,
    FlatVector<To>* resultFlatVector) {
  // Special handling for string target type
  if constexpr (CppToType<To>::typeKind == TypeKind::VARCHAR) {
    auto output =
        util::Converter<CppToType<To>::typeKind, void, Truncate>::cast(
            input.valueAt<From>(row));
    // Write the result output to the output vector
    auto proxy =
        exec::StringProxy<FlatVector<StringView>>(resultFlatVector, row);
    proxy.resize(output.size());
    if (output.size()) {
      std::memcpy(proxy.data(), output.data(), output.size());
    }
    proxy.finalize();
  } else {
    auto result =
        util::Converter<CppToType<To>::typeKind, void, Truncate>::cast(
            input.valueAt<From>(row));
    resultFlatVector->set(row, result);
  }
}

void populateNestedRows(
    const SelectivityVector& rows,
    const vector_size_t* rawSizes,
    const vector_size_t* rawOffsets,
    SelectivityVector& nestedRows) {
  nestedRows.clearAll();
  rows.applyToSelected([&](auto row) {
    nestedRows.setValidRange(
        rawOffsets[row], rawOffsets[row] + rawSizes[row], true);
  });
  nestedRows.updateBounds();
}

} // namespace

template <typename To, typename From>
void CastExpr::applyCastWithTry(
    const SelectivityVector& rows,
    exec::EvalCtx* context,
    const DecodedVector& input,
    FlatVector<To>* resultFlatVector) {
  const auto& queryCtx = context->execCtx()->queryCtx();
  auto isCastIntByTruncate = queryCtx->isCastIntByTruncate();

  if (!nullOnFailure_) {
    if (!isCastIntByTruncate) {
      rows.applyToSelected([&](int row) {
        // Passing a false truncate flag
        try {
          applyCastKernel<To, From, false>(row, input, resultFlatVector);
        } catch (const std::exception& e) {
          context->setError(row, std::current_exception());
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        // Passing a true truncate flag
        try {
          applyCastKernel<To, From, true>(row, input, resultFlatVector);
        } catch (const std::exception& e) {
          context->setError(row, std::current_exception());
        }
      });
    }
  } else {
    if (!isCastIntByTruncate) {
      rows.applyToSelected([&](int row) {
        // TRY_CAST implementation
        try {
          applyCastKernel<To, From, false>(row, input, resultFlatVector);
        } catch (...) {
          resultFlatVector->setNull(row, true);
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        // TRY_CAST implementation
        try {
          applyCastKernel<To, From, true>(row, input, resultFlatVector);
        } catch (...) {
          resultFlatVector->setNull(row, true);
        }
      });
    }
  }

  // If we're converting to a TIMESTAMP, check if we need to adjust the current
  // GMT timezone to the user provided session timezone.
  if constexpr (CppToType<To>::typeKind == TypeKind::TIMESTAMP) {
    // If user explicitly asked us to adjust the timezone.
    if (queryCtx->adjustTimestampToTimezone()) {
      auto sessionTzName = queryCtx->sessionTimezone();
      if (!sessionTzName.empty()) {
        // locate_zone throws runtime_error if the timezone couldn't be found
        // (so we're safe to dereference the pointer).
        auto* timeZone = date::locate_zone(sessionTzName);
        auto rawTimestamps = resultFlatVector->mutableRawValues();

        rows.applyToSelected(
            [&](int row) { rawTimestamps[row].toTimezone(*timeZone); });
      }
    }
  }
}

template <TypeKind Kind>
void CastExpr::applyCast(
    const TypeKind fromType,
    const SelectivityVector& rows,
    exec::EvalCtx* context,
    const DecodedVector& input,
    VectorPtr* result) {
  using To = typename TypeTraits<Kind>::NativeType;
  auto* resultFlatVector = (*result)->as<FlatVector<To>>();

  // Unwrapping fromType pointer. VERY IMPORTANT: dynamic type pointer and
  // static type templates in each cast must match exactly
  // @TODO Add support for needed complex types in T74045702
  switch (fromType) {
    case TypeKind::TINYINT: {
      return applyCastWithTry<To, int8_t>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::SMALLINT: {
      return applyCastWithTry<To, int16_t>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::INTEGER: {
      return applyCastWithTry<To, int32_t>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::BIGINT: {
      return applyCastWithTry<To, int64_t>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::BOOLEAN: {
      return applyCastWithTry<To, bool>(rows, context, input, resultFlatVector);
    }
    case TypeKind::REAL: {
      return applyCastWithTry<To, float>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::DOUBLE: {
      return applyCastWithTry<To, double>(
          rows, context, input, resultFlatVector);
    }
    case TypeKind::VARCHAR: {
      return applyCastWithTry<To, StringView>(
          rows, context, input, resultFlatVector);
    }
    // TODO(beroy2000): Will add support for TimeStamp after the converters are
    // fixed
    default: {
      VELOX_UNSUPPORTED("Invalid from type in casting: {}", fromType);
    }
  }
}

void CastExpr::applyMap(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx* context,
    const MapType& fromType,
    const MapType& toType,
    VectorPtr* result) {
  // Cast input keys/values vector to output keys/values vector using their
  // element selectivity vector
  auto inputMap = std::dynamic_pointer_cast<MapVector>(input);
  auto rawSizes = inputMap->rawSizes();
  auto rawOffsets = inputMap->rawOffsets();

  // Initialize nested rows
  auto mapKeys = inputMap->mapKeys();
  auto mapValues = inputMap->mapValues();

  LocalSelectivityVector nestedRows(context);
  if (fromType.keyType() != toType.keyType() ||
      fromType.valueType() != toType.valueType()) {
    nestedRows.allocate(mapKeys->size());
    populateNestedRows(rows, rawSizes, rawOffsets, *nestedRows.get());
  }

  // Cast keys
  VectorPtr newMapKeys;
  if (fromType.keyType() == toType.keyType()) {
    newMapKeys = inputMap->mapKeys();
  } else {
    apply(
        *nestedRows.get(),
        mapKeys,
        context,
        fromType.keyType(),
        toType.keyType(),
        &newMapKeys);
  }

  // Cast values
  VectorPtr newMapValues;
  if (fromType.valueType() == toType.valueType()) {
    newMapValues = mapValues;
  } else {
    apply(
        *nestedRows.get(),
        mapValues,
        context,
        fromType.valueType(),
        toType.valueType(),
        &newMapValues);
  }

  // Assemble the output map
  auto newMap = std::make_shared<MapVector>(
      context->pool(),
      MAP(toType.keyType(), toType.valueType()),
      inputMap->nulls(),
      rows.size(),
      inputMap->offsets(),
      inputMap->sizes(),
      newMapKeys,
      newMapValues,
      inputMap->getNullCount());
  context->moveOrCopyResult(newMap, rows, result);
}

void CastExpr::applyArray(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx* context,
    const ArrayType& fromType,
    const ArrayType& toType,
    VectorPtr* result) {
  auto inputArray = std::dynamic_pointer_cast<ArrayVector>(input);
  auto inputRawSizes = inputArray->rawSizes();
  auto inputOffsets = inputArray->rawOffsets();

  // Cast input array elements to output array elements based on their types
  // using their linear selectivity vector
  auto arrayElements = inputArray->elements();

  LocalSelectivityVector nestedRows(context->execCtx(), arrayElements->size());
  populateNestedRows(rows, inputRawSizes, inputOffsets, *nestedRows.get());

  VectorPtr newElements;
  apply(
      *nestedRows.get(),
      arrayElements,
      context,
      fromType.elementType(),
      toType.elementType(),
      &newElements);

  // Assemble the output array
  auto newArray = std::make_shared<ArrayVector>(
      context->pool(),
      ARRAY(toType.elementType()),
      inputArray->nulls(),
      rows.size(),
      inputArray->offsets(),
      inputArray->sizes(),
      newElements,
      inputArray->getNullCount());
  context->moveOrCopyResult(newArray, rows, result);
}

void CastExpr::applyRow(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx* context,
    const RowType& fromType,
    const RowType& toType,
    VectorPtr* result) {
  auto inputRow = std::dynamic_pointer_cast<RowVector>(input);
  int numInputChildren = inputRow->children().size();
  int numOutputChildren = toType.size();

  // Extract the flag indicating matching of children must be done by name or
  // position
  auto matchByName = context->execCtx()->queryCtx()->isMatchStructByName();

  // Cast each row child to its corresponding output child
  std::vector<VectorPtr> newChildren;
  newChildren.reserve(numOutputChildren);

  for (auto toChildrenIndex = 0; toChildrenIndex < numOutputChildren;
       toChildrenIndex++) {
    // For each child, find the corresponding column index in the output
    auto toFieldName = toType.nameOf(toChildrenIndex);
    bool matchNotFound = false;

    // If match is by field name and the input field name is not found
    // in the output row type, do not consider it in the output
    int fromChildrenIndex = -1;
    if (matchByName) {
      if (!fromType.containsChild(toFieldName)) {
        matchNotFound = true;
      } else {
        fromChildrenIndex = fromType.getChildIdx(toFieldName);
        toChildrenIndex = toType.getChildIdx(toFieldName);
      }
    } else {
      fromChildrenIndex = toChildrenIndex;
      if (fromChildrenIndex >= numInputChildren) {
        matchNotFound = true;
      }
    }

    // Updating output types and names
    VectorPtr outputChild;
    auto toChildType = toType.childAt(toChildrenIndex);

    if (matchNotFound) {
      if (nullOnFailure_) {
        VELOX_USER_FAIL(
            "Invalid complex cast the match is not found for the field {}",
            toFieldName)
      }
      // Create a vector for null for this child
      BaseVector::ensureWritable(
          rows, toChildType, context->pool(), &outputChild);
      outputChild->addNulls(nullptr, rows);
    } else {
      auto inputChild = inputRow->children()[fromChildrenIndex];
      if (toChildType == inputChild->type()) {
        outputChild = inputChild;
      } else {
        // Apply cast for the child
        apply(
            rows,
            inputChild,
            context,
            inputChild->type(),
            toChildType,
            &outputChild);
      }
    }
    newChildren.emplace_back(outputChild);
  }

  // Assemble the output row
  auto toNames = toType.names();
  auto toTypes = toType.children();
  auto finalRowType =
      std::make_shared<RowType>(std::move(toNames), std::move(toTypes));
  auto row = std::make_shared<RowVector>(
      context->pool(),
      finalRowType,
      input->nulls(),
      rows.size(),
      std::move(newChildren),
      input->getNullCount());
  context->moveOrCopyResult(row, rows, result);
}

void CastExpr::apply(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx* context,
    const std::shared_ptr<const Type>& fromType,
    const std::shared_ptr<const Type>& toType,
    VectorPtr* result) {
  LocalSelectivityVector nonNullRows(context->execCtx(), rows.end());
  *nonNullRows.get() = rows;
  if (input->mayHaveNulls()) {
    nonNullRows.get()->deselectNulls(
        input->flatRawNulls(rows), rows.begin(), rows.end());
  }

  LocalSelectivityVector nullRows(context->execCtx(), rows.end());
  nullRows.get()->clearAll();
  if (input->mayHaveNulls()) {
    *nullRows.get() = rows;
    nullRows.get()->deselectNonNulls(
        input->flatRawNulls(rows), rows.begin(), rows.end());
  }

  switch (toType->kind()) {
    // Handle complex type conversions
    case TypeKind::MAP:
      applyMap(
          *nonNullRows.get(),
          input,
          context,
          fromType->asMap(),
          toType->asMap(),
          result);
      break;
    case TypeKind::ARRAY:
      applyArray(
          *nonNullRows.get(),
          input,
          context,
          fromType->asArray(),
          toType->asArray(),
          result);
      break;
    case TypeKind::ROW:
      applyRow(
          *nonNullRows.get(),
          input,
          context,
          fromType->asRow(),
          toType->asRow(),
          result);
      break;
    default: {
      // Handling primitive type conversions
      DecodedVector decoded(*input.get(), rows);
      BaseVector::ensureWritable(rows, toType, context->pool(), result);

      // Unwrapping toType pointer. VERY IMPORTANT: dynamic type pointer and
      // static type templates in each cast must match exactly
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          applyCast,
          toType->kind(),
          fromType->kind(),
          *nonNullRows.get(),
          context,
          decoded,
          result);
    }
  }

  // Copy nulls from "input".
  if (nullRows.get()->hasSelections()) {
    auto targetNulls = (*result)->mutableRawNulls();
    nullRows.get()->applyToSelected(
        [&](auto row) { bits::setNull(targetNulls, row, true); });
  }
}

void CastExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx* context,
    VectorPtr* result) {
  VectorPtr input;
  inputs_[0]->eval(rows, context, &input);
  auto fromType = inputs_[0]->type();
  auto toType = std::const_pointer_cast<const Type>(type_);
  apply(rows, input, context, fromType, toType, result);
}

} // namespace facebook::velox::exec
