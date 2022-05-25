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

#include <fmt/format.h>

#include <velox/common/base/VeloxException.h>
#include "velox/common/base/Exceptions.h"
#include "velox/core/CoreTypeSystem.h"
#include "velox/expression/StringWriter.h"
#include "velox/external/date/tz.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/FunctionVector.h"
#include "velox/vector/SelectivityVector.h"

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
    FlatVector<To>* resultFlatVector,
    bool& nullOutput) {
  // Special handling for string target type
  if constexpr (CppToType<To>::typeKind == TypeKind::VARCHAR) {
    if (nullOutput) {
      resultFlatVector->setNull(row, true);
    } else {
      auto output =
          util::Converter<CppToType<To>::typeKind, void, Truncate>::cast(
              input.valueAt<From>(row), nullOutput);
      // Write the result output to the output vector
      auto writer = exec::StringWriter<>(resultFlatVector, row);
      writer.resize(output.size());
      if (output.size()) {
        std::memcpy(writer.data(), output.data(), output.size());
      }
      writer.finalize();
    }
  } else {
    auto result =
        util::Converter<CppToType<To>::typeKind, void, Truncate>::cast(
            input.valueAt<From>(row), nullOutput);
    if (nullOutput) {
      resultFlatVector->setNull(row, true);
    } else {
      resultFlatVector->set(row, result);
    }
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

std::string makeErrorMessage(
    const DecodedVector& input,
    vector_size_t row,
    const TypePtr& toType) {
  return fmt::format(
      "Failed to cast from {} to {}: {}.",
      input.base()->type()->toString(),
      toType->toString(),
      input.base()->toString(input.index(row)));
}

} // namespace

template <typename To, typename From>
void CastExpr::applyCastWithTry(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const DecodedVector& input,
    FlatVector<To>* resultFlatVector) {
  const auto& queryConfig = context.execCtx()->queryCtx()->config();
  auto isCastIntByTruncate = queryConfig.isCastIntByTruncate();

  if (!nullOnFailure_) {
    if (!isCastIntByTruncate) {
      context.applyToSelectedNoThrow(rows, [&](int row) {
        try {
          // Passing a false truncate flag
          bool nullOutput = false;
          applyCastKernel<To, From, false>(
              row, input, resultFlatVector, nullOutput);
          if (nullOutput) {
            throw std::invalid_argument("");
          }
        } catch (const VeloxRuntimeError& re) {
          VELOX_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              re.message());
        } catch (const VeloxUserError& ue) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              e.what());
        }
      });
    } else {
      context.applyToSelectedNoThrow(rows, [&](int row) {
        try {
          // Passing a true truncate flag
          bool nullOutput = false;
          applyCastKernel<To, From, true>(
              row, input, resultFlatVector, nullOutput);
          if (nullOutput) {
            throw std::invalid_argument("");
          }
        } catch (const VeloxRuntimeError& re) {
          VELOX_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              re.message());
        } catch (const VeloxUserError& ue) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              ue.message());
        } catch (const std::exception& e) {
          VELOX_USER_FAIL(
              makeErrorMessage(input, row, resultFlatVector->type()) + " " +
              e.what());
        }
      });
    }
  } else {
    if (!isCastIntByTruncate) {
      rows.applyToSelected([&](int row) {
        // TRY_CAST implementation
        try {
          bool nullOutput = false;
          applyCastKernel<To, From, false>(
              row, input, resultFlatVector, nullOutput);
          if (nullOutput) {
            resultFlatVector->setNull(row, true);
          }
        } catch (...) {
          resultFlatVector->setNull(row, true);
        }
      });
    } else {
      rows.applyToSelected([&](int row) {
        // TRY_CAST implementation
        try {
          bool nullOutput = false;
          applyCastKernel<To, From, true>(
              row, input, resultFlatVector, nullOutput);
          if (nullOutput) {
            resultFlatVector->setNull(row, true);
          }
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
    if (queryConfig.adjustTimestampToTimezone()) {
      auto sessionTzName = queryConfig.sessionTimezone();
      if (!sessionTzName.empty()) {
        // locate_zone throws runtime_error if the timezone couldn't be found
        // (so we're safe to dereference the pointer).
        auto* timeZone = date::locate_zone(sessionTzName);
        auto rawTimestamps = resultFlatVector->mutableRawValues();

        rows.applyToSelected(
            [&](int row) { rawTimestamps[row].toGMT(*timeZone); });
      }
    }
  }
}

template <TypeKind Kind>
void CastExpr::applyCast(
    const TypeKind fromType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const DecodedVector& input,
    VectorPtr& result) {
  using To = typename TypeTraits<Kind>::NativeType;
  auto* resultFlatVector = result->as<FlatVector<To>>();

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
    case TypeKind::VARCHAR:
    case TypeKind::VARBINARY: {
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

VectorPtr CastExpr::applyMap(
    const SelectivityVector& rows,
    const MapVector* input,
    exec::EvalCtx& context,
    const MapType& fromType,
    const MapType& toType) {
  // Cast input keys/values vector to output keys/values vector using their
  // element selectivity vector
  auto rawSizes = input->rawSizes();
  auto rawOffsets = input->rawOffsets();

  // Initialize nested rows
  auto mapKeys = input->mapKeys();
  auto mapValues = input->mapValues();

  LocalSelectivityVector nestedRows(context);
  if (fromType.keyType() != toType.keyType() ||
      fromType.valueType() != toType.valueType()) {
    nestedRows.allocate(mapKeys->size());
    populateNestedRows(rows, rawSizes, rawOffsets, *nestedRows);
  }

  // Cast keys
  VectorPtr newMapKeys;
  if (fromType.keyType() == toType.keyType()) {
    newMapKeys = input->mapKeys();
  } else {
    apply(
        *nestedRows,
        mapKeys,
        context,
        fromType.keyType(),
        toType.keyType(),
        newMapKeys);
  }

  // Cast values
  VectorPtr newMapValues;
  if (fromType.valueType() == toType.valueType()) {
    newMapValues = mapValues;
  } else {
    apply(
        *nestedRows,
        mapValues,
        context,
        fromType.valueType(),
        toType.valueType(),
        newMapValues);
  }

  // Assemble the output map
  return std::make_shared<MapVector>(
      context.pool(),
      MAP(toType.keyType(), toType.valueType()),
      input->nulls(),
      rows.size(),
      input->offsets(),
      input->sizes(),
      newMapKeys,
      newMapValues);
}

VectorPtr CastExpr::applyArray(
    const SelectivityVector& rows,
    const ArrayVector* input,
    exec::EvalCtx& context,
    const ArrayType& fromType,
    const ArrayType& toType) {
  auto inputRawSizes = input->rawSizes();
  auto inputOffsets = input->rawOffsets();

  // Cast input array elements to output array elements based on their types
  // using their linear selectivity vector
  auto arrayElements = input->elements();

  LocalSelectivityVector nestedRows(*context.execCtx(), arrayElements->size());
  populateNestedRows(rows, inputRawSizes, inputOffsets, *nestedRows);

  VectorPtr newElements;
  apply(
      *nestedRows,
      arrayElements,
      context,
      fromType.elementType(),
      toType.elementType(),
      newElements);

  // Assemble the output array
  return std::make_shared<ArrayVector>(
      context.pool(),
      ARRAY(toType.elementType()),
      input->nulls(),
      rows.size(),
      input->offsets(),
      input->sizes(),
      newElements);
}

VectorPtr CastExpr::applyRow(
    const SelectivityVector& rows,
    const RowVector* input,
    exec::EvalCtx& context,
    const RowType& fromType,
    const RowType& toType) {
  int numInputChildren = input->children().size();
  int numOutputChildren = toType.size();

  // Extract the flag indicating matching of children must be done by name or
  // position
  auto matchByName =
      context.execCtx()->queryCtx()->config().isMatchStructByName();

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
          rows, toChildType, context.pool(), &outputChild);
      outputChild->addNulls(nullptr, rows);
    } else {
      auto inputChild = input->children()[fromChildrenIndex];
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
            outputChild);
      }
    }
    newChildren.emplace_back(outputChild);
  }

  // Assemble the output row
  auto toNames = toType.names();
  auto toTypes = toType.children();
  auto finalRowType =
      std::make_shared<RowType>(std::move(toNames), std::move(toTypes));
  return std::make_shared<RowVector>(
      context.pool(),
      finalRowType,
      input->nulls(),
      rows.size(),
      std::move(newChildren));
}

/// Apply casting between a custom type and another type.
/// @param castTo The boolean indicating whether to cast an input to the custom
/// type or from the custom type
/// @param input The input vector
/// @param allRows The selectivity vector of all rows in input
/// @param nonNullRows The selectivity vector of non-null rows in input
/// @param castOperator The cast operator for the custom type
/// @param thisType The custom type
/// @param otherType The other type involved in this casting
/// @param context The context
/// @param nullOnFailure Whether this is a cast or try_cast operation
/// @param result The output vector
template <bool castTo>
void applyCustomTypeCast(
    VectorPtr& input,
    const SelectivityVector& allRows,
    const SelectivityVector& nonNullRows,
    const CastOperatorPtr& castOperator,
    const TypePtr& thisType,
    const TypePtr& otherType,
    exec::EvalCtx& context,
    bool nullOnFailure,
    VectorPtr& result) {
  VELOX_CHECK_NE(
      thisType,
      otherType,
      "Attempting to cast from {} to itself.",
      thisType->toString());

  LocalDecodedVector inputDecoded(context, *input, allRows);

  exec::LocalSelectivityVector baseRows(
      *context.execCtx(), inputDecoded->base()->size());
  baseRows->clearAll();
  context.applyToSelectedNoThrow(nonNullRows, [&](auto row) {
    baseRows->setValid(inputDecoded->index(row), true);
  });
  baseRows->updateBounds();

  VectorPtr localResult;
  if constexpr (castTo) {
    BaseVector::ensureWritable(
        *baseRows, thisType, context.pool(), &localResult);

    castOperator->castTo(
        *inputDecoded->base(), context, *baseRows, nullOnFailure, *localResult);
  } else {
    VELOX_NYI(
        "Casting from {} to {} is not implemented yet.",
        thisType->toString(),
        otherType->toString());
  }

  if (!inputDecoded->isIdentityMapping()) {
    localResult = inputDecoded->wrap(localResult, *input, allRows);
  }

  context.moveOrCopyResult(localResult, nonNullRows, result);
}

void CastExpr::apply(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx& context,
    const std::shared_ptr<const Type>& fromType,
    const std::shared_ptr<const Type>& toType,
    VectorPtr& result) {
  LocalSelectivityVector nonNullRows(*context.execCtx(), rows.end());
  *nonNullRows = rows;
  if (input->mayHaveNulls()) {
    nonNullRows->deselectNulls(
        input->flatRawNulls(rows), rows.begin(), rows.end());
  }

  LocalSelectivityVector nullRows(*context.execCtx(), rows.end());
  nullRows->clearAll();
  if (input->mayHaveNulls()) {
    *nullRows = rows;
    nullRows->deselectNonNulls(
        input->flatRawNulls(rows), rows.begin(), rows.end());
  }

  if (castToOperator_) {
    applyCustomTypeCast<true>(
        input,
        rows,
        *nonNullRows,
        castToOperator_,
        toType,
        fromType,
        context,
        nullOnFailure_,
        result);
  } else if (castFromOperator_) {
    applyCustomTypeCast<false>(
        input,
        rows,
        *nonNullRows,
        castFromOperator_,
        fromType,
        toType,
        context,
        nullOnFailure_,
        result);
  } else {
    LocalDecodedVector decoded(context, *input, rows);

    if (toType->isArray() || toType->isMap() || toType->isRow()) {
      LocalSelectivityVector translatedRows(
          *context.execCtx(), decoded->base()->size());
      translatedRows->clearAll();
      nonNullRows->applyToSelected([&](auto row) {
        translatedRows->setValid(decoded->index(row), true);
      });
      translatedRows->updateBounds();

      VectorPtr localResult;

      switch (toType->kind()) {
        // Handle complex type conversions
        case TypeKind::MAP:
          localResult = applyMap(
              *translatedRows,
              decoded->base()->asUnchecked<MapVector>(),
              context,
              fromType->asMap(),
              toType->asMap());
          break;
        case TypeKind::ARRAY:
          localResult = applyArray(
              *translatedRows,
              decoded->base()->asUnchecked<ArrayVector>(),
              context,
              fromType->asArray(),
              toType->asArray());
          break;
        case TypeKind::ROW:
          localResult = applyRow(
              *translatedRows,
              decoded->base()->asUnchecked<RowVector>(),
              context,
              fromType->asRow(),
              toType->asRow());
          break;
        default: {
          VELOX_UNREACHABLE();
        }
      }

      if (!decoded->isIdentityMapping()) {
        localResult = decoded->wrap(localResult, *input, rows);
      }

      context.moveOrCopyResult(localResult, rows, result);
    } else {
      // Handling primitive type conversions
      BaseVector::ensureWritable(rows, toType, context.pool(), &result);
      // Unwrapping toType pointer. VERY IMPORTANT: dynamic type pointer and
      // static type templates in each cast must match exactly
      VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
          applyCast,
          toType->kind(),
          fromType->kind(),
          *nonNullRows,
          context,
          *decoded,
          result);
    }
  }

  // Copy nulls from "input".
  if (nullRows->hasSelections()) {
    auto targetNulls = result->mutableRawNulls();
    nullRows->applyToSelected(
        [&](auto row) { bits::setNull(targetNulls, row, true); });
  }
}

void CastExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  ExceptionContextSetter exceptionContext(
      {[](auto* expr) { return static_cast<Expr*>(expr)->toString(); }, this});

  VectorPtr input;
  inputs_[0]->eval(rows, context, input);
  auto fromType = inputs_[0]->type();
  auto toType = std::const_pointer_cast<const Type>(type_);

  stats_.numProcessedVectors += 1;
  stats_.numProcessedRows += rows.countSelected();
  auto timer = cpuWallTimer();
  apply(rows, input, context, fromType, toType, result);
}

std::string CastExpr::toString(bool recursive) const {
  std::stringstream out;
  out << "cast(";
  if (recursive) {
    appendInputs(out);
  } else {
    out << inputs_[0]->toString(false);
  }
  out << " as " << type_->toString() << ")";
  return out.str();
}

} // namespace facebook::velox::exec
