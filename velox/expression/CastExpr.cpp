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
    const BaseVector& input,
    FlatVector<To>* resultFlatVector,
    bool& nullOutput) {
  auto* inputVector = input.asUnchecked<SimpleVector<From>>();

  // Special handling for string target type
  if constexpr (CppToType<To>::typeKind == TypeKind::VARCHAR) {
    if (nullOutput) {
      resultFlatVector->setNull(row, true);
    } else {
      auto output =
          util::Converter<CppToType<To>::typeKind, void, Truncate>::cast(
              inputVector->valueAt(row), nullOutput);
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
            inputVector->valueAt(row), nullOutput);
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
    const BaseVector& input,
    vector_size_t row,
    const TypePtr& toType) {
  return fmt::format(
      "Failed to cast from {} to {}: {}.",
      input.type()->toString(),
      toType->toString(),
      input.toString(row));
}

template <typename TInput, typename TOutput>
void applyDecimalCastKernel(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr castResult,
    const bool nullOnFailure) {
  auto sourceVector = input.as<SimpleVector<TInput>>();
  auto castResultRawBuffer =
      castResult->asUnchecked<FlatVector<TOutput>>()->mutableRawValues();
  const auto& fromPrecisionScale = getDecimalPrecisionScale(*fromType);
  const auto& toPrecisionScale = getDecimalPrecisionScale(*toType);
  context.applyToSelectedNoThrow(rows, [&](vector_size_t row) {
    auto rescaledValue = DecimalUtil::rescaleWithRoundUp<TInput, TOutput>(
        sourceVector->valueAt(row),
        fromPrecisionScale.first,
        fromPrecisionScale.second,
        toPrecisionScale.first,
        toPrecisionScale.second,
        nullOnFailure);
    if (rescaledValue.has_value()) {
      castResultRawBuffer[row] = rescaledValue.value();
    } else {
      castResult->setNull(row, true);
    }
  });
}
} // namespace

template <typename To, typename From>
void CastExpr::applyCastWithTry(
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
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
    const TypePtr& fromType,
    const TypePtr& toType,
    const SelectivityVector& rows,
    exec::EvalCtx& context,
    const BaseVector& input,
    VectorPtr& result) {
  using To = typename TypeTraits<Kind>::NativeType;
  context.ensureWritable(rows, toType, result);
  auto* resultFlatVector = result->as<FlatVector<To>>();

  // Unwrapping fromType pointer. VERY IMPORTANT: dynamic type pointer and
  // static type templates in each cast must match exactly
  // @TODO Add support for needed complex types in T74045702
  switch (fromType->kind()) {
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
    case TypeKind::DATE: {
      return applyCastWithTry<To, Date>(rows, context, input, resultFlatVector);
    }
    case TypeKind::TIMESTAMP: {
      return applyCastWithTry<To, Timestamp>(
          rows, context, input, resultFlatVector);
    }
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
      context.ensureWritable(rows, toChildType, outputChild);
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
  auto finalRowType = ROW(std::move(toNames), std::move(toTypes));
  return std::make_shared<RowVector>(
      context.pool(),
      finalRowType,
      input->nulls(),
      rows.size(),
      std::move(newChildren));
}

VectorPtr CastExpr::applyDecimal(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType) {
  VectorPtr castResult;
  context.ensureWritable(rows, toType, castResult);
  (*castResult).clearNulls(rows);
  switch (fromType->kind()) {
    case TypeKind::SHORT_DECIMAL: {
      if (toType->kind() == TypeKind::SHORT_DECIMAL) {
        applyDecimalCastKernel<UnscaledShortDecimal, UnscaledShortDecimal>(
            rows, input, context, fromType, toType, castResult, nullOnFailure_);
      } else {
        applyDecimalCastKernel<UnscaledShortDecimal, UnscaledLongDecimal>(
            rows, input, context, fromType, toType, castResult, nullOnFailure_);
      }
      break;
    }
    case TypeKind::LONG_DECIMAL: {
      if (toType->kind() == TypeKind::SHORT_DECIMAL) {
        applyDecimalCastKernel<UnscaledLongDecimal, UnscaledShortDecimal>(
            rows, input, context, fromType, toType, castResult, nullOnFailure_);
      } else {
        applyDecimalCastKernel<UnscaledLongDecimal, UnscaledLongDecimal>(
            rows, input, context, fromType, toType, castResult, nullOnFailure_);
      }
      break;
    }
    default:
      VELOX_UNSUPPORTED(
          "Cast from {} to {} is not supported",
          fromType->toString(),
          toType->toString());
  }
  return castResult;
}

void CastExpr::applyPeeled(
    const SelectivityVector& rows,
    const BaseVector& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  if (castFromOperator_ || castToOperator_) {
    VELOX_CHECK_NE(
        fromType,
        toType,
        "Attempting to cast from {} to itself.",
        fromType->toString());

    if (castToOperator_) {
      castToOperator_->castTo(
          input, context, rows, nullOnFailure_, toType, result);
    } else {
      castFromOperator_->castFrom(
          input, context, rows, nullOnFailure_, toType, result);
    }
  } else {
    switch (toType->kind()) {
      case TypeKind::MAP:
        result = applyMap(
            rows,
            input.asUnchecked<MapVector>(),
            context,
            fromType->asMap(),
            toType->asMap());
        break;
      case TypeKind::ARRAY:
        result = applyArray(
            rows,
            input.asUnchecked<ArrayVector>(),
            context,
            fromType->asArray(),
            toType->asArray());
        break;
      case TypeKind::ROW:
        result = applyRow(
            rows,
            input.asUnchecked<RowVector>(),
            context,
            fromType->asRow(),
            toType->asRow());
        break;
      case TypeKind::SHORT_DECIMAL:
      case TypeKind::LONG_DECIMAL:
        result = applyDecimal(rows, input, context, fromType, toType);
        break;
      default: {
        // Handle primitive type conversions.
        VELOX_DYNAMIC_SCALAR_TYPE_DISPATCH(
            applyCast,
            toType->kind(),
            fromType,
            toType,
            rows,
            context,
            input,
            result);
      }
    }
  }
}

void CastExpr::apply(
    const SelectivityVector& rows,
    VectorPtr& input,
    exec::EvalCtx& context,
    const TypePtr& fromType,
    const TypePtr& toType,
    VectorPtr& result) {
  LocalDecodedVector decoded(context, *input, rows);
  auto* rawNulls = decoded->nulls();

  LocalSelectivityVector nonNullRows(*context.execCtx(), rows.end());
  *nonNullRows = rows;
  if (rawNulls) {
    nonNullRows->deselectNulls(rawNulls, rows.begin(), rows.end());
  }

  LocalSelectivityVector nullRows(*context.execCtx(), rows.end());
  nullRows->clearAll();
  if (rawNulls) {
    *nullRows = rows;
    nullRows->deselectNonNulls(rawNulls, rows.begin(), rows.end());
  }

  VectorPtr localResult;
  if (!nonNullRows->hasSelections()) {
    localResult =
        BaseVector::createNullConstant(toType, rows.end(), context.pool());
  } else if (decoded->isIdentityMapping()) {
    applyPeeled(
        *nonNullRows, *decoded->base(), context, fromType, toType, localResult);

  } else {
    ContextSaver saver;
    LocalSelectivityVector translatedRowsHolder(*context.execCtx());

    if (decoded->isConstantMapping()) {
      auto index = decoded->index(nonNullRows->begin());
      singleRow(translatedRowsHolder, index);
      context.saveAndReset(saver, *nonNullRows);
      context.setConstantWrap(index);
    } else {
      translateToInnerRows(*nonNullRows, *decoded, translatedRowsHolder);
      context.saveAndReset(saver, *nonNullRows);
      auto wrapping = decoded->dictionaryWrapping(*input, *nonNullRows);
      context.setDictionaryWrap(
          std::move(wrapping.indices), std::move(wrapping.nulls));
    }

    applyPeeled(
        *translatedRowsHolder,
        *decoded->base(),
        context,
        fromType,
        toType,
        localResult);

    localResult = context.applyWrapToPeeledResult(toType, localResult, rows);
  }
  context.moveOrCopyResult(localResult, rows, result);
  context.releaseVector(localResult);

  // If we have a mix of null and non-null in input, add nulls to the result.
  VELOX_CHECK_NOT_NULL(result);
  if (nullRows->hasSelections() && nonNullRows->hasSelections()) {
    auto targetNulls = result->mutableRawNulls();
    nullRows->applyToSelected(
        [&](auto row) { bits::setNull(targetNulls, row, true); });
  }
}

void CastExpr::evalSpecialForm(
    const SelectivityVector& rows,
    EvalCtx& context,
    VectorPtr& result) {
  VectorPtr input;
  inputs_[0]->eval(rows, context, input);
  auto fromType = inputs_[0]->type();
  auto toType = std::const_pointer_cast<const Type>(type_);

  apply(rows, input, context, fromType, toType, result);
  // Return 'input' back to the vector pool in 'context' so it can be reused.
  context.releaseVector(input);
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

namespace {

/// Appends type's SQL string to 'out'. Uses DuckDB SQL.
void toTypeSql(const TypePtr& type, std::ostream& out) {
  if (type->isPrimitiveType()) {
    out << type->toString();
    return;
  }

  switch (type->kind()) {
    case TypeKind::ARRAY:
      // Append <type>[], e.g. bigint[].
      toTypeSql(type->childAt(0), out);
      out << "[]";
      break;
    case TypeKind::MAP:
      // Append map(<key>, <value>), e.g. map(varchar, bigint).
      out << "map(";
      toTypeSql(type->childAt(0), out);
      out << ", ";
      toTypeSql(type->childAt(1), out);
      out << ")";
      break;
    case TypeKind::ROW: {
      // Append struct(name1 type1, name2 type2,..), e.g.
      // struct(a bigint, b real);
      const auto& rowType = type->asRow();
      out << "struct(";
      for (auto i = 0; i < type->size(); ++i) {
        if (i > 0) {
          out << ", ";
        }
        out << rowType.nameOf(i) << " ";
        toTypeSql(type->childAt(i), out);
      }
      out << ")";
      break;
    }
    default:
      VELOX_UNSUPPORTED("Type is not supported: {}", type->toString());
  }
}
} // namespace

std::string CastExpr::toSql() const {
  std::stringstream out;
  out << "cast(";
  appendInputsSql(out);
  out << " as ";
  toTypeSql(type_, out);
  out << ")";
  return out.str();
}
} // namespace facebook::velox::exec
