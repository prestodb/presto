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
#include "velox/functions/sparksql/ConcatWs.h"
#include "velox/expression/ConstantExpr.h"
#include "velox/expression/VectorFunction.h"

namespace facebook::velox::functions::sparksql {

namespace {
class ConcatWs : public exec::VectorFunction {
 public:
  explicit ConcatWs(const std::optional<StringView>& separator)
      : separator_(separator) {}

  bool isConstantSeparator() const {
    return separator_.has_value();
  }

  // Calculate the total number of bytes in the result.
  size_t calculateTotalResultBytes(
      const SelectivityVector& rows,
      exec::EvalCtx& context,
      std::vector<exec::LocalDecodedVector>& decodedArrays,
      const std::vector<DecodedVector>& decodedElements,
      const std::vector<std::optional<std::string>>& constantStrings,
      const std::vector<exec::LocalDecodedVector>& decodedStringArgs,
      const exec::LocalDecodedVector& decodedSeparator) const {
    uint64_t totalResultBytes = 0;
    rows.applyToSelected([&](auto row) {
      // NULL separator produces NULL result.
      if (!isConstantSeparator() && decodedSeparator->isNullAt(row)) {
        return;
      }
      int32_t allElements = 0;
      // Calculate size for array columns data.
      for (auto i = 0; i < decodedArrays.size(); i++) {
        if (decodedArrays[i]->isNullAt(row)) {
          continue;
        }
        auto indices = decodedArrays[i].get()->indices();
        auto arrayVector = decodedArrays[i].get()->base()->as<ArrayVector>();
        auto size = arrayVector->sizeAt(indices[row]);
        auto offset = arrayVector->offsetAt(indices[row]);

        for (auto j = 0; j < size; ++j) {
          if (!decodedElements[i].isNullAt(offset + j)) {
            // No matter empty string or not.
            ++allElements;
            totalResultBytes +=
                decodedElements[i].valueAt<StringView>(offset + j).size();
          }
        }
      }

      // Calculate size for string arg.
      auto it = decodedStringArgs.begin();
      for (const auto& constantString : constantStrings) {
        int32_t valueSize;
        if (constantString.has_value()) {
          valueSize = constantString->size();
        } else {
          VELOX_CHECK(
              it < decodedStringArgs.end(),
              "Unexpected end when iterating over decodedStringArgs.");
          // Skip NULL.
          if ((*it)->isNullAt(row)) {
            ++it;
            continue;
          }
          valueSize = (*it++)->valueAt<StringView>(row).size();
        }
        // No matter empty string or not.
        allElements++;
        totalResultBytes += valueSize;
      }

      const auto separatorSize = isConstantSeparator()
          ? separator_.value().size()
          : decodedSeparator->valueAt<StringView>(row).size();

      if (allElements > 1) {
        totalResultBytes += (allElements - 1) * separatorSize;
      }
    });
    VELOX_USER_CHECK_LE(totalResultBytes, UINT32_MAX);
    return totalResultBytes;
  }

  // Initialize some vectors for inputs. And concatenate consecutive
  // constant string arguments in advance.
  // @param rows The rows to process.
  // @param args The arguments to the function.
  // @param context The evaluation context.
  // @param decodedArrays The decoded vectors for array arguments.
  // @param decodedElements The decoded vectors for array elements.
  // @param argMapping The mapping of the string arguments.
  // @param constantStrings The constant string arguments concatenated in
  // advance.
  // @param decodedStringArgs The decoded vectors for non-constant string
  // arguments.
  void initVectors(
      const SelectivityVector& rows,
      const std::vector<VectorPtr>& args,
      const exec::EvalCtx& context,
      std::vector<exec::LocalDecodedVector>& decodedArrays,
      std::vector<DecodedVector>& decodedElements,
      std::vector<column_index_t>& argMapping,
      std::vector<std::optional<std::string>>& constantStrings,
      std::vector<exec::LocalDecodedVector>& decodedStringArgs) const {
    for (auto i = 1; i < args.size(); ++i) {
      if (args[i] && args[i]->typeKind() == TypeKind::ARRAY) {
        decodedArrays.emplace_back(context, *args[i], rows);
        auto arrayVector =
            decodedArrays.back().get()->base()->as<ArrayVector>();
        SelectivityVector nestedRows(arrayVector->elements()->size());
        decodedElements.emplace_back(*arrayVector->elements(), nestedRows);
        continue;
      }
      argMapping.push_back(i);
      if (!isConstantSeparator()) {
        // Cannot concat consecutive constant string args in advance.
        constantStrings.push_back(std::nullopt);
        continue;
      }
      if (args[i] && args[i]->as<ConstantVector<StringView>>() &&
          !args[i]->isNullAt(0)) {
        std::ostringstream out;
        out << args[i]->as<ConstantVector<StringView>>()->valueAt(0);
        column_index_t j = i + 1;
        // Concat constant string args in advance.
        for (; j < args.size(); ++j) {
          if (!args[j] || args[j]->typeKind() == TypeKind::ARRAY ||
              !args[j]->as<ConstantVector<StringView>>() ||
              args[j]->isNullAt(0)) {
            break;
          }
          out << separator_.value()
              << args[j]->as<ConstantVector<StringView>>()->valueAt(0);
        }
        constantStrings.emplace_back(out.str());
        i = j - 1;
      } else {
        constantStrings.push_back(std::nullopt);
      }
    }

    for (auto i = 0; i < constantStrings.size(); ++i) {
      if (!constantStrings[i].has_value()) {
        auto index = argMapping[i];
        decodedStringArgs.emplace_back(context, *args[index], rows);
      }
    }
  }

  // ConcatWs implementation. It concatenates the arguments with the separator.
  // Mixed using of VARCHAR & ARRAY<VARCHAR> is considered. If separator is
  // constant, concatenate consecutive constant string args in advance. Then,
  // concatenate the intermediate result with neighboring array args or
  // non-constant string args.
  void doApply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      exec::EvalCtx& context,
      VectorPtr& result) const {
    auto& flatResult = *result->asFlatVector<StringView>();
    // Holds string arg indexes.
    std::vector<column_index_t> argMapping;
    std::vector<std::optional<std::string>> constantStrings;
    const auto numArgs = args.size();
    argMapping.reserve(numArgs - 1);
    // Save intermediate result for consecutive constant string args.
    // They will be concatenated in advance.
    constantStrings.reserve(numArgs - 1);
    std::vector<exec::LocalDecodedVector> decodedArrays;
    decodedArrays.reserve(numArgs - 1);
    // For column string arg decoding.
    std::vector<exec::LocalDecodedVector> decodedStringArgs;
    decodedStringArgs.reserve(numArgs);

    std::vector<DecodedVector> decodedElements;
    decodedElements.reserve(numArgs - 1);
    initVectors(
        rows,
        args,
        context,
        decodedArrays,
        decodedElements,
        argMapping,
        constantStrings,
        decodedStringArgs);
    exec::LocalDecodedVector decodedSeparator(context);
    if (!isConstantSeparator()) {
      decodedSeparator = exec::LocalDecodedVector(context, *args[0], rows);
    }

    const auto totalResultBytes = calculateTotalResultBytes(
        rows,
        context,
        decodedArrays,
        decodedElements,
        constantStrings,
        decodedStringArgs,
        decodedSeparator);

    // Allocate a string buffer.
    auto rawBuffer =
        flatResult.getRawStringBufferWithSpace(totalResultBytes, true);
    rows.applyToSelected([&](auto row) {
      // NULL separator produces NULL result.
      if (!isConstantSeparator() && decodedSeparator->isNullAt(row)) {
        result->setNull(row, true);
        return;
      }
      uint32_t bufferOffset = 0;
      auto isFirst = true;
      // For array arg.
      int32_t i = 0;
      // For string arg.
      int32_t j = 0;
      auto it = decodedStringArgs.begin();

      const auto separator = isConstantSeparator()
          ? separator_.value()
          : decodedSeparator->valueAt<StringView>(row);

      const auto copyToBuffer = [&](const char* value, const size_t valueSize) {
        if (isFirst) {
          isFirst = false;
        } else {
          // Add separator before the current value.
          memcpy(rawBuffer + bufferOffset, separator.data(), separator.size());
          bufferOffset += separator.size();
        }
        memcpy(rawBuffer + bufferOffset, value, valueSize);
        bufferOffset += valueSize;
      };

      for (auto itArgs = args.begin() + 1; itArgs != args.end(); ++itArgs) {
        if ((*itArgs)->typeKind() == TypeKind::ARRAY) {
          if ((*itArgs)->isNullAt(row)) {
            ++i;
            continue;
          }
          auto indices = decodedArrays[i].get()->indices();
          auto arrayVector = decodedArrays[i].get()->base()->as<ArrayVector>();
          auto size = arrayVector->sizeAt(indices[row]);
          auto offset = arrayVector->offsetAt(indices[row]);

          for (auto k = 0; k < size; ++k) {
            if (!decodedElements[i].isNullAt(offset + k)) {
              auto element = decodedElements[i].valueAt<StringView>(offset + k);
              copyToBuffer(element.data(), element.size());
            }
          }
          ++i;
          continue;
        }

        if (j >= constantStrings.size()) {
          continue;
        }

        if (constantStrings[j].has_value()) {
          copyToBuffer(constantStrings[j]->data(), constantStrings[j]->size());
        } else {
          VELOX_CHECK(
              it < decodedStringArgs.end(),
              "Unexpected end when iterating over decodedStringArgs.");
          // Skip NULL.
          if ((*it)->isNullAt(row)) {
            ++it;
            ++j;
            continue;
          }
          const auto value = (*it++)->valueAt<StringView>(row);
          copyToBuffer(value.data(), value.size());
        }
        ++j;
      }
      VELOX_USER_CHECK_LE(bufferOffset, INT32_MAX);
      flatResult.setNoCopy(row, StringView(rawBuffer, bufferOffset));
      rawBuffer += bufferOffset;
    });
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& outputType,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    context.ensureWritable(rows, VARCHAR(), result);
    auto flatResult = result->asFlatVector<StringView>();
    const auto numArgs = args.size();
    // If separator is NULL, result is NULL.
    if (isConstantSeparator() && args[0]->isNullAt(0)) {
      auto localResult = BaseVector::createNullConstant(
          outputType, rows.end(), context.pool());
      context.moveOrCopyResult(localResult, rows, result);
      return;
    }
    // If only separator (not a NULL) is provided, result is an empty string.
    if (numArgs == 1) {
      auto decodedSeparator = exec::LocalDecodedVector(context, *args[0], rows);
      //  1. Separator is constant and not a NULL.
      //  2. Separator is column and have no NULL.
      if (isConstantSeparator() || !decodedSeparator->mayHaveNulls()) {
        auto localResult = BaseVector::createConstant(
            VARCHAR(), "", rows.end(), context.pool());
        context.moveOrCopyResult(localResult, rows, result);
      } else {
        rows.applyToSelected([&](auto row) {
          if (decodedSeparator->isNullAt(row)) {
            result->setNull(row, true);
          } else {
            flatResult->setNoCopy(row, StringView(""));
          }
        });
      }
      return;
    }
    doApply(rows, args, context, result);
  }

 private:
  // For holding constant separator.
  const std::optional<StringView> separator_;
};
} // namespace

TypePtr ConcatWsCallToSpecialForm::resolveType(
    const std::vector<TypePtr>& /*argTypes*/) {
  return VARCHAR();
}

exec::ExprPtr ConcatWsCallToSpecialForm::constructSpecialForm(
    const TypePtr& type,
    std::vector<exec::ExprPtr>&& args,
    bool trackCpuUsage,
    const core::QueryConfig& config) {
  auto numArgs = args.size();
  VELOX_USER_CHECK_GE(
      numArgs,
      1,
      "concat_ws requires one arguments at least, but got {}.",
      numArgs);
  VELOX_USER_CHECK(
      args[0]->type()->isVarchar(),
      "The first argument of concat_ws must be a varchar.");
  for (const auto& arg : args) {
    VELOX_USER_CHECK(
        arg->type()->isVarchar() ||
            (arg->type()->isArray() &&
             arg->type()->asArray().elementType()->isVarchar()),
        "The 2nd and following arguments for concat_ws should be varchar or array(varchar), but got {}.",
        arg->type()->toString());
  }

  std::optional<StringView> separator = std::nullopt;
  auto constantExpr = std::dynamic_pointer_cast<exec::ConstantExpr>(args[0]);

  if (constantExpr) {
    separator = constantExpr->value()
                    ->asUnchecked<ConstantVector<StringView>>()
                    ->valueAt(0);
  }
  auto concatWsFunction = std::make_shared<ConcatWs>(separator);
  return std::make_shared<exec::Expr>(
      type,
      std::move(args),
      std::move(concatWsFunction),
      exec::VectorFunctionMetadataBuilder().defaultNullBehavior(false).build(),
      kConcatWs,
      trackCpuUsage);
}

} // namespace facebook::velox::functions::sparksql
