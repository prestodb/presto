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
#include "velox/expression/DecodedArgs.h"
#include "velox/expression/StringWriter.h"
#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/Utf8Utils.h"
#include "velox/functions/lib/string/StringImpl.h"

namespace facebook::velox::functions {
namespace {

class FromUtf8Function : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    const auto& input = args[0];

    // Optimize common cases:
    // (1) all-ASCII input;
    // (2) valid UTF-8 input;
    // (3) constant replacement character.

    exec::DecodedArgs decodedArgs(rows, args, context);
    auto& decodedInput = *decodedArgs.at(0);

    // Read the constant replacement if it exisits and verify that it is valid.
    bool constantReplacement =
        args.size() == 1 || decodedArgs.at(1)->isConstantMapping();
    std::string constantReplacementValue = kReplacementChar;

    if (constantReplacement) {
      if (args.size() > 1) {
        auto& decodedReplacement = *decodedArgs.at(1);
        try {
          constantReplacementValue = getReplacementCharacter(
              args[1]->type(), decodedReplacement, rows.begin());
        } catch (const std::exception&) {
          context.setErrors(rows, std::current_exception());
          return;
        }
      }
    }

    // We can only do valid UTF-8 input optimization if replacement is valid and
    // constant otherwise we have to check replacement for each row.
    if (constantReplacement) {
      if (input->loadedVector()
              ->as<SimpleVector<StringView>>()
              ->computeAndSetIsAscii(rows)) {
        // Input strings are all-ASCII.
        toVarcharNoCopy(input, decodedInput, rows, context, result);
        return;
      }
    }

    auto firstInvalidRow = findFirstInvalidRow(decodedInput, rows);

    // We can only do this optimization if replacement is valid and
    // constant otherwise we have to check replacement for each row.
    if (constantReplacement) {
      if (!firstInvalidRow.has_value()) {
        // All inputs are valid UTF-8 strings.
        toVarcharNoCopy(input, decodedInput, rows, context, result);
        return;
      }
    }

    BaseVector::ensureWritable(rows, VARCHAR(), context.pool(), result);
    auto flatResult = result->as<FlatVector<StringView>>();

    // Reserve string buffer capacity.
    size_t totalInputSize = 0;
    rows.applyToSelected([&](auto row) {
      totalInputSize += decodedInput.valueAt<StringView>(row).size();
    });

    flatResult->getBufferWithSpace(totalInputSize);

    if (constantReplacement) {
      rows.applyToSelected([&](auto row) {
        exec::StringWriter<false> writer(flatResult, row);
        auto value = decodedInput.valueAt<StringView>(row);
        if (row < firstInvalidRow) {
          writer.append(value);
          writer.finalize();
        } else {
          fixInvalidUtf8(value, constantReplacementValue, writer);
        }
      });
    } else {
      auto& decodedReplacement = *decodedArgs.at(1);
      context.applyToSelectedNoThrow(rows, [&](auto row) {
        auto replacement =
            getReplacementCharacter(args[1]->type(), decodedReplacement, row);
        exec::StringWriter<false> writer(flatResult, row);
        auto value = decodedInput.valueAt<StringView>(row);
        if (row < firstInvalidRow) {
          writer.append(value);
          writer.finalize();
        } else {
          fixInvalidUtf8(value, replacement, writer);
        }
      });
    }
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    return {
        // varbinary -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varbinary")
            .build(),

        // varbinary, bigint -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varbinary")
            .argumentType("bigint")
            .build(),

        // varbinary, varchar -> varchar
        exec::FunctionSignatureBuilder()
            .returnType("varchar")
            .argumentType("varbinary")
            .argumentType("varchar")
            .build(),
    };
  }

 private:
  static const std::string kReplacementChar;

  static std::string codePointToString(int64_t codePoint) {
    std::string result;
    result.resize(4);
    stringImpl::codePointToString(result, codePoint);
    return result;
  }

  std::string getReplacementCharacter(
      const TypePtr& type,
      DecodedVector& decoded,
      vector_size_t row) const {
    if (type->isBigint()) {
      return codePointToString(decoded.valueAt<int64_t>(row));
    }

    auto replacement = decoded.valueAt<StringView>(row);
    if (!replacement.empty()) {
      auto charLength =
          tryGetCharLength(replacement.data(), replacement.size());
      VELOX_USER_CHECK_GT(
          charLength, 0, "Replacement is not a valid UTF-8 character");
      VELOX_USER_CHECK_EQ(
          charLength,
          replacement.size(),
          "Replacement string must be empty or a single character");
    }
    return replacement;
  }

  /// Returns first row that contains invalid UTF-8 string or std::nullopt if
  /// all rows are valid.
  static std::optional<vector_size_t> findFirstInvalidRow(
      const DecodedVector& decodedInput,
      const SelectivityVector& rows) {
    std::optional<vector_size_t> firstInvalidRow;
    rows.testSelected([&](auto row) {
      auto value = decodedInput.valueAt<StringView>(row);

      int32_t pos = 0;
      while (pos < value.size()) {
        auto charLength =
            tryGetCharLength(value.data() + pos, value.size() - pos);
        if (charLength < 0) {
          firstInvalidRow = row;
          return false;
        }

        pos += charLength;
      }

      return true;
    });
    return firstInvalidRow;
  }

  void toVarcharNoCopy(
      const VectorPtr& input,
      DecodedVector& decodedInput,
      const SelectivityVector& rows,
      const exec::EvalCtx& context,
      VectorPtr& result) const {
    VectorPtr localResult;
    if (decodedInput.isConstantMapping()) {
      auto value = decodedInput.valueAt<StringView>(rows.begin());
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, VARCHAR(), std::move(value));
    } else if (decodedInput.isIdentityMapping()) {
      auto flatInput = decodedInput.base()->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      VELOX_CHECK_LE(rows.end(), flatInput->size());
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          VARCHAR(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    } else {
      auto base = decodedInput.base();
      if (base->isConstantEncoding()) {
        auto value = decodedInput.valueAt<StringView>(rows.begin());
        localResult = std::make_shared<ConstantVector<StringView>>(
            context.pool(), rows.end(), false, VARCHAR(), std::move(value));
      } else {
        auto flatBase = base->asFlatVector<StringView>();
        auto stringBuffers = flatBase->stringBuffers();

        auto values = AlignedBuffer::allocate<StringView>(
            rows.end(), context.pool(), StringView());
        auto* rawValues = values->asMutable<StringView>();
        rows.applyToSelected([&](auto row) {
          rawValues[row] = decodedInput.valueAt<StringView>(row);
        });

        localResult = std::make_shared<FlatVector<StringView>>(
            context.pool(),
            VARCHAR(),
            nullptr,
            rows.end(),
            std::move(values),
            std::move(stringBuffers));
      }
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  void fixInvalidUtf8(
      StringView input,
      const std::string& replacement,
      exec::StringWriter<false>& fixedWriter) const {
    if (input.empty()) {
      fixedWriter.setEmpty();
      return;
    }

    int32_t pos = 0;
    while (pos < input.size()) {
      auto charLength =
          tryGetCharLength(input.data() + pos, input.size() - pos);
      if (charLength > 0) {
        fixedWriter.append(std::string_view(input.data() + pos, charLength));
        pos += charLength;
        continue;
      }

      if (!replacement.empty()) {
        fixedWriter.append(replacement);
      }

      pos += -charLength;
    }

    fixedWriter.finalize();
  }
};

// static
const std::string FromUtf8Function::kReplacementChar =
    codePointToString(0xFFFD);
} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_from_utf8,
    FromUtf8Function::signatures(),
    std::make_unique<FromUtf8Function>());

} // namespace facebook::velox::functions
