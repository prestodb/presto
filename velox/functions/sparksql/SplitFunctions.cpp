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

#include <utility>

#include "velox/expression/VectorFunction.h"
#include "velox/functions/lib/ArrayBuilder.h"

namespace facebook::velox::functions::sparksql {
namespace {

/// This class only implements the basic split version in which the pattern is a
/// single character
class SplitCharacter final : public exec::VectorFunction {
 public:
  explicit SplitCharacter(const char pattern) : pattern_{pattern} {
    static constexpr std::string_view kRegexChars = ".$|()[{^?*+\\";
    VELOX_CHECK(
        kRegexChars.find(pattern) == std::string::npos,
        "This version of split supports single-length non-regex patterns");
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    exec::LocalDecodedVector input(context, *args[0], rows);

    ArrayBuilder<Varchar> builder(
        /*numArrays=*/rows.size(),
        /*estimatedNumElements=*/rows.countSelected() * 3,
        context.pool());

    rows.applyToSelected([&](vector_size_t row) {
      ArrayBuilder<Varchar>::Ref array = builder.startArray(row);
      const StringView& current = input->valueAt<StringView>(row);
      const char* pos = current.begin();
      const char* end = pos + current.size();
      const char* delim;
      do {
        delim = std::find(pos, end, pattern_);
        array.emplace_back(pos, delim - pos);
        pos = delim + 1; // Skip past delim.
      } while (delim != end);
    });
    // Reference the input StringBuffers since we did not deep copy above.
    builder.setStringBuffers(
        args[0]->asFlatVector<StringView>()->stringBuffers());
    std::shared_ptr<ArrayVector> arrayVector =
        std::move(builder).finish(context.pool());
    context.moveOrCopyResult(arrayVector, rows, result);
  }

 private:
  const char pattern_;
};

/// This class will be updated in the future as we support more variants of
/// split
class Split final : public exec::VectorFunction {
 public:
  Split() {}

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    auto delimiterVector = args[1]->as<ConstantVector<StringView>>();
    VELOX_CHECK(
        delimiterVector, "Split function supports only constant delimiter");
    auto patternString = args[1]->as<ConstantVector<StringView>>()->valueAt(0);
    VELOX_CHECK_EQ(
        patternString.size(),
        1,
        "split only supports only single-character pattern");
    char pattern = patternString.data()[0];
    SplitCharacter splitCharacter(pattern);
    splitCharacter.apply(rows, args, nullptr, context, result);
  }
};

/// The function returns specialized version of split based on the constant
/// inputs.
/// \param inputArgs the inputs types (VARCHAR, VARCHAR, int64) and constant
///     values (if provided).
std::shared_ptr<exec::VectorFunction> createSplit(
    const std::string& /*name*/,
    const std::vector<exec::VectorFunctionArg>& inputArgs) {
  BaseVector* constantPattern = inputArgs[1].constantValue.get();

  if (inputArgs.size() > 3 || inputArgs[0].type->isVarchar() ||
      inputArgs[1].type->isVarchar() || (constantPattern == nullptr)) {
    return std::make_shared<Split>();
  }
  auto pattern = constantPattern->as<ConstantVector<StringView>>()->valueAt(0);
  if (pattern.size() != 1) {
    return std::make_shared<Split>();
  }
  char charPattern = pattern.data()[0];
  // TODO: Add support for zero-length pattern, 2-character pattern
  // TODO: add support for general regex pattern using R2
  return std::make_shared<SplitCharacter>(charPattern);
}

std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
  // varchar, varchar -> array(varchar)
  return {exec::FunctionSignatureBuilder()
              .returnType("array(varchar)")
              .argumentType("varchar")
              .argumentType("varchar")
              .build()};
}

} // namespace

VELOX_DECLARE_STATEFUL_VECTOR_FUNCTION(
    udf_regexp_split,
    signatures(),
    createSplit);
} // namespace facebook::velox::functions::sparksql
