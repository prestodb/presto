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
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions {

namespace {
class JsonFormatFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;

    // Input can be constant or flat.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, VARCHAR(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      VELOX_CHECK_LE(rows.end(), flatInput->size());
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          VARCHAR(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // json -> varchar
    return {exec::FunctionSignatureBuilder()
                .returnType("varchar")
                .argumentType("json")
                .build()};
  }
};

class JsonParseFunction : public exec::VectorFunction {
 public:
  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /* outputType */,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VectorPtr localResult;

    // Input can be constant or flat.
    // TODO(arpitporwal2293) Replace folly::parseJson with a lightweight
    // validation of JSON syntax that doesn't allocate memory or copy data.
    assert(args.size() > 0);
    const auto& arg = args[0];
    if (arg->isConstantEncoding()) {
      auto value = arg->as<ConstantVector<StringView>>()->valueAt(0);
      try {
        folly::parseJson(value);
      } catch (const std::exception& e) {
        context.setErrors(rows, std::current_exception());
        return;
      }
      localResult = std::make_shared<ConstantVector<StringView>>(
          context.pool(), rows.end(), false, JSON(), std::move(value));
    } else {
      auto flatInput = arg->asFlatVector<StringView>();

      auto stringBuffers = flatInput->stringBuffers();
      VELOX_CHECK_LE(rows.end(), flatInput->size());

      context.applyToSelectedNoThrow(
          rows, [&](auto row) { folly::parseJson(flatInput->valueAt(row)); });
      localResult = std::make_shared<FlatVector<StringView>>(
          context.pool(),
          JSON(),
          nullptr,
          rows.end(),
          flatInput->values(),
          std::move(stringBuffers));
    }

    context.moveOrCopyResult(localResult, rows, result);
  }

  static std::vector<std::shared_ptr<exec::FunctionSignature>> signatures() {
    // varchar -> json
    return {exec::FunctionSignatureBuilder()
                .returnType("json")
                .argumentType("varchar")
                .build()};
  }
};

} // namespace

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_json_format,
    JsonFormatFunction::signatures(),
    std::make_unique<JsonFormatFunction>());

VELOX_DECLARE_VECTOR_FUNCTION(
    udf_json_parse,
    JsonParseFunction::signatures(),
    std::make_unique<JsonParseFunction>());
} // namespace facebook::velox::functions
