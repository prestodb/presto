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

#include "velox/functions/sparksql/InputFileName.h"
#include <utility>
#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"
#include "velox/expression/VectorWriters.h"
#include "velox/functions/prestosql/URLFunctions.h"
#include "velox/type/StringView.h"

namespace facebook::velox::functions::sparksql {
namespace {
class InputFileName final : public exec::VectorFunction {
 private:
  uint64_t doNotEncodeSymbolsBits_[4] = {};

 public:
  InputFileName() {
    for (auto p : "!$&'()*+,;=/:@") {
      bits::setBit(doNotEncodeSymbolsBits_, static_cast<size_t>(p), true);
    }
  }

  void apply(
      const SelectivityVector& rows,
      std::vector<VectorPtr>& args,
      const TypePtr& /*outputType*/,
      exec::EvalCtx& context,
      VectorPtr& result) const override {
    VELOX_CHECK(args.empty());
    context.ensureWritable(rows, VARCHAR(), result);
    auto driverCtx = context.driverCtx();
    auto inputFileName = driverCtx->inputFileName;
    std::string outFileName;
    facebook::velox::functions::detail::urlEscape(
        outFileName, inputFileName, false, doNotEncodeSymbolsBits_);
    context.moveOrCopyResult(
        std::make_shared<ConstantVector<StringView>>(
            context.pool(),
            rows.end(),
            false /*isNull*/,
            VARCHAR(),
            std::move(outFileName.c_str())),
        rows,
        result);
  }
};
} // namespace
std::unique_ptr<exec::VectorFunction> makeInputFileName() {
  return std::make_unique<InputFileName>();
}

std::vector<std::shared_ptr<exec::FunctionSignature>>
inputFileNameSignatures() {
  return {exec::FunctionSignatureBuilder().returnType("varchar").build()};
}
} // namespace facebook::velox::functions::sparksql
