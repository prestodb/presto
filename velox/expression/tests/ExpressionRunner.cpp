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

#include <gtest/gtest.h>

#include "velox/common/memory/Memory.h"
#include "velox/core/QueryCtx.h"
#include "velox/expression/tests/ExpressionRunner.h"
#include "velox/expression/tests/ExpressionVerifier.h"
#include "velox/parse/Expressions.h"
#include "velox/parse/ExpressionsParser.h"
#include "velox/parse/TypeResolver.h"
#include "velox/vector/VectorSaver.h"

namespace facebook::velox::test {

using namespace facebook::velox;

void ExpressionRunner::run(
    const std::string& inputPath,
    const std::string& sqlPath,
    const std::string& resultPath,
    const std::string& mode) {
  std::shared_ptr<core::QueryCtx> queryCtx{core::QueryCtx::createForTest()};
  std::unique_ptr<memory::MemoryPool> pool{
      memory::getDefaultScopedMemoryPool()};
  core::ExecCtx execCtx{pool.get(), queryCtx.get()};

  VELOX_CHECK(!inputPath.empty());
  VELOX_CHECK(!sqlPath.empty());

  auto inputVector = std::dynamic_pointer_cast<RowVector>(
      restoreVectorFromFile(inputPath.c_str(), pool.get()));
  VELOX_CHECK_NOT_NULL(inputVector, "Input vector is not a RowVector");
  auto sql = restoreStringFromFile(sqlPath.c_str());

  parse::registerTypeResolver();
  parse::ParseOptions options;
  auto typedExpr = core::Expressions::inferTypes(
      parse::parseExpr(sql, options), inputVector->type(), pool.get());
  VectorPtr resultVector;
  if (!resultPath.empty()) {
    resultVector = restoreVectorFromFile(resultPath.c_str(), pool.get());
  }

  if (mode == "verify") {
    test::ExpressionVerifier(&execCtx, {false, ""})
        .verify(typedExpr, inputVector, std::move(resultVector), true);
  } else {
    LOG(ERROR) << "Unknown expression runner mode '" << mode << "'.";
  }
}

} // namespace facebook::velox::test
