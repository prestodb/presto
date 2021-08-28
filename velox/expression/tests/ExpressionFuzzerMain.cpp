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

#include "velox/core/FunctionRegistry.h"
#include "velox/expression/VectorFunctionRegistry.h"
#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/functions/prestosql/CoreFunctions.h"

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

DEFINE_int32(steps, 1, "Number of expressions to generate.");

namespace facebook::velox::test {

// Fetches all available function signatures.
// TODO: Only simple functions for now.
std::vector<CallableSignature> getAllSignatures() {
  std::vector<CallableSignature> functions;

  // TODO: Skipping buggy functions for now.
  std::unordered_set<std::string> skipFunctions = {"xxhash64", "from_unixtime"};
  auto keys = exec::AdaptedVectorFunctions().Keys();

  for (const auto& key : keys) {
    auto func = facebook::velox::core::ScalarFunctions().Create(key);

    if (!func->isDeterministic()) {
      LOG(WARNING) << "Skipping non-deterministic function: " << key.name();
      continue;
    }

    if (skipFunctions.count(key.name()) > 0) {
      LOG(WARNING) << "Skipping known buggy function: " << key.name();
      continue;
    }

    CallableSignature temp{key.name(), key.types(), func->returnType()};
    functions.emplace_back(temp);
  }
  return functions;
}

} // namespace facebook::velox::test

using namespace facebook::velox;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // TODO: Only simple functions for now.
  functions::registerFunctions();
  auto signatures = test::getAllSignatures();

  test::expressionFuzzer(std::move(signatures), FLAGS_steps, FLAGS_seed);
  return 0;
}
