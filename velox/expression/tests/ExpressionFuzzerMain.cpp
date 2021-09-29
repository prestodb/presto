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

#include "velox/expression/tests/ExpressionFuzzer.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/CoreFunctions.h"
#include "velox/functions/prestosql/VectorFunctions.h"

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

DEFINE_int32(steps, 1, "Number of expressions to generate.");

using namespace facebook::velox;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);
  google::InitGoogleLogging(argv[0]);

  functions::registerFunctions();
  functions::registerVectorFunctions();

  test::expressionFuzzer(getFunctionSignatures(), FLAGS_steps, FLAGS_seed);
  return 0;
}
