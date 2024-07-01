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

#include <folly/Benchmark.h>
#include <folly/init/Init.h>

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook;
using namespace facebook::velox;

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});

  functions::prestosql::registerAllScalarFunctions();

  ExpressionBenchmarkBuilder benchmarkBuilder;

  benchmarkBuilder.addBenchmarkSet("cast", ROW({}))
      .addExpression("no_cast", "uuid()")
      .addExpression("as_varchar", "cast(uuid() as varchar)");

  benchmarkBuilder.registerBenchmarks();
  folly::runBenchmarks();
  return 0;
}
