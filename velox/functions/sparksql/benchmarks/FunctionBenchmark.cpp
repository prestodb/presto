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
#include <functions/FunctionRegistry.h>

#include "velox/benchmarks/ExpressionBenchmarkBuilder.h"
#include "velox/functions/sparksql/registration/Register.h"

using namespace facebook::velox;
class FunctionBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  FunctionBenchmark() : FunctionBenchmarkBase() {
    functions::sparksql::registerFunctions("");
  }

  void run() {
    int total = 0;
    for (auto i = 0; i < 1000; i++) {
      auto functions = getFunctionSignatures();
      auto size = functions.size();
      total = total + size;
    }
    std::cout << total << std::endl;
  }
};

BENCHMARK(get_function_signatures_1000) {
  FunctionBenchmark benchmark;
  benchmark.run();
}

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize(memory::MemoryManager::Options{});
  folly::runBenchmarks();
  return 0;
}
