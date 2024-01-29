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
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace {

class MapBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  MapBenchmark() {
    functions::prestosql::registerAllScalarFunctions();

    VectorFuzzer::Options options;
    options.vectorSize = 10'000;

    VectorFuzzer fuzzer(options, pool());

    // Generate flat vectors to use as keys and values for the map.
    data_ = std::dynamic_pointer_cast<RowVector>(fuzzer.fuzzFlat(
        ROW({"k0", "k1", "k2", "v0", "v1", "v2"},
            {BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT(), BIGINT()})));
  }

  size_t runConstantKeys(size_t times) {
    return run(
        "map(array_constructor(35, 2, 101), array_constructor(v0, v1, v2))",
        times);
  }

  size_t runFlatKeys(size_t times) {
    return run(
        "map(array_constructor(k0, k1, k2), array_constructor(v0, v1, v2))",
        times);
  }

 private:
  size_t run(const std::string& expression, size_t times) {
    folly::BenchmarkSuspender suspender;
    auto exprSet = compileExpression(expression, asRowType(data_->type()));
    suspender.dismiss();

    int cnt = 0;
    for (auto i = 0; i < times * 1'000; i++) {
      cnt += evaluate(exprSet, data_)->size();
    }
    return cnt;
  }

  RowVectorPtr data_;
};

BENCHMARK_MULTI(constantKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runConstantKeys(n);
}

BENCHMARK_MULTI(flatKeys, n) {
  MapBenchmark benchmark;
  return benchmark.runFlatKeys(n);
}

} // namespace

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  folly::runBenchmarks();
  return 0;
}
