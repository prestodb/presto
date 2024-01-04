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
#include "velox/expression/ComplexViewTypes.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"
#include "velox/vector/ComplexVector.h"
#include "velox/vector/DecodedVector.h"

using namespace facebook::velox;

namespace {
// The following two functions is used to measure the cost of the cast of
// generic view.

// A function that takes array<X> and returns the summation as int64_t. For
// simplicity is here handling double and int64_t.
template <typename T>
struct GenericInputArraySum {
  template <typename TInput>
  void call(int64_t& out, const TInput& arrayOfGeneric) {
    out = 0;
    if (arrayOfGeneric.elementKind() == TypeKind::DOUBLE) {
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<double>();
        }
      }
    } else if (arrayOfGeneric.elementKind() == TypeKind::BIGINT) {
      for (auto e : arrayOfGeneric) {
        if (e.has_value()) {
          out += e.value().template castTo<int64_t>();
        }
      }
    } else {
      out = 0;
    }
  }
};

template <typename T>
struct TypedArraySum {
  template <typename TInput>
  void call(int64_t& out, const TInput& typedArray) {
    out = 0;
    for (auto e : typedArray) {
      if (e.has_value()) {
        out += e.value();
      }
    }
  }
};

} // namespace

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);

  ExpressionBenchmarkBuilder benchmarkBuilder;

  facebook::velox::registerFunction<GenericInputArraySum, int64_t, Array<Any>>(
      {"generic_input_sum"});
  facebook::velox::registerFunction<TypedArraySum, int64_t, Array<double>>(
      {"typed_sum"});
  facebook::velox::registerFunction<TypedArraySum, int64_t, Array<int64_t>>(
      {"typed_sum"});

  auto* pool = benchmarkBuilder.pool();
  auto& vm = benchmarkBuilder.vectorMaker();

  benchmarkBuilder
      .addBenchmarkSet(
          fmt::format("array_sum"),
          ROW({"c0", "c1"}, {ARRAY(BIGINT()), ARRAY(DOUBLE())}))
      .addExpression("generic_input_int", "generic_input_sum(c0)")
      .addExpression("typed_int", "typed_sum(c0)")
      .addExpression("generic_input_double", "generic_input_sum(c1)")
      .addExpression("typed_double", "typed_sum(c1)");

  benchmarkBuilder.registerBenchmarks();

  folly::runBenchmarks();
  return 0;
}
