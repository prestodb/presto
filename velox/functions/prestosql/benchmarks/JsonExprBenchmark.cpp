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
/**
 * This file tests the performance of each JsonXXXFunction.call() with
 * expression framework and Velox vectors.
 */
#include <folly/Benchmark.h>
#include <folly/init/Init.h>
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/SIMDJsonFunctions.h"
#include "velox/functions/prestosql/json/JsonExtractor.h"
#include "velox/functions/prestosql/types/JsonType.h"

namespace facebook::velox::functions::prestosql {
namespace {

template <typename T>
struct IsJsonScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE void call(bool& result, const arg_type<Json>& json) {
    auto parsedJson = folly::parseJson(json);
    result = parsedJson.isNumber() || parsedJson.isString() ||
        parsedJson.isBool() || parsedJson.isNull();
  }
};

// jsonExtractScalar(json, json_path) -> varchar
// Current implementation support UTF-8 in json, but not in json_path.
// Like jsonExtract(), but returns the result value as a string (as opposed
// to being encoded as JSON). The value referenced by json_path must be a scalar
// (boolean, number or string)
template <typename T>
struct JsonExtractScalarFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    const folly::StringPiece& jsonStringPiece = json;
    const folly::StringPiece& jsonPathStringPiece = jsonPath;
    auto extractResult =
        jsonExtractScalar(jsonStringPiece, jsonPathStringPiece);
    if (extractResult.hasValue()) {
      UDFOutputString::assign(result, *extractResult);
      return true;

    } else {
      return false;
    }
  }
};

template <typename T>
struct JsonExtractFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Json>& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    auto extractResult =
        jsonExtract(folly::StringPiece(json), folly::StringPiece(jsonPath));
    if (!extractResult.hasValue() || extractResult.value().isNull()) {
      return false;
    }

    folly::json::serialization_opts opts;
    opts.sort_keys = true;
    folly::json::serialize(*extractResult, opts);

    UDFOutputString::assign(
        result, folly::json::serialize(*extractResult, opts));
    return true;
  }
};

template <typename T>
struct JsonArrayLengthFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Json>& json) {
    auto parsedJson = folly::parseJson(json);
    if (!parsedJson.isArray()) {
      return false;
    }

    result = parsedJson.size();
    return true;
  }
};

template <typename T>
struct JsonArrayContainsFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  template <typename TInput>
  FOLLY_ALWAYS_INLINE bool
  call(bool& result, const arg_type<Json>& json, const TInput& value) {
    auto parsedJson = folly::parseJson(json);
    if (!parsedJson.isArray()) {
      return false;
    }

    result = false;
    for (const auto& v : parsedJson) {
      if constexpr (std::is_same_v<TInput, bool>) {
        if (v.isBool() && v == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, int64_t>) {
        if (v.isInt() && v == value) {
          result = true;
          break;
        }
      } else if constexpr (std::is_same_v<TInput, double>) {
        if (v.isDouble() && v == value) {
          result = true;
          break;
        }
      } else {
        if (v.isString() && v == value) {
          result = true;
          break;
        }
      }
    }
    return true;
  }
};

template <typename T>
struct JsonSizeFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  FOLLY_ALWAYS_INLINE bool call(
      int64_t& result,
      const arg_type<Json>& json,
      const arg_type<Varchar>& jsonPath) {
    const folly::StringPiece& jsonStringPiece = json;
    const folly::StringPiece& jsonPathStringPiece = jsonPath;
    auto extractResult = jsonExtract(jsonStringPiece, jsonPathStringPiece);
    if (!extractResult.has_value()) {
      return false;
    }
    // The size of the object or array is the number of members, otherwise the
    // size is zero
    if (extractResult->isArray() || extractResult->isObject()) {
      result = extractResult->size();
    } else {
      result = 0;
    }

    return true;
  }
};

const std::string smallJson = R"({"k1":"v1"})";

class JsonBenchmark : public velox::functions::test::FunctionBenchmarkBase {
 public:
  JsonBenchmark() : FunctionBenchmarkBase() {
    registerJsonType();
    registerFunction<IsJsonScalarFunction, bool, Json>(
        {"folly_is_json_scalar"});
    registerFunction<SIMDIsJsonScalarFunction, bool, Json>(
        {"simd_is_json_scalar"});
    registerFunction<JsonArrayContainsFunction, bool, Json, bool>(
        {"folly_json_array_contains"});
    registerFunction<SIMDJsonArrayContainsFunction, bool, Json, bool>(
        {"simd_json_array_contains"});
    registerFunction<JsonArrayLengthFunction, int64_t, Json>(
        {"folly_json_array_length"});
    registerFunction<SIMDJsonArrayLengthFunction, int64_t, Json>(
        {"simd_json_array_length"});
    registerFunction<JsonExtractScalarFunction, Varchar, Json, Varchar>(
        {"folly_json_extract_scalar"});
    registerFunction<SIMDJsonExtractScalarFunction, Varchar, Json, Varchar>(
        {"simd_json_extract_scalar"});
    registerFunction<JsonExtractFunction, Varchar, Json, Varchar>(
        {"folly_json_extract"});
    registerFunction<SIMDJsonExtractFunction, Varchar, Json, Varchar>(
        {"simd_json_extract"});
    registerFunction<JsonSizeFunction, int64_t, Json, Varchar>(
        {"folly_json_size"});
    registerFunction<SIMDJsonSizeFunction, int64_t, Json, Varchar>(
        {"simd_json_size"});
  }

  std::string prepareData(int jsonSize) {
    std::string jsonData = R"({"key": [)" + smallJson;
    for (int i = 0; i < jsonSize / 10; i++) {
      jsonData += "," + smallJson;
    }
    jsonData += "]}";
    return jsonData;
  }

  velox::VectorPtr makeJsonData(const std::string& json, int vectorSize) {
    auto jsonVector =
        vectorMaker_.flatVector<velox::StringView>(vectorSize, JSON());
    for (auto i = 0; i < vectorSize; i++) {
      jsonVector->set(i, velox::StringView(json));
    }
    return jsonVector;
  }

  void runWithJson(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);

    auto rowVector = vectorMaker_.rowVector({jsonVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void runWithJsonExtract(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json,
      const std::string& path) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);
    auto pathVector = vectorMaker_.constantVector(
        std::vector<std::optional<StringView>>(vectorSize, StringView(path)));

    auto rowVector = vectorMaker_.rowVector({jsonVector, pathVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0, c1)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void runWithJsonContains(
      int iter,
      int vectorSize,
      const std::string& fnName,
      const std::string& json) {
    folly::BenchmarkSuspender suspender;

    auto jsonVector = makeJsonData(json, vectorSize);
    auto boolVector = vectorMaker_.flatVector<bool>({true});

    auto rowVector = vectorMaker_.rowVector({jsonVector, boolVector});
    auto exprSet =
        compileExpression(fmt::format("{}(c0, c1)", fnName), rowVector->type());
    suspender.dismiss();
    doRun(iter, exprSet, rowVector);
  }

  void doRun(
      const int iter,
      velox::exec::ExprSet& exprSet,
      const velox::RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < iter; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

void FollyIsJsonScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "folly_is_json_scalar", json);
}

void SIMDIsJsonScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "simd_is_json_scalar", json);
}

void FollyJsonArrayContains(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonContains(
      iter, vectorSize, "folly_json_array_contains", json);
}

void SIMDJsonArrayContains(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonContains(
      iter, vectorSize, "simd_json_array_contains", json);
}

void FollyJsonArrayLength(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "folly_json_array_length", json);
}

void SIMDJsonArrayLength(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJson(iter, vectorSize, "simd_json_array_length", json);
}

void FollyJsonExtractScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "folly_json_extract_scalar", json, "$.key[7].k1");
}

void SIMDJsonExtractScalar(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "simd_json_extract_scalar", json, "$.key[7].k1");
}

void FollyJsonExtract(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "folly_json_extract", json, "$.key[*].k1");
}

void SIMDJsonExtract(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "simd_json_extract", json, "$.key[*].k1");
}

void FollyJsonSize(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "folly_json_size", json, "$.key");
}

void SIMDJsonSize(int iter, int vectorSize, int jsonSize) {
  folly::BenchmarkSuspender suspender;
  JsonBenchmark benchmark;
  auto json = benchmark.prepareData(jsonSize);
  suspender.dismiss();
  benchmark.runWithJsonExtract(
      iter, vectorSize, "simd_json_size", json, "$.key");
}

BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_10bytes_size,
    100,
    10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_100bytes_size, 100, 100);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_1000bytes_size, 100, 1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyIsJsonScalar, 100_iters_10000bytes_size, 100, 10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDIsJsonScalar,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(FollyJsonArrayContains, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayContains,
    100_iters_10bytes_size,
    100,
    10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonArrayContains,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayContains,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonArrayContains,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayContains,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonArrayContains,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayContains,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(FollyJsonArrayLength, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayLength,
    100_iters_10bytes_size,
    100,
    10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonArrayLength, 100_iters_100bytes_size, 100, 100);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayLength,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonArrayLength,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayLength,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonArrayLength,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonArrayLength,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(FollyJsonExtractScalar, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtractScalar,
    100_iters_10bytes_size,
    100,
    10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonExtractScalar,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtractScalar,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonExtractScalar,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtractScalar,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(
    FollyJsonExtractScalar,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtractScalar,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(FollyJsonExtract, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtract,
    100_iters_10bytes_size,
    100,
    10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonExtract, 100_iters_100bytes_size, 100, 100);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtract,
    100_iters_100bytes_size,
    100,
    100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonExtract, 100_iters_1000bytes_size, 100, 1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtract,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonExtract, 100_iters_10000bytes_size, 100, 10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonExtract,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

BENCHMARK_DRAW_LINE();
BENCHMARK_NAMED_PARAM(FollyJsonSize, 100_iters_10bytes_size, 100, 10);
BENCHMARK_RELATIVE_NAMED_PARAM(SIMDJsonSize, 100_iters_10bytes_size, 100, 10);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonSize, 100_iters_100bytes_size, 100, 100);
BENCHMARK_RELATIVE_NAMED_PARAM(SIMDJsonSize, 100_iters_100bytes_size, 100, 100);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonSize, 100_iters_1000bytes_size, 100, 1000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonSize,
    100_iters_1000bytes_size,
    100,
    1000);
BENCHMARK_DRAW_LINE();

BENCHMARK_NAMED_PARAM(FollyJsonSize, 100_iters_10000bytes_size, 100, 10000);
BENCHMARK_RELATIVE_NAMED_PARAM(
    SIMDJsonSize,
    100_iters_10000bytes_size,
    100,
    10000);
BENCHMARK_DRAW_LINE();

} // namespace
} // namespace facebook::velox::functions::prestosql

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};

  facebook::velox::memory::MemoryManager::initialize({});

  folly::runBenchmarks();
  return 0;
}
