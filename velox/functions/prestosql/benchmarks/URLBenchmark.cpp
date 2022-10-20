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
#include "folly/Uri.h"
#include "velox/functions/Macros.h"
#include "velox/functions/Registerer.h"
#include "velox/functions/lib/benchmarks/FunctionBenchmarkBase.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions;

namespace {

template <typename Fn, typename TOutString, typename TInputString>
FOLLY_ALWAYS_INLINE bool
runURIFn(Fn func, TOutString& result, TInputString& url) {
  try {
    auto parsedUrl = folly::Uri(url);
    auto datum = (parsedUrl.*func)();
    result.resize(datum.size());
    if (datum.size() != 0) {
      std::strncpy(result.data(), datum.c_str(), datum.size());
    }
  } catch (const std::invalid_argument&) { // thrown if URI is invalid.
    result.resize(0);
  }

  return true;
}

template <typename T>
struct FollyUrlExtractFragmentFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& urlString) {
    return runURIFn(&folly::Uri::fragment, result, urlString);
  }
};

template <typename T>
struct FollyUrlExtractHostFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& urlString) {
    return runURIFn(&folly::Uri::host, result, urlString);
  }
};

template <typename T>
struct FollyUrlExtractPathFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& urlString) {
    return runURIFn(&folly::Uri::path, result, urlString);
  }
};

template <typename T>
struct FollyUrlExtractQueryFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& urlString) {
    return runURIFn(&folly::Uri::query, result, urlString);
  }
};

template <typename T>
struct FollyUrlExtractProtocolFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);

  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& urlString) {
    return runURIFn(&folly::Uri::scheme, result, urlString);
  }
};

template <typename T>
struct FollyUrlExtractPortFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(int64_t& result, const arg_type<Varchar>& url) {
    try {
      auto parsedUrl = folly::Uri(url);
      result = parsedUrl.port();
    } catch (const folly::ConversionError&) {
      return false;
    } catch (const std::invalid_argument& e) {
      return false;
    }
    return true;
  }
};

template <typename T>
struct FollyUrlExtractParameterFunction {
  VELOX_DEFINE_FUNCTION_TYPES(T);
  // ASCII input always produces ASCII result.
  static constexpr bool is_default_ascii_behavior = true;

  FOLLY_ALWAYS_INLINE bool call(
      out_type<Varchar>& result,
      const arg_type<Varchar>& url,
      const arg_type<Varchar>& param) {
    try {
      auto parsedUrl = folly::Uri(url);
      auto params = parsedUrl.getQueryParams();

      for (const auto& pair : params) {
        if (std::string(param) == pair.first) {
          result.resize(pair.second.length());
          strncpy(result.data(), pair.second.c_str(), pair.second.length());
        }
      }
    } catch (const std::invalid_argument& e) {
      return false;
    }
    return false;
  }
};

class UrlBenchmark : public functions::test::FunctionBenchmarkBase {
 public:
  UrlBenchmark() : FunctionBenchmarkBase() {
    functions::prestosql::registerURLFunctions();

    // Register folly based implementations.
    registerFunction<FollyUrlExtractFragmentFunction, Varchar, Varchar>(
        {"folly_url_extract_fragment"});
    registerFunction<FollyUrlExtractHostFunction, Varchar, Varchar>(
        {"folly_url_extract_host"});
    registerFunction<FollyUrlExtractPathFunction, Varchar, Varchar>(
        {"folly_url_extract_path"});
    registerFunction<FollyUrlExtractQueryFunction, Varchar, Varchar>(
        {"folly_url_extract_query"});
    registerFunction<FollyUrlExtractProtocolFunction, Varchar, Varchar>(
        {"folly_url_extract_protocol"});
    registerFunction<FollyUrlExtractPortFunction, int64_t, Varchar>(
        {"folly_url_extract_port"});
    registerFunction<
        FollyUrlExtractParameterFunction,
        Varchar,
        Varchar,
        Varchar>({"folly_url_extract_parameter"});
  }

  void runUrlExtract(const std::string& fnName, bool isParameter = false) {
    folly::BenchmarkSuspender suspender;

    size_t size = 1000;
    auto vectorUrls = vectorMaker_.flatVector<StringView>(
        size,
        [](auto row) {
          // construct some pseudo random url
          return StringView(fmt::format(
              "http://somehost{}.com:8080/somepath{}/p.php?k1={}#Refi",
              row,
              row % 2,
              row % 3));
        },
        nullptr);
    auto constVector = BaseVector::createConstant("k1", size, pool());
    auto rowVector = isParameter
        ? vectorMaker_.rowVector({vectorUrls, constVector})
        : vectorMaker_.rowVector({vectorUrls});

    auto queryString = isParameter ? "{}(c0, c1)" : "{}(c0)";
    auto exprSet = compileExpression(
        fmt::format(fmt::runtime(queryString), fnName), rowVector->type());

    suspender.dismiss();

    doRun(exprSet, rowVector);
  }

  void doRun(ExprSet& exprSet, const RowVectorPtr& rowVector) {
    uint32_t cnt = 0;
    for (auto i = 0; i < 100; i++) {
      cnt += evaluate(exprSet, rowVector)->size();
    }
    folly::doNotOptimizeAway(cnt);
  }
};

BENCHMARK(folly_fragment) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_fragment");
}

BENCHMARK_RELATIVE(velox_fragment) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_fragment");
}

BENCHMARK(folly_host) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_host");
}

BENCHMARK_RELATIVE(velox_host) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_host");
}

BENCHMARK(folly_path) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_path");
}

BENCHMARK_RELATIVE(velox_path) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_path");
}

BENCHMARK(folly_query) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_query");
}

BENCHMARK_RELATIVE(velox_query) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_query");
}

BENCHMARK(folly_port) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_port");
}

BENCHMARK_RELATIVE(velox_port) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_port");
}

BENCHMARK(folly_protocol) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_protocol");
}

BENCHMARK_RELATIVE(velox_protocol) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_protocol");
}

BENCHMARK(folly_param) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("folly_url_extract_parameter", true);
}

BENCHMARK_RELATIVE(velox_param) {
  UrlBenchmark benchmark;
  benchmark.runUrlExtract("url_extract_parameter", true);
}

} // namespace

int main(int argc, char** argv) {
  folly::init(&argc, &argv);

  folly::runBenchmarks();
  return 0;
}
