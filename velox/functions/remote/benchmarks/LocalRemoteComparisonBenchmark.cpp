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
#include "velox/functions/Registerer.h"
#include "velox/functions/prestosql/Arithmetic.h"
#include "velox/functions/prestosql/StringFunctions.h"
#include "velox/functions/prestosql/URLFunctions.h"
#include "velox/functions/remote/client/Remote.h"
#include "velox/functions/remote/utils/RemoteFunctionServiceProvider.h"
#include "velox/type/Type.h"

using namespace facebook::velox;
using namespace facebook::velox::functions;

DEFINE_int32(batch_size, 1000, "Batch size for benchmarks");

int main(int argc, char** argv) {
  folly::Init init(&argc, &argv);
  memory::MemoryManager::initialize({});

  auto param = startLocalThriftServiceAndGetParams();
  RemoteVectorFunctionMetadata metadata;
  metadata.location = param.serverAddress;

  // Register the remote adapter for PlusFunction
  auto plusSignatures = {exec::FunctionSignatureBuilder()
                             .returnType("bigint")
                             .argumentType("bigint")
                             .argumentType("bigint")
                             .build()};
  registerRemoteFunction("remote_plus", plusSignatures, metadata);
  // Registers the actual function under a different prefix. This is only
  // needed when thrift service runs in the same process.
  registerFunction<PlusFunction, int64_t, int64_t, int64_t>(
      {param.functionPrefix + ".remote_plus"});
  // register this function again, because the benchmark builder somehow doesn't
  // recognize the function registered above (remote.xxx).
  registerFunction<PlusFunction, int64_t, int64_t, int64_t>({"plus"});

  // Register the remote adapter for SubstrFunction
  auto substrSignatures = {exec::FunctionSignatureBuilder()
                               .returnType("varchar")
                               .argumentType("varchar")
                               .argumentType("integer")
                               .build()};
  registerRemoteFunction("remote_substr", substrSignatures, metadata);
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>(
      {param.functionPrefix + ".remote_substr"});
  registerFunction<SubstrFunction, Varchar, Varchar, int32_t>({"substr"});

  // Register the remote adapter for UrlEncodeFunction
  auto urlSignatures = {exec::FunctionSignatureBuilder()
                            .returnType("varchar")
                            .argumentType("varchar")
                            .build()};
  registerRemoteFunction("remote_url_encode", urlSignatures, metadata);
  registerFunction<UrlEncodeFunction, Varchar, Varchar>(
      {param.functionPrefix + ".remote_url_encode"});
  registerFunction<UrlEncodeFunction, Varchar, Varchar>({"url_encode"});

  ExpressionBenchmarkBuilder benchmarkBuilder;

  VectorFuzzer::Options opts;
  opts.vectorSize = FLAGS_batch_size;
  opts.stringVariableLength = true;
  opts.containerVariableLength = true;
  VectorFuzzer fuzzer(opts, benchmarkBuilder.pool());
  auto vectorMaker = benchmarkBuilder.vectorMaker();

  // benchmark comparaing PlusFunction running locally (same thread)
  // and running with RemoteFunction (different threads).
  benchmarkBuilder
      .addBenchmarkSet(
          "local_vs_remote_plus",
          vectorMaker.rowVector(
              {"c0", "c1"},
              {fuzzer.fuzzFlat(BIGINT()), fuzzer.fuzzFlat(BIGINT())}))
      .addExpression("local_plus", "plus(c0, c1) ")
      .addExpression("remote_plus", "remote_plus(c0, c1) ")
      .withIterations(1000);

  // benchmark comparaing SubstrFunction running locally (same thread)
  // and running with RemoteFunction (different threads).
  benchmarkBuilder
      .addBenchmarkSet(
          "local_vs_remote_substr",
          vectorMaker.rowVector(
              {"c0", "c1"},
              {fuzzer.fuzzFlat(VARCHAR()), fuzzer.fuzzFlat(INTEGER())}))
      .addExpression("local_substr", "substr(c0, c1) ")
      .addExpression("remote_substr", "remote_substr(c0, c1) ")
      .withIterations(1000);

  // benchmark comparaing UrlEncodeFunction running locally (same thread)
  // and running with RemoteFunction (different threads).
  benchmarkBuilder
      .addBenchmarkSet(
          "local_vs_remote_url_encode",
          vectorMaker.rowVector({"c0"}, {fuzzer.fuzzFlat(VARCHAR())}))
      .addExpression("local_url_encode", "url_encode(c0) ")
      .addExpression("remote_url_encode", "remote_url_encode(c0) ")
      .withIterations(1000);

  benchmarkBuilder.registerBenchmarks();
  benchmarkBuilder.testBenchmarks();
  folly::runBenchmarks();
  return 0;
}
