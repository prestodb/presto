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

#include <folly/init/Init.h>
#include <unordered_set>

#include "velox/exec/fuzzer/PrestoQueryRunner.h"
#include "velox/expression/fuzzer/ArgGenerator.h"
#include "velox/expression/fuzzer/FuzzerRunner.h"
#include "velox/functions/prestosql/fuzzer/DivideArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/FloorAndRoundArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/ModulusArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/MultiplyArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/PlusMinusArgGenerator.h"
#include "velox/functions/prestosql/fuzzer/TruncateArgGenerator.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    presto_url,
    "",
    "Presto coordinator URI along with port. If set, we use Presto as the "
    "source of truth. Otherwise, use the Velox simplified expression evaluation. Example: "
    "--presto_url=http://127.0.0.1:8080");

DEFINE_uint32(
    req_timeout_ms,
    10000,
    "Timeout in milliseconds for HTTP requests made to reference DB, "
    "such as Presto. Example: --req_timeout_ms=2000");

using namespace facebook::velox::exec::test;
using facebook::velox::exec::test::PrestoQueryRunner;
using facebook::velox::fuzzer::ArgGenerator;
using facebook::velox::fuzzer::FuzzerRunner;
using facebook::velox::test::ReferenceQueryRunner;

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  facebook::velox::memory::MemoryManager::initialize({});

  // TODO: List of the functions that at some point crash or fail and need to
  // be fixed before we can enable.
  // This list can include a mix of function names and function signatures.
  // Use function name to exclude all signatures of a given function from
  // testing. Use function signature to exclude only a specific signature.
  std::unordered_set<std::string> skipFunctions = {
      // Fuzzer and the underlying engine are confused about cardinality(HLL)
      // (since HLL is a user defined type), and end up trying to use
      // cardinality passing a VARBINARY (since HLL's implementation uses an
      // alias to VARBINARY).
      "cardinality",
      "element_at",
      "width_bucket",
      // Fuzzer cannot generate valid 'comparator' lambda.
      "array_sort(array(T),constant function(T,T,bigint)) -> array(T)",
      "split_to_map(varchar,varchar,varchar,function(varchar,varchar,varchar,varchar)) -> map(varchar,varchar)",
      // https://github.com/facebookincubator/velox/issues/8919
      "plus(date,interval year to month) -> date",
      "minus(date,interval year to month) -> date",
      "plus(timestamp,interval year to month) -> timestamp",
      "plus(interval year to month,timestamp) -> timestamp",
      "minus(timestamp,interval year to month) -> timestamp",
      // https://github.com/facebookincubator/velox/issues/8438#issuecomment-1907234044
      "regexp_extract",
      "regexp_extract_all",
      "regexp_like",
      "regexp_replace",
      "regexp_split",
      // date_format and format_datetime throw VeloxRuntimeError when input
      // timestamp is out of the supported range.
      "date_format",
      "format_datetime",
      // from_unixtime can generate timestamps out of the supported range that
      // make other functions throw VeloxRuntimeErrors.
      "from_unixtime",
  };
  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  std::unordered_map<std::string, std::shared_ptr<ArgGenerator>> argGenerators =
      {{"plus", std::make_shared<PlusMinusArgGenerator>()},
       {"minus", std::make_shared<PlusMinusArgGenerator>()},
       {"multiply", std::make_shared<MultiplyArgGenerator>()},
       {"divide", std::make_shared<DivideArgGenerator>()},
       {"floor", std::make_shared<FloorAndRoundArgGenerator>()},
       {"round", std::make_shared<FloorAndRoundArgGenerator>()},
       {"mod", std::make_shared<ModulusArgGenerator>()},
       {"truncate", std::make_shared<TruncateArgGenerator>()}};

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  if (!FLAGS_presto_url.empty()) {
    referenceQueryRunner = std::make_shared<PrestoQueryRunner>(
        rootPool.get(),
        FLAGS_presto_url,
        "expression_fuzzer",
        static_cast<std::chrono::milliseconds>(FLAGS_req_timeout_ms));
    LOG(INFO) << "Using Presto as the reference DB.";
  }
  FuzzerRunner::runFromGtest(
      initialSeed,
      skipFunctions,
      {{"session_timezone", "America/Los_Angeles"},
       {"adjust_timestamp_to_session_timezone", "true"}},
      argGenerators,
      referenceQueryRunner);
}
