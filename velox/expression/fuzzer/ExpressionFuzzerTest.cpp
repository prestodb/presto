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
#include "velox/expression/fuzzer/ArgTypesGenerator.h"
#include "velox/expression/fuzzer/ArgValuesGenerators.h"
#include "velox/expression/fuzzer/ExpressionFuzzer.h"
#include "velox/expression/fuzzer/FuzzerRunner.h"
#include "velox/expression/fuzzer/SpecialFormSignatureGenerator.h"
#include "velox/functions/prestosql/fuzzer/DivideArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/FloorAndRoundArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/ModulusArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/MultiplyArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/PlusMinusArgTypesGenerator.h"
#include "velox/functions/prestosql/fuzzer/SortArrayTransformer.h"
#include "velox/functions/prestosql/fuzzer/TruncateArgTypesGenerator.h"
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
using facebook::velox::fuzzer::ArgTypesGenerator;
using facebook::velox::fuzzer::ArgValuesGenerator;
using facebook::velox::fuzzer::ExpressionFuzzer;
using facebook::velox::fuzzer::FuzzerRunner;
using facebook::velox::fuzzer::JsonExtractArgValuesGenerator;
using facebook::velox::fuzzer::JsonParseArgValuesGenerator;
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
      // Fuzzer and the underlying engine are confused about TDigest functions
      // (since TDigest is a user defined type), and tries to pass a
      // VARBINARY (since TDigest's implementation uses an
      // alias to VARBINARY).
      "value_at_quantile",
      "values_at_quantiles",
      "merge_tdigest",
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
      // JSON not supported, Real doesn't match exactly, etc.
      "array_join",
      // BingTiles throw VeloxUserError when zoom/x/y are out of range.
      "bing_tile",
      "bing_tile_zoom_level",
      "bing_tile_coordinates",
      "bing_tile_parent",
      "bing_tile_children",
      "bing_tile_quadkey",
      "array_min_by", // https://github.com/facebookincubator/velox/issues/12934
      "array_max_by", // https://github.com/facebookincubator/velox/issues/12934
  };
  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  std::unordered_map<std::string, std::shared_ptr<ArgTypesGenerator>>
      argTypesGenerators = {
          {"plus", std::make_shared<PlusMinusArgTypesGenerator>()},
          {"minus", std::make_shared<PlusMinusArgTypesGenerator>()},
          {"multiply", std::make_shared<MultiplyArgTypesGenerator>()},
          {"divide", std::make_shared<DivideArgTypesGenerator>()},
          {"floor", std::make_shared<FloorAndRoundArgTypesGenerator>()},
          {"round", std::make_shared<FloorAndRoundArgTypesGenerator>()},
          {"mod", std::make_shared<ModulusArgTypesGenerator>()},
          {"truncate", std::make_shared<TruncateArgTypesGenerator>()}};

  std::unordered_map<std::string, std::shared_ptr<ExprTransformer>>
      exprTransformers = {
          {"array_intersect", std::make_shared<SortArrayTransformer>()},
          {"array_except", std::make_shared<SortArrayTransformer>()},
          {"map_keys", std::make_shared<SortArrayTransformer>()},
          {"map_values", std::make_shared<SortArrayTransformer>()}};

  std::unordered_map<std::string, std::shared_ptr<ArgValuesGenerator>>
      argValuesGenerators = {
          {"json_parse", std::make_shared<JsonParseArgValuesGenerator>()},
          {"json_extract", std::make_shared<JsonExtractArgValuesGenerator>()}};

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};
  if (!FLAGS_presto_url.empty()) {
    // Add additional functions to skip since we are now querying Presto
    // directly and are aware of certain failures.
    skipFunctions.insert({
        "cast(real) -> varchar", // https://github.com/facebookincubator/velox/issues/11034
        "round", // https://github.com/facebookincubator/velox/issues/10634
        "json_size", // https://github.com/facebookincubator/velox/issues/10700
        "bitwise_right_shift_arithmetic", // https://github.com/facebookincubator/velox/issues/10841
        "map_size_with", // https://github.com/facebookincubator/velox/issues/10964
        "binomial_cdf", // https://github.com/facebookincubator/velox/issues/10702
        "inverse_laplace_cdf", // https://github.com/facebookincubator/velox/issues/10699
        "gamma_cdf", // https://github.com/facebookincubator/velox/issues/10749
        "inverse_normal_cdf", // https://github.com/facebookincubator/velox/issues/10836
        "regr_avgx", // https://github.com/facebookincubator/velox/issues/10610
        "is_json_scalar", // https://github.com/facebookincubator/velox/issues/10748
        "json_extract_scalar", // https://github.com/facebookincubator/velox/issues/10698
        "rtrim", // https://github.com/facebookincubator/velox/issues/10973
        "split", // https://github.com/facebookincubator/velox/issues/10867
        "array_intersect", // https://github.com/facebookincubator/velox/issues/10740
        "f_cdf", // https://github.com/facebookincubator/velox/issues/10633
        "truncate", // https://github.com/facebookincubator/velox/issues/10628
        "url_extract_query", // https://github.com/facebookincubator/velox/issues/10659
        "laplace_cdf", // https://github.com/facebookincubator/velox/issues/10974
        "inverse_beta_cdf", // https://github.com/facebookincubator/velox/issues/11802
        "conjunct", // https://github.com/facebookincubator/velox/issues/10678
        "url_extract_host", // https://github.com/facebookincubator/velox/issues/10578
        "weibull_cdf", // https://github.com/facebookincubator/velox/issues/10977
        "zip_with", // https://github.com/facebookincubator/velox/issues/10844
        "url_extract_path", // https://github.com/facebookincubator/velox/issues/10579
        "bitwise_shift_left", // https://github.com/facebookincubator/velox/issues/10870
        "split_part", // https://github.com/facebookincubator/velox/issues/10839
        "bitwise_arithmetic_shift_right", // https://github.com/facebookincubator/velox/issues/10750
        "date_format", // https://github.com/facebookincubator/velox/issues/10968
        "substr", // https://github.com/facebookincubator/velox/issues/10660
        "cauchy_cdf", // https://github.com/facebookincubator/velox/issues/10976
        "covar_pop", // https://github.com/facebookincubator/velox/issues/10627
        "lpad", // https://github.com/facebookincubator/velox/issues/10757
        "format_datetime", // https://github.com/facebookincubator/velox/issues/10779
        "inverse_cauchy_cdf", // https://github.com/facebookincubator/velox/issues/10840
        "array_position", // https://github.com/facebookincubator/velox/issues/10580
        "url_extract_fragment", // https://github.com/facebookincubator/velox/issues/12324
        "url_extract_protocol", // https://github.com/facebookincubator/velox/issues/12325
        "chi_squared_cdf", // https://github.com/facebookincubator/velox/issues/12327
        "bitwise_left_shift", // https://github.com/facebookincubator/velox/issues/12330
        "log2", // https://github.com/facebookincubator/velox/issues/12338
        "bitwise_right_shift", // https://github.com/facebookincubator/velox/issues/12339
        "word_stem", // https://github.com/facebookincubator/velox/issues/12341
        "rpad", // https://github.com/facebookincubator/velox/issues/12343
        "json_extract", // https://github.com/facebookincubator/velox/issues/12344
        "to_ieee754_32", // https://github.com/facebookincubator/velox/issues/12345
        "beta_cdf", // https://github.com/facebookincubator/velox/issues/12346
        "wilson_interval_upper", // https://github.com/facebookincubator/velox/issues/12347
        "json_parse", // https://github.com/facebookincubator/velox/issues/12371
        "to_ieee754_64", // https://github.com/facebookincubator/velox/issues/12372
        // No default Hive type provided for unsupported Hive type: json
        "json_format",
        "json_array_length",
        "json_array_get",
        "json_array_contains",
        "clamp", // Function clamp not registered
        "current_date", // Non-deterministic
        "xxhash64_internal",
        "combine_hash_internal",
        "map_keys_by_top_n_values", // requires
                                    // https://github.com/prestodb/presto/pull/24570
        "inverse_gamma_cdf", // https://github.com/facebookincubator/velox/issues/12918
        "inverse_binomial_cdf", // https://github.com/facebookincubator/velox/issues/12981
        "inverse_poisson_cdf", // https://github.com/facebookincubator/velox/issues/12982
    });

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
      exprTransformers,
      {{"session_timezone", "America/Los_Angeles"},
       {"adjust_timestamp_to_session_timezone", "true"}},
      argTypesGenerators,
      argValuesGenerators,
      referenceQueryRunner,
      std::make_shared<
          facebook::velox::fuzzer::SpecialFormSignatureGenerator>());
}
