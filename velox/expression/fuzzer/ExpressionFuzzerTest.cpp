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
using facebook::velox::fuzzer::CastVarcharAndJsonArgValuesGenerator;
using facebook::velox::fuzzer::ExpressionFuzzer;
using facebook::velox::fuzzer::FuzzerRunner;
using facebook::velox::fuzzer::JsonExtractArgValuesGenerator;
using facebook::velox::fuzzer::JsonParseArgValuesGenerator;
using facebook::velox::fuzzer::QDigestArgValuesGenerator;
using facebook::velox::fuzzer::TDigestArgValuesGenerator;
using facebook::velox::fuzzer::UnifiedDigestArgValuesGenerator;
using facebook::velox::fuzzer::URLArgValuesGenerator;
using facebook::velox::test::ReferenceQueryRunner;

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
        {"array_duplicates", std::make_shared<SortArrayTransformer>()},
        {"map_entries", std::make_shared<SortArrayTransformer>()},
        {"map_keys", std::make_shared<SortArrayTransformer>()},
        {"map_values", std::make_shared<SortArrayTransformer>()}};

std::unordered_map<std::string, std::shared_ptr<ArgValuesGenerator>>
    argValuesGenerators = {
        {"cast", std::make_shared<CastVarcharAndJsonArgValuesGenerator>()},
        {"json_parse", std::make_shared<JsonParseArgValuesGenerator>()},
        {"json_extract", std::make_shared<JsonExtractArgValuesGenerator>()},
        {"scale_tdigest",
         std::make_shared<TDigestArgValuesGenerator>("scale_tdigest")},
        {"url_extract_fragment", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_host", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_parameter", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_path", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_port", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_protocol", std::make_shared<URLArgValuesGenerator>()},
        {"url_extract_query", std::make_shared<URLArgValuesGenerator>()},
        {"value_at_quantile",
         std::make_shared<UnifiedDigestArgValuesGenerator>(
             "value_at_quantile")},
        {"values_at_quantiles",
         std::make_shared<UnifiedDigestArgValuesGenerator>(
             "values_at_quantiles")},
        {"scale_qdigest",
         std::make_shared<QDigestArgValuesGenerator>("scale_qdigest")},
        {"scale_tdigest",
         std::make_shared<TDigestArgValuesGenerator>("scale_tdigest")},
        {"quantile_at_value",
         std::make_shared<UnifiedDigestArgValuesGenerator>(
             "quantile_at_value")},
        {"destructure_tdigest",
         std::make_shared<TDigestArgValuesGenerator>("destructure_tdigest")},
        {"trimmed_mean",
         std::make_shared<TDigestArgValuesGenerator>("trimmed_mean")}};

// TODO: List of the functions that at some point crash or fail and need to
// be fixed before we can enable.
// This list can include a mix of function names and function signatures.
// Use function name to exclude all signatures of a given function from
// testing. Use function signature to exclude only a specific signature.
std::unordered_set<std::string> skipFunctions = {
    "noisy_empty_approx_set_sfm", // Non-deterministic because of privacy.
    "merge_sfm", // Fuzzer can generate sketches of different sizes.
    "element_at",
    "width_bucket",
    // Fuzzer and the underlying engine are confused about TDigest functions
    // (since TDigest is a user defined type), and tries to pass a
    // VARBINARY (since TDigest's implementation uses an
    // alias to VARBINARY).
    "value_at_quantile",
    "values_at_quantiles",
    "merge_tdigest",
    "scale_tdigest",
    "quantiles_at_values",
    "construct_tdigest",
    "destructure_tdigest",
    "trimmed_mean",
    // Fuzzer cannot generate valid 'comparator' lambda.
    "array_sort(array(T),constant function(T,T,bigint)) -> array(T)",
    "array_sort(array(T),constant function(T,U)) -> array(T)",
    "array_sort_desc(array(T),constant function(T,U)) -> array(T)",
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
    "map_top_n", // https://github.com/facebookincubator/velox/issues/9497
    // These can take 2 random int32s and makes an NxM matrix. Naturally this
    // matrix can be very large taking a long time to populate and OOM'ing the
    // host.
    "bootstrap_metrics",
    "bootstrap_metrics_list",
    // This function invokes a Boost library function with a known issue where
    // it hits integer overflows when the arguments are very large.
    // https://github.com/facebookincubator/velox/issues/11802
    "inverse_beta_cdf",
    // https://github.com/facebookincubator/velox/issues/13047
    "inverse_poisson_cdf",
    "map_subset", // https://github.com/facebookincubator/velox/issues/12654
    // JSON not supported, Real doesn't match exactly, etc.
    "array_join(array(json),varchar) -> varchar",
    "array_join(array(json),varchar,varchar) -> varchar",
    "array_join(array(real),varchar) -> varchar",
    "array_join(array(real),varchar,varchar) -> varchar",
    "array_join(array(double),varchar) -> varchar",
    "array_join(array(double),varchar,varchar) -> varchar",
    // Distance functions
    "cosine_similarity", // T224131333
    "l2_squared", // T228562088
    "dot_product", // T228562088
    // Geometry functions don't yet have a ValuesGenerator
    "st_geometryfromtext",
    "st_geomfrombinary",
    "st_area",
    "st_astext",
    "st_asbinary",
    "st_boundary",
    "st_centroid",
    "st_distance",
    "st_geometrytype",
    "st_polygon",
    "st_isclosed",
    "st_isempty",
    "st_length",
    "st_pointn",
    "st_isring",
    "st_relate",
    "st_contains",
    "st_crosses",
    "st_disjoint",
    "st_equals",
    "st_intersects",
    "st_overlaps",
    "st_touches",
    "st_within",
    "st_difference",
    "st_intersection",
    "st_symdifference",
    "st_union",
    "st_point",
    "st_points",
    "st_x",
    "st_y",
    "st_isvalid",
    "st_issimple",
    "st_startpoint",
    "st_endpoint",
    "st_geometryn",
    "st_interiorringn",
    "st_numinteriorring",
    "st_numgeometries",
    "st_convexhull",
    "st_coorddim",
    "st_dimension",
    "st_exteriorring",
    "st_envelope",
    "ST_EnvelopeAsPts",
    "st_buffer",
    "geometry_invalid_reason",
    "simplify_geometry",
    "st_xmax",
    "st_xmin",
    "st_ymax",
    "st_ymin",
};

std::unordered_set<std::string> skipFunctionsSOT = {
    "noisy_empty_approx_set_sfm", // non-deterministic because of privacy.
    // https://github.com/facebookincubator/velox/issues/11034
    "cast(real) -> varchar",
    "cast(row(real)) -> row(varchar)",
    "cast(double) -> varchar",
    "cast(row(double)) -> row(varchar)",
    "cast(array(double)) -> array(varchar)",
    "cast(array(real)) -> array(varchar)",
    "cast(map(varchar,double)) -> map(varchar,varchar)",
    "cast(map(varchar,real)) -> map(varchar,varchar)",
    "round", // https://github.com/facebookincubator/velox/issues/10634
    "bitwise_right_shift_arithmetic", // https://github.com/facebookincubator/velox/issues/10841
    "map_size_with", // https://github.com/facebookincubator/velox/issues/10964
    "binomial_cdf", // https://github.com/facebookincubator/velox/issues/10702
    "inverse_laplace_cdf", // https://github.com/facebookincubator/velox/issues/10699
    "gamma_cdf", // https://github.com/facebookincubator/velox/issues/10749
    "inverse_normal_cdf", // https://github.com/facebookincubator/velox/issues/10836
    "regr_avgx", // https://github.com/facebookincubator/velox/issues/10610
    "rtrim", // https://github.com/facebookincubator/velox/issues/10973
    "split", // https://github.com/facebookincubator/velox/issues/10867
    "array_intersect", // https://github.com/facebookincubator/velox/issues/10740
    "f_cdf", // https://github.com/facebookincubator/velox/issues/10633
    "truncate", // https://github.com/facebookincubator/velox/issues/10628
    "laplace_cdf", // https://github.com/facebookincubator/velox/issues/10974
    "inverse_beta_cdf", // https://github.com/facebookincubator/velox/issues/11802
    "conjunct", // https://github.com/facebookincubator/velox/issues/10678
    "weibull_cdf", // https://github.com/facebookincubator/velox/issues/10977
    "zip_with", // https://github.com/facebookincubator/velox/issues/10844
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
    "chi_squared_cdf", // https://github.com/facebookincubator/velox/issues/12327
    "bitwise_left_shift", // https://github.com/facebookincubator/velox/issues/12330
    "log2", // https://github.com/facebookincubator/velox/issues/12338
    "bitwise_right_shift", // https://github.com/facebookincubator/velox/issues/12339
    "word_stem", // https://github.com/facebookincubator/velox/issues/12341
    "rpad", // https://github.com/facebookincubator/velox/issues/12343
    "to_ieee754_32", // https://github.com/facebookincubator/velox/issues/12345
    "beta_cdf", // https://github.com/facebookincubator/velox/issues/12346
    "wilson_interval_upper", // https://github.com/facebookincubator/velox/issues/12347
    "to_ieee754_64", // https://github.com/facebookincubator/velox/issues/12372
    "inverse_weibull_cdf", // https://github.com/facebookincubator/velox/issues/12550
    "from_utf8", // https://github.com/facebookincubator/velox/issues/12554
    // https://github.com/facebookincubator/velox/issues/12555
    "year",
    "year_of_week",
    "day_of_year",
    "week_of_year",
    "doy",
    "minute",
    "quarter",
    "yow",
    "day",
    "day_of_month",
    "dow",
    "week",
    "cast(timestamp) -> varchar", // https://github.com/facebookincubator/velox/issues/12803
    "from_iso8601_timestamp", // https://github.com/facebookincubator/velox/issues/13605
    "typeof", // https://github.com/facebookincubator/velox/issues/12804
    "escape", // https://github.com/facebookincubator/velox/issues/12558
    "from_base64url", // https://github.com/facebookincubator/velox/issues/12562
    "array_top_n", // https://github.com/prestodb/presto/issues/24700
    "codepoint", // https://github.com/facebookincubator/velox/issues/12598
    "in", // https://github.com/facebookincubator/velox/issues/12597
    "multimap_from_entries", // https://github.com/facebookincubator/velox/issues/12628
    "map_zip_with", // https://github.com/facebookincubator/velox/issues/10964
    "transform_keys", // https://github.com/facebookincubator/velox/issues/12992
    "arrays_overlap", // https://github.com/facebookincubator/velox/issues/12651
    "array_normalize", // https://github.com/facebookincubator/velox/issues/12623
    "array_distinct", // https://github.com/prestodb/presto/issues/24719
    "power", // https://github.com/facebookincubator/velox/issues/12557
    "like", // https://github.com/prestodb/presto/issues/24747
    "slice", // https://github.com/facebookincubator/velox/issues/12782
    "inverse_gamma_cdf", // https://github.com/facebookincubator/velox/issues/12918
    "atan2", // T219988962
    "cosine_similarity", // https://github.com/facebookincubator/velox/issues/12915
    "l2_squared", // https://github.com/facebookincubator/velox/issues/12915
    "dot_product", // https://github.com/facebookincubator/velox/issues/12915
    "subscript", // https://github.com/facebookincubator/velox/issues/12959
    "reduce", // T220595632
    "inverse_binomial_cdf", // https://github.com/facebookincubator/velox/issues/12981
    "inverse_poisson_cdf", // https://github.com/facebookincubator/velox/issues/12982
    // https://github.com/facebookincubator/velox/issues/13002
    "ceiling",
    "ceil",
    // https://github.com/facebookincubator/velox/issues/13132
    "normalize",
    "concat",
    "trim", // https://github.com/facebookincubator/velox/issues/13176
    "map_key_exists", // https://github.com/facebookincubator/velox/issues/13190
    "map_from_entries", // https://github.com/facebookincubator/velox/issues/13191
    "switch", // https://github.com/prestodb/presto/issues/25077
    "degrees", // https://github.com/facebookincubator/velox/issues/13608
    "substring", // T225620908
    "json_extract", // https://github.com/facebookincubator/velox/issues/12344
    "to_milliseconds", // https://github.com/prestodb/presto/issues/25275
    "parse_duration", // https://github.com/prestodb/presto/issues/25340
    "json_format",
    "json_array_length", // https://github.com/facebookincubator/velox/issues/13356
    "is_json_scalar", // https://github.com/facebookincubator/velox/issues/10748
    "json_size", // https://github.com/facebookincubator/velox/issues/12371
    "json_extract_scalar", // https://github.com/facebookincubator/velox/issues/10698
    "json_array_contains", // https://github.com/facebookincubator/velox/issues/13685
    "clamp", // Function clamp not registered
    "current_date", // Non-deterministic
    "xxhash64_internal",
    "combine_hash_internal",
    "noisy_avg_gaussian", // Non-deterministic
    "noisy_count_if_gaussian", // Non-deterministic
    "noisy_count_gaussian", // Non-deterministic
    "noisy_sum_gaussian", // Non-deterministic
    "second", // https://github.com/prestodb/presto/pull/25090
    "inverse_f_cdf", // https://github.com/facebookincubator/velox/issues/13715
    // https://github.com/facebookincubator/velox/issues/13767
    "array_max(array(__user_T1)) -> __user_T1",
    "array_min(array(__user_T1)) -> __user_T1",
    "inverse_chi_squared_cdf", // https://github.com/facebookincubator/velox/issues/13788
    "bing_tile_children", // Velox limits the max zoom shift
                          // https://github.com/facebookincubator/velox/pull/13604
    // Not registered
    "array_sum_propagate_element_null",
    // Skipping until the new signature is merged and released in Presto:
    // https://github.com/prestodb/presto/pull/25521
    "xxhash64(varbinary,bigint) -> varbinary",
    "$internal$canonicalize",
    "$internal$contains",
};

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);

  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::functions::prestosql::registerInternalFunctions();

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;

  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  std::shared_ptr<ReferenceQueryRunner> referenceQueryRunner{nullptr};

  if (!FLAGS_presto_url.empty()) {
    // Add additional functions to skip since we are now querying Presto
    // directly and are aware of certain failures.
    skipFunctions.insert(skipFunctionsSOT.begin(), skipFunctionsSOT.end());

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
