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
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include <unordered_set>

#include "velox/dwio/parquet/RegisterParquetWriter.h"
#include "velox/exec/fuzzer/AggregationFuzzerOptions.h"
#include "velox/exec/fuzzer/AggregationFuzzerRunner.h"
#include "velox/exec/fuzzer/TransformResultVerifier.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/sparksql/aggregates/Register.h"
#include "velox/functions/sparksql/fuzzer/SparkQueryRunner.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

DEFINE_int64(
    seed,
    0,
    "Initial seed for random number generator used to reproduce previous "
    "results (0 means start with random seed).");

DEFINE_string(
    only,
    "",
    "If specified, Fuzzer will only choose functions from "
    "this comma separated list of function names "
    "(e.g: --only \"min\" or --only \"sum,avg\").");

int main(int argc, char** argv) {
  facebook::velox::functions::aggregate::sparksql::registerAggregateFunctions(
      "", false);
  facebook::velox::parquet::registerParquetWriterFactory();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  facebook::velox::functions::prestosql::registerInternalFunctions();
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kPresto)) {
    facebook::velox::serializer::presto::PrestoVectorSerde::
        registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kCompactRow)) {
    facebook::velox::serializer::CompactRowVectorSerde::
        registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(
          facebook::velox::VectorSerde::Kind::kUnsafeRow)) {
    facebook::velox::serializer::spark::UnsafeRowVectorSerde::
        registerNamedVectorSerde();
  }
  facebook::velox::memory::MemoryManager::initialize(
      facebook::velox::memory::MemoryManager::Options{});

  // Spark does not provide user-accessible aggregate functions with the
  // following names.
  std::unordered_set<std::string> skipFunctions = {
      "bloom_filter_agg",
      "first_ignore_null",
      "last_ignore_null",
      "regr_replacement",
  };

  using facebook::velox::exec::test::TransformResultVerifier;

  auto makeArrayVerifier = []() {
    return TransformResultVerifier::create("\"$internal$canonicalize\"({})");
  };

  // The results of the following functions depend on the order of input
  // rows. For some functions, the result can be transformed to a value that
  // doesn't depend on the order of inputs. If such transformation exists, it
  // can be specified to be used for results verification. If no transformation
  // is specified, results are not verified.
  std::unordered_map<
      std::string,
      std::shared_ptr<facebook::velox::exec::test::ResultVerifier>>
      customVerificationFunctions = {
          {"last", nullptr},
          {"last_ignore_null", nullptr},
          {"first", nullptr},
          {"first_ignore_null", nullptr},
          {"max_by", nullptr},
          {"min_by", nullptr},
          // If multiple values have the same greatest frequency, the return
          // value is indeterminate.
          {"mode", nullptr},
          {"skewness", nullptr},
          {"kurtosis", nullptr},
          {"collect_list", makeArrayVerifier()},
          {"collect_set", makeArrayVerifier()},
          // Nested nulls are handled as values in Spark. But nested nulls
          // comparison always generates null in DuckDB.
          {"min", nullptr},
          {"max", nullptr},
      };

  size_t initialSeed = FLAGS_seed == 0 ? std::time(nullptr) : FLAGS_seed;
  std::shared_ptr<facebook::velox::memory::MemoryPool> rootPool{
      facebook::velox::memory::memoryManager()->addRootPool()};
  auto sparkQueryRunner = std::make_unique<
      facebook::velox::functions::sparksql::fuzzer::SparkQueryRunner>(
      rootPool.get(), "localhost:15002", "fuzzer", "aggregate");

  using Runner = facebook::velox::exec::test::AggregationFuzzerRunner;
  using Options = facebook::velox::exec::test::AggregationFuzzerOptions;

  Options options;
  options.onlyFunctions = FLAGS_only;
  options.skipFunctions = skipFunctions;
  options.customVerificationFunctions = customVerificationFunctions;
  options.orderableGroupKeys = true;
  options.timestampPrecision =
      facebook::velox::VectorFuzzer::Options::TimestampPrecision::kMicroSeconds;
  options.hiveConfigs = {
      {facebook::velox::connector::hive::HiveConfig::kReadTimestampUnit, "6"}};
  return Runner::run(initialSeed, std::move(sparkQueryRunner), options);
}
