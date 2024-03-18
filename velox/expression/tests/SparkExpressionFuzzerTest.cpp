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

#include <folly/String.h>
#include <folly/init/Init.h>
#include <gtest/gtest.h>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "velox/expression/tests/FuzzerRunner.h"
#include "velox/functions/sparksql/Register.h"

DEFINE_int64(
    seed,
    123456,
    "Initial seed for random number generator "
    "(use it to reproduce previous results).");

using facebook::velox::test::FuzzerRunner;

int main(int argc, char** argv) {
  facebook::velox::functions::sparksql::registerFunctions("");

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::Init init(&argc, &argv);

  // The following list are the Spark UDFs that hit issues
  // For rlike you need the following combo in the only list:
  // rlike, md5 and upper
  std::unordered_set<std::string> skipFunctions = {
      "regexp_extract",
      // https://github.com/facebookincubator/velox/issues/8438
      "regexp_replace",
      "rlike",
      "chr",
      "replace",
      "might_contain",
      "unix_timestamp"};

  // Required by spark_partition_id function.
  std::unordered_map<std::string, std::string> queryConfigs = {
      {facebook::velox::core::QueryConfig::kSparkPartitionId, "123"}};

  return FuzzerRunner::run(FLAGS_seed, skipFunctions, queryConfigs);
}
