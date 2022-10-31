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

#include "velox/expression/tests/ExpressionRunner.h"
#include <folly/init/Init.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/vector/VectorSaver.h"

DEFINE_string(
    input_path,
    "",
    "Path for vector to be restored from disk. This will enable single run "
    "of the fuzzer with the on-disk persisted repro information. This has to "
    "be set with sql_path and optionally result_path.");

DEFINE_string(
    sql_path,
    "",
    "Path for expression SQL to be restored from disk. This will enable "
    "single run of the fuzzer with the on-disk persisted repro information. "
    "This has to be set with input_path and optionally result_path.");

DEFINE_string(
    sql,
    "",
    "Comma separated SQL expressions to evaluate. This flag and --sql_path "
    "flag are mutually exclusive. If both are specified, --sql is used and "
    "--sql_path is ignored.");

DEFINE_string(
    result_path,
    "",
    "Path for result vector to restore from disk. This is optional for "
    "on-disk reproduction. Don't set if the initial repro result vector is "
    "nullptr");

DEFINE_string(
    mode,
    "common",
    "Mode for expression runner: \n"
    "verify: evaluate the expression and compare results between common and "
    "simplified paths.\n"
    "common: evaluate the expression using common path and print out results.\n"
    "simplified: evaluate the expression using simplified path and print out "
    "results.");

DEFINE_int32(
    num_rows,
    10,
    "Maximum number of rows to process. Zero means 'all rows'. Applies to "
    "'common' and 'simplified' modes only. Ignord for 'verify' mode.");

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::init(&argc, &argv);

  VELOX_CHECK(
      !FLAGS_sql.empty() || !FLAGS_sql_path.empty(),
      "One of --sql or --sql_path flags must be set.");

  auto sql = FLAGS_sql;
  if (sql.empty()) {
    VELOX_CHECK(!FLAGS_sql_path.empty());
    sql = facebook::velox::restoreStringFromFile(FLAGS_sql_path.c_str());
    VELOX_CHECK(!sql.empty());
  }

  facebook::velox::functions::prestosql::registerAllScalarFunctions();
  facebook::velox::test::ExpressionRunner::run(
      FLAGS_input_path, sql, FLAGS_result_path, FLAGS_mode, FLAGS_num_rows);
}
