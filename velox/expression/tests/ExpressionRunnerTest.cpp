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
    result_path,
    "",
    "Path for result vector to restore from disk. This is optional for "
    "on-disk reproduction. Don't set if the initial repro result vector is "
    "nullptr");

// TODO(jtan6): Support common mode and simplified mode.
DEFINE_string(
    mode,
    "verify",
    "Mode for expression runner: \n"
    "verify: run expression and compare results between common and simplified "
    "path.\n"
    "common: run expression only using common paths. (to be supported)\n"
    "simplified: run expression only using simplified path. (to be supported)");

int main(int argc, char** argv) {
  facebook::velox::functions::prestosql::registerAllScalarFunctions();

  ::testing::InitGoogleTest(&argc, argv);

  // Calls common init functions in the necessary order, initializing
  // singletons, installing proper signal handlers for better debugging
  // experience, and initialize glog and gflags.
  folly::init(&argc, &argv);
  facebook::velox::test::ExpressionRunner::run(
      FLAGS_input_path, FLAGS_sql_path, FLAGS_result_path, FLAGS_mode);
}
