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

#include <gflags/gflags.h>
#include "velox/exec/Aggregate.h"
#include "velox/exec/WindowFunction.h"
#include "velox/functions/CoverageUtil.h"
#include "velox/functions/FunctionRegistry.h"
#include "velox/functions/prestosql/aggregates/RegisterAggregateFunctions.h"
#include "velox/functions/prestosql/registration/RegistrationFunctions.h"
#include "velox/functions/prestosql/window/WindowFunctionsRegistration.h"

DEFINE_bool(all, false, "Generate coverage map for all Presto functions");
DEFINE_bool(
    most_used,
    false,
    "Generate coverage map for a subset of most-used Presto functions");

using namespace facebook::velox;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Register all simple and vector scalar functions.
  functions::prestosql::registerAllScalarFunctions();

  // Register Presto aggregate functions.
  aggregate::prestosql::registerAllAggregateFunctions();

  // Register Presto window functions.
  window::prestosql::registerAllWindowFunctions();

  if (FLAGS_all) {
    functions::printCoverageMapForAll();
  } else if (FLAGS_most_used) {
    functions::printCoverageMapForMostUsed();
  } else {
    const std::unordered_set<std::string> linkBlockList = {
        "checked_divide",
        "checked_minus",
        "checked_modulus",
        "checked_multiply",
        "checked_negate",
        "checked_plus",
        "in",
        "modulus",
        "not"};
    functions::printVeloxFunctions(linkBlockList);
  }

  return 0;
}
