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
#include "velox/functions/CoverageUtil.h"
#include "velox/functions/sparksql/Register.h"
#include "velox/functions/sparksql/aggregates/Register.h"

DEFINE_bool(all, false, "Generate coverage map for all Spark functions");

using namespace facebook::velox;

int main(int argc, char** argv) {
  gflags::ParseCommandLineFlags(&argc, &argv, true);

  // Register all simple and vector scalar functions.
  functions::sparksql::registerFunctions("");

  // Register Spark aggregate functions.
  functions::aggregate::sparksql::registerAggregateFunctions("");

  if (FLAGS_all) {
    functions::printCoverageMapForAll(":spark");
  } else {
    functions::printVeloxFunctions({}, ":spark");
  }

  return 0;
}
