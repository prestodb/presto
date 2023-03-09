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
#pragma once

#include <string>
#include <unordered_set>

namespace facebook::velox::functions {

// Print a rst format string which contains all Presto/Spark scalar
// functions and aggragate functions. This function will read
// all_scalar_functions.txt and all_aggregate_functions.txt from certain path.
// for Presto, path is velox/functions/prestosql/coverage/data/
// for Spark, path is velox/functions/sparksql/coverage/data/
// domain is like a namespace. for spark function, domain is ":spark".
void printCoverageMapForAll(const std::string& domain = "");

// Print a rst format string which contains Presto/Spark functions were
// supported by velox. domain is like a namespace. for spark function, domain
// is ":spark".
void printVeloxFunctions(
    const std::unordered_set<std::string>& linkBlockList,
    const std::string& domain = "");

// Print a rst format string which contains most used Presto/Spark
// functions.This function will read all_scalar_functions.txt and
// all_aggregate_functions.txt and most_used_functions from certain path.
// for Presto, path is velox/functions/prestosql/coverage/data/
// for Spark, path is velox/functions/sparksql/coverage/data/
// domain is like a namespace. for spark function, domain is ":spark".
void printCoverageMapForMostUsed(const std::string& domain = "");

} // namespace facebook::velox::functions
