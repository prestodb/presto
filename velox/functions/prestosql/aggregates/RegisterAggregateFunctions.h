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

namespace facebook::velox::aggregate::prestosql {

/// Entery point to register aggregate functions.
/// @param prefix Prefix for the aggregate functions.
/// @param withCompanionFunctions Also register companion functions, defaults
/// to true.
/// @param onlyPrestoSignatures Register only function signatures
/// that are compatible with Presto.
void registerAllAggregateFunctions(
    const std::string& prefix = "",
    bool withCompanionFunctions = true,
    bool onlyPrestoSignatures = false,
    bool overwrite = true);

/// Register internal aggregation functions only for testing.
/// @param prefix Prefix for the aggregate functions.
void registerInternalAggregateFunctions(const std::string& prefix);

} // namespace facebook::velox::aggregate::prestosql
