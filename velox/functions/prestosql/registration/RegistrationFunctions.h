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

namespace facebook::velox::functions::prestosql {
void registerArithmeticFunctions(const std::string& prefix = "");

void registerCheckedArithmeticFunctions(const std::string& prefix = "");

void registerComparisonFunctions(const std::string& prefix = "");

void registerArrayFunctions(const std::string& prefix = "");

void registerInternalFunctions();

void registerMapFunctions(const std::string& prefix = "");

void registerJsonFunctions(const std::string& prefix = "");

void registerHyperLogFunctions(const std::string& prefix = "");

void registerGeneralFunctions(const std::string& prefix = "");

void registerDateTimeFunctions(const std::string& prefix = "");

void registerURLFunctions(const std::string& prefix = "");

void registerStringFunctions(const std::string& prefix = "");

void registerBinaryFunctions(const std::string& prefix = "");

void registerBitwiseFunctions(const std::string& prefix = "");

void registerAllScalarFunctions(const std::string& prefix = "");

void registerMapAllowingDuplicates(
    const std::string& name,
    const std::string& prefix = "");
} // namespace facebook::velox::functions::prestosql
