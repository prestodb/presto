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

namespace facebook::velox::functions::window {

// Register the Presto function nth_value(x, offset) with the bigint data type
// for the offset parameter.
void registerNthValueBigint(const std::string& name);

// Register the Spark function nth_value(x, offset) with the integer data type
// for the offset parameter.
void registerNthValueInteger(const std::string& name);

// Register the Presto function row_number() with the bigint data type
// for the return value.
void registerRowNumberBigint(const std::string& name);

// Register the Spark function row_number() with the integer data type
// for the return value.
void registerRowNumberInteger(const std::string& name);

// Register the Presto function rank() with the bigint data type
// for the return value.
void registerRankBigint(const std::string& name);

// Register the Spark function rank() with the integer data type
// for the return value.
void registerRankInteger(const std::string& name);

// Register the Presto function dense_rank() with the bigint data type
// for the return value.
void registerDenseRankBigint(const std::string& name);

// Register the Spark function dense_rank() with the integer data type
// for the return value.
void registerDenseRankInteger(const std::string& name);

// Returns the percentage ranking of a value in a group of values.
void registerPercentRank(const std::string& name);

// Register the Presto function ntile() with the bigint data type
// for the return and input value.
void registerNtileBigint(const std::string& name);

// Register the Spark function ntile() with the integer data type
// for the return and input value.
void registerNtileInteger(const std::string& name);

} // namespace facebook::velox::functions::window
