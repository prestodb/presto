/*
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

namespace facebook::velox::aggregate {

const char* const kApproxDistinct = "approx_distinct";
const char* const kApproxPercentile = "approx_percentile";
const char* const kArbitrary = "arbitrary";
const char* const kArrayAgg = "array_agg";
const char* const kAvg = "avg";
const char* const kBitwiseAnd = "bitwise_and_agg";
const char* const kBitwiseOr = "bitwise_or_agg";
const char* const kBoolAnd = "bool_and";
const char* const kBoolOr = "bool_or";
const char* const kCount = "count";
const char* const kCountIf = "count_if";
const char* const kMapAgg = "map_agg";
const char* const kMax = "max";
const char* const kMaxBy = "max_by";
const char* const kMin = "min";
const char* const kMinBy = "min_by";
const char* const kSum = "sum";

} // namespace facebook::velox::aggregate
