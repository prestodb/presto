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

namespace facebook::velox::aggregate {

const char* const kApproxDistinct = "approx_distinct";
const char* const kApproxMostFrequent = "approx_most_frequent";
const char* const kApproxPercentile = "approx_percentile";
const char* const kApproxSet = "approx_set";
const char* const kArbitrary = "arbitrary";
const char* const kArrayAgg = "array_agg";
const char* const kAvg = "avg";
const char* const kBitwiseAnd = "bitwise_and_agg";
const char* const kBitwiseOr = "bitwise_or_agg";
const char* const kBoolAnd = "bool_and";
const char* const kBoolOr = "bool_or";
const char* const kChecksum = "checksum";
const char* const kCorr = "corr";
const char* const kCount = "count";
const char* const kCountIf = "count_if";
const char* const kCovarPop = "covar_pop";
const char* const kCovarSamp = "covar_samp";
const char* const kEvery = "every";
const char* const kHistogram = "histogram";
const char* const kMapAgg = "map_agg";
const char* const kMapUnion = "map_union";
const char* const kMax = "max";
const char* const kMaxBy = "max_by";
const char* const kMerge = "merge";
const char* const kMin = "min";
const char* const kMinBy = "min_by";
const char* const kStdDev = "stddev"; // Alias for stddev_samp.
const char* const kStdDevPop = "stddev_pop";
const char* const kStdDevSamp = "stddev_samp";
const char* const kSum = "sum";
const char* const kVariance = "variance"; // Alias for var_samp.
const char* const kVarPop = "var_pop";
const char* const kVarSamp = "var_samp";
const char* const kMaxSizeForStats = "max_data_size_for_stats";
} // namespace facebook::velox::aggregate
