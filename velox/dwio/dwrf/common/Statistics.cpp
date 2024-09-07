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

#include "velox/dwio/dwrf/common/Statistics.h"

namespace facebook::velox::dwrf {

using namespace dwio::common;

std::unique_ptr<ColumnStatistics> buildColumnStatisticsFromProto(
    const ColumnStatisticsWrapper& stats,
    const StatsContext& statsContext) {
  ColumnStatistics colStats(
      stats.hasNumberOfValues() ? std::optional(stats.numberOfValues())
                                : std::nullopt,
      stats.hasHasNull() ? std::optional(stats.hasNull()) : std::nullopt,
      stats.hasRawSize() ? std::optional(stats.rawSize()) : std::nullopt,
      stats.hasSize() ? std::optional(stats.size()) : std::nullopt);

  // detailed stats is only defined when has non-null value
  if (!stats.hasNumberOfValues() || stats.numberOfValues() > 0) {
    if (stats.hasIntStatistics()) {
      const auto& intStats = stats.intStatistics();
      return std::make_unique<IntegerColumnStatistics>(
          colStats,
          intStats.hasMinimum() ? std::optional(intStats.minimum())
                                : std::nullopt,
          intStats.hasMaximum() ? std::optional(intStats.maximum())
                                : std::nullopt,
          intStats.hasSum() ? std::optional(intStats.sum()) : std::nullopt);
    } else if (stats.hasDoubleStatistics()) {
      const auto& doubleStats = stats.doubleStatistics();
      // Comparing against NaN doesn't make sense, and to prevent downstream
      // from incorrectly using it, need to make sure min/max/sum doens't have
      // NaN.
      const auto hasNan =
          (doubleStats.hasMinimum() && std::isnan(doubleStats.minimum())) ||
          (doubleStats.hasMaximum() && std::isnan(doubleStats.maximum())) ||
          (doubleStats.hasSum() && std::isnan(doubleStats.sum()));
      if (!hasNan) {
        return std::make_unique<DoubleColumnStatistics>(
            colStats,
            doubleStats.hasMinimum() ? std::optional(doubleStats.minimum())
                                     : std::nullopt,
            doubleStats.hasMaximum() ? std::optional(doubleStats.maximum())
                                     : std::nullopt,
            doubleStats.hasSum() ? std::optional(doubleStats.sum())
                                 : std::nullopt);
      }
    } else if (stats.hasStringStatistics()) {
      // DWRF_5_0 is the first version that string stats are saved as UTF8
      // bytes, hence only process string stats for version >= DWRF_5_0
      if (statsContext.writerVersion >= WriterVersion::DWRF_5_0 ||
          statsContext.writerName == kPrestoWriter ||
          statsContext.writerName == kDwioWriter) {
        const auto& strStats = stats.stringStatistics();
        return std::make_unique<StringColumnStatistics>(
            colStats,
            strStats.hasMinimum() ? std::optional(strStats.minimum())
                                  : std::nullopt,
            strStats.hasMaximum() ? std::optional(strStats.maximum())
                                  : std::nullopt,
            // In proto, length(sum) is defined as sint. We need to make sure
            // length is not negative
            (strStats.hasSum() && strStats.sum() >= 0)
                ? std::optional(strStats.sum())
                : std::nullopt);
      }
    } else if (stats.hasBucketStatistics()) {
      const auto& bucketStats = stats.bucketStatistics();
      // Need to make sure there is at least one bucket. True count is saved in
      // bucket 0
      if (bucketStats.countSize() > 0) {
        return std::make_unique<BooleanColumnStatistics>(
            colStats, bucketStats.count(0));
      }
    } else if (stats.hasBinaryStatistics()) {
      const auto& binStats = stats.binaryStatistics();
      // In proto, length(sum) is defined as sint. We need to make sure length
      // is not negative
      if (binStats.hasSum() && binStats.sum() >= 0) {
        return std::make_unique<BinaryColumnStatistics>(
            colStats, static_cast<uint64_t>(binStats.sum()));
      }
    }
  }

  // for all other case, return only basic stats
  return std::make_unique<ColumnStatistics>(colStats);
}
} // namespace facebook::velox::dwrf
