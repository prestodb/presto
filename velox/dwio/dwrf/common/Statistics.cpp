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
    const proto::ColumnStatistics& s,
    const StatsContext& statsContext) {
  ColumnStatistics colStats(
      s.has_numberofvalues() ? std::optional(s.numberofvalues()) : std::nullopt,
      s.has_hasnull() ? std::optional(s.hasnull()) : std::nullopt,
      s.has_rawsize() ? std::optional(s.rawsize()) : std::nullopt,
      s.has_size() ? std::optional(s.size()) : std::nullopt);

  // detailed stats is only defined when has non-null value
  if (!s.has_numberofvalues() || s.numberofvalues() > 0) {
    if (s.has_intstatistics()) {
      const auto& intStats = s.intstatistics();
      return std::make_unique<IntegerColumnStatistics>(
          colStats,
          intStats.has_minimum() ? std::optional(intStats.minimum())
                                 : std::nullopt,
          intStats.has_maximum() ? std::optional(intStats.maximum())
                                 : std::nullopt,
          intStats.has_sum() ? std::optional(intStats.sum()) : std::nullopt);
    } else if (s.has_doublestatistics()) {
      const auto& dStats = s.doublestatistics();
      // Comparing against NaN doesn't make sense, and to prevent downstream
      // from incorrectly using it, need to make sure min/max/sum doens't have
      // NaN.
      auto hasNan = (dStats.has_minimum() && std::isnan(dStats.minimum())) ||
          (dStats.has_maximum() && std::isnan(dStats.maximum())) ||
          (dStats.has_sum() && std::isnan(dStats.sum()));
      if (!hasNan) {
        return std::make_unique<DoubleColumnStatistics>(
            colStats,
            dStats.has_minimum() ? std::optional(dStats.minimum())
                                 : std::nullopt,
            dStats.has_maximum() ? std::optional(dStats.maximum())
                                 : std::nullopt,
            dStats.has_sum() ? std::optional(dStats.sum()) : std::nullopt);
      }
    } else if (s.has_stringstatistics()) {
      // DWRF_5_0 is the first version that string stats are saved as UTF8
      // bytes, hence only process string stats for version >= DWRF_5_0
      if (statsContext.writerVersion >= WriterVersion::DWRF_5_0 ||
          statsContext.writerName == kPrestoWriter ||
          statsContext.writerName == kDwioWriter) {
        const auto& strStats = s.stringstatistics();
        return std::make_unique<StringColumnStatistics>(
            colStats,
            strStats.has_minimum() ? std::optional(strStats.minimum())
                                   : std::nullopt,
            strStats.has_maximum() ? std::optional(strStats.maximum())
                                   : std::nullopt,
            // In proto, length(sum) is defined as sint. We need to make sure
            // length is not negative
            (strStats.has_sum() && strStats.sum() >= 0)
                ? std::optional(strStats.sum())
                : std::nullopt);
      }
    } else if (s.has_bucketstatistics()) {
      const auto& bucketStats = s.bucketstatistics();
      // Need to make sure there is at least one bucket. True count is saved in
      // bucket 0
      if (bucketStats.count_size() > 0) {
        return std::make_unique<BooleanColumnStatistics>(
            colStats, bucketStats.count(0));
      }
    } else if (s.has_binarystatistics()) {
      const auto& binStats = s.binarystatistics();
      // In proto, length(sum) is defined as sint. We need to make sure length
      // is not negative
      if (binStats.has_sum() && binStats.sum() >= 0) {
        return std::make_unique<BinaryColumnStatistics>(
            colStats, static_cast<uint64_t>(binStats.sum()));
      }
    }
  }

  // for all other case, return only basic stats
  return std::make_unique<ColumnStatistics>(colStats);
}
} // namespace facebook::velox::dwrf
