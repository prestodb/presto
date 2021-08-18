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

std::unique_ptr<ColumnStatistics> ColumnStatistics::fromProto(
    const proto::ColumnStatistics& s,
    const StatsContext& statsContext) {
  // detailed stats is only defined when has non-null value
  if (!s.has_numberofvalues() || s.numberofvalues() > 0) {
    if (s.has_intstatistics()) {
      return std::make_unique<IntegerColumnStatistics>(s);
    } else if (s.has_doublestatistics()) {
      const auto& dStats = s.doublestatistics();
      // Comparing against NaN doesn't make sense, and to prevent downstream
      // from incorrectly using it, need to make sure min/max/sum doens't have
      // NaN.
      auto hasNan = (dStats.has_minimum() && std::isnan(dStats.minimum())) ||
          (dStats.has_maximum() && std::isnan(dStats.maximum())) ||
          (dStats.has_sum() && std::isnan(dStats.sum()));
      if (!hasNan) {
        return std::make_unique<DoubleColumnStatistics>(s);
      }
    } else if (s.has_stringstatistics()) {
      // DWRF_5_0 is the first version that string stats are saved as UTF8
      // bytes, hence only process string stats for version >= DWRF_5_0
      if (statsContext.writerVersion >= WriterVersion::DWRF_5_0 ||
          statsContext.writerName == kPrestoWriter ||
          statsContext.writerName == kDwioWriter) {
        return std::make_unique<StringColumnStatistics>(s);
      }
    } else if (s.has_bucketstatistics()) {
      return std::make_unique<BooleanColumnStatistics>(s);
    } else if (s.has_binarystatistics()) {
      return std::make_unique<BinaryColumnStatistics>(s);
    }
  }

  // for all other case, return only basic stats
  return std::make_unique<ColumnStatistics>(s);
}
} // namespace facebook::velox::dwrf
