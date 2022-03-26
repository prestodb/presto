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

#include "velox/dwio/dwrf/writer/FlushPolicy.h"

namespace facebook::velox::dwrf {

DefaultFlushPolicy::DefaultFlushPolicy(
    uint64_t stripeSizeThreshold,
    uint64_t dictionarySizeThreshold)
    : stripeSizeThreshold_{stripeSizeThreshold},
      dictionarySizeThreshold_{dictionarySizeThreshold} {}

bool DefaultFlushPolicy::operator()(
    bool overMemoryBudget,
    const WriterContext& context) const {
  const int64_t dictionaryMemUsage =
      context.getMemoryUsage(MemoryUsageCategory::DICTIONARY).getCurrentBytes();
  const int64_t generalMemUsage =
      context.getMemoryUsage(MemoryUsageCategory::GENERAL).getCurrentBytes();
  const int64_t outputStreamSize = context.getEstimatedOutputStreamSize();
  const bool decision =
      overMemoryBudget ||
      operator()(
          std::max(
              context.getEstimatedStripeSize(context.stripeRawSize),
              context.stripeIndex == 0 ? outputStreamSize : 0),
          dictionaryMemUsage);
  if (decision) {
    VLOG(1) << fmt::format(
        "overMemoryBudget: {}, dictionaryMemUsage: {}, outputStreamSize: {}, generalMemUsage: {}, estimatedStripeSize: {}",
        overMemoryBudget,
        dictionaryMemUsage,
        outputStreamSize,
        generalMemUsage,
        context.getEstimatedStripeSize(context.stripeRawSize));
  }
  return decision;
}

bool DefaultFlushPolicy::operator()(
    uint64_t estimatedStripeSize,
    uint64_t dictionarySize) const {
  return dictionarySize >= dictionarySizeThreshold_ ||
      estimatedStripeSize >= stripeSizeThreshold_;
}

RowsPerStripeFlushPolicy::RowsPerStripeFlushPolicy(
    std::vector<uint64_t> rowsPerStripe)
    : rowsPerStripe_{std::move(rowsPerStripe)} {
  // Note: Vector will be empty for empty files.
  for (auto i = 0; i < rowsPerStripe_.size(); i++) {
    DWIO_ENSURE_GT(
        rowsPerStripe_.at(i),
        0,
        "More than 0 rows expected in the stripe at ",
        i,
        folly::join(",", rowsPerStripe_));
  }
}

// We can throw if writer reported the incoming write to be over memory budget.
bool RowsPerStripeFlushPolicy::operator()(
    bool /* unused */,
    const WriterContext& context) const {
  const auto stripeIndex = context.stripeIndex;
  DWIO_ENSURE_LT(
      stripeIndex,
      rowsPerStripe_.size(),
      "Stripe index is bigger than expected");

  DWIO_ENSURE_LE(
      context.stripeRowCount,
      rowsPerStripe_.at(stripeIndex),
      "More rows in Stripe than expected ",
      stripeIndex)

  if ((stripeIndex + 1) == rowsPerStripe_.size()) {
    // Last Stripe is always flushed at the time of close.
    return false;
  }

  return context.stripeRowCount == rowsPerStripe_.at(stripeIndex);
}

} // namespace facebook::velox::dwrf
