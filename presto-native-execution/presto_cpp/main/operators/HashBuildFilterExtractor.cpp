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
#include "presto_cpp/main/operators/HashBuildFilterExtractor.h"
#include <algorithm>
#include <fmt/format.h>
#include <glog/logging.h>
#include "presto_cpp/main/types/TupleDomainBuilder.h"
#include "velox/exec/VectorHasher.h"
#include "velox/type/Filter.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {

/// Converts a variant to the correct type for integer-like columns.
static variant toTypedVariant(int64_t value, const TypePtr& type) {
  switch (type->kind()) {
    case TypeKind::INTEGER:
      return variant(static_cast<int32_t>(value));
    case TypeKind::SMALLINT:
      return variant(static_cast<int16_t>(value));
    case TypeKind::TINYINT:
      return variant(static_cast<int8_t>(value));
    default:
      return variant(value); // BIGINT, DATE
  }
}

} // namespace

/// Extracts filter data from a common::Filter and adds to the accumulators.
void convertFilter(
    const common::Filter& filter,
    const TypePtr& type,
    std::vector<variant>& discreteValues,
    std::optional<variant>& minValue,
    std::optional<variant>& maxValue) {
  switch (filter.kind()) {
    case common::FilterKind::kBigintValuesUsingHashTable: {
      const auto& f =
          static_cast<const common::BigintValuesUsingHashTable&>(filter);
      discreteValues.reserve(discreteValues.size() + f.values().size());
      for (auto v : f.values()) {
        discreteValues.push_back(toTypedVariant(v, type));
      }
      minValue = toTypedVariant(f.min(), type);
      maxValue = toTypedVariant(f.max(), type);
      break;
    }
    case common::FilterKind::kBigintValuesUsingBitmask: {
      const auto& f =
          static_cast<const common::BigintValuesUsingBitmask&>(filter);
      auto vals = f.values();
      discreteValues.reserve(discreteValues.size() + vals.size());
      for (auto v : vals) {
        discreteValues.push_back(toTypedVariant(v, type));
      }
      if (!vals.empty()) {
        auto [minIt, maxIt] = std::minmax_element(vals.begin(), vals.end());
        minValue = toTypedVariant(*minIt, type);
        maxValue = toTypedVariant(*maxIt, type);
      }
      break;
    }
    case common::FilterKind::kBigintRange: {
      const auto& f = static_cast<const common::BigintRange&>(filter);
      minValue = toTypedVariant(f.lower(), type);
      maxValue = toTypedVariant(f.upper(), type);
      // Single-value range (lower == upper) is a discrete value.
      // This happens when createBigintValues is called with 1 element.
      if (f.lower() == f.upper()) {
        discreteValues.push_back(toTypedVariant(f.lower(), type));
      }
      break;
    }
    case common::FilterKind::kBytesValues: {
      const auto& f = static_cast<const common::BytesValues&>(filter);
      discreteValues.reserve(discreteValues.size() + f.values().size());
      for (const auto& v : f.values()) {
        discreteValues.push_back(variant(v));
      }
      break;
    }
    case common::FilterKind::kBytesRange: {
      const auto& f = static_cast<const common::BytesRange&>(filter);
      if (!f.isLowerUnbounded()) {
        minValue = variant(f.lower());
      }
      if (!f.isUpperUnbounded()) {
        maxValue = variant(f.upper());
      }
      break;
    }
    default:
      VLOG(1) << "Unsupported filter kind for dynamic filter extraction: "
              << static_cast<int>(filter.kind());
      break;
  }
}

namespace {

/// Finds the VectorHasher in a hash table that processes the given column.
const VectorHasher* findHasher(
    const BaseHashTable& table,
    column_index_t columnIndex) {
  for (const auto& h : table.hashers()) {
    if (h->channel() == columnIndex) {
      return h.get();
    }
  }
  return nullptr;
}

} // namespace

void extractAndDeliverFilters(
    const std::string& taskId,
    const std::vector<DynamicFilterChannel>& channels,
    const BaseHashTable& mainTable,
    const std::vector<std::unique_ptr<BaseHashTable>>& otherTables,
    memory::MemoryPool* pool,
    DppErrorCallback errorCallback) {

  std::map<std::string, protocol::TupleDomain<std::string>> filters;
  std::unordered_set<std::string> filterIds;

  for (const auto& channel : channels) {
    filterIds.insert(channel.filterId);
  }

  // Maximum estimated byte size for the discrete-values TupleDomain JSON
  // across ALL channels in this response before collapsing to min/max
  // ranges. The Java HTTP client's default maxContentLength is 16 MB;
  // stay well under that. Each discrete value produces ~120 bytes of JSON
  // (two Markers with base64 blocks). The budget is cumulative across all
  // channels to prevent multi-channel responses from exceeding the limit.
  static constexpr uint64_t kMaxDiscreteFilterJsonBytes = 10 << 20; // 10 MB
  static constexpr uint64_t kJsonBytesPerDiscreteValue = 120;
  // A range filter (single min/max pair) contributes ~240 bytes.
  static constexpr uint64_t kJsonBytesPerRange = 2 * kJsonBytesPerDiscreteValue;
  uint64_t cumulativeEstimatedBytes = 0;

  for (const auto& channel : channels) {
    std::vector<variant> discreteValues;
    std::optional<variant> minValue;
    std::optional<variant> maxValue;
    bool allDiscrete = true;
    uint64_t estimatedDiscreteBytes = 0;
    // Track whether any hasher returned a real filter (non-null,
    // non-AlwaysFalse). If all hashers returned nullptr, the type is
    // unsupported for filter extraction (e.g., distinctOverflow) and we
    // must NOT produce none() — that would incorrectly prune all data.
    bool hasFilterableHasher = false;

    auto collectFromTable = [&](const BaseHashTable& table) {
      const auto* hasher = findHasher(table, channel.columnIndex);
      if (!hasher) {
        return;
      }
      auto filter = hasher->getFilter(false);
      if (!filter) {
        if (hasher->distinctOverflow()) {
          // VectorHasher overflowed distinct tracking. Use O(1) range
          // from the hasher (min/max) instead of scanning hash table
          // rows. Row scanning is too expensive to run serially on the
          // last driver's thread for large build sides.
          if (hasher->hasRange() && !hasher->rangeOverflow()) {
            // Integer types: int64 range is valid.
            hasFilterableHasher = true;
            allDiscrete = false;
            VLOG(1) << "DPP range fallback: type="
                    << channel.type->toString()
                    << " min=" << hasher->min()
                    << " max=" << hasher->max();
            auto lo = toTypedVariant(hasher->min(), channel.type);
            auto hi = toTypedVariant(hasher->max(), channel.type);
            VELOX_CHECK(
                !lo.isNull() && !hi.isNull(),
                "toTypedVariant produced null from hasher min={} max={} type={}",
                hasher->min(),
                hasher->max(),
                channel.type->toString());
            minValue = minValue.has_value()
                ? std::min(minValue.value(), lo)
                : lo;
            maxValue = maxValue.has_value()
                ? (maxValue.value() < hi ? hi : maxValue.value())
                : hi;
          } else if (hasher->hasStringRange()) {
            // VARCHAR/VARBINARY: lexicographic string range is valid.
            hasFilterableHasher = true;
            allDiscrete = false;
            auto lo = variant(hasher->minString());
            auto hi = variant(hasher->maxString());
            VELOX_CHECK(
                !lo.isNull() && !hi.isNull(),
                "String range produced null variants");
            minValue = minValue.has_value()
                ? std::min(minValue.value(), lo)
                : lo;
            maxValue = maxValue.has_value()
                ? (maxValue.value() < hi ? hi : maxValue.value())
                : hi;
          }
          // else: unsupported type — skip this driver.
        }
        return;
      }
      // AlwaysFalse means empty input — note that a filterable hasher
      // exists but don't contribute values.
      if (filter->kind() == common::FilterKind::kAlwaysFalse) {
        hasFilterableHasher = true;
        return;
      }
      hasFilterableHasher = true;
      if (!allDiscrete) {
        // Already collapsed to range — just extract min/max, skip
        // discrete values. convertFilter updates minValue/maxValue
        // from the filter's bounds regardless of discrete extraction.
        std::vector<variant> unused;
        convertFilter(*filter, channel.type, unused, minValue, maxValue);
        return;
      }
      size_t prevSize = discreteValues.size();
      convertFilter(*filter, channel.type, discreteValues, minValue, maxValue);
      if (discreteValues.size() == prevSize) {
        // This driver had data but produced no discrete values (range overflow).
        allDiscrete = false;
        return;
      }
      // Check estimated cumulative response size. Each discrete value
      // produces ~120 bytes of JSON (two Markers with base64 blocks, type,
      // bound). Compare against the remaining budget (total cap minus bytes
      // already committed by prior channels) to prevent multi-channel
      // responses from exceeding the HTTP maxContentLength limit.
      estimatedDiscreteBytes =
          discreteValues.size() * kJsonBytesPerDiscreteValue;
      if (cumulativeEstimatedBytes + estimatedDiscreteBytes >
          kMaxDiscreteFilterJsonBytes) {
        allDiscrete = false;
        discreteValues.clear();
        discreteValues.shrink_to_fit();
      }
    };

    collectFromTable(mainTable);
    for (const auto& other : otherTables) {
      collectFromTable(*other);
    }

    // If any non-empty driver fell back to range, discard discrete values.
    if (!allDiscrete) {
      discreteValues.clear();
    }

    if (discreteValues.empty() && !minValue.has_value() && !maxValue.has_value()) {
      if (hasFilterableHasher) {
        // All filterable hashers returned AlwaysFalse: build side is
        // truly empty. Produce none() so the probe side is pruned.
        filters[channel.filterId] = buildNoneTupleDomain();
      }
      // else: no hasher supports this type (all returned nullptr).
      // Don't add a filter — the coordinator treats the absence of a
      // domain for a flushed filter ID as "all" (no constraint).
      continue;
    }

    // Update cumulative byte estimate for this channel.
    if (!discreteValues.empty()) {
      cumulativeEstimatedBytes +=
          discreteValues.size() * kJsonBytesPerDiscreteValue;
    } else {
      // Range-only: one range with two markers.
      cumulativeEstimatedBytes += kJsonBytesPerRange;
    }

    try {
      filters[channel.filterId] = buildTupleDomain(
          channel.filterId,
          channel.type,
          discreteValues,
          minValue,
          maxValue,
          false, // nullAllowed
          pool);
    } catch (const std::exception& e) {
      // Skip this filter channel rather than losing all filters for
      // the entire join node. The coordinator treats a missing filter as
      // "no constraint" (all partitions pass).
      auto errorMsg = fmt::format(
          "buildTupleDomain failed for filter={} type={} "
          "discreteValues={} hasMin={} hasMax={} columnIndex={}: {}",
          channel.filterId,
          channel.type->toString(),
          discreteValues.size(),
          minValue.has_value(),
          maxValue.has_value(),
          channel.columnIndex,
          e.what());
      LOG(ERROR) << "DPP: " << errorMsg;
      if (errorCallback) {
        errorCallback(errorMsg);
      }
    }
  }

  DynamicFilterCallbackRegistry::instance().fire(
      taskId, std::move(filters), std::move(filterIds));
}

} // namespace facebook::presto::operators
