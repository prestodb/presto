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
#include <glog/logging.h>
#include <folly/container/F14Set.h>
#include "presto_cpp/main/types/TupleDomainBuilder.h"
#include "velox/exec/RowContainer.h"
#include "velox/exec/VectorHasher.h"
#include "velox/type/Filter.h"
#include "velox/vector/FlatVector.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;

namespace facebook::presto::operators {

namespace {

/// Converts a variant to the correct type for integer-like columns.
variant toTypedVariant(int64_t value, const TypePtr& type) {
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

/// Finds the VectorHasher in a hash table that processes the given column.
/// Sets keyColumnIndex to the hasher's position, which is also the column
/// index in the RowContainer.
const VectorHasher* findHasher(
    const BaseHashTable& table,
    column_index_t columnIndex,
    int32_t& keyColumnIndex) {
  for (int32_t i = 0; i < static_cast<int32_t>(table.hashers().size()); ++i) {
    if (table.hashers()[i]->channel() == columnIndex) {
      keyColumnIndex = i;
      return table.hashers()[i].get();
    }
  }
  keyColumnIndex = -1;
  return nullptr;
}

/// Maximum total byte size for collected filter values before falling back
/// to a min/max range. Matches the old Java DynamicFilterSourceOperator's
/// DEFAULT_MAX_SIZE_BYTES and VectorHasher's kMaxDistinctStringsBytes.
static constexpr uint64_t kMaxFilterSizeBytes = 1 << 20; // 1MB

static constexpr int32_t kBatchSize = 1024;

/// Scans hash table rows to collect values for a column whose VectorHasher
/// overflowed. Accumulates unique values into a shared set (for cross-table
/// dedup). If total bytes exceed kMaxFilterSizeBytes, stops collecting
/// discrete values and only tracks min/max for range fallback.
void collectStringValuesFromRows(
    const BaseHashTable& table,
    int32_t keyColumnIndex,
    folly::F14FastSet<std::string>& uniqueStrings,
    uint64_t& totalBytes,
    bool& exceeded,
    std::optional<variant>& minValue,
    std::optional<variant>& maxValue,
    memory::MemoryPool* pool) {
  auto* rows = table.rows();
  auto resultVector = BaseVector::create(VARCHAR(), kBatchSize, pool);
  auto* flatResult = resultVector->asFlatVector<StringView>();

  RowContainerIterator iter;
  std::vector<char*> rowPtrs(kBatchSize);
  for (;;) {
    auto numRows = rows->listRows(&iter, kBatchSize, rowPtrs.data());
    if (numRows == 0) {
      break;
    }
    rows->extractColumn(
        rowPtrs.data(), numRows, keyColumnIndex, resultVector);
    for (int32_t i = 0; i < numRows; ++i) {
      if (resultVector->isNullAt(i)) {
        continue;
      }
      auto sv = flatResult->valueAt(i);
      std::string s(sv.data(), sv.size());

      // Track min/max unconditionally.
      auto v = variant(s);
      if (!minValue.has_value() || v < minValue.value()) {
        minValue = v;
      }
      if (!maxValue.has_value() || maxValue.value() < v) {
        maxValue = v;
      }

      if (!exceeded) {
        auto [it, inserted] = uniqueStrings.insert(std::move(s));
        if (inserted) {
          totalBytes += it->size();
          if (totalBytes > kMaxFilterSizeBytes) {
            exceeded = true;
            uniqueStrings.clear();
          }
        }
      }
    }
  }
}

/// Scans hash table rows to collect integer values for a column whose
/// VectorHasher overflowed.
/// Reads an integer value from a FlatVector at position i, widening to
/// int64_t regardless of the underlying integer type.
int64_t readIntValue(const BaseVector& vector, int32_t i) {
  switch (vector.typeKind()) {
    case TypeKind::BIGINT:
      return vector.asFlatVector<int64_t>()->valueAt(i);
    case TypeKind::INTEGER:
      return vector.asFlatVector<int32_t>()->valueAt(i);
    case TypeKind::SMALLINT:
      return vector.asFlatVector<int16_t>()->valueAt(i);
    case TypeKind::TINYINT:
      return vector.asFlatVector<int8_t>()->valueAt(i);
    default:
      VELOX_UNREACHABLE(
          "Unsupported type for integer filter extraction: {}",
          vector.type()->toString());
  }
}

void collectIntegerValuesFromRows(
    const BaseHashTable& table,
    int32_t keyColumnIndex,
    const TypePtr& type,
    folly::F14FastSet<int64_t>& uniqueInts,
    uint64_t& totalBytes,
    bool& exceeded,
    std::optional<variant>& minValue,
    std::optional<variant>& maxValue,
    memory::MemoryPool* pool) {
  auto* rows = table.rows();
  auto resultVector = BaseVector::create(type, kBatchSize, pool);

  RowContainerIterator iter;
  std::vector<char*> rowPtrs(kBatchSize);
  for (;;) {
    auto numRows = rows->listRows(&iter, kBatchSize, rowPtrs.data());
    if (numRows == 0) {
      break;
    }
    rows->extractColumn(
        rowPtrs.data(), numRows, keyColumnIndex, resultVector);
    for (int32_t i = 0; i < numRows; ++i) {
      if (resultVector->isNullAt(i)) {
        continue;
      }
      auto val = readIntValue(*resultVector, i);
      auto v = toTypedVariant(val, type);

      // Track min/max unconditionally.
      if (!minValue.has_value() || v < minValue.value()) {
        minValue = v;
      }
      if (!maxValue.has_value() || maxValue.value() < v) {
        maxValue = v;
      }

      if (!exceeded) {
        auto [it, inserted] = uniqueInts.insert(val);
        if (inserted) {
          totalBytes += sizeof(int64_t);
          if (totalBytes > kMaxFilterSizeBytes) {
            exceeded = true;
            uniqueInts.clear();
          }
        }
      }
    }
  }
}

} // namespace

void extractAndDeliverFilters(
    const std::string& taskId,
    const std::vector<DynamicFilterChannel>& channels,
    const BaseHashTable& mainTable,
    const std::vector<std::unique_ptr<BaseHashTable>>& otherTables,
    memory::MemoryPool* pool) {

  std::map<std::string, protocol::TupleDomain<std::string>> filters;
  std::unordered_set<std::string> filterIds;

  for (const auto& channel : channels) {
    filterIds.insert(channel.filterId);
  }

  for (const auto& channel : channels) {
    std::vector<variant> discreteValues;
    std::optional<variant> minValue;
    std::optional<variant> maxValue;
    bool allDiscrete = true;
    // Track whether any hasher returned a real filter (non-null,
    // non-AlwaysFalse). If all hashers returned nullptr, the type is
    // unsupported for filter extraction (e.g., distinctOverflow) and we
    // must NOT produce none() — that would incorrectly prune all data.
    bool hasFilterableHasher = false;

    // State for row-scanning fallback when VectorHasher overflows. Shared
    // across tables for cross-table deduplication.
    bool needsRowScan = false;
    bool exceeded = false;
    uint64_t totalBytes = 0;
    folly::F14FastSet<std::string> uniqueStrings;
    folly::F14FastSet<int64_t> uniqueInts;

    auto collectFromTable = [&](const BaseHashTable& table) {
      int32_t keyColumnIndex = -1;
      const auto* hasher =
          findHasher(table, channel.columnIndex, keyColumnIndex);
      if (!hasher) {
        return;
      }
      auto filter = hasher->getFilter(false);
      if (!filter) {
        if (hasher->distinctOverflow()) {
          // VectorHasher overflowed — scan hash table rows directly to
          // collect values up to a byte budget.
          hasFilterableHasher = true;
          needsRowScan = true;
          bool isStringType = channel.type->kind() == TypeKind::VARCHAR ||
              channel.type->kind() == TypeKind::VARBINARY;
          if (isStringType) {
            collectStringValuesFromRows(
                table,
                keyColumnIndex,
                uniqueStrings,
                totalBytes,
                exceeded,
                minValue,
                maxValue,
                pool);
          } else {
            collectIntegerValuesFromRows(
                table,
                keyColumnIndex,
                channel.type,
                uniqueInts,
                totalBytes,
                exceeded,
                minValue,
                maxValue,
                pool);
          }
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
      size_t prevSize = discreteValues.size();
      convertFilter(*filter, channel.type, discreteValues, minValue, maxValue);
      if (discreteValues.size() == prevSize) {
        // This driver had data but produced no discrete values (range overflow).
        allDiscrete = false;
      }
    };

    collectFromTable(mainTable);
    for (const auto& other : otherTables) {
      collectFromTable(*other);
    }

    // Convert row-scan results into discreteValues or range.
    if (needsRowScan) {
      if (exceeded) {
        allDiscrete = false;
      } else {
        bool isStringType = channel.type->kind() == TypeKind::VARCHAR ||
            channel.type->kind() == TypeKind::VARBINARY;
        if (isStringType) {
          discreteValues.reserve(discreteValues.size() + uniqueStrings.size());
          for (const auto& s : uniqueStrings) {
            discreteValues.push_back(variant(s));
          }
        } else {
          discreteValues.reserve(discreteValues.size() + uniqueInts.size());
          for (auto val : uniqueInts) {
            discreteValues.push_back(toTypedVariant(val, channel.type));
          }
        }
      }
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

    filters[channel.filterId] = buildTupleDomain(
        channel.filterId,
        channel.type,
        discreteValues,
        minValue,
        maxValue,
        false, // nullAllowed
        pool);
  }

  DynamicFilterCallbackRegistry::instance().fire(
      taskId, std::move(filters), std::move(filterIds));
}

} // namespace facebook::presto::operators
