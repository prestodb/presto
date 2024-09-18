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

#include "velox/dwio/common/tests/utils/FilterGenerator.h"

#include <algorithm>
#include <memory>
#include <typeinfo>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

using namespace facebook::velox;
using namespace facebook::velox::common;

// Encodes a batch number and an index into the batch into an int32_t
uint64_t batchPosition(uint32_t batchNumber, vector_size_t batchRow) {
  return (uint64_t)batchNumber << 32 | (uint64_t)batchRow;
}

uint32_t batchNumber(uint64_t position) {
  return position >> 32;
}

vector_size_t batchRow(uint64_t position) {
  return position & 0xffffffff;
}

VectorPtr getChildBySubfield(
    const RowVector* rowVector,
    const Subfield& subfield,
    const RowTypePtr& rootType) {
  const Type* type = rootType ? rootType.get() : rowVector->type().get();
  auto& path = subfield.path();
  VELOX_CHECK(!path.empty());
  auto* rowType = &type->asRow();
  auto* field = dynamic_cast<const Subfield::NestedField*>(path[0].get());
  VELOX_CHECK(field);
  auto fieldIndex = rowType->getChildIdx(field->name());
  type = rowType->childAt(fieldIndex).get();
  auto vector = rowVector->childAt(fieldIndex);
  for (int i = 1; i < path.size(); ++i) {
    switch (type->kind()) {
      case TypeKind::ROW:
        rowType = &type->asRow();
        field = dynamic_cast<const Subfield::NestedField*>(path[i].get());
        VELOX_CHECK(field);
        fieldIndex = rowType->getChildIdx(field->name());
        type = rowType->childAt(fieldIndex).get();
        vector = vector->asUnchecked<RowVector>()->childAt(fieldIndex);
        break;
      case TypeKind::ARRAY:
        VELOX_CHECK(
            dynamic_cast<const Subfield::AllSubscripts*>(path[i].get()));
        type = type->childAt(0).get();
        vector = vector->asUnchecked<ArrayVector>()->elements();
        break;
      case TypeKind::MAP:
        VELOX_CHECK(
            dynamic_cast<const Subfield::AllSubscripts*>(path[i].get()));
        type = type->childAt(1).get();
        vector = vector->asUnchecked<MapVector>()->mapValues();
        break;
      default:
        VELOX_FAIL();
    }
  }
  return vector;
}

uint32_t AbstractColumnStats::counter_ = 0;

template <>
std::unique_ptr<Filter> ColumnStats<bool>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  bool value = valueAtPct(filterSpec.startPct + filterSpec.selectPct);
  return std::make_unique<velox::common::BoolValue>(
      value, filterSpec.selectPct > 50);
}

template <>
std::unique_ptr<Filter> ColumnStats<float>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  float lower = valueAtPct(filterSpec.startPct);
  float upper = valueAtPct(filterSpec.startPct + filterSpec.selectPct);
  bool lowerUnbounded = std::isnan(lower);
  bool upperUnbounded = std::isnan(upper);
  if (lowerUnbounded && upperUnbounded) {
    return std::make_unique<velox::common::IsNotNull>();
  }

  return std::make_unique<velox::common::FloatRange>(
      lower,
      lowerUnbounded,
      false,
      upper,
      upperUnbounded,
      false,
      filterSpec.selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<double>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    if (filterSpec.allowNulls_) {
      return std::make_unique<velox::common::IsNull>();
    } else {
      return std::make_unique<velox::common::DoubleRange>(
          0, false, false, 0, false, false, false);
    }
  }

  double lower = valueAtPct(filterSpec.startPct);
  double upper = valueAtPct(filterSpec.startPct + filterSpec.selectPct);
  bool lowerUnbounded = std::isnan(lower);
  bool upperUnbounded = std::isnan(upper);
  if (lowerUnbounded && upperUnbounded) {
    return std::make_unique<velox::common::IsNotNull>();
  }
  if (!filterSpec.allowNulls_) {
    return std::make_unique<velox::common::DoubleRange>(
        lower,
        lowerUnbounded,
        filterSpec.selectPct == 0,
        upper,
        upperUnbounded,
        false,
        false);
  }
  return std::make_unique<velox::common::DoubleRange>(
      lower,
      lowerUnbounded,
      false,
      upper,
      upperUnbounded,
      false,
      filterSpec.selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<int128_t>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  int128_t lower = valueAtPct(filterSpec.startPct);
  int128_t upper = valueAtPct(filterSpec.startPct + filterSpec.selectPct);

  return std::make_unique<velox::common::HugeintRange>(
      lower, upper, filterSpec.allowNulls_);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }

  int32_t lowerIndex;
  int32_t upperIndex;
  StringView lower = valueAtPct(filterSpec.startPct, &lowerIndex);
  StringView upper =
      valueAtPct(filterSpec.startPct + filterSpec.selectPct, &upperIndex);

  // When the filter rate is 0%, we should not allow the value at the boundary.
  if (filterSpec.selectPct == 0) {
    return std::make_unique<velox::common::BytesRange>(
        std::string(lower),
        false,
        true,
        std::string(upper),
        false,
        true,
        filterSpec.allowNulls_);
  }
  return std::make_unique<velox::common::BytesRange>(
      std::string(lower),
      false,
      false,
      std::string(upper),
      false,
      false,
      filterSpec.allowNulls_);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRandomFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }

  // used to determine if we can test a values filter reasonably
  int32_t lowerIndex;
  int32_t upperIndex;
  StringView lower = valueAtPct(filterSpec.startPct, &lowerIndex);
  StringView upper =
      valueAtPct(filterSpec.startPct + filterSpec.selectPct, &upperIndex);
  if (upperIndex - lowerIndex < 1000 && ++counter_ % 10 <= 3) {
    std::vector<std::string> inRange;
    inRange.reserve(upperIndex - lowerIndex);
    for (StringView s : values_) {
      // do comparison
      if (lower <= s && s <= upper) {
        inRange.push_back(s.getString());
      }
    }
    if (counter_ % 2 == 0 && filterSpec.selectPct != 100.0) {
      return std::make_unique<velox::common::NegatedBytesValues>(
          inRange, filterSpec.selectPct < 75);
    }
    return std::make_unique<velox::common::BytesValues>(
        inRange, filterSpec.selectPct > 25);
  }

  // sometimes create a negated filter instead
  if (counter_ % 4 == 1 && filterSpec.selectPct < 100.0) {
    return std::make_unique<velox::common::NegatedBytesRange>(
        std::string(lower),
        false,
        false,
        std::string(upper),
        false,
        false,
        filterSpec.selectPct < 75);
  }

  return std::make_unique<velox::common::BytesRange>(
      std::string(lower),
      false,
      false,
      std::string(upper),
      false,
      false,
      filterSpec.selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<Timestamp>::makeRangeFilter(
    const FilterSpec& filterSpec) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  int32_t lowerIndex;
  int32_t upperIndex;
  Timestamp lower = valueAtPct(filterSpec.startPct, &lowerIndex);
  Timestamp upper =
      valueAtPct(filterSpec.startPct + filterSpec.selectPct, &upperIndex);

  return std::make_unique<velox::common::TimestampRange>(
      lower, upper, filterSpec.selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRowGroupSkipRangeFilter(
    const std::vector<RowVectorPtr>& /*batches*/,
    const Subfield& /*subfield*/) {
  static std::string max = kMaxString;
  return std::make_unique<velox::common::BytesRange>(
      max, false, false, max, false, false, false);
}

template <>
std::unique_ptr<Filter> ColumnStats<Timestamp>::makeRowGroupSkipRangeFilter(
    const std::vector<RowVectorPtr>& batches,
    const Subfield& subfield) {
  Timestamp max;
  bool hasMax = false;
  for (const auto& batch : batches) {
    auto values = getChildBySubfield(batch.get(), subfield, rootType_)
                      ->as<SimpleVector<Timestamp>>();
    DWIO_ENSURE_NOT_NULL(
        values,
        "Failed to convert to SimpleVector<Timestamp> for batch of kind ",
        batch->type()->kindName());
    for (auto i = 0; i < values->size(); ++i) {
      if (values->isNullAt(i)) {
        continue;
      }
      if (hasMax && max < values->valueAt(i)) {
        max = values->valueAt(i);
      } else if (!hasMax) {
        max = values->valueAt(i);
        hasMax = true;
      }
    }
  }
  return std::make_unique<velox::common::TimestampRange>(max, max, false);
}

std::string FilterGenerator::specsToString(
    const std::vector<FilterSpec>& specs) {
  std::stringstream out;
  bool first = true;
  for (auto& spec : specs) {
    if (!first) {
      out << ", ";
    }
    first = false;
    out << spec.field;
    if (spec.filterKind == FilterKind::kIsNull) {
      out << " is null";
    } else if (spec.filterKind == FilterKind::kIsNotNull) {
      out << " is not null";
    } else {
      out << ":" << spec.selectPct << "," << spec.startPct << " ";
    }
  }
  return out.str();
}

SubfieldFilters FilterGenerator::cloneSubfieldFilters(
    const SubfieldFilters& src) {
  SubfieldFilters copy;
  for (const auto& sf : src) {
    copy[Subfield(sf.first.toString())] = sf.second->clone();
  }

  return copy;
}

void FilterGenerator::collectFilterableSubFields(
    const RowType* rowType,
    std::vector<std::string>& subFields) {
  for (int i = 0; i < rowType->size(); ++i) {
    auto kind = rowType->childAt(i)->kind();
    switch (kind) {
      // ignore these types for filtering
      case TypeKind::ARRAY:
      case TypeKind::MAP:
      case TypeKind::UNKNOWN:
      case TypeKind::FUNCTION:
      case TypeKind::OPAQUE:
      case TypeKind::VARBINARY:
      case TypeKind::TIMESTAMP:
      case TypeKind::ROW:
      case TypeKind::INVALID:
        continue;

      default:
        subFields.push_back(rowType->nameOf(i));
    }
  }
}

std::vector<std::string> FilterGenerator::makeFilterables(
    uint32_t count,
    float pct) {
  std::vector<std::string> filterables;
  filterables.reserve(rowType_->size());
  collectFilterableSubFields(rowType_.get(), filterables);
  if (filterables.empty()) {
    // It could be empty if none of the columns is filterable, for example,
    // there are only (or mix of) map, array columns.
    return filterables;
  }
  uint32_t countTotal = filterables.size();
  uint32_t countSelect = std::min(count, countTotal);
  if (countSelect == 0) {
    countSelect = std::max(1, (int)(countTotal * pct) / 100);
  }

  // probability of selection = (number needed)/(candidates left)
  for (int i = 0, j = 0; i < countTotal && j < countSelect; ++i) {
    if (folly::Random::rand32(countTotal - i, rng_) < countSelect - j) {
      filterables[j++] = filterables[i];
    }
  }
  filterables.resize(countSelect);
  return filterables;
}

std::vector<FilterSpec> FilterGenerator::makeRandomSpecs(
    const std::vector<std::string>& filterable,
    int32_t countX100) {
  std::vector<FilterSpec> specs;
  auto deck = filterable;
  for (size_t i = 0; i < filterable.size(); ++i) {
    if (countX100 > 0) {
      // We aim at picking countX100 / 100 fields
      if (folly::Random::rand32(rng_) % (100 * filterable.size()) >=
          countX100) {
        continue;
      }
    }

    auto idx = folly::Random::rand32(rng_) % deck.size();
    auto name = deck[idx];
    if (specs.empty()) {
      ++filterCoverage_[name][0];
    } else {
      ++filterCoverage_[name][1];
    }
    deck.erase(deck.begin() + idx);
    specs.emplace_back();
    specs.back().field = name;
    auto category = folly::Random::rand32(rng_) % 13;
    if (category == 0) {
      specs.back().selectPct = 1;
    } else if (category < 4) {
      specs.back().selectPct = category * 10;
    } else if (category == 11) {
      specs.back().filterKind = FilterKind::kIsNull;
    } else if (category == 12) {
      specs.back().filterKind = FilterKind::kIsNotNull;
    } else {
      specs.back().selectPct = 60 + category * 4;
    }

    specs.back().startPct = specs.back().selectPct < 100
        ? folly::Random::rand32(rng_) %
            static_cast<int32_t>(100 - specs.back().selectPct)
        : 0;
  }

  return specs;
}

std::shared_ptr<ScanSpec> FilterGenerator::makeScanSpec(
    const SubfieldFilters& filters) {
  auto spec = std::make_shared<ScanSpec>("root");
  spec->addAllChildFields(*rowType_);
  addToScanSpec(filters, *spec);
  return spec;
}

void FilterGenerator::addToScanSpec(
    const SubfieldFilters& filters,
    ScanSpec& spec) {
  for (auto& pair : filters) {
    spec.getOrCreateChild(pair.first)->addFilter(*pair.second);
  }
}

SubfieldFilters FilterGenerator::makeSubfieldFilters(
    const std::vector<FilterSpec>& filterSpecs,
    const std::vector<RowVectorPtr>& batches,
    MutationSpec* mutationSpec,
    std::vector<uint64_t>& hitRows) {
  vector_size_t totalSize = 0;
  for (auto& batch : batches) {
    totalSize += batch->size();
  }
  hitRows.reserve(totalSize);
  int64_t index = 0;
  for (auto i = 0; i < batches.size(); ++i) {
    auto batch = batches[i];
    for (auto j = 0; j < batch->size(); ++j, ++index) {
      if (mutationSpec && folly::Random::randDouble01(rng_) < 0.02) {
        mutationSpec->deletedRows.push_back(index);
      } else {
        hitRows.push_back(batchPosition(i, j));
      }
    }
  }

  RowVector* first = batches[0].get();

  SubfieldFilters filters;
  for (auto& filterSpec : filterSpecs) {
    Subfield subfield(filterSpec.field);
    auto vector = getChildBySubfield(first, subfield, rowType_);
    std::unique_ptr<AbstractColumnStats> stats;
    switch (vector->typeKind()) {
      case TypeKind::BOOLEAN:
        stats = makeStats<TypeKind::BOOLEAN>(vector->type(), rowType_);
        break;
      case TypeKind::TINYINT:
        stats = makeStats<TypeKind::TINYINT>(vector->type(), rowType_);
        break;
      case TypeKind::SMALLINT:
        stats = makeStats<TypeKind::SMALLINT>(vector->type(), rowType_);
        break;
      case TypeKind::INTEGER:
        stats = makeStats<TypeKind::INTEGER>(vector->type(), rowType_);
        break;
      case TypeKind::BIGINT:
        stats = makeStats<TypeKind::BIGINT>(vector->type(), rowType_);
        break;
      case TypeKind::HUGEINT:
        stats = makeStats<TypeKind::HUGEINT>(vector->type(), rowType_);
        break;
      case TypeKind::VARCHAR:
        stats = makeStats<TypeKind::VARCHAR>(vector->type(), rowType_);
        break;
      case TypeKind::VARBINARY:
        stats = makeStats<TypeKind::VARBINARY>(vector->type(), rowType_);
        break;
      case TypeKind::REAL:
        stats = makeStats<TypeKind::REAL>(vector->type(), rowType_);
        break;
      case TypeKind::DOUBLE:
        stats = makeStats<TypeKind::DOUBLE>(vector->type(), rowType_);
        break;
      case TypeKind::TIMESTAMP:
        stats = makeStats<TypeKind::TIMESTAMP>(vector->type(), rowType_);
        break;
      case TypeKind::ROW:
        stats = makeStats<TypeKind::ROW>(vector->type(), rowType_);
        break;
      case TypeKind::ARRAY:
        stats = makeStats<TypeKind::ARRAY>(vector->type(), rowType_);
        break;
      case TypeKind::MAP:
        stats = makeStats<TypeKind::MAP>(vector->type(), rowType_);
        break;
      default:
        VELOX_CHECK(
            false,
            std::string("Type not supported: ") + vector->type()->kindName());
    }

    stats->sample(batches, subfield, hitRows);
    std::unique_ptr<Filter> filter;
    if (filterSpec.isForRowGroupSkip) {
      filter = stats->rowGroupSkipFilter(batches, subfield, hitRows);
    } else {
      filter = stats->filter(batches, filterSpec, hitRows);
    }

    if (filter) {
      filters[Subfield(filterSpec.field)] = std::move(filter);
    }
  }

  return filters;
}

namespace {

void pruneRandomSubfield(
    Subfield& subfield,
    const Type& type,
    ScanSpec& spec,
    memory::MemoryPool* pool,
    folly::Random::DefaultGenerator& rng,
    const RowTypePtr& rootType,
    std::vector<RowVectorPtr>& batches) {
  switch (type.kind()) {
    case TypeKind::ROW: {
      // Prune one of the child.
      auto& rowType = type.asRow();
      VELOX_CHECK_GT(rowType.size(), 0);
      int pruned = -1;
      if (rowType.size() > 1) {
        pruned = folly::Random::rand32(rowType.size(), rng);
      }
      for (int i = 0; i < rowType.size(); ++i) {
        auto& childType = rowType.childAt(i);
        auto& childName = rowType.nameOf(i);
        auto* childSpec = spec.childByName(childName);
        if (i == pruned) {
          childSpec->setConstantValue(
              BaseVector::createNullConstant(childType, 1, pool));
          for (auto& batch : batches) {
            auto* data = getChildBySubfield(batch.get(), subfield, rootType)
                             ->asUnchecked<RowVector>();
            data->childAt(i) =
                BaseVector::createNullConstant(childType, data->size(), pool);
          }
        } else {
          subfield.path().push_back(
              std::make_unique<Subfield::NestedField>(childName));
          pruneRandomSubfield(
              subfield, *childType, *childSpec, pool, rng, rootType, batches);
          subfield.path().pop_back();
        }
      }
      break;
    }
    case TypeKind::ARRAY: {
      // Cap the max element length at the average length.
      vector_size_t totalSize = 0;
      int totalCount = 0;
      for (auto& batch : batches) {
        auto* data = getChildBySubfield(batch.get(), subfield, rootType)
                         ->asUnchecked<ArrayVector>();
        for (int i = 0; i < data->size(); ++i) {
          if (!data->isNullAt(i)) {
            totalSize += data->sizeAt(i);
            ++totalCount;
          }
        }
      }
      int maxElementsCount = std::max(1, totalSize / totalCount);
      for (auto& batch : batches) {
        auto* data = getChildBySubfield(batch.get(), subfield, rootType)
                         ->asUnchecked<ArrayVector>();
        for (int i = 0; i < data->size(); ++i) {
          if (!data->isNullAt(i)) {
            auto newSize = std::min(maxElementsCount, data->sizeAt(i));
            data->setOffsetAndSize(i, data->offsetAt(i), newSize);
          }
        }
      }
      spec.setMaxArrayElementsCount(maxElementsCount);
      auto* elementsSpec = spec.childByName(ScanSpec::kArrayElementsFieldName);
      subfield.path().push_back(std::make_unique<Subfield::AllSubscripts>());
      pruneRandomSubfield(
          subfield,
          *type.childAt(0),
          *elementsSpec,
          pool,
          rng,
          rootType,
          batches);
      subfield.path().pop_back();
      break;
    }
    case TypeKind::MAP: {
      // Sample half of the keys.
      auto& keyType = type.childAt(0);
      bool isStringKey = keyType->isVarchar() || keyType->isVarbinary();
      std::vector<std::string> stringKeys;
      std::vector<int64_t> longKeys;
      for (auto& batch : batches) {
        auto* data = getChildBySubfield(batch.get(), subfield, rootType)
                         ->asUnchecked<MapVector>();
        auto& keys = data->mapKeys();
        for (int i = 0; i < data->size(); ++i) {
          if (data->isNullAt(i)) {
            continue;
          }
          for (int j = 0; j < data->sizeAt(i); ++j) {
            int jj = data->offsetAt(i) + j;
            switch (keyType->kind()) {
              case TypeKind::TINYINT:
                longKeys.push_back(
                    keys->asUnchecked<SimpleVector<int8_t>>()->valueAt(jj));
                break;
              case TypeKind::SMALLINT:
                longKeys.push_back(
                    keys->asUnchecked<SimpleVector<int16_t>>()->valueAt(jj));
                break;
              case TypeKind::INTEGER:
                longKeys.push_back(
                    keys->asUnchecked<SimpleVector<int32_t>>()->valueAt(jj));
                break;
              case TypeKind::BIGINT:
                longKeys.push_back(
                    keys->asUnchecked<SimpleVector<int64_t>>()->valueAt(jj));
                break;
              case TypeKind::VARCHAR:
              case TypeKind::VARBINARY:
                stringKeys.push_back(
                    keys->asUnchecked<SimpleVector<StringView>>()->valueAt(jj));
                break;
              default:
                VELOX_FAIL();
            }
          }
        }
      }
      std::unique_ptr<Filter> filter;
      if (isStringKey) {
        std::shuffle(stringKeys.begin(), stringKeys.end(), rng);
        stringKeys.resize((stringKeys.size() + 1) / 2);
        filter = std::make_unique<BytesValues>(stringKeys, false);
      } else {
        std::shuffle(longKeys.begin(), longKeys.end(), rng);
        longKeys.resize((longKeys.size() + 1) / 2);
        filter = createBigintValues(longKeys, false);
      }
      for (auto& batch : batches) {
        auto* data = getChildBySubfield(batch.get(), subfield, rootType)
                         ->asUnchecked<MapVector>();
        auto& keys = data->mapKeys();
        auto indices = allocateIndices(keys->size(), pool);
        auto* rawIndices = indices->asMutable<vector_size_t>();
        vector_size_t offset = 0;
        for (int i = 0; i < data->size(); ++i) {
          if (data->isNullAt(i)) {
            continue;
          }
          int newSize = 0;
          for (int j = 0; j < data->sizeAt(i); ++j) {
            int jj = data->offsetAt(i) + j;
            bool passed;
            switch (keyType->kind()) {
              case TypeKind::TINYINT:
                passed = applyFilter(
                    *filter,
                    keys->asUnchecked<SimpleVector<int8_t>>()->valueAt(jj));
                break;
              case TypeKind::SMALLINT:
                passed = applyFilter(
                    *filter,
                    keys->asUnchecked<SimpleVector<int16_t>>()->valueAt(jj));
                break;
              case TypeKind::INTEGER:
                passed = applyFilter(
                    *filter,
                    keys->asUnchecked<SimpleVector<int32_t>>()->valueAt(jj));
                break;
              case TypeKind::BIGINT:
                passed = applyFilter(
                    *filter,
                    keys->asUnchecked<SimpleVector<int64_t>>()->valueAt(jj));
                break;
              case TypeKind::VARCHAR:
              case TypeKind::VARBINARY:
                passed = applyFilter(
                    *filter,
                    keys->asUnchecked<SimpleVector<StringView>>()->valueAt(jj));
                break;
              default:
                VELOX_FAIL();
            }
            if (passed) {
              rawIndices[offset + newSize++] = jj;
            }
          }
          data->setOffsetAndSize(i, offset, newSize);
          offset += newSize;
        }
        auto newKeys =
            BaseVector::wrapInDictionary(nullptr, indices, offset, keys);
        auto newValues = BaseVector::wrapInDictionary(
            nullptr, indices, offset, data->mapValues());
        BaseVector::flattenVector(newKeys);
        BaseVector::flattenVector(newValues);
        data->setKeysAndValues(newKeys, newValues);
      }
      spec.childByName(ScanSpec::kMapKeysFieldName)
          ->setFilter(std::move(filter));
      auto* valuesSpec = spec.childByName(ScanSpec::kMapValuesFieldName);
      subfield.path().push_back(std::make_unique<Subfield::AllSubscripts>());
      pruneRandomSubfield(
          subfield,
          *type.childAt(1),
          *valuesSpec,
          pool,
          rng,
          rootType,
          batches);
      subfield.path().pop_back();
      break;
    }
    default:
      // Ignore non-complex types.
      break;
  }
}

} // namespace

std::shared_ptr<ScanSpec> FilterGenerator::makeScanSpec(
    const std::vector<std::string>& prunable,
    std::vector<RowVectorPtr>& batches,
    memory::MemoryPool* pool) {
  auto root = std::make_shared<ScanSpec>("<root>");
  root->addAllChildFields(*rowType_);
  auto* first = batches[0].get();
  for (auto& path : prunable) {
    Subfield subfield(path);
    auto* spec = root->getOrCreateChild(subfield);
    auto type = getChildBySubfield(first, subfield, rowType_)->type();
    pruneRandomSubfield(subfield, *type, *spec, pool, rng_, rowType_, batches);
  }
  return root;
}

} // namespace facebook::velox::dwio::common
