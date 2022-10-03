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
uint32_t batchPosition(uint32_t batchNumber, vector_size_t batchRow) {
  return batchNumber << 16 | batchRow;
}

uint32_t batchNumber(uint32_t position) {
  return position >> 16;
}

vector_size_t batchRow(uint32_t position) {
  return position & 0xffff;
}

VectorPtr getChildBySubfield(
    RowVector* rowVector,
    const Subfield& subfield,
    const RowTypePtr& type) {
  auto& path = subfield.path();
  auto container = rowVector;
  auto parentType = type.get();
  for (int i = 0; i < path.size(); ++i) {
    auto nestedField =
        dynamic_cast<const Subfield::NestedField*>(path[i].get());
    VELOX_CHECK(nestedField, "Path does not consist of nested fields");

    auto rowType = parentType == nullptr
        ? container->type()->as<TypeKind::ROW>()
        : *parentType;
    auto childIdx = rowType.getChildIdx(nestedField->name());
    auto child = container->childAt(childIdx);

    if (i == path.size() - 1) {
      return child;
    }
    VELOX_CHECK(child->typeKind() == TypeKind::ROW);
    container = child->as<RowVector>();
    parentType = dynamic_cast<const RowType*>(rowType.childAt(childIdx).get());
    VELOX_CHECK_NOT_NULL(
        parentType,
        "Expecting child to be row type",
        subfield.toString().c_str(),
        i);
  }
  // Never reached.
  VELOX_CHECK(false);
  return nullptr;
}

uint32_t AbstractColumnStats::counter_ = 0;

template <>
std::unique_ptr<Filter> ColumnStats<bool>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  bool value = valueAtPct(startPct + selectPct);
  return std::make_unique<velox::common::BoolValue>(value, selectPct > 50);
}

template <>
std::unique_ptr<Filter> ColumnStats<float>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  float lower = valueAtPct(startPct);
  float upper = valueAtPct(startPct + selectPct);
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
      selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<double>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  double lower = valueAtPct(startPct);
  double upper = valueAtPct(startPct + selectPct);
  bool lowerUnbounded = std::isnan(lower);
  bool upperUnbounded = std::isnan(upper);
  if (lowerUnbounded && upperUnbounded) {
    return std::make_unique<velox::common::IsNotNull>();
  }
  return std::make_unique<velox::common::DoubleRange>(
      lower,
      lowerUnbounded,
      false,
      upper,
      upperUnbounded,
      false,
      selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }

  // used to determine if we can test a values filter reasonably
  int32_t lowerIndex;
  int32_t upperIndex;
  StringView lower = valueAtPct(startPct, &lowerIndex);
  StringView upper = valueAtPct(startPct + selectPct, &upperIndex);
  if (upperIndex - lowerIndex < 1000 && ++counter_ % 10 <= 3) {
    std::vector<std::string> inRange;
    inRange.reserve(upperIndex - lowerIndex);
    for (StringView s : values_) {
      // do comparison
      if (lower <= s && s <= upper) {
        inRange.push_back(s.getString());
      }
    }
    if (counter_ % 2 == 0 && selectPct != 100.0) {
      return std::make_unique<velox::common::NegatedBytesValues>(
          inRange, selectPct < 75);
    }
    return std::make_unique<velox::common::BytesValues>(
        inRange, selectPct > 25);
  }

  // sometimes create a negated filter instead
  if (counter_ % 4 == 1 && selectPct < 100.0) {
    return std::make_unique<velox::common::NegatedBytesRange>(
        std::string(lower),
        false,
        false,
        std::string(upper),
        false,
        false,
        selectPct < 75);
  }

  return std::make_unique<velox::common::BytesRange>(
      std::string(lower),
      false,
      false,
      std::string(upper),
      false,
      false,
      selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRowGroupSkipRangeFilter(
    const std::vector<RowVectorPtr>& /*batches*/,
    const Subfield& /*subfield*/) {
  static std::string max = kMaxString;
  return std::make_unique<velox::common::BytesRange>(
      max, false, false, max, false, false, false);
}

void FilterGenerator::makeFieldSpecs(
    const std::string& pathPrefix,
    int32_t level,
    const std::shared_ptr<const Type>& type,
    ScanSpec* spec) {
  switch (type->kind()) {
    case TypeKind::ROW: {
      VELOX_CHECK_EQ(type->kind(), velox::TypeKind::ROW);
      auto rowType = dynamic_cast<const RowType*>(type.get());
      VELOX_CHECK_NOT_NULL(rowType, "Expecting a row type", type->kindName());
      for (auto i = 0; i < type->size(); ++i) {
        std::string path = level == 0 ? rowType->nameOf(i)
                                      : pathPrefix + "." + rowType->nameOf(i);
        Subfield subfield(path);
        ScanSpec* fieldSpec = spec->getOrCreateChild(subfield);
        fieldSpec->setProjectOut(true);
        fieldSpec->setExtractValues(true);
        fieldSpec->setChannel(i);
        makeFieldSpecs(path, level + 1, type->childAt(i), spec);
      }
      break;
    }
    case TypeKind::MAP: {
      auto keySpec = spec->getOrCreateChild(Subfield(pathPrefix + ".keys"));
      keySpec->setProjectOut(true);
      keySpec->setExtractValues(true);
      makeFieldSpecs(pathPrefix + ".keys", level + 1, type->childAt(0), spec);
      auto valueSpec =
          spec->getOrCreateChild(Subfield(pathPrefix + ".elements"));
      valueSpec->setProjectOut(true);
      valueSpec->setExtractValues(true);
      makeFieldSpecs(
          pathPrefix + ".elements", level + 1, type->childAt(1), spec);
      break;
    }
    case TypeKind::ARRAY: {
      auto childSpec =
          spec->getOrCreateChild(Subfield(pathPrefix + ".elements"));
      childSpec->setProjectOut(true);
      childSpec->setExtractValues(true);
      makeFieldSpecs(
          pathPrefix + ".elements", level + 1, type->childAt(0), spec);
      break;
    }

    default:
      break;
  }
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
    SubfieldFilters filters) {
  auto spec = std::make_shared<ScanSpec>("root");
  makeFieldSpecs("", 0, rowType_, spec.get());

  for (auto& pair : filters) {
    auto fieldSpec = spec->getOrCreateChild(pair.first);
    fieldSpec->setFilter(std::move(pair.second));
  }
  return spec;
}

SubfieldFilters FilterGenerator::makeSubfieldFilters(
    const std::vector<FilterSpec>& filterSpecs,
    const std::vector<RowVectorPtr>& batches,
    std::vector<uint32_t>& hitRows) {
  vector_size_t totalSize = 0;
  for (auto& batch : batches) {
    totalSize += batch->size();
  }
  hitRows.reserve(totalSize);
  for (auto i = 0; i < batches.size(); ++i) {
    auto batch = batches[i];
    for (auto j = 0; j < batch->size(); ++j) {
      hitRows.push_back(batchPosition(i, j));
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
      case TypeKind::VARCHAR:
        stats = makeStats<TypeKind::VARCHAR>(vector->type(), rowType_);
        break;
      case TypeKind::REAL:
        stats = makeStats<TypeKind::REAL>(vector->type(), rowType_);
        break;
      case TypeKind::DOUBLE:
        stats = makeStats<TypeKind::DOUBLE>(vector->type(), rowType_);
        break;
        // TODO:
        // Add support for TTypeKind::IMESTAMP and TypeKind::ROW
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
      filter = stats->filter(
          filterSpec.startPct,
          filterSpec.selectPct,
          filterSpec.filterKind,
          batches,
          subfield,
          hitRows);
    }
    filters[Subfield(filterSpec.field)] = std::move(filter);
  }

  return filters;
}

} // namespace facebook::velox::dwio::common
