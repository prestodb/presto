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

#include "velox/dwio/dwrf/test/utils/FilterGenerator.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::dwrf {
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

VectorPtr getChildBySubfield(RowVector* rowVector, const Subfield& subfield) {
  auto& path = subfield.path();
  auto container = rowVector;
  for (int i = 0; i < path.size(); ++i) {
    auto nestedField =
        dynamic_cast<const Subfield::NestedField*>(path[i].get());
    VELOX_CHECK(nestedField, "Path does not consist of nested fields");

    auto rowType = container->type()->as<TypeKind::ROW>();
    auto child = rowVector->childAt(rowType.getChildIdx(nestedField->name()));

    if (i == path.size() - 1) {
      return child;
    }
    VELOX_CHECK(child->typeKind() == TypeKind::ROW);
    container = child->as<RowVector>();
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
  return std::make_unique<velox::common::FloatRange>(
      lower, false, false, upper, false, false, selectPct > 25);
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
  return std::make_unique<velox::common::DoubleRange>(
      lower, false, false, upper, false, false, selectPct > 25);
}

template <>
std::unique_ptr<Filter> ColumnStats<StringView>::makeRangeFilter(
    float startPct,
    float selectPct) {
  if (values_.empty()) {
    return std::make_unique<velox::common::IsNull>();
  }
  StringView lower = valueAtPct(startPct);
  StringView upper = valueAtPct(startPct + selectPct);
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
      VELOX_CHECK_NOT_NULL(rowType, "");
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

std::unique_ptr<ScanSpec> FilterGenerator::makeScanSpec(
    SubfieldFilters filters) {
  auto spec = std::make_unique<ScanSpec>("root");
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
    auto vector = getChildBySubfield(first, subfield);
    std::unique_ptr<AbstractColumnStats> stats;
    switch (vector->typeKind()) {
      case TypeKind::BOOLEAN:
        stats = makeStats<TypeKind::BOOLEAN>(vector->type());
        break;
      case TypeKind::TINYINT:
        stats = makeStats<TypeKind::TINYINT>(vector->type());
        break;
      case TypeKind::SMALLINT:
        stats = makeStats<TypeKind::SMALLINT>(vector->type());
        break;
      case TypeKind::INTEGER:
        stats = makeStats<TypeKind::INTEGER>(vector->type());
        break;
      case TypeKind::BIGINT:
        stats = makeStats<TypeKind::BIGINT>(vector->type());
        break;
      case TypeKind::VARCHAR:
        stats = makeStats<TypeKind::VARCHAR>(vector->type());
        break;

      case TypeKind::REAL:
        stats = makeStats<TypeKind::REAL>(vector->type());
        break;
      case TypeKind::DOUBLE:
        stats = makeStats<TypeKind::DOUBLE>(vector->type());
        break;
      default:
        VELOX_CHECK(false, "Type not supported");
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

std::vector<FilterSpec> FilterGenerator::makeRandomSpecs(
    const std::vector<std::string>& filterable) {
  std::vector<FilterSpec> specs;
  auto deck = filterable;
  for (int i = 0; i < filterable.size(); ++i) {
    // We aim at 1.5
    if (folly::Random::rand32(rng_) % (100 * filterable.size()) < 125) {
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
  }

  return specs;
}

} // namespace facebook::velox::dwio::dwrf
