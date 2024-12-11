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

#include "velox/dwio/common/tests/utils/DataSetBuilder.h"

#include "velox/dwio/common/tests/utils/BatchMaker.h"
#include "velox/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::test {

using namespace facebook::velox::dwio::common;

RowTypePtr DataSetBuilder::makeRowType(
    const std::string& columns,
    bool wrapInStruct) {
  std::string schema = wrapInStruct
      ? fmt::format("struct<{},struct_val:struct<{}>>", columns, columns)
      : fmt::format("struct<{}>", columns);
  type::fbhive::HiveTypeParser parser;
  return std::dynamic_pointer_cast<const RowType>(parser.parse(schema));
}

DataSetBuilder& DataSetBuilder::makeDataset(
    RowTypePtr rowType,
    const size_t batchCount,
    const size_t numRows,
    const bool withRecursiveNulls) {
  if (batches_) {
    batches_->clear();
  } else {
    batches_ = std::make_unique<std::vector<RowVectorPtr>>();
  }

  for (size_t i = 0; i < batchCount; ++i) {
    if (withRecursiveNulls) {
      batches_->push_back(std::static_pointer_cast<RowVector>(
          BatchMaker::createBatch(rowType, numRows, pool_, nullptr, i)));
    } else {
      batches_->push_back(
          std::static_pointer_cast<RowVector>(BatchMaker::createBatch(
              rowType,
              numRows,
              pool_,
              [](vector_size_t /*index*/) { return false; },
              i)));
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withRowGroupSpecificData(
    int32_t numRowsPerGroup) {
  auto type = (*batches_)[0]->type();
  for (auto i = 0; i < type->size(); ++i) {
    if (type->childAt(i)->kind() == TypeKind::BIGINT) {
      setRowGroupMarkers<int64_t>(
          i, numRowsPerGroup, std::numeric_limits<int64_t>::max());
    }

    if (type->childAt(i)->kind() == TypeKind::VARCHAR) {
      static StringView marker(
          AbstractColumnStats::kMaxString,
          strlen(AbstractColumnStats::kMaxString));
      setRowGroupMarkers<StringView>(i, numRowsPerGroup, marker);
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withNoNullsAfter(int32_t firstRow) {
  for (const auto& batch : *batches_) {
    for (auto& values : batch->children()) {
      std::vector<vector_size_t> nonNulls =
          getSomeNonNullRowNumbers(values, 23);

      if (nonNulls.empty()) {
        continue;
      }

      int32_t nonNullCounter = 0;
      std::vector<BaseVector::CopyRange> ranges;
      for (auto row = firstRow; row < values->size(); ++row) {
        if (values->isNullAt(row)) {
          ranges.push_back({
              .sourceIndex = nonNulls[nonNullCounter % nonNulls.size()],
              .targetIndex = row,
              .count = 1,
          });
          ++nonNullCounter;
        }
      }
      values->copyRanges(values.get(), ranges);
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withAllNullsForField(
    const common::Subfield& field) {
  for (RowVectorPtr batch : *batches_) {
    auto fieldValues = getChildBySubfield(batch.get(), field);
    SelectivityVector rows(fieldValues->size());
    fieldValues->addNulls(rows);
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withNullsForField(
    const common::Subfield& field,
    uint8_t nullsPercent) {
  for (RowVectorPtr batch : *batches_) {
    auto fieldValues = getChildBySubfield(batch.get(), field);

    SelectivityVector rows(fieldValues->size());
    if (nullsPercent == 0) {
      fieldValues->clearNulls(rows);
    } else if (nullsPercent >= 100) {
      fieldValues->addNulls(rows);
    } else {
      std::vector<vector_size_t> nonNullRows =
          getSomeNonNullRowNumbers(fieldValues, 23);

      int32_t nonNullCounter = 0;
      for (auto row = 0; row < fieldValues->size(); ++row) {
        bool expectNull =
            (folly::Random::rand32(rng_) % 99 + 1) <= nullsPercent;

        if (expectNull) {
          fieldValues->setNull(row, true);
        } else if (fieldValues->isNullAt(row)) {
          fieldValues->copy(
              fieldValues.get(),
              row,
              nonNullRows[nonNullCounter % nonNullRows.size()],
              1);
          ++nonNullCounter;
        }
      }
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withStringDistributionForField(
    const Subfield& field,
    int cardinality,
    bool keepNulls,
    bool addOneOffs) {
  int counter = 0;
  for (RowVectorPtr batch : *batches_) {
    auto strings =
        getChildBySubfield(batch.get(), field)->as<FlatVector<StringView>>();

    for (auto row = 0; row < strings->size(); ++row) {
      if (keepNulls && strings->isNullAt(row)) {
        continue;
      }
      std::string value;
      if (counter % 2251 < 100 || cardinality == 1) {
        // Run of 100 ascending values every 2251 rows. If cardinality is 1, the
        // value is repeated here.
        value = fmt::format("s{}", counter % cardinality);
        strings->set(row, StringView(value));
      } else if (counter % 100 > 90 && row > 0) {
        // Sequence of 10 identical values every 100 rows.
        strings->copy(strings, row, row - 1, 1);
      } else if (addOneOffs && counter % 234 == 0) {
        value = fmt::format(
            "s{}", folly::Random::rand32(rng_) % (111 * cardinality));
        strings->set(row, StringView(value));
      } else {
        value = fmt::format("s{}", folly::Random::rand32(rng_) % cardinality);
        strings->set(row, StringView(value));
      }
      ++counter;
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withUniqueStringsForField(
    const Subfield& field) {
  for (RowVectorPtr batch : *batches_) {
    auto strings =
        getChildBySubfield(batch.get(), field)->as<FlatVector<StringView>>();
    for (auto row = 0; row < strings->size(); ++row) {
      if (strings->isNullAt(row)) {
        continue;
      }
      std::string value = strings->valueAt(row);
      value += fmt::format("{}", row);
      strings->set(row, StringView(value));
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::makeUniformMapKeys(
    const common::Subfield& field) {
  for (auto& batch : *batches_) {
    auto* map = dwio::common::getChildBySubfield(batch.get(), field)
                    ->asUnchecked<MapVector>();
    int index = -1;
    for (int i = 0; i < map->size(); ++i) {
      if (index == -1 || map->sizeAt(i) > map->sizeAt(index)) {
        index = i;
      }
    }
    if (index == -1) {
      continue;
    }
    auto& keys = map->mapKeys();
    std::vector<BaseVector::CopyRange> ranges;
    for (int i = 0; i < map->size(); ++i) {
      if (i != index) {
        ranges.push_back({
            .sourceIndex = map->offsetAt(index),
            .targetIndex = map->offsetAt(i),
            .count = map->sizeAt(i),
        });
      }
    }
    keys->copyRanges(keys.get(), ranges);
  }
  return *this;
}

DataSetBuilder& DataSetBuilder::makeMapStringValues(
    const common::Subfield& field) {
  for (auto& batch : *batches_) {
    auto* map = dwio::common::getChildBySubfield(batch.get(), field)
                    ->asUnchecked<MapVector>();
    auto keyKind = map->type()->childAt(0)->kind();
    auto valueKind = map->type()->childAt(1)->kind();
    auto offsets = map->rawOffsets();
    auto sizes = map->rawSizes();
    int32_t offsetIndex = 0;
    auto mapSize = map->size();
    auto getNextOffset = [&]() {
      while (offsetIndex < mapSize) {
        if (offsets[offsetIndex] != 0) {
          return offsets[offsetIndex++];
        }
        ++offsetIndex;
      }
      return 0;
    };

    int32_t nextOffset = offsets[0];
    int32_t nullCounter = 0;
    auto size = map->mapKeys()->size();
    if (keyKind == TypeKind::VARCHAR) {
      if (auto keys = map->mapKeys()->as<FlatVector<StringView>>()) {
        for (auto i = 0; i < size; ++i) {
          if (i == nextOffset) {
            // The first key of every map is fixed. The first value is limited
            // cardinality so that at least one column of flat map comes out as
            // dict.
            std::string str = "dictEncodedValue";
            keys->set(i, StringView(str));
            nextOffset = getNextOffset();
            continue;
          }
          if (!keys->isNullAt(i) && i % 3 == 0) {
            std::string str = keys->valueAt(i);
            str += "----123456789";
            keys->set(i, StringView(str));
          }
        }
      }
    }
    if (valueKind == TypeKind::VARCHAR) {
      if (auto values = map->mapValues()->as<FlatVector<StringView>>()) {
        offsetIndex = 0;
        nextOffset = offsets[0];
        for (auto i = 0; i < size; ++i) {
          if (i == nextOffset) {
            std::string str = fmt::format("dictEncoded{}", i % 3);
            values->set(i, StringView(str));
            if (nullCounter++ % 4 == 0) {
              values->setNull(i, true);
            }
            nextOffset = getNextOffset();
            continue;
          }
          if (!values->isNullAt(i) && i % 3 == 0) {
            std::string str = values->valueAt(i);
            str += "----123456789";
            values->set(i, StringView(str));
          }
        }
      }
    }
  }
  return *this;
}

std::unique_ptr<std::vector<RowVectorPtr>> DataSetBuilder::build() {
  return std::move(batches_);
}

std::vector<vector_size_t> DataSetBuilder::getSomeNonNullRowNumbers(
    VectorPtr batch,
    uint32_t numRows) {
  std::vector<vector_size_t> nonNulls;

  vector_size_t probe = 0;
  for (auto counter = 0; counter < numRows; ++counter) {
    // Sample with a prime stride for a handful of non-null values.
    probe = (probe + 47) % batch->size();
    if (!batch->isNullAt(probe)) {
      nonNulls.push_back(probe);
    }
  }
  return nonNulls;
}

} // namespace facebook::velox::test
