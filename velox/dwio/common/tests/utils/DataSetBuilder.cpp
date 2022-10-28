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
#include "velox/dwio/type/fbhive/HiveTypeParser.h"
#include "velox/vector/FlatVector.h"

namespace facebook::velox::test {

using namespace facebook::velox::dwio::common;

RowTypePtr DataSetBuilder::makeRowType(
    const std::string& columns,
    bool wrapInStruct) {
  std::string schema = wrapInStruct
      ? fmt::format("struct<{},struct_val:struct<{}>>", columns, columns)
      : fmt::format("struct<{}>", columns);
  dwio::type::fbhive::HiveTypeParser parser;
  return std::dynamic_pointer_cast<const RowType>(parser.parse(schema));
}

DataSetBuilder& DataSetBuilder::makeDataset(
    RowTypePtr rowType,
    const size_t batchCount,
    const size_t numRows) {
  if (batches_) {
    batches_->clear();
  } else {
    batches_ = std::make_unique<std::vector<RowVectorPtr>>();
  }

  for (size_t i = 0; i < batchCount; ++i) {
    batches_->push_back(std::static_pointer_cast<RowVector>(
        BatchMaker::createBatch(rowType, numRows, pool_, nullptr, i)));
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
      for (auto row = firstRow; row < values->size(); ++row) {
        if (values->isNullAt(row)) {
          values->copy(
              values.get(), row, nonNulls[nonNullCounter % nonNulls.size()], 1);
          ++nonNullCounter;
        }
      }
    }
  }

  return *this;
}

DataSetBuilder& DataSetBuilder::withAllNullsForField(
    const common::Subfield& field) {
  for (RowVectorPtr batch : *batches_) {
    auto fieldValues = getChildBySubfield(batch.get(), field);
    SelectivityVector rows(fieldValues->size());
    fieldValues->addNulls(nullptr, rows);
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
      fieldValues->addNulls(nullptr, rows);
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
