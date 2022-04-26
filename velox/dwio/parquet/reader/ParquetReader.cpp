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

#include "velox/dwio/parquet/reader/ParquetReader.h"
#include "velox/duckdb/conversion/DuckConversion.h"
#include "velox/duckdb/conversion/DuckWrapper.h"
#include "velox/dwio/parquet/reader/Statistics.h"

namespace facebook::velox::parquet {

namespace {

::duckdb::Value makeValue(::duckdb::LogicalType type, int64_t val) {
  switch (type.id()) {
    case ::duckdb::LogicalTypeId::INTEGER:
      return ::duckdb::Value::INTEGER(val);
    case ::duckdb::LogicalTypeId::BIGINT:
      return ::duckdb::Value::BIGINT(val);
    case ::duckdb::LogicalTypeId::DATE:
      return ::duckdb::Value::DATE(::duckdb::date_t(val));
    default:
      VELOX_UNSUPPORTED(
          "Unsupported column type for integer filter: {}", type.ToString());
  }
}

std::unique_ptr<::duckdb::ConstantFilter> constantFilter(
    ::duckdb::ExpressionType expressionType,
    ::duckdb::Value value) {
  return std::make_unique<::duckdb::ConstantFilter>(
      expressionType, std::move(value));
}

std::unique_ptr<::duckdb::ConstantFilter> constantEqualFilter(
    ::duckdb::Value value) {
  return std::make_unique<::duckdb::ConstantFilter>(
      ::duckdb::ExpressionType::COMPARE_EQUAL, std::move(value));
}

void buildConjunctOrFilter(
    uint64_t colIdx,
    ::duckdb::LogicalType type,
    const std::vector<int64_t>& values,
    ::duckdb::TableFilterSet& filters) {
  if (values.size() == 1) {
    filters.PushFilter(
        colIdx, constantEqualFilter(makeValue(type, *values.begin())));
  } else {
    auto duckFilter = std::make_unique<::duckdb::ConjunctionOrFilter>();
    for (const auto& value : values) {
      duckFilter->child_filters.push_back(
          constantEqualFilter(makeValue(type, value)));
    }
    filters.PushFilter(colIdx, std::move(duckFilter));
  }
}

void toDuckDbFilter(
    uint64_t colIdx,
    ::duckdb::LogicalType type,
    common::Filter* filter,
    ::duckdb::TableFilterSet& filters) {
  switch (filter->kind()) {
    case common::FilterKind::kBigintRange: {
      auto rangeFilter = static_cast<common::BigintRange*>(filter);
      if (rangeFilter->isSingleValue()) {
        filters.PushFilter(
            colIdx, constantEqualFilter(makeValue(type, rangeFilter->lower())));
      } else {
        if (rangeFilter->lower() != std::numeric_limits<int64_t>::min()) {
          filters.PushFilter(
              colIdx,
              constantFilter(
                  ::duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO,
                  makeValue(type, rangeFilter->lower())));
        }
        if (rangeFilter->upper() != std::numeric_limits<int64_t>::max()) {
          filters.PushFilter(
              colIdx,
              constantFilter(
                  ::duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO,
                  makeValue(type, rangeFilter->upper())));
        }
      }
      break;
    }

    case common::FilterKind::kDoubleRange: {
      auto rangeFilter = static_cast<common::DoubleRange*>(filter);
      if (!rangeFilter->lowerUnbounded()) {
        auto expressionType = rangeFilter->lowerExclusive()
            ? ::duckdb::ExpressionType::COMPARE_GREATERTHAN
            : ::duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
        filters.PushFilter(
            colIdx,
            constantFilter(
                expressionType, ::duckdb::Value(rangeFilter->lower())));
      }
      if (!rangeFilter->upperUnbounded()) {
        auto expressionType = rangeFilter->upperExclusive()
            ? ::duckdb::ExpressionType::COMPARE_LESSTHAN
            : ::duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
        filters.PushFilter(
            colIdx,
            constantFilter(
                expressionType, ::duckdb::Value(rangeFilter->upper())));
      }
      break;
    }

    case common::FilterKind::kBytesValues: {
      auto valuesFilter = static_cast<common::BytesValues*>(filter);
      const auto& values = valuesFilter->values();
      if (values.size() == 1) {
        filters.PushFilter(colIdx, constantEqualFilter(*values.begin()));
      } else {
        auto duckFilter = std::make_unique<::duckdb::ConjunctionOrFilter>();
        for (const auto& value : values) {
          duckFilter->child_filters.push_back(constantEqualFilter(value));
        }
        filters.PushFilter(colIdx, std::move(duckFilter));
      }
      break;
    }

    case common::FilterKind::kBytesRange: {
      auto rangeFilter = static_cast<common::BytesRange*>(filter);
      if (!rangeFilter->lowerUnbounded()) {
        auto expressionType = rangeFilter->lowerExclusive()
            ? ::duckdb::ExpressionType::COMPARE_GREATERTHAN
            : ::duckdb::ExpressionType::COMPARE_GREATERTHANOREQUALTO;
        filters.PushFilter(
            colIdx,
            constantFilter(
                expressionType, ::duckdb::Value(rangeFilter->lower())));
      }
      if (!rangeFilter->upperUnbounded()) {
        auto expressionType = rangeFilter->upperExclusive()
            ? ::duckdb::ExpressionType::COMPARE_LESSTHAN
            : ::duckdb::ExpressionType::COMPARE_LESSTHANOREQUALTO;
        filters.PushFilter(
            colIdx,
            constantFilter(
                expressionType, ::duckdb::Value(rangeFilter->upper())));
      }
      break;
    }
    case common::FilterKind::kBigintValuesUsingBitmask: {
      auto valuesFilter =
          static_cast<common::BigintValuesUsingBitmask*>(filter);
      const auto values = valuesFilter->values();
      buildConjunctOrFilter(colIdx, type, values, filters);
      break;
    }
    case common::FilterKind::kBigintValuesUsingHashTable: {
      auto valuesFilter =
          static_cast<common::BigintValuesUsingHashTable*>(filter);
      const auto& values = valuesFilter->values();
      buildConjunctOrFilter(colIdx, type, values, filters);
      break;
    }
    case common::FilterKind::kAlwaysFalse:
    case common::FilterKind::kAlwaysTrue:
    case common::FilterKind::kIsNull:
    case common::FilterKind::kIsNotNull:
    case common::FilterKind::kBoolValue:
    case common::FilterKind::kFloatRange:
    case common::FilterKind::kBigintMultiRange:
    case common::FilterKind::kMultiRange:
    default:
      VELOX_UNSUPPORTED(
          "Unsupported filter in parquet reader: {}", filter->toString());
  }
}

std::optional<common::Filter*> findFilter(
    const std::string& columnName,
    const common::ScanSpec* scanSpec) {
  const auto& childSpecs = scanSpec->children();
  for (const auto& childSpec : childSpecs) {
    VELOX_CHECK(
        !childSpec->extractValues(),
        "Subfield access is NYI in parquet reader");

    if (childSpec->fieldName() == columnName && childSpec->filter()) {
      return childSpec->filter();
    }
  }

  return std::nullopt;
}

} // anonymous namespace

ParquetRowReader::ParquetRowReader(
    std::shared_ptr<::duckdb::ParquetReader> reader,
    const dwio::common::RowReaderOptions& options,
    memory::MemoryPool& pool)
    : reader_(std::move(reader)),
      pool_(pool),
      scanSpec_{options.getScanSpec()} {
  auto& selector = *options.getSelector();
  rowType_ = selector.buildSelectedReordered();
  duckdbRowType_.reserve(rowType_->size());

  auto& projection = selector.getProjection();
  VELOX_CHECK_EQ(rowType_->size(), projection.size());

  std::vector<::duckdb::column_t> columnIds;
  columnIds.reserve(rowType_->size());
  for (uint64_t i = 0; i < projection.size(); i++) {
    uint64_t columnId = projection[i].column;
    VELOX_CHECK_LT(
        columnId,
        reader_->names.size(),
        "Unexpected column name: {}",
        projection[i].name);
    columnIds.push_back(columnId);

    // DuckDB ParquetReader::return_types contains all columns present in the
    // file.
    ::duckdb::LogicalType duckDbType = reader_->return_types[columnId];
    duckdbRowType_.push_back(duckDbType);

    if (options.getScanSpec()) {
      auto veloxFilter = findFilter(projection[i].name, options.getScanSpec());
      if (veloxFilter) {
        toDuckDbFilter(i, duckDbType, veloxFilter.value(), filters_);
      }
    }
  }

  std::vector<idx_t> groups;
  for (idx_t i = 0; i < reader_->NumRowGroups(); i++) {
    auto groupOffset = reader_->GetFileMetadata()->row_groups[i].file_offset;
    if (groupOffset >= options.getOffset() &&
        groupOffset < (options.getLength() + options.getOffset())) {
      groups.push_back(i);
    }
  }

  reader_->InitializeScan(
      state_, std::move(columnIds), std::move(groups), &filters_);
}

uint64_t ParquetRowReader::next(uint64_t /*size*/, velox::VectorPtr& result) {
  ::duckdb::DataChunk output;
  output.Initialize(duckdbRowType_);

  reader_->Scan(state_, output);

  if (output.size() > 0) {
    std::vector<VectorPtr> columns;
    columns.resize(output.data.size());
    for (auto& spec : scanSpec_->children()) {
      if (spec->isConstant()) {
        columns[spec->channel()] =
            BaseVector::wrapInConstant(output.size(), 0, spec->constantValue());
      } else if (spec->projectOut()) {
        auto index = rowType_->getChildIdx(spec->fieldName());
        columns[spec->channel()] = duckdb::toVeloxVector(
            output.size(),
            output.data[index],
            rowType_->childAt(index),
            &pool_);
      }
    }

    result = std::make_shared<RowVector>(
        &pool_,
        rowType_,
        BufferPtr(nullptr),
        output.size(),
        columns,
        std::nullopt);
  }

  return output.size();
}

void ParquetRowReader::updateRuntimeStats(
    dwio::common::RuntimeStatistics& /*stats*/) const {}

void ParquetRowReader::resetFilterCaches() {
  // No filter caches to reset.
}

std::optional<size_t> ParquetRowReader::estimatedRowSize() const {
  // TODO Implement.
  return std::nullopt;
}

ParquetReader::ParquetReader(
    std::unique_ptr<dwio::common::InputStream> stream,
    const dwio::common::ReaderOptions& options)
    : allocator_(options.getMemoryPool()),
      fileSystem_(
          std::make_unique<duckdb::InputStreamFileSystem>(std::move(stream))),
      reader_(std::make_shared<::duckdb::ParquetReader>(
          allocator_,
          fileSystem_->OpenFile())),
      pool_(options.getMemoryPool()) {
  auto names = reader_->names;
  std::vector<TypePtr> types;
  types.reserve(reader_->return_types.size());
  for (auto& t : reader_->return_types) {
    types.emplace_back(duckdb::toVeloxType(t));
  }
  type_ = ROW(std::move(names), std::move(types));
}

std::optional<uint64_t> ParquetReader::numberOfRows() const {
  return const_cast<::duckdb::ParquetReader*>(reader_.get())->NumRows();
}

std::unique_ptr<dwio::common::ColumnStatistics> ParquetReader::columnStatistics(
    uint32_t /*index*/) const {
  // TODO: implement proper stats
  return std::make_unique<ColumnStatistics>();
}

const velox::RowTypePtr& ParquetReader::rowType() const {
  return type_;
}

const std::shared_ptr<const dwio::common::TypeWithId>&
ParquetReader::typeWithId() const {
  if (!typeWithId_) {
    typeWithId_ = dwio::common::TypeWithId::create(type_);
  }
  return typeWithId_;
}

std::unique_ptr<dwio::common::RowReader> ParquetReader::createRowReader(
    const dwio::common::RowReaderOptions& options) const {
  return std::make_unique<ParquetRowReader>(reader_, options, pool_);
}

void registerParquetReaderFactory() {
  dwio::common::registerReaderFactory(std::make_shared<ParquetReaderFactory>());
}

void unregisterParquetReaderFactory() {
  dwio::common::unregisterReaderFactory(dwio::common::FileFormat::PARQUET);
}

} // namespace facebook::velox::parquet
