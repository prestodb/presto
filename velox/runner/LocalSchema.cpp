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

#include "velox/runner/LocalSchema.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ReaderFactory.h"

#include "velox/common/base/Fs.h"

namespace facebook::velox::runner {

LocalSchema::LocalSchema(
    const std::string& path,
    dwio::common::FileFormat format,
    connector::hive::HiveConnector* hiveConnector,
    std::shared_ptr<connector::ConnectorQueryCtx> ctx)
    : Schema(path, ctx->memoryPool()),
      hiveConnector_(hiveConnector),
      connectorId_(hiveConnector_->connectorId()),
      connectorQueryCtx_(ctx),
      format_(format) {
  initialize(path);
}

void LocalSchema::initialize(const std::string& path) {
  for (auto const& dirEntry : fs::directory_iterator{path}) {
    if (!dirEntry.is_directory() ||
        dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    loadTable(dirEntry.path().filename(), dirEntry.path());
  }
}
// Feeds the values in 'vector' into 'builder'.
template <typename Builder, typename T>
void addStats(
    velox::dwrf::StatisticsBuilder* builder,
    const BaseVector& vector) {
  auto* typedVector = vector.asUnchecked<SimpleVector<T>>();
  for (auto i = 0; i < typedVector->size(); ++i) {
    if (!typedVector->isNullAt(i)) {
      reinterpret_cast<Builder*>(builder)->addValues(typedVector->valueAt(i));
    }
  }
}

std::pair<int64_t, int64_t> LocalTable::sample(
    float pct,
    const std::vector<common::Subfield>& fields,
    velox::connector::hive::SubfieldFilters filters,
    const velox::core::TypedExprPtr& remainingFilter,
    HashStringAllocator* /*allocator*/,
    std::vector<std::unique_ptr<velox::dwrf::StatisticsBuilder>>*
        statsBuilders) {
  dwrf::StatisticsBuilderOptions options(
      /*stringLengthLimit=*/100, /*initialSize=*/0);
  std::vector<std::unique_ptr<dwrf::StatisticsBuilder>> builders;
  auto tableHandle = std::make_shared<connector::hive::HiveTableHandle>(
      schema_->connector()->connectorId(),
      name_,
      true,
      std::move(filters),
      remainingFilter);

  std::unordered_map<
      std::string,
      std::shared_ptr<velox::connector::ColumnHandle>>
      columnHandles;
  std::vector<std::string> names;
  std::vector<TypePtr> types;
  for (auto& field : fields) {
    auto& path = field.path();
    auto column =
        dynamic_cast<const common::Subfield::NestedField*>(path[0].get())
            ->name();
    const auto idx = type_->getChildIdx(column);
    names.push_back(type_->nameOf(idx));
    types.push_back(type_->childAt(idx));
    columnHandles[names.back()] =
        std::make_shared<connector::hive::HiveColumnHandle>(
            names.back(),
            connector::hive::HiveColumnHandle::ColumnType::kRegular,
            types.back(),
            types.back());
    switch (types.back()->kind()) {
      case TypeKind::BIGINT:
      case TypeKind::INTEGER:
      case TypeKind::SMALLINT:
        builders.push_back(
            std::make_unique<dwrf::IntegerStatisticsBuilder>(options));
        break;
      case TypeKind::REAL:
      case TypeKind::DOUBLE:
        builders.push_back(
            std::make_unique<dwrf::DoubleStatisticsBuilder>(options));
        break;
      case TypeKind::VARCHAR:
        builders.push_back(
            std::make_unique<dwrf::StringStatisticsBuilder>(options));
        break;

      default:
        builders.push_back(nullptr);
    }
  }

  const auto outputType = ROW(std::move(names), std::move(types));
  int64_t passingRows = 0;
  int64_t scannedRows = 0;
  for (auto& file : files_) {
    auto dataSource = schema_->connector()->createDataSource(
        outputType,
        tableHandle,
        columnHandles,
        schema_->connectorQueryCtx().get());

    auto split = connector::hive::HiveConnectorSplitBuilder(file)
                     .fileFormat(format_)
                     .connectorId(schema_->connector()->connectorId())
                     .build();
    dataSource->addSplit(split);
    constexpr int32_t kBatchSize = 1000;
    for (;;) {
      ContinueFuture ignore{ContinueFuture::makeEmpty()};

      auto data = dataSource->next(kBatchSize, ignore).value();
      if (data == nullptr) {
        break;
      }
      passingRows += data->size();
      for (auto column = 0; column < builders.size(); ++column) {
        if (!builders[column]) {
          continue;
        }
        auto* builder = builders[column].get();
        auto loadChild = [](RowVectorPtr data, int32_t column) {
          data->childAt(column) =
              BaseVector::loadedVectorShared(data->childAt(column));
        };
        switch (type_->childAt(column)->kind()) {
          case TypeKind::SMALLINT:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, short>(
                builder, *data->childAt(column));
            break;
          case TypeKind::INTEGER:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, int32_t>(
                builder, *data->childAt(column));
            break;
          case TypeKind::BIGINT:
            loadChild(data, column);
            addStats<dwrf::IntegerStatisticsBuilder, int64_t>(
                builder, *data->childAt(column));
            break;
          case TypeKind::REAL:
            loadChild(data, column);
            addStats<dwrf::DoubleStatisticsBuilder, float>(
                builder, *data->childAt(column));
            break;
          case TypeKind::DOUBLE:
            loadChild(data, column);
            addStats<dwrf::DoubleStatisticsBuilder, double>(
                builder, *data->childAt(column));
            break;
          case TypeKind::VARCHAR:
            loadChild(data, column);
            addStats<dwrf::StringStatisticsBuilder, StringView>(
                builder, *data->childAt(column));
            break;

          default:
            break;
        }
      }
      break;
    }
    scannedRows += dataSource->getCompletedRows();
    if (scannedRows > numRows_ * (pct / 100)) {
      break;
    }
  }
  if (statsBuilders) {
    *statsBuilders = std::move(builders);
  }
  return std::pair(scannedRows, passingRows);
}

void LocalSchema::loadTable(
    const std::string& tableName,
    const fs::path& tablePath) {
  // open each file in the directory and check their type and add up the row
  // counts.
  RowTypePtr tableType;
  LocalTable* table = nullptr;

  for (auto const& dirEntry : fs::directory_iterator{tablePath}) {
    if (!dirEntry.is_regular_file()) {
      continue;
    }
    // Ignore hidden files.
    if (dirEntry.path().filename().c_str()[0] == '.') {
      continue;
    }
    auto it = tables_.find(tableName);
    if (it != tables_.end()) {
      table = reinterpret_cast<LocalTable*>(it->second.get());
    } else {
      tables_[tableName] =
          std::make_unique<LocalTable>(tableName, format_, this);
      table = reinterpret_cast<LocalTable*>(tables_[tableName].get());
    }
    dwio::common::ReaderOptions readerOptions{pool_};
    readerOptions.setFileFormat(format_);
    auto input = std::make_unique<dwio::common::BufferedInput>(
        std::make_shared<LocalReadFile>(dirEntry.path().string()),
        readerOptions.memoryPool());
    std::unique_ptr<dwio::common::Reader> reader =
        dwio::common::getReaderFactory(readerOptions.fileFormat())
            ->createReader(std::move(input), readerOptions);
    const auto fileType = reader->rowType();
    if (!tableType) {
      tableType = fileType;
    } else if (fileType->size() > tableType->size()) {
      // The larger type is the later since there is only addition of columns.
      // TODO: Check the column types are compatible where they overlap.
      tableType = fileType;
    }
    const auto rows = reader->numberOfRows();

    if (rows.has_value()) {
      table->numRows_ += rows.value();
    }
    for (auto i = 0; i < fileType->size(); ++i) {
      auto name = fileType->nameOf(i);
      LocalColumn* column;
      auto columnIt = table->columns().find(name);
      if (columnIt != table->columns().end()) {
        column = columnIt->second.get();
      } else {
        table->columns()[name] =
            std::make_unique<LocalColumn>(name, fileType->childAt(i));
        column = table->columns()[name].get();
      }
      // Initialize the stats from the first file.
      if (column->stats() == nullptr) {
        column->setStats(reader->columnStatistics(i));
      }
    }
    table->files_.push_back(dirEntry.path());
  }
  VELOX_CHECK_NOT_NULL(table, "Table directory {} is empty", tablePath);

  table->setType(tableType);
  table->sampleNumDistincts(2, pool_);
}

void LocalTable::sampleNumDistincts(float samplePct, memory::MemoryPool* pool) {
  std::vector<common::Subfield> fields;
  for (auto i = 0; i < type_->size(); ++i) {
    fields.push_back(common::Subfield(type_->nameOf(i)));
  }

  // Sample the table. Adjust distinct values according to the samples.
  auto allocator = std::make_unique<HashStringAllocator>(pool);
  std::vector<std::unique_ptr<dwrf::StatisticsBuilder>> statsBuilders;
  auto [sampled, passed] =
      sample(samplePct, fields, {}, nullptr, allocator.get(), &statsBuilders);
  numSampledRows_ = sampled;
  for (auto i = 0; i < statsBuilders.size(); ++i) {
    if (statsBuilders[i]) {
      // TODO: Use HLL estimate of distinct values here after this is added to
      // the stats builder. Now assume that all rows have a different value.
      // Later refine this by observed min-max range.
      int64_t approxNumDistinct = numRows_;
      // For tiny tables the sample is 100% and the approxNumDistinct is
      // accurate. For partial samples, the distinct estimate is left to be the
      // distinct estimate of the sample if there are few distincts. This is an
      // enumeration where values in unsampled rows are likely the same. If
      // there are many distincts, we multiply by 1/sample rate assuming that
      // unsampled rows will mostly have new values.

      if (numSampledRows_ < numRows_) {
        if (approxNumDistinct > sampled / 50) {
          float numDups =
              numSampledRows_ / static_cast<float>(approxNumDistinct);
          approxNumDistinct = std::min<float>(numRows_, numRows_ / numDups);

          // If the type is an integer type, num distincts cannot be larger than
          // max - min.
          if (auto* ints = dynamic_cast<dwrf::IntegerStatisticsBuilder*>(
                  statsBuilders[i].get())) {
            auto min = ints->getMinimum();
            auto max = ints->getMaximum();
            if (min.has_value() && max.has_value()) {
              auto range = max.value() - min.value();
              approxNumDistinct = std::min<float>(approxNumDistinct, range);
            }
          }
        }

        columns()[type_->nameOf(i)]->setNumDistinct(approxNumDistinct);
      }
    }
  }
}

} // namespace facebook::velox::runner
