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
#pragma once

#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/FileHandle.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/BufferedInput.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/common/ScanSpec.h"
#include "velox/exec/OperatorUtils.h"
#include "velox/expression/Expr.h"

namespace facebook::velox::connector::hive {

class HiveDataSource : public DataSource {
 public:
  HiveDataSource(
      const RowTypePtr& outputType,
      const std::shared_ptr<connector::ConnectorTableHandle>& tableHandle,
      const std::unordered_map<
          std::string,
          std::shared_ptr<connector::ColumnHandle>>& columnHandles,
      FileHandleFactory* fileHandleFactory,
      velox::memory::MemoryPool* pool,
      core::ExpressionEvaluator* expressionEvaluator,
      memory::MemoryAllocator* allocator,
      const std::string& scanId,
      folly::Executor* executor);

  void addSplit(std::shared_ptr<ConnectorSplit> split) override;

  std::optional<RowVectorPtr> next(uint64_t size, velox::ContinueFuture& future)
      override;

  void addDynamicFilter(
      column_index_t outputChannel,
      const std::shared_ptr<common::Filter>& filter) override;

  uint64_t getCompletedBytes() override {
    return ioStats_->rawBytesRead();
  }

  uint64_t getCompletedRows() override {
    return completedRows_;
  }

  std::unordered_map<std::string, RuntimeCounter> runtimeStats() override;

  bool allPrefetchIssued() const override {
    return rowReader_ && rowReader_->allPrefetchIssued();
  }

  void setFromDataSource(std::unique_ptr<DataSource> sourceUnique) override;

  int64_t estimatedRowSize() override;

  // Internal API, made public to be accessible in unit tests.  Do not use in
  // other places.
  static std::shared_ptr<common::ScanSpec> makeScanSpec(
      const SubfieldFilters& filters,
      const RowTypePtr& rowType,
      const std::vector<const HiveColumnHandle*>& columnHandles,
      const std::vector<common::Subfield>& remainingFilterInputs,
      memory::MemoryPool* pool);

 protected:
  virtual uint64_t readNext(uint64_t size) {
    return rowReader_->next(size, output_);
  }

  std::unique_ptr<dwio::common::BufferedInput> createBufferedInput(
      const FileHandle&,
      const dwio::common::ReaderOptions&);

  virtual std::unique_ptr<dwio::common::RowReader> createRowReader(
      dwio::common::RowReaderOptions& options) {
    return reader_->createRowReader(options);
  }

  std::shared_ptr<HiveConnectorSplit> split_;
  FileHandleFactory* fileHandleFactory_;
  dwio::common::ReaderOptions readerOpts_;
  memory::MemoryPool* pool_;
  VectorPtr output_;
  RowTypePtr readerOutputType_;
  std::unique_ptr<dwio::common::RowReader> rowReader_;

 private:
  // Evaluates remainingFilter_ on the specified vector. Returns number of rows
  // passed. Populates filterEvalCtx_.selectedIndices and selectedBits if only
  // some rows passed the filter. If none or all rows passed
  // filterEvalCtx_.selectedIndices and selectedBits are not updated.
  vector_size_t evaluateRemainingFilter(RowVectorPtr& rowVector);

  void setConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type,
      const velox::variant& value) const;

  void setNullConstantValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const TypePtr& type) const;

  void setPartitionValue(
      common::ScanSpec* FOLLY_NONNULL spec,
      const std::string& partitionKey,
      const std::optional<std::string>& value) const;

  // Clear split_ after split has been fully processed.  Keep readers around to
  // hold adaptation.
  void resetSplit();

  void configureRowReaderOptions(dwio::common::RowReaderOptions&) const;

  const RowTypePtr outputType_;
  // Column handles for the partition key columns keyed on partition key column
  // name.
  std::unordered_map<std::string, std::shared_ptr<HiveColumnHandle>>
      partitionKeys_;
  std::shared_ptr<dwio::common::IoStatistics> ioStats_;
  std::shared_ptr<common::ScanSpec> scanSpec_;
  std::shared_ptr<common::MetadataFilter> metadataFilter_;
  dwio::common::RowReaderOptions rowReaderOpts_;
  std::unique_ptr<dwio::common::Reader> reader_;
  std::unique_ptr<exec::ExprSet> remainingFilterExprSet_;
  bool emptySplit_;

  dwio::common::RuntimeStatistics runtimeStats_;

  std::shared_ptr<FileHandle> fileHandle_;
  core::ExpressionEvaluator* expressionEvaluator_;
  uint64_t completedRows_ = 0;

  // Reusable memory for remaining filter evaluation.
  VectorPtr filterResult_;
  SelectivityVector filterRows_;
  exec::FilterEvalCtx filterEvalCtx_;

  memory::MemoryAllocator* const allocator_;
  const std::string& scanId_;
  folly::Executor* executor_;
};

} // namespace facebook::velox::connector::hive
