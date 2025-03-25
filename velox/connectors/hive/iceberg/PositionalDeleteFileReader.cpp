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

#include "velox/connectors/hive/iceberg/PositionalDeleteFileReader.h"

#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/iceberg/IcebergDeleteFile.h"
#include "velox/connectors/hive/iceberg/IcebergMetadataColumns.h"
#include "velox/dwio/common/ReaderFactory.h"

namespace facebook::velox::connector::hive::iceberg {

PositionalDeleteFileReader::PositionalDeleteFileReader(
    const IcebergDeleteFile& deleteFile,
    const std::string& baseFilePath,
    FileHandleFactory* fileHandleFactory,
    const ConnectorQueryCtx* connectorQueryCtx,
    folly::Executor* executor,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const std::shared_ptr<io::IoStatistics>& ioStats,
    const std::shared_ptr<filesystems::File::IoStats>& fsStats,
    dwio::common::RuntimeStatistics& runtimeStats,
    uint64_t splitOffset,
    const std::string& connectorId)
    : deleteFile_(deleteFile),
      baseFilePath_(baseFilePath),
      fileHandleFactory_(fileHandleFactory),
      executor_(executor),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(hiveConfig),
      ioStats_(ioStats),
      fsStats_(fsStats),
      pool_(connectorQueryCtx->memoryPool()),
      filePathColumn_(IcebergMetadataColumn::icebergDeleteFilePathColumn()),
      posColumn_(IcebergMetadataColumn::icebergDeletePosColumn()),
      splitOffset_(splitOffset),
      deleteSplit_(nullptr),
      deleteRowReader_(nullptr),
      deletePositionsOutput_(nullptr),
      deletePositionsOffset_(0),
      totalNumRowsScanned_(0) {
  VELOX_CHECK(deleteFile_.content == FileContent::kPositionalDeletes);
  VELOX_CHECK(deleteFile_.recordCount);

  // TODO: check if the lowerbounds and upperbounds in deleteFile overlap with
  //  this batch. If not, no need to proceed.

  // Create the ScanSpec for this delete file
  auto scanSpec = std::make_shared<common::ScanSpec>("<root>");
  scanSpec->addField(posColumn_->name, 0);
  auto* pathSpec = scanSpec->getOrCreateChild(filePathColumn_->name);
  pathSpec->setFilter(std::make_unique<common::BytesValues>(
      std::vector<std::string>({baseFilePath_}), false));

  // Create the file schema (in RowType) and split that will be used by readers
  std::vector<std::string> deleteColumnNames(
      {filePathColumn_->name, posColumn_->name});
  std::vector<std::shared_ptr<const Type>> deleteColumnTypes(
      {filePathColumn_->type, posColumn_->type});
  RowTypePtr deleteFileSchema =
      ROW(std::move(deleteColumnNames), std::move(deleteColumnTypes));

  deleteSplit_ = std::make_shared<HiveConnectorSplit>(
      connectorId,
      deleteFile_.filePath,
      deleteFile_.fileFormat,
      0,
      deleteFile_.fileSizeInBytes);

  // Create the Reader and RowReader

  dwio::common::ReaderOptions deleteReaderOpts(pool_);
  configureReaderOptions(
      hiveConfig_,
      connectorQueryCtx,
      deleteFileSchema,
      deleteSplit_,
      /*tableParameters=*/{},
      deleteReaderOpts);

  auto deleteFileHandleCachePtr =
      fileHandleFactory_->generate(deleteFile_.filePath);
  auto deleteFileInput = createBufferedInput(
      *deleteFileHandleCachePtr,
      deleteReaderOpts,
      connectorQueryCtx,
      ioStats_,
      fsStats_,
      executor_);

  auto deleteReader =
      dwio::common::getReaderFactory(deleteReaderOpts.fileFormat())
          ->createReader(std::move(deleteFileInput), deleteReaderOpts);

  // Check if the whole delete file split can be skipped. This could happen when
  // 1) the delete file doesn't contain the base file that is being read; 2) The
  // delete file does not contain the positions in the current batch for the
  // base file.
  if (!testFilters(
          scanSpec.get(),
          deleteReader.get(),
          deleteSplit_->filePath,
          deleteSplit_->partitionKeys,
          {},
          hiveConfig_->readTimestampPartitionValueAsLocalTime(
              connectorQueryCtx_->sessionProperties()))) {
    // We only count the number of base splits skipped as skippedSplits runtime
    // statistics in Velox.  Skipped delta split is only counted as skipped
    // bytes.
    runtimeStats.skippedSplitBytes += deleteSplit_->length;
    deleteSplit_.reset();
    return;
  }

  dwio::common::RowReaderOptions deleteRowReaderOpts;
  configureRowReaderOptions(
      {},
      scanSpec,
      nullptr,
      deleteFileSchema,
      deleteSplit_,
      nullptr,
      nullptr,
      deleteRowReaderOpts);

  deleteRowReader_.reset();
  deleteRowReader_ = deleteReader->createRowReader(deleteRowReaderOpts);
}

void PositionalDeleteFileReader::readDeletePositions(
    uint64_t baseReadOffset,
    uint64_t size,
    BufferPtr deleteBitmapBuffer) {
  // We are going to read to the row number up to the end of the batch. For the
  // same base file, the deleted rows are in ascending order in the same delete
  // file. rowNumberUpperBound is the upperbound for the row number in this
  // batch, excluding boundaries
  int64_t rowNumberUpperBound = splitOffset_ + baseReadOffset + size;

  // Finish unused delete positions from last batch. Note that at this point we
  // don't know how many rows the base row reader would scan yet.
  if (deletePositionsOutput_ &&
      deletePositionsOffset_ < deletePositionsOutput_->size()) {
    updateDeleteBitmap(
        std::dynamic_pointer_cast<RowVector>(deletePositionsOutput_)
            ->childAt(0),
        baseReadOffset,
        rowNumberUpperBound,
        deleteBitmapBuffer);

    if (readFinishedForBatch(rowNumberUpperBound)) {
      return;
    }
  }

  if (!deleteRowReader_ || !deleteSplit_) {
    return;
  }

  // Read the new delete positions for this batch into deletePositionsOutput_
  // and update the delete bitmap

  auto outputType = posColumn_->type;
  RowTypePtr outputRowType = ROW({posColumn_->name}, {posColumn_->type});
  if (!deletePositionsOutput_) {
    deletePositionsOutput_ = BaseVector::create(outputRowType, 0, pool_);
  }

  do {
    auto rowsScanned = deleteRowReader_->next(size, deletePositionsOutput_);
    totalNumRowsScanned_ += rowsScanned;

    if (rowsScanned > 0) {
      VELOX_CHECK(
          !deletePositionsOutput_->mayHaveNulls(),
          "Iceberg delete file pos column cannot have nulls");

      auto numDeletedRows = deletePositionsOutput_->size();
      if (numDeletedRows > 0) {
        deletePositionsOutput_->loadedVector();
        deletePositionsOffset_ = 0;

        // Convert the row numbers to set bits, up to rowNumberUpperBound.
        // Beyond that the buffer of deleteBitMap is not available.
        updateDeleteBitmap(
            std::dynamic_pointer_cast<RowVector>(deletePositionsOutput_)
                ->childAt(0),
            baseReadOffset,
            rowNumberUpperBound,
            deleteBitmapBuffer);
      }
    } else {
      // Reaching the end of the file
      deleteSplit_.reset();
      break;
    }
  } while (!readFinishedForBatch(rowNumberUpperBound));
}

bool PositionalDeleteFileReader::noMoreData() {
  return totalNumRowsScanned_ >= deleteFile_.recordCount &&
      deletePositionsOutput_ &&
      deletePositionsOffset_ >= deletePositionsOutput_->size();
}

void PositionalDeleteFileReader::updateDeleteBitmap(
    VectorPtr deletePositionsVector,
    uint64_t baseReadOffset,
    int64_t rowNumberUpperBound,
    BufferPtr deleteBitmapBuffer) {
  auto deleteBitmap = deleteBitmapBuffer->asMutable<uint8_t>();

  // Convert the positions in file into positions relative to the start of the
  // split.
  const int64_t* deletePositions =
      deletePositionsVector->as<FlatVector<int64_t>>()->rawValues();
  int64_t rowNumberLowerBound = baseReadOffset + splitOffset_;

  // If the rowNumberLowerBound is greater than the last position in this delete
  // rows batch, nothing to delete.
  if (rowNumberLowerBound >
      deletePositions[deletePositionsVector->size() - 1]) {
    return;
  }

  // Skip the delete positions in deletePositionsVector until they are in the
  // [rowNumberLowerBound, rowNumberUpperBound) range.
  while (deletePositionsOffset_ < deletePositionsVector->size() &&
         deletePositions[deletePositionsOffset_] < rowNumberLowerBound) {
    deletePositionsOffset_++;
  }
  while (deletePositionsOffset_ < deletePositionsVector->size() &&
         deletePositions[deletePositionsOffset_] < rowNumberUpperBound) {
    bits::setBit(
        deleteBitmap,
        deletePositions[deletePositionsOffset_] - rowNumberLowerBound);
    deletePositionsOffset_++;
  }

  // There might be multiple delete files for a single base file. The size of
  // the deleteBitmapBuffer should be the largest position among all delte files
  deleteBitmapBuffer->setSize(std::max(
      static_cast<uint64_t>(deleteBitmapBuffer->size()),
      deletePositionsOffset_ == 0 ||
              (deletePositionsOffset_ < deletePositionsVector->size() &&
               deletePositions[deletePositionsOffset_] >= rowNumberUpperBound)
          ? 0
          : bits::nbytes(
                deletePositions[deletePositionsOffset_ - 1] + 1 -
                rowNumberLowerBound)));
}

bool PositionalDeleteFileReader::readFinishedForBatch(
    int64_t rowNumberUpperBound) {
  VELOX_CHECK_NOT_NULL(deletePositionsOutput_);

  auto deletePositionsVector =
      std::dynamic_pointer_cast<RowVector>(deletePositionsOutput_)->childAt(0);
  const int64_t* deletePositions =
      deletePositionsVector->as<FlatVector<int64_t>>()->rawValues();

  // We've read enough of the delete positions from this delete file when 1) it
  // reaches the end of the file, or 2) the last read delete position is greater
  // than the largest base file row number that is going to be read in this
  // batch
  if (totalNumRowsScanned_ >= deleteFile_.recordCount ||
      (deletePositionsVector->size() != 0 &&
       (deletePositionsOffset_ < deletePositionsVector->size() &&
        deletePositions[deletePositionsOffset_] >= rowNumberUpperBound))) {
    return true;
  }
  return false;
}

} // namespace facebook::velox::connector::hive::iceberg
