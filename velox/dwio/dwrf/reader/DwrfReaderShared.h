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

#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Reader.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/ReaderBase.h"
#include "velox/dwio/dwrf/reader/StripeStream.h"

namespace facebook::velox::dwrf {

class DwrfRowReaderShared : public StrideIndexProvider,
                            public StripeReaderBase,
                            public dwio::common::RowReader {
 protected:
  // footer
  std::vector<uint64_t> firstRowOfStripe;
  mutable std::shared_ptr<const dwio::common::TypeWithId> selectedSchema;

  // reading state
  uint64_t previousRow;
  uint32_t firstStripe;
  uint32_t currentStripe;
  uint32_t lastStripe; // the stripe AFTER the last one
  uint64_t currentRowInStripe;
  bool newStripeLoaded;
  uint64_t rowsInCurrentStripe;
  uint64_t strideIndex_;
  std::shared_ptr<StripeDictionaryCache> stripeDictionaryCache_;
  dwio::common::RowReaderOptions options_;
  std::unique_ptr<ColumnReaderFactory> columnReaderFactory_;

  // column selector
  std::shared_ptr<dwio::common::ColumnSelector> columnSelector_;

  // internal methods
  void startNextStripe();

  std::optional<size_t> estimatedRowSizeHelper(
      const proto::Footer& footer,
      const dwio::common::Statistics& stats,
      uint32_t nodeId) const;

  std::shared_ptr<const RowType> getType() const {
    return columnSelector_->getSchema();
  }

  bool isEmptyFile() const {
    return (lastStripe == 0);
  }

  // Actual implementation that depends on column reader
  virtual void resetColumnReaderImpl() = 0;

  virtual void createColumnReaderImpl(StripeStreams& stripeStreams) = 0;

  virtual void seekImpl() = 0;

  void setStrideIndex(uint64_t index) {
    strideIndex_ = index;
  }

 public:
  /**
   * Constructor that lets the user specify additional options.
   * @param contents of the file
   * @param options options for reading
   */
  DwrfRowReaderShared(
      const std::shared_ptr<ReaderBase>& reader,
      const dwio::common::RowReaderOptions& options);

  ~DwrfRowReaderShared() override = default;

  // Select the columns from the options object
  const dwio::common::ColumnSelector& getColumnSelector() const {
    return *columnSelector_;
  }

  const std::shared_ptr<dwio::common::ColumnSelector>& getColumnSelectorPtr()
      const {
    return columnSelector_;
  }

  const dwio::common::RowReaderOptions& getRowReaderOptions() const {
    return options_;
  }

  std::shared_ptr<const dwio::common::TypeWithId> getSelectedType() const {
    if (!selectedSchema) {
      selectedSchema = columnSelector_->buildSelected();
    }

    return selectedSchema;
  }

  uint64_t getRowNumber() const {
    return previousRow;
  }

  uint64_t seekToRow(uint64_t rowNumber);

  uint64_t skipRows(uint64_t numberOfRowsToSkip);

  uint32_t getCurrentStripe() const {
    return currentStripe;
  }

  uint64_t getStrideIndex() const override {
    return strideIndex_;
  }

  // Estimate the space used by the reader
  size_t estimatedReaderMemory() const;

  // Estimate the row size for projected columns
  std::optional<size_t> estimatedRowSize() const override;
};

class DwrfReaderShared : public dwio::common::Reader {
 protected:
  std::shared_ptr<ReaderBase> readerBase_;
  const dwio::common::ReaderOptions& options_;

 public:
  /**
   * Constructor that lets the user specify reader options and input stream.
   */
  DwrfReaderShared(
      const dwio::common::ReaderOptions& options,
      std::unique_ptr<dwio::common::InputStream> input);

  virtual ~DwrfReaderShared() = default;

  CompressionKind getCompression() const {
    return readerBase_->getCompressionKind();
  }

  WriterVersion getWriterVersion() const {
    return readerBase_->getWriterVersion();
  }

  const std::string& getWriterName() const {
    return readerBase_->getWriterName();
  }

  std::vector<std::string> getMetadataKeys() const;

  std::string getMetadataValue(const std::string& key) const;

  bool hasMetadataValue(const std::string& key) const;

  uint64_t getCompressionBlockSize() const {
    return readerBase_->getCompressionBlockSize();
  }

  uint32_t getNumberOfStripes() const {
    return readerBase_->getFooter().stripes_size();
  }

  std::vector<uint64_t> getRowsPerStripe() const {
    return readerBase_->getRowsPerStripe();
  }
  uint32_t strideSize() const {
    return readerBase_->getFooter().rowindexstride();
  }

  std::unique_ptr<StripeInformation> getStripe(uint32_t) const;

  uint64_t getFileLength() const {
    return readerBase_->getFileLength();
  }

  std::unique_ptr<dwio::common::Statistics> getStatistics() const {
    return readerBase_->getStatistics();
  }

  std::unique_ptr<dwio::common::ColumnStatistics> columnStatistics(
      uint32_t nodeId) const override {
    return readerBase_->getColumnStatistics(nodeId);
  }

  const std::shared_ptr<const RowType>& rowType() const override {
    return readerBase_->getSchema();
  }

  const std::shared_ptr<const dwio::common::TypeWithId>& typeWithId()
      const override {
    return readerBase_->getSchemaWithId();
  }

  const proto::PostScript& getPostscript() const {
    return readerBase_->getPostScript();
  }

  const proto::Footer& getFooter() const {
    return readerBase_->getFooter();
  }

  std::optional<uint64_t> numberOfRows() const override {
    auto& footer = readerBase_->getFooter();
    if (footer.has_numberofrows()) {
      return footer.numberofrows();
    }
    return std::nullopt;
  }

  static uint64_t getMemoryUse(
      ReaderBase& readerBase,
      int32_t stripeIx,
      const dwio::common::ColumnSelector& cs);

  uint64_t getMemoryUse(int32_t stripeIx = -1);

  uint64_t getMemoryUseByFieldId(
      const std::vector<uint64_t>& include,
      int32_t stripeIx = -1);

  uint64_t getMemoryUseByName(
      const std::vector<std::string>& names,
      int32_t stripeIx = -1);

  uint64_t getMemoryUseByTypeId(
      const std::vector<uint64_t>& include,
      int32_t stripeIx = -1);
};

} // namespace facebook::velox::dwrf
