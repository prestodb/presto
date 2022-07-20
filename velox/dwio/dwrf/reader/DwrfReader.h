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

#include "velox/dwio/common/ReaderFactory.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/DwrfReaderShared.h"
#include "velox/dwio/dwrf/reader/SelectiveDwrfReader.h"

namespace facebook::velox::dwrf {

class DwrfRowReader : public DwrfRowReaderShared {
 protected:
  void resetColumnReaderImpl() override {
    columnReader_.reset();
    selectiveColumnReader_.reset();
  }

  void createColumnReaderImpl(StripeStreams& stripeStreams) override {
    auto scanSpec = options_.getScanSpec().get();
    auto requestedType = getColumnSelector().getSchemaWithId();
    auto dataType = getReader().getSchemaWithId();
    auto flatMapContext = FlatMapContext::nonFlatMapContext();

    if (scanSpec) {
      selectiveColumnReader_ = SelectiveDwrfReader::build(
          requestedType, dataType, stripeStreams, scanSpec, flatMapContext);
      selectiveColumnReader_->setIsTopLevel();
    } else {
      columnReader_ = ColumnReader::build(
          requestedType, dataType, stripeStreams, flatMapContext);
    }
    DWIO_ENSURE(
        (columnReader_ != nullptr) != (selectiveColumnReader_ != nullptr),
        "ColumnReader was not created");
  }

  void seekImpl() override {
    if (selectiveColumnReader_) {
      selectiveColumnReader_->skip(currentRowInStripe);
    } else {
      columnReader_->skip(currentRowInStripe);
    }
  }

 public:
  /**
   * Constructor that lets the user specify additional options.
   * @param contents of the file
   * @param options options for reading
   */
  DwrfRowReader(
      const std::shared_ptr<ReaderBase>& reader,
      const dwio::common::RowReaderOptions& options)
      : DwrfRowReaderShared{reader, options} {}

  ~DwrfRowReader() override = default;

  // Returns number of rows read. Guaranteed to be less then or equal to size.
  uint64_t next(uint64_t size, VectorPtr& result) override;

  void updateRuntimeStats(
      dwio::common::RuntimeStatistics& stats) const override {
    stats.skippedStrides += skippedStrides_;
  }

  void resetFilterCaches() override;

  // Returns the skipped strides for 'stripe'. Used for testing.
  std::optional<std::vector<uint32_t>> stridesToSkip(uint32_t stripe) const {
    auto it = stripeStridesToSkip_.find(stripe);
    if (it == stripeStridesToSkip_.end()) {
      return std::nullopt;
    }
    return it->second;
  }

 private:
  void checkSkipStrides(const StatsContext& context, uint64_t strideSize);

  std::unique_ptr<ColumnReader> columnReader_;
  std::unique_ptr<SelectiveColumnReader> selectiveColumnReader_;
  std::vector<uint32_t> stridesToSkip_;
  // Record of strides to skip in each visited stripe. Used for diagnostics.
  std::unordered_map<uint32_t, std::vector<uint32_t>> stripeStridesToSkip_;
  // Number of skipped strides.
  int64_t skippedStrides_{0};

  // Set to true after clearing filter caches, i.e.  adding a dynamic
  // filter. Causes filters to be re-evaluated against stride stats on
  // next stride instead of next stripe.
  bool recomputeStridesToSkip_{false};
};

class DwrfReader : public DwrfReaderShared {
 public:
  /**
   * Constructor that lets the user specify additional options.
   * @param contents of the file
   * @param options options for reading
   * @param fileLength the length of the file in bytes
   * @param postscriptLength the length of the postscript in bytes
   */
  DwrfReader(
      const dwio::common::ReaderOptions& options,
      std::unique_ptr<dwio::common::InputStream> input)
      : DwrfReaderShared{options, std::move(input)} {}

  ~DwrfReader() override = default;

  std::unique_ptr<dwio::common::RowReader> createRowReader(
      const dwio::common::RowReaderOptions& options = {}) const override;

  std::unique_ptr<DwrfRowReader> createDwrfRowReader(
      const dwio::common::RowReaderOptions& options = {}) const;

  /**
   * Create a reader to the for the dwrf file.
   * @param stream the stream to read
   * @param options the options for reading the file
   */
  static std::unique_ptr<DwrfReader> create(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options);

  friend class E2EEncryptionTest;
};

class DwrfReaderFactory : public dwio::common::ReaderFactory {
 public:
  DwrfReaderFactory() : ReaderFactory(dwio::common::FileFormat::DWRF) {}

  std::unique_ptr<dwio::common::Reader> createReader(
      std::unique_ptr<dwio::common::InputStream> stream,
      const dwio::common::ReaderOptions& options) override {
    return DwrfReader::create(std::move(stream), options);
  }
};

void registerDwrfReaderFactory();

void unregisterDwrfReaderFactory();

} // namespace facebook::velox::dwrf
