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

#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/DwrfReaderShared.h"

namespace facebook::velox::dwrf {

class DwrfRowReader : public DwrfRowReaderShared {
 protected:
  void resetColumnReaderImpl() override {
    columnReader_.reset();
  }

  void createColumnReaderImpl(StripeStreams& stripeStreams) override {
    columnReader_ =
        (options_.getColumnReaderFactory() ? options_.getColumnReaderFactory()
                                           : ColumnReaderFactory::baseFactory())
            ->build(
                getColumnSelector().getSchemaWithId(),
                getReader().getSchemaWithId(),
                stripeStreams);
  }

  void seekImpl() override {
    columnReader_->skip(currentRowInStripe);
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
  uint64_t next(uint64_t size, VectorPtr& result);

  int64_t skippedStrides() const {
    return skippedStrides_;
  }

  ColumnReader* columnReader() {
    return columnReader_.get();
  }

 private:
  void checkSkipStrides(const StatsContext& context, uint64_t strideSize);

  std::unique_ptr<ColumnReader> columnReader_;
  std::vector<uint32_t> stridesToSkip_;

  // Number of skipped strides.
  int64_t skippedStrides_{0};
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

  std::unique_ptr<DwrfRowReader> createRowReader() const;

  std::unique_ptr<DwrfRowReader> createRowReader(
      const dwio::common::RowReaderOptions& options) const;

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

} // namespace facebook::velox::dwrf
