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

#include <cstddef>
#include <cstdint>
#include <optional>
#include <string>

#include "velox/dwio/common/InputStream.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/common/TypeWithId.h"
#include "velox/type/Type.h"
#include "velox/vector/BaseVector.h"

namespace facebook::velox::dwio::common {

/**
 * Abstract row reader interface.
 *
 * RowReader objects are used to fetch a specified subset of rows
 * and columns from a file.
 *
 * RowReader objects are created through Reader objects.
 */
class RowReader {
 public:
  virtual ~RowReader() = default;

  /**
   * Fetch the next portion of rows.
   * @param size Max number of rows to read
   * @param result output vector
   * @return number of fetched rows, 0 if there are no more rows to read
   */
  virtual uint64_t next(uint64_t size, velox::VectorPtr& result) = 0;

  /**
   * Update current reader statistics. The set of updated values is
   * implementation specific and depends on a format of a file being read.
   * @param stats stats to update
   */
  virtual void updateRuntimeStats(RuntimeStatistics& stats) const = 0;

  /**
   * This method should be called whenever filter is modified in a ScanSpec
   * object passed to Reader::createRowReader to create this object.
   */
  virtual void resetFilterCaches() = 0;

  // Moves the adaptively acquired filters/filter order from 'other'
  // to 'this'. Returns true if 'this' is ready to read, false if
  // 'this' is known to be empty.
  virtual bool moveAdaptationFrom(RowReader& /*other*/) {
    return true;
  }

  /**
   * Get an estimated row size basing on available statistics. Can
   * differ from the actual row size due to variable-length values.
   * @return Estimate of the row size or std::nullopt if cannot estimate.
   */
  virtual std::optional<size_t> estimatedRowSize() const = 0;

  // Returns true if the expected IO for 'this' is scheduled. If this
  // is true it makes sense to prefetch the next split.
  virtual bool allPrefetchIssued() const {
    return false;
  }
};

/**
 * Abstract reader class.
 *
 * Reader object is used to process a single file. It provides
 * basic file information like data schema and statistics.
 *
 * To fetch the actual data RowReader should be created using
 * createRowReader method.
 *
 * Reader objects are created through factories implementing
 * ReaderFactory interface.
 */
class Reader {
 public:
  virtual ~Reader() = default;

  /**
   * Get the total number of rows in a file.
   * @return the total number of rows in a file
   */
  virtual std::optional<uint64_t> numberOfRows() const = 0;

  /**
   * Get statistics for a specified column.
   * @param index column index
   * @return column statisctics
   */
  virtual std::unique_ptr<ColumnStatistics> columnStatistics(
      uint32_t index) const = 0;

  /**
   * Get the file schema.
   * @return file schema
   */
  virtual const velox::RowTypePtr& rowType() const = 0;

  /**
   * Get the file schema attributed with type and column ids.
   * @return file schema
   */
  virtual const std::shared_ptr<const TypeWithId>& typeWithId() const = 0;

  /**
   * Create row reader object to fetch the data.
   * @param options Row reader options describing the data to fetch
   * @return Row reader
   */
  virtual std::unique_ptr<RowReader> createRowReader(
      const RowReaderOptions& options = {}) const = 0;
};

} // namespace facebook::velox::dwio::common
