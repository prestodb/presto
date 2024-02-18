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
#include "velox/dwio/common/Mutation.h"
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
  static constexpr int64_t kAtEnd = -1;

  virtual ~RowReader() = default;

  /**
   * Fetch the next portion of rows.
   * @param size Max number of rows to read
   * @param result output vector
   * @param mutation The mutation to be applied during the read, null means no
   *  mutation
   * @return number of rows scanned in the file (including any rows filtered out
   *  or deleted in mutation), 0 if there are no more rows to read.
   */
  virtual uint64_t next(
      uint64_t size,
      velox::VectorPtr& result,
      const Mutation* mutation = nullptr) = 0;

  /**
   * Return the next row number that will be scanned in the next next() call,
   * kAtEnd when at end of file.  This row number is relative to beginning of
   * the file (0 for the first row), including all rows in the file, no matter
   * whether it's deleted or filtered during the previous next() call.
   *
   * This function is mainly used to compute the bit mask used for mutation.
   * Given a list of row numbers in the file, we can calculate the offset of
   * each rows in the bit mask based on value returned from this call.
   */
  virtual int64_t nextRowNumber() = 0;

  /**
   * Given the max number of rows to read, return the actual number of rows that
   * will be scanned, including any rows to be deleted or filtered.  Return
   * kAtEnd when at end of file.  This is also used to compute the bit mask used
   * in mutation.
   */
  virtual int64_t nextReadSize(uint64_t size) = 0;

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

  enum class FetchResult {
    kFetched, // This function did the fetch
    kInProgress, // Another thread already started the IO
    kAlreadyFetched // Another thread already finished the IO
  };

  // Struct describing 1 prefetch unit. A prefetch unit is defined by
  // a rowCount and a function, that when called, will trigger the prefetch
  // or report that it was already triggered.
  struct PrefetchUnit {
    // Number of rows in the prefetch unit
    uint64_t rowCount;
    // Task to trigger the prefetch for this unit
    std::function<FetchResult()> prefetch;
  };

  /**
   * Returns a vector of PrefetchUnit objects describing all the prefetch units
   * owned by this RowReader. For example, a returned vector {{50, func1}, {50,
   * func2}} would represent a RowReader which has 2 prefetch units (for
   * example, a stripe for dwrf and alpha file formats). Each prefetch unit has
   * 50 rows, and func1 and func2 represent callables which will run the
   * prefetch and report a FetchResult. The FetchResult reports if the prefetch
   * was completed by the caller, if the prefetch was in progress when the
   * function was called or if the prefetch was already completed, as a result
   * of i.e. calling next and having the main thread load the stripe.
   * @return std::nullopt if the reader implementation does not support
   * prefetching.
   */
  virtual std::optional<std::vector<PrefetchUnit>> prefetchUnits() {
    return std::nullopt;
  }

  /**
   * Helper function used by non-selective reader to project top level columns
   * according to the scan spec.
   */
  static VectorPtr projectColumns(
      const VectorPtr& input,
      const velox::common::ScanSpec&);
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
