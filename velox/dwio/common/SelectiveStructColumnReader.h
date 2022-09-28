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

#include "velox/dwio/common/SelectiveColumnReaderInternal.h"

namespace facebook::velox::dwio::common {

class SelectiveStructColumnReader : public SelectiveColumnReader {
 public:
  SelectiveStructColumnReader(
      const std::shared_ptr<const dwio::common::TypeWithId>& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& dataType,
      FormatParams& params,
      velox::common::ScanSpec& scanSpec)
      : SelectiveColumnReader(dataType, params, scanSpec, dataType->type),
        requestedType_(requestedType),
        debugString_(
            getExceptionContext().message(VeloxException::Type::kSystem)) {}

  void resetFilterCaches() override {
    for (auto& child : children_) {
      child->resetFilterCaches();
    }
  }

  uint64_t skip(uint64_t numValues) override;

  void next(
      uint64_t numValues,
      VectorPtr& result,
      const uint64_t* incomingNulls) override;

  std::vector<uint32_t> filterRowGroups(
      uint64_t rowGroupSize,
      const dwio::common::StatsContext& context) const override;

  bool rowGroupMatches(uint32_t rowGroupId) const override;

  void read(vector_size_t offset, RowSet rows, const uint64_t* incomingNulls)
      override;

  void getValues(RowSet rows, VectorPtr* result) override;

  uint64_t numReads() const {
    return numReads_;
  }

  vector_size_t lazyVectorReadOffset() const {
    return lazyVectorReadOffset_;
  }

  /// Advance field reader to the row group closest to specified offset by
  /// calling seekToRowGroup.
  virtual void advanceFieldReader(
      SelectiveColumnReader* reader,
      vector_size_t offset) = 0;

  // Returns the nulls bitmap from reading this. Used in LazyVector loaders.
  const uint64_t* nulls() const {
    return nullsInReadRange_ ? nullsInReadRange_->as<uint64_t>() : nullptr;
  }

  void setReadOffsetRecursive(vector_size_t readOffset) override {
    readOffset_ = readOffset;
    for (auto& child : children_) {
      child->setReadOffsetRecursive(readOffset);
    }
  }

  void setIsTopLevel() override {
    isTopLevel_ = true;
    if (!formatData_->hasNulls()) {
      for (auto& child : children_) {
        child->setIsTopLevel();
      }
    }
  }

  // Sets 'rows' as the set of rows for which 'this' or its children
  // may be loaded as LazyVectors. When a struct is loaded as lazy,
  // its children will be lazy if the struct does not add nulls. The
  // children will reference the struct reader, whih must have a live
  // and up-to-date set of rows for which children can be loaded.
  void setLoadableRows(RowSet rows) {
    setOutputRows(rows);
    inputRows_ = outputRows_;
  }

  const std::string& debugString() const {
    return debugString_;
  }

 protected:
  const std::shared_ptr<const dwio::common::TypeWithId> requestedType_;
  std::vector<std::unique_ptr<SelectiveColumnReader>> children_;
  // Sequence number of output batch. Checked against ColumnLoaders
  // created by 'this' to verify they are still valid at load.
  uint64_t numReads_ = 0;
  vector_size_t lazyVectorReadOffset_;

  // Dense set of rows to read in next().
  raw_vector<vector_size_t> rows_;

  // Context information obtained from ExceptionContext. Stored here
  // so that LazyVector readers under this can add this to their
  // ExceptionContext. Allows contextualizing reader errors to split
  // and query. Set at construction, which takes place on first
  // use. If no ExceptionContext is in effect, this is "".
  const std::string debugString_;
};

} // namespace facebook::velox::dwio::common
