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

#include "velox/dwio/common/SelectiveStructColumnReader.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

class SelectiveStructColumnReaderBase
    : public dwio::common::SelectiveStructColumnReaderBase {
 public:
  SelectiveStructColumnReaderBase(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      bool isRoot = false,
      bool generateLazyChildren = true)
      : dwio::common::SelectiveStructColumnReaderBase(
            requestedType,
            fileType,
            params,
            scanSpec,
            isRoot,
            generateLazyChildren),
        rowsPerRowGroup_(formatData_->rowsPerRowGroup().value()) {
    VELOX_CHECK_EQ(fileType_->id(), fileType->id(), "working on the same node");
  }

  void seekTo(int64_t offset, bool readsNullsOnly) override;

  void seekToRowGroup(int64_t index) override {
    seekToRowGroupFixedRowsPerRowGroup(index, rowsPerRowGroup_);
  }

  /// Advance field reader to the row group closest to specified offset by
  /// calling seekToRowGroup.
  void advanceFieldReader(SelectiveColumnReader* reader, int64_t offset)
      override {
    advanceFieldReaderFixedRowsPerRowGroup(reader, offset, rowsPerRowGroup_);
  }

 private:
  const int32_t rowsPerRowGroup_;
};

class SelectiveStructColumnReader : public SelectiveStructColumnReaderBase {
 public:
  SelectiveStructColumnReader(
      const dwio::common::ColumnReaderOptions& columnReaderOptions,
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      bool isRoot = false);

 private:
  void addChild(std::unique_ptr<SelectiveColumnReader> child) {
    children_.push_back(child.get());
    childrenOwned_.push_back(std::move(child));
  }

  std::vector<std::unique_ptr<SelectiveColumnReader>> childrenOwned_;
};

} // namespace facebook::velox::dwrf
