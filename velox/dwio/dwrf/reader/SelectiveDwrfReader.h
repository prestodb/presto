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

#include "velox/dwio/common/SelectiveColumnReader.h"
#include "velox/dwio/dwrf/reader/ColumnReader.h"
#include "velox/dwio/dwrf/reader/DwrfData.h"

namespace facebook::velox::dwrf {

// Wrapper for static functions for making DWRF readers
class SelectiveDwrfReader {
 public:
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      DwrfParams& params,
      common::ScanSpec& scanSpec,
      bool isRoot = false);

  // Compatibility wrapper for tests. Takes the components of DwrfParams as
  // separate.
  static std::unique_ptr<dwio::common::SelectiveColumnReader> build(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      dwio::common::ColumnReaderStatistics& stats,
      common::ScanSpec* scanSpec,
      FlatMapContext flatMapContext = {},
      bool isRoot = false) {
    auto params = DwrfParams(stripe, streamLabels, stats, flatMapContext);
    return build(requestedType, fileType, params, *scanSpec, isRoot);
  }
};

class SelectiveColumnReaderFactory : public ColumnReaderFactory {
 public:
  explicit SelectiveColumnReaderFactory(
      std::shared_ptr<common::ScanSpec> scanSpec)
      : scanSpec_(scanSpec) {}

  std::unique_ptr<dwio::common::SelectiveColumnReader> buildSelective(
      const TypePtr& requestedType,
      const std::shared_ptr<const dwio::common::TypeWithId>& fileType,
      StripeStreams& stripe,
      const StreamLabels& streamLabels,
      dwio::common::ColumnReaderStatistics& stats,
      FlatMapContext flatMapContext = {}) {
    auto params =
        DwrfParams(stripe, streamLabels, stats, std::move(flatMapContext));
    auto reader =
        SelectiveDwrfReader::build(requestedType, fileType, params, *scanSpec_);
    reader->setIsTopLevel();
    return reader;
  }

 private:
  std::shared_ptr<common::ScanSpec> const scanSpec_;
};
} // namespace facebook::velox::dwrf
