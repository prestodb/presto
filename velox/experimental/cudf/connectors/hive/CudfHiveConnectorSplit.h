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
#include "velox/dwio/common/Options.h"

namespace cudf {
namespace io {
struct source_info;
}
} // namespace cudf

#include <memory>
#include <string>

namespace facebook::velox::cudf_velox::connector::hive {

struct CudfHiveConnectorSplit
    : public facebook::velox::connector::ConnectorSplit {
  const std::string filePath;
  const facebook::velox::dwio::common::FileFormat fileFormat{
      facebook::velox::dwio::common::FileFormat::PARQUET};
  const std::unique_ptr<cudf::io::source_info> cudfSourceInfo;

  CudfHiveConnectorSplit(
      const std::string& connectorId,
      const std::string& _filePath,
      int64_t _splitWeight = 0)
      : facebook::velox::connector::ConnectorSplit(connectorId, _splitWeight),
        filePath(_filePath),
        cudfSourceInfo(std::make_unique<cudf::io::source_info>(filePath)) {}

  std::string toString() const override;
  std::string getFileName() const;

  const cudf::io::source_info& getCudfSourceInfo() const {
    return *cudfSourceInfo;
  }

  static std::shared_ptr<CudfHiveConnectorSplit> create(
      const folly::dynamic& obj);
};

class CudfHiveConnectorSplitBuilder {
 public:
  explicit CudfHiveConnectorSplitBuilder(std::string filePath)
      : filePath_{std::move(filePath)} {}

  CudfHiveConnectorSplitBuilder& splitWeight(int64_t splitWeight) {
    splitWeight_ = splitWeight;
    return *this;
  }

  CudfHiveConnectorSplitBuilder& connectorId(const std::string& connectorId) {
    connectorId_ = connectorId;
    return *this;
  }

  std::shared_ptr<CudfHiveConnectorSplit> build() const {
    return std::make_shared<CudfHiveConnectorSplit>(
        connectorId_, filePath_, splitWeight_);
  }

 private:
  const std::string filePath_;
  std::string connectorId_;
  int64_t splitWeight_{0};
};

} // namespace facebook::velox::cudf_velox::connector::hive
