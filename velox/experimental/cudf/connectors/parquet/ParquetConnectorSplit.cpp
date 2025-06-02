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

#include "velox/experimental/cudf/connectors/parquet/ParquetConnectorSplit.h"

#include <string>

namespace facebook::velox::cudf_velox::connector::parquet {

std::string ParquetConnectorSplit::toString() const {
  return fmt::format("Parquet: {}", filePath);
}

std::string ParquetConnectorSplit::getFileName() const {
  const auto i = filePath.rfind('/');
  return i == std::string::npos ? filePath : filePath.substr(i + 1);
}

// static
std::shared_ptr<ParquetConnectorSplit> ParquetConnectorSplit::create(
    const folly::dynamic& obj) {
  const auto connectorId = obj["connectorId"].asString();
  const auto splitWeight = obj["splitWeight"].asInt();
  const auto filePath = obj["filePath"].asString();

  return std::make_shared<ParquetConnectorSplit>(
      connectorId, filePath, splitWeight);
}

} // namespace facebook::velox::cudf_velox::connector::parquet
