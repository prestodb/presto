/*
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

namespace facebook::presto {

struct SystemSplit : public velox::connector::ConnectorSplit {
  explicit SystemSplit(
      const std::string& connectorId,
      const std::string& schemaName,
      const std::string& tableName)
      : ConnectorSplit(connectorId),
        schemaName_(schemaName),
        tableName_(tableName) {}

  const std::string& schemaName() {
    return schemaName_;
  }

  const std::string& tableName() {
    return tableName_;
  }

 private:
  const std::string schemaName_;
  const std::string tableName_;
};

} // namespace facebook::presto
