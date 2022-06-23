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

#include <string>

namespace facebook::presto::protocol {

// There are two concepts connector name and catalog name.
// Catalog name is inferred from the catalog file name. For
// example: catalog/foo.properties defines catalog "foo". Each worker loads the
// catalogs and sends the catalog names to the coordinator. Catalog names are
// defined by the system administrator. Connector names on the other hand are
// fixed hard-coded names given to connectors. Connector names are specified via
// "connector.name" property in the catalog file. For example: "hive" and
// "hive-hadoop2" are two different Hive connectors sharing the same
// implementation. Two catalogs could use the same connector name. The "type"
// property in the connector-specific protocol messages specifies the connector
// name. In Presto, multiple connectors re-use the same protocol data
// structures. The registration below maps connector names that re-use the same
// protocol implementations. For example: "hive" and "hive-hadoop2" use the same
// HivePartitionHandle implementation. The map stores a pair of (connectorName,
// connectorKey) entries. Each connectorName uses the corresponding connectorKey
// to determine the protocol structures.

// Register known Hive connectors
void registerHiveConnectors();

bool registerConnector(
    const std::string& connectorName,
    const std::string& connectorKey);

bool unregisterConnector(const std::string& connectorName);

// Return the connectorKey used to infer the protocol implementations.
const std::string& getConnectorKey(const std::string& connectorName);
} // namespace facebook::presto::protocol
