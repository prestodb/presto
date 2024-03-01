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
package com.facebook.presto.spi.prerequisites;

import com.facebook.presto.spi.ConnectorId;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryPrerequisitesContext
{
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String query;
    private final Map<String, String> systemProperties;
    private final Map<ConnectorId, Map<String, String>> connectorProperties;

    public QueryPrerequisitesContext(
            Optional<String> catalog,
            Optional<String> schema,
            String query,
            Map<String, String> systemProperties,
            Map<ConnectorId, Map<String, String>> connectorProperties)
    {
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.query = requireNonNull(query, "query is null");
        this.systemProperties = requireNonNull(systemProperties, "systemProperties is null");
        this.connectorProperties = requireNonNull(connectorProperties, "connectorProperties is null");
    }

    public Optional<String> getCatalog()
    {
        return catalog;
    }

    public Optional<String> getSchema()
    {
        return schema;
    }

    public String getQuery()
    {
        return query;
    }

    public Map<String, String> getSystemProperties()
    {
        return systemProperties;
    }

    public Map<ConnectorId, Map<String, String>> getConnectorProperties()
    {
        return connectorProperties;
    }
}
