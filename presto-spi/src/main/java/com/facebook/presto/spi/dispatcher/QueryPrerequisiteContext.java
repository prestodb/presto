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
package com.facebook.presto.spi.dispatcher;

import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.security.Identity;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class QueryPrerequisiteContext
{
    private final QueryId queryId;
    private final Identity identity;
    private final Optional<String> catalog;
    private final Optional<String> schema;
    private final String query;
    private final Map<String, String> systemProperties;
    private final Map<ConnectorId, Map<String, String>> connectorProperties;
    private final Map<String, Map<String, String>> unprocessedCatalogProperties;

    public QueryPrerequisiteContext(
            QueryId queryId,
            Identity identity,
            Optional<String> catalog,
            Optional<String> schema,
            String query,
            Map<String, String> systemProperties,
            Map<ConnectorId, Map<String, String>> connectorProperties,
            Map<String, Map<String, String>> unprocessedCatalogProperties)
    {
        this.queryId = requireNonNull(queryId, "queryId is null");
        this.identity = requireNonNull(identity, "identity is null");
        this.catalog = requireNonNull(catalog, "catalog is null");
        this.schema = requireNonNull(schema, "schema is null");
        this.query = requireNonNull(query, "query is null");
        this.systemProperties = requireNonNull(systemProperties, "systemProperties is null");
        this.connectorProperties = requireNonNull(connectorProperties, "connectorProperties is null");
        this.unprocessedCatalogProperties = requireNonNull(unprocessedCatalogProperties, "unprocessedCatalogProperties is null");
    }

    public QueryId getQueryId()
    {
        return queryId;
    }

    public Identity getIdentity()
    {
        return identity;
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

    public Map<String, Map<String, String>> getUnprocessedCatalogProperties()
    {
        return unprocessedCatalogProperties;
    }
}
