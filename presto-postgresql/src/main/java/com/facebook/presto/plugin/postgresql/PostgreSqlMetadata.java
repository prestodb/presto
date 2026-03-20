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
package com.facebook.presto.plugin.postgresql;

import com.facebook.presto.plugin.jdbc.JdbcIdentity;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.ConnectorViewDefinition;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.SchemaTablePrefix;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * PostgreSQL-specific metadata implementation that adds view support.
 * This class extends JdbcMetadata to provide PostgreSQL-specific view operations
 * including listing views and retrieving view definitions from information_schema.
 */
public class PostgreSqlMetadata
        extends JdbcMetadata
{
    private final PostgreSqlClient postgreSqlClient;

    public PostgreSqlMetadata(
            JdbcMetadataCache jdbcMetadataCache,
            PostgreSqlClient postgreSqlClient,
            boolean allowDropTable,
            TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, postgreSqlClient, allowDropTable, tableLocationProvider);
        this.postgreSqlClient = postgreSqlClient;
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);

        // Determine which views to retrieve based on the prefix
        List<SchemaTableName> viewNames;

        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            // Specific view requested
            viewNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else if (prefix.getSchemaName() != null) {
            // All views in a specific schema
            viewNames = postgreSqlClient.listViews(session, identity, Optional.of(prefix.getSchemaName()));
        }
        else {
            // All views in all schemas
            viewNames = postgreSqlClient.listSchemasForViews(session, identity);
        }

        if (viewNames.isEmpty()) {
            return ImmutableMap.of();
        }

        // Retrieve view definitions for the identified views
        return postgreSqlClient.getViews(session, identity, viewNames);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        return postgreSqlClient.listViews(session, identity, schemaName);
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        postgreSqlClient.createView(session, identity, viewMetadata.getTable(), viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        JdbcIdentity identity = JdbcIdentity.from(session);
        postgreSqlClient.dropView(session, identity, viewName);
    }
}
