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
package com.facebook.presto.plugin.sqlserver;

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

import static java.util.Objects.requireNonNull;

public class SqlServerMetadata
        extends JdbcMetadata
{
    private final SqlServerClient sqlServerClient;

    public SqlServerMetadata(
            JdbcMetadataCache jdbcMetadataCache,
            SqlServerClient sqlServerClient,
            boolean allowDropTable,
            TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, sqlServerClient, allowDropTable, tableLocationProvider);
        this.sqlServerClient = requireNonNull(sqlServerClient, "sqlServerClient is null");
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // Determine which views to retrieve based on the prefix
        List<SchemaTableName> viewNames;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            // Specific view requested
            viewNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else if (prefix.getSchemaName() != null) {
            // All views in a specific schema
            viewNames = sqlServerClient.listViews(session, Optional.of(prefix.getSchemaName()));
        }
        else {
            return ImmutableMap.of();
        }

        if (viewNames.isEmpty()) {
            return ImmutableMap.of();
        }

        // Retrieve view definitions for the identified views
        return sqlServerClient.getViews(session, viewNames);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return sqlServerClient.listViews(session, schemaName);
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        sqlServerClient.createView(session, viewMetadata.getTable(), viewData, replace);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        sqlServerClient.dropView(session, viewName);
    }
}
