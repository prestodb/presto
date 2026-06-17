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
package com.facebook.presto.plugin.redshift;

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

public class RedshiftMetadata
        extends JdbcMetadata
{
    private final RedshiftClient redshiftClient;
    private final boolean datasourceManagedViewsEnabled;

    public RedshiftMetadata(
            JdbcMetadataCache jdbcMetadataCache,
            RedshiftClient redshiftClient,
            boolean allowDropTable,
            TableLocationProvider tableLocationProvider,
            RedshiftConfig redshiftConfig)
    {
        super(jdbcMetadataCache, redshiftClient, allowDropTable, tableLocationProvider);
        this.redshiftClient = requireNonNull(redshiftClient, "redshiftClient is null");
        this.datasourceManagedViewsEnabled = redshiftConfig.isDatasourceManagedViewsEnabled();
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        redshiftClient.createView(session, viewMetadata.getTable(), viewData, replace);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        if (datasourceManagedViewsEnabled) {
            return ImmutableMap.of();
        }

        List<SchemaTableName> viewNames;
        if (prefix.getSchemaName() != null && prefix.getTableName() != null) {
            if (!redshiftClient.isView(session, prefix.getSchemaName(), prefix.getTableName())) {
                return ImmutableMap.of();
            }
            viewNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            viewNames = listViews(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        return redshiftClient.getViews(session, viewNames);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        if (schemaName.isEmpty()) {
            return redshiftClient.listViewsFromAllSchemas(session);
        }
        return redshiftClient.listViews(session, schemaName);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        redshiftClient.dropView(session, viewName);
    }
}
