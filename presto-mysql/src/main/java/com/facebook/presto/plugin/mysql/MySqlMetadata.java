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
package com.facebook.presto.plugin.mysql;

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

public class MySqlMetadata
        extends JdbcMetadata
{
    private final MySqlClient mySqlClient;
    private final boolean datasourceManagedViewsEnabled;

    public MySqlMetadata(JdbcMetadataCache jdbcMetadataCache, MySqlClient client, boolean allowDropTable, TableLocationProvider tableLocationProvider, MySqlConfig mySqlConfig)
    {
        super(jdbcMetadataCache, client, allowDropTable, tableLocationProvider);
        this.mySqlClient = requireNonNull(client, "client is null");
        requireNonNull(mySqlConfig, "mySqlConfig is null");
        this.datasourceManagedViewsEnabled = mySqlConfig.isDatasourceManagedViewsEnabled();
    }

    @Override
    public void createView(ConnectorSession session, ConnectorTableMetadata viewMetadata, String viewData, boolean replace)
    {
        mySqlClient.createView(session, viewMetadata, viewData, replace);
    }

    @Override
    public void renameView(ConnectorSession session, SchemaTableName viewName, SchemaTableName newViewName)
    {
        mySqlClient.renameView(session, viewName, newViewName);
    }

    @Override
    public void dropView(ConnectorSession session, SchemaTableName viewName)
    {
        mySqlClient.dropView(session, viewName);
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        return mySqlClient.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // Skip view analysis, resolve via normal table flow.
        if (datasourceManagedViewsEnabled) {
            return ImmutableMap.of();
        }

        List<SchemaTableName> tableNames;
        if (prefix.getTableName() != null) {
            tableNames = ImmutableList.of(new SchemaTableName(prefix.getSchemaName(), prefix.getTableName()));
        }
        else {
            tableNames = listViews(session, Optional.ofNullable(prefix.getSchemaName()));
        }
        return mySqlClient.getViews(session, tableNames);
    }
}
