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
    private final boolean datasourceManagedViewsEnabled;

    public MySqlMetadata(JdbcMetadataCache jdbcMetadataCache, MySqlClient client, boolean allowDropTable, TableLocationProvider tableLocationProvider, MySqlConfig mySqlConfig)
    {
        super(jdbcMetadataCache, client, allowDropTable, tableLocationProvider);
        requireNonNull(mySqlConfig, "mySqlConfig is null");
        this.datasourceManagedViewsEnabled = mySqlConfig.isDatasourceManagedViewsEnabled();
    }

    @Override
    public List<SchemaTableName> listViews(ConnectorSession session, Optional<String> schemaName)
    {
        // Listed views would be reported by SHOW VIEWS and typed as views in information_schema
        // even though getViews reports none of them, so passthrough mode lists none either.
        if (datasourceManagedViewsEnabled) {
            return ImmutableList.of();
        }
        return super.listViews(session, schemaName);
    }

    @Override
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // Reporting no views leaves MySQL to resolve the view definition itself: Presto never
        // analyzes the stored SQL and reaches the view through the normal table flow instead.
        if (datasourceManagedViewsEnabled) {
            return ImmutableMap.of();
        }
        return super.getViews(session, prefix);
    }
}
