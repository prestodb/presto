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
import com.google.common.collect.ImmutableMap;

import java.util.Map;

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
    public Map<SchemaTableName, ConnectorViewDefinition> getViews(ConnectorSession session, SchemaTablePrefix prefix)
    {
        // When datasource-managed views are enabled, Presto does not analyze the view
        // definition — MySQL resolves it natively. Return empty so views appear as tables.
        if (datasourceManagedViewsEnabled) {
            return ImmutableMap.of();
        }
        return super.getViews(session, prefix);
    }
}
