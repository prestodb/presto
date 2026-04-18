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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import jakarta.inject.Inject;

public class SqlServerMetadataFactory
        extends JdbcMetadataFactory
{
    private final JdbcMetadataCache jdbcMetadataCache;
    private final SqlServerClient sqlServerClient;
    private final boolean allowDropTable;
    private final TableLocationProvider tableLocationProvider;

    @Inject
    public SqlServerMetadataFactory(
            JdbcMetadataCache jdbcMetadataCache,
            JdbcClient jdbcClient,
            JdbcMetadataConfig config,
            TableLocationProvider tableLocationProvider)
    {
        super(jdbcMetadataCache, jdbcClient, config, tableLocationProvider);
        this.jdbcMetadataCache = jdbcMetadataCache;
        this.sqlServerClient = (SqlServerClient) jdbcClient;
        this.allowDropTable = config.isAllowDropTable();
        this.tableLocationProvider = tableLocationProvider;
    }

    @Override
    public JdbcMetadata create()
    {
        return new SqlServerMetadata(jdbcMetadataCache, sqlServerClient, allowDropTable, tableLocationProvider);
    }
}
