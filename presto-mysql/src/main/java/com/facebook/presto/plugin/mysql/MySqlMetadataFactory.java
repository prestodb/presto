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
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

public class MySqlMetadataFactory
        extends JdbcMetadataFactory
{
    private final JdbcMetadataCache jdbcMetadataCache;
    private final MySqlClient mySqlClient;
    private final boolean allowDropTable;
    private final boolean metadataTransactionCacheEnabled;
    private final long metadataTransactionCacheMaximumSize;
    private final TableLocationProvider tableLocationProvider;
    private final MySqlConfig mySqlConfig;

    @Inject
    public MySqlMetadataFactory(JdbcMetadataCache jdbcMetadataCache, MySqlClient mySqlClient, JdbcMetadataConfig config, TableLocationProvider tableLocationProvider, MySqlConfig mySqlConfig)
    {
        super(jdbcMetadataCache, mySqlClient, config, tableLocationProvider);
        this.jdbcMetadataCache = requireNonNull(jdbcMetadataCache, "jdbcMetadataCache is null");
        this.mySqlClient = requireNonNull(mySqlClient, "mySqlClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
        this.metadataTransactionCacheEnabled = config.isMetadataTransactionCacheEnabled();
        this.metadataTransactionCacheMaximumSize = config.getMetadataTransactionCacheMaximumSize();
        this.tableLocationProvider = requireNonNull(tableLocationProvider, "tableLocationProvider is null");
        this.mySqlConfig = requireNonNull(mySqlConfig, "mySqlConfig is null");
    }

    @Override
    public JdbcMetadata create()
    {
        // Overriding create() bypasses the transaction cache that JdbcMetadataFactory sets up, so
        // repeat it here. Without this, metadata-transaction-cache-enabled and
        // metadata-transaction-cache-maximum-size would be silently ignored for MySQL catalogs.
        JdbcMetadataCache transactionMetadataCache = metadataTransactionCacheEnabled ?
                JdbcMetadataCache.createTransactionCache(jdbcMetadataCache, metadataTransactionCacheMaximumSize) :
                jdbcMetadataCache;
        return new MySqlMetadata(
                transactionMetadataCache,
                mySqlClient,
                allowDropTable,
                tableLocationProvider,
                mySqlConfig);
    }
}
