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

import com.facebook.presto.plugin.jdbc.JdbcClient;
import com.facebook.presto.plugin.jdbc.JdbcMetadata;
import com.facebook.presto.plugin.jdbc.JdbcMetadataCache;
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import jakarta.inject.Inject;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class MySqlMetadataFactory
        extends JdbcMetadataFactory
{
    private final MySqlConfig mySqlConfig;

    @Inject
    public MySqlMetadataFactory(
            JdbcMetadataCache jdbcMetadataCache,
            JdbcClient jdbcClient,
            JdbcMetadataConfig config,
            TableLocationProvider tableLocationProvider,
            MySqlConfig mySqlConfig)
    {
        super(jdbcMetadataCache, jdbcClient, config, tableLocationProvider);
        verify(jdbcClient instanceof MySqlClient, "jdbcClient must be a MySqlClient, got %s", jdbcClient.getClass().getName());
        this.mySqlConfig = requireNonNull(mySqlConfig, "mySqlConfig is null");
    }

    @Override
    public JdbcMetadata create()
    {
        JdbcMetadataCache transactionMetadataCache = metadataTransactionCacheEnabled ?
                JdbcMetadataCache.createTransactionCache(jdbcMetadataCache, metadataTransactionCacheMaximumSize) :
                jdbcMetadataCache;
        return new MySqlMetadata(
                transactionMetadataCache,
                (MySqlClient) jdbcClient,
                allowDropTable,
                tableLocationProvider,
                mySqlConfig);
    }
}
