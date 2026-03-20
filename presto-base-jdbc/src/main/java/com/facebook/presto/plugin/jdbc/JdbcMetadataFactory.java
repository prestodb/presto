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
package com.facebook.presto.plugin.jdbc;

import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

public class JdbcMetadataFactory
{
    protected final JdbcMetadataCache jdbcMetadataCache;
    protected final JdbcClient jdbcClient;
    protected final boolean allowDropTable;
    protected final TableLocationProvider tableLocationProvider;

    @Inject
    public JdbcMetadataFactory(JdbcMetadataCache jdbcMetadataCache, JdbcClient jdbcClient, JdbcMetadataConfig config, TableLocationProvider tableLocationProvider)
    {
        this.jdbcMetadataCache = requireNonNull(jdbcMetadataCache, "jdbcMetadataCache is null");
        this.jdbcClient = requireNonNull(jdbcClient, "jdbcClient is null");
        requireNonNull(config, "config is null");
        this.allowDropTable = config.isAllowDropTable();
        this.tableLocationProvider = requireNonNull(tableLocationProvider, "tableLocationProvider is null");
    }

    public JdbcMetadata create()
    {
        // Check if this is an Oracle client and create appropriate metadata
        String clientClassName = jdbcClient.getClass().getName();
        if (clientClassName.contains("OracleClient")) {
            try {
                // Use reflection to create OracleMetadata
                Class<?> oracleClientClass = Class.forName("com.facebook.presto.plugin.oracle.OracleClient");
                Class<?> oracleMetadataClass = Class.forName("com.facebook.presto.plugin.oracle.OracleMetadata");
                if (oracleClientClass.isInstance(jdbcClient)) {
                    return (JdbcMetadata) oracleMetadataClass
                            .getConstructor(
                                    JdbcMetadataCache.class,
                                    oracleClientClass,
                                    boolean.class,
                                    TableLocationProvider.class)
                            .newInstance(jdbcMetadataCache, jdbcClient, allowDropTable, tableLocationProvider);
                }
            }
            catch (Exception e) {
                // Fall back to default if reflection fails
            }
        }
        return createMetadata();
    }

    protected JdbcMetadata createMetadata()
    {
        return new JdbcMetadata(jdbcMetadataCache, jdbcClient, allowDropTable, tableLocationProvider);
    }
}
