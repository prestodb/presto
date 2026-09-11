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
import com.facebook.presto.plugin.jdbc.JdbcMetadataConfig;
import com.facebook.presto.plugin.jdbc.JdbcMetadataFactory;
import com.facebook.presto.plugin.jdbc.TableLocationProvider;
import jakarta.inject.Inject;

import static java.util.Objects.requireNonNull;

public class RedshiftMetadataFactory
        extends JdbcMetadataFactory
{
    private final JdbcMetadataCache jdbcMetadataCache;
    private final RedshiftClient redshiftClient;
    private final boolean allowDropTable;
    private final TableLocationProvider tableLocationProvider;
    private final RedshiftConfig redshiftConfig;

    @Inject
    public RedshiftMetadataFactory(
            JdbcMetadataCache jdbcMetadataCache,
            RedshiftClient redshiftClient,
            JdbcMetadataConfig config,
            TableLocationProvider tableLocationProvider,
            RedshiftConfig redshiftConfig)
    {
        super(jdbcMetadataCache, redshiftClient, config, tableLocationProvider);
        this.jdbcMetadataCache = requireNonNull(jdbcMetadataCache, "jdbcMetadataCache is null");
        this.redshiftClient = requireNonNull(redshiftClient, "jdbcClient is null");
        this.allowDropTable = requireNonNull(config, "config is null").isAllowDropTable();
        this.tableLocationProvider = requireNonNull(tableLocationProvider, "tableLocationProvider is null");
        this.redshiftConfig = requireNonNull(redshiftConfig, "redshiftConfig is null");
    }

    @Override
    public JdbcMetadata create()
    {
        return new RedshiftMetadata(jdbcMetadataCache, redshiftClient, allowDropTable, tableLocationProvider, redshiftConfig);
    }
}
