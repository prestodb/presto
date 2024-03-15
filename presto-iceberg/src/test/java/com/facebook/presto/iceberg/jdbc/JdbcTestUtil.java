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
package com.facebook.presto.iceberg.jdbc;

import com.facebook.presto.hive.gcs.HiveGcsConfig;
import com.facebook.presto.hive.gcs.HiveGcsConfigurationInitializer;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ConfigurationUpdater;
import com.facebook.presto.iceberg.IcebergCatalogName;
import com.facebook.presto.iceberg.IcebergConfig;
import com.facebook.presto.iceberg.IcebergResourceFactory;
import com.facebook.presto.iceberg.nessie.NessieConfig;
import com.facebook.presto.testing.containers.JdbcContainer;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.iceberg.CatalogType.JDBC;
import static com.facebook.presto.iceberg.IcebergQueryRunner.ICEBERG_CATALOG;

public class JdbcTestUtil
{
    private JdbcTestUtil()
    {
    }

    public static Map<String, String> jdbcConnectorProperties(String serverUrl)
    {
        return ImmutableMap.of("iceberg.catalog.type", JDBC.name(), "iceberg.jdbc.connection-url", serverUrl,
                "iceberg.jdbc.user", "postgres", "iceberg.jdbc.password", "password",
                "iceberg.jdbc.driver-class", "org.postgresql.Driver");
    }

    public static IcebergResourceFactory getIcebergResourceFactory(IcebergConfig icebergConfig, JdbcContainer jdbcContainer)
    {
        JdbcConfig jdbcConfig = new JdbcConfig();
        jdbcConfig.setServerUrl(jdbcContainer.getJdbcURI());
        jdbcConfig.setUser(jdbcContainer.getDefaultUser());
        jdbcConfig.setPassword(jdbcContainer.getDefaultPassword());
        jdbcConfig.setDriverClass(jdbcContainer.getDefaultDriverClass());

        return new IcebergResourceFactory(icebergConfig,
                new IcebergCatalogName(ICEBERG_CATALOG),
                new NessieConfig(),
                jdbcConfig,
                new PrestoS3ConfigurationUpdater(new HiveS3Config()),
                new HiveGcsConfigurationInitializer(new HiveGcsConfig()));
    }
}
