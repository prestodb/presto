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
package com.facebook.presto.iceberg;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.apache.iceberg.FileFormat;

import java.nio.file.Path;
import java.util.Map;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class IcebergQueryRunner
{
    private static final Logger log = Logger.get(IcebergQueryRunner.class);

    public static final String ICEBERG_CATALOG = "iceberg";

    private IcebergQueryRunner() {}

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties) throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createNativeIcebergQueryRunner(Map<String, String> extraProperties, CatalogType catalogType) throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of(
                "iceberg.native-mode", "true",
                "iceberg.catalog.type", catalogType.name()));
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Map<String, String> extraConnectorProperties)
            throws Exception
    {
        FileFormat defaultFormat = new IcebergConfig().getFileFormat();
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, defaultFormat, true);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Map<String, String> extraConnectorProperties, FileFormat format)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, true);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables)
            throws Exception
    {
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDir = queryRunner.getCoordinator().getBaseDataDir().resolve("iceberg_data");
        Path catalogDir = dataDir.getParent().resolve("catalog");

        queryRunner.installPlugin(new IcebergPlugin());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("hive.metastore", "file")
                .put("hive.metastore.catalog.dir", catalogDir.toFile().toURI().toString())
                .put("iceberg.file-format", format.name())
                .put("iceberg.catalog.warehouse", dataDir.getParent().toFile().toURI().toString())
                .putAll(extraConnectorProperties)
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        queryRunner.execute("CREATE SCHEMA tpch");

        if (createTpchTables) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());
        }

        return queryRunner;
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = createIcebergQueryRunner(properties);
        }
        catch (Throwable t) {
            log.error(t);
            System.exit(1);
        }
        Thread.sleep(10);
        Logger log = Logger.get(IcebergQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
