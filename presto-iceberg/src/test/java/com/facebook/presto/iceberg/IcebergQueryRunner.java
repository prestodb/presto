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
import com.facebook.presto.connector.jmx.JmxPlugin;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import org.apache.iceberg.FileFormat;

import java.nio.file.Path;
import java.util.Map;
import java.util.OptionalInt;

import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;

public final class IcebergQueryRunner
{
    private static final Logger log = Logger.get(IcebergQueryRunner.class);

    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_DATA_DIRECTORY = "iceberg_data";
    public static final String TEST_CATALOG_DIRECTORY = "catalog";

    private IcebergQueryRunner() {}

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, CatalogType catalogType)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of("iceberg.catalog.type", catalogType.name()));
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, CatalogType catalogType, Map<String, String> extraConnectorProperties)
            throws Exception
    {
        return createIcebergQueryRunner(
                extraProperties,
                ImmutableMap.<String, String>builder()
                        .putAll(extraConnectorProperties)
                        .put("iceberg.catalog.type", catalogType.name())
                        .build());
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
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, createTpchTables, false, OptionalInt.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables,
            boolean addJmxPlugin,
            OptionalInt nodeCount)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("org.apache.iceberg", WARN);
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties);

        nodeCount.ifPresent(queryRunnerBuilder::setNodeCount);

        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path dataDirectory = queryRunner.getCoordinator().getDataDirectory();

        queryRunner.installPlugin(new IcebergPlugin());

        String catalogType = extraConnectorProperties.getOrDefault("iceberg.catalog.type", HIVE.name());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.file-format", format.name())
                .putAll(getConnectorProperties(CatalogType.valueOf(catalogType), dataDirectory))
                .putAll(extraConnectorProperties)
                .build();

        queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", icebergProperties);

        if (addJmxPlugin) {
            queryRunner.installPlugin(new JmxPlugin());
            queryRunner.createCatalog("jmx", "jmx");
        }

        queryRunner.execute("CREATE SCHEMA tpch");

        if (createTpchTables) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables());
        }

        return queryRunner;
    }

    private static Map<String, String> getConnectorProperties(CatalogType icebergCatalogType, Path icebergDataDirectory)
    {
        Path testDataDirectory = icebergDataDirectory.resolve(TEST_DATA_DIRECTORY);
        switch (icebergCatalogType) {
            case HADOOP:
            case NESSIE:
                return ImmutableMap.of("iceberg.catalog.warehouse", testDataDirectory.getParent().toFile().toURI().toString());
            case HIVE:
                Path testCatalogDirectory = testDataDirectory.getParent().resolve(TEST_CATALOG_DIRECTORY);
                return ImmutableMap.of(
                        "hive.metastore", "file",
                        "hive.metastore.catalog.dir", testCatalogDirectory.toFile().toURI().toString());
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported Presto Iceberg catalog type " + icebergCatalogType);
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
