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

import java.io.File;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
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

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Optional<Path> dataDirectory)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of(), dataDirectory);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, CatalogType catalogType)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, ImmutableMap.of("iceberg.catalog.type", catalogType.name()), Optional.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, CatalogType catalogType, Map<String, String> extraConnectorProperties)
            throws Exception
    {
        return createIcebergQueryRunner(
                extraProperties,
                ImmutableMap.<String, String>builder()
                        .putAll(extraConnectorProperties)
                        .put("iceberg.catalog.type", catalogType.name())
                        .build(),
                Optional.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Map<String, String> extraConnectorProperties)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, Optional.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Map<String, String> extraConnectorProperties, Optional<Path> dataDirectory)
            throws Exception
    {
        FileFormat defaultFormat = new IcebergConfig().getFileFormat();
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, defaultFormat, true, dataDirectory);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(Map<String, String> extraProperties, Map<String, String> extraConnectorProperties, FileFormat format)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, true, Optional.empty());
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables,
            Optional<Path> dataDirectory)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, createTpchTables, false, OptionalInt.empty(), dataDirectory);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables,
            boolean addJmxPlugin,
            OptionalInt nodeCount,
            Optional<Path> dataDirectory)
            throws Exception
    {
        Logging logger = Logging.initialize();
        logger.setLevel("org.apache.iceberg", WARN);
        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner.Builder queryRunnerBuilder = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .setDataDirectory(dataDirectory);

        nodeCount.ifPresent(queryRunnerBuilder::setNodeCount);

        DistributedQueryRunner queryRunner = queryRunnerBuilder.build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        Path icebergDataDirectory = queryRunner.getCoordinator().getDataDirectory();

        queryRunner.installPlugin(new IcebergPlugin());

        String catalogType = extraConnectorProperties.getOrDefault("iceberg.catalog.type", HIVE.name());
        Map<String, String> icebergProperties = ImmutableMap.<String, String>builder()
                .put("iceberg.file-format", format.name())
                .putAll(getConnectorProperties(CatalogType.valueOf(catalogType), icebergDataDirectory))
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
        Optional<Path> dataDirectory = Optional.empty();
        if (args.length > 0) {
            if (args.length != 1) {
                log.error("usage: IcebergQueryRunner [dataDirectory]\n");
                log.error("       [dataDirectory] is a local directory under which you want the iceberg_data directory to be created.]\n");
                System.exit(1);
            }

            File dataDirectoryFile = new File(args[0]);
            if (dataDirectoryFile.exists()) {
                if (!dataDirectoryFile.isDirectory()) {
                    log.error("Error: " + dataDirectoryFile.getAbsolutePath() + " is not a directory.");
                    System.exit(1);
                }
                else if (!dataDirectoryFile.canRead() || !dataDirectoryFile.canWrite()) {
                    log.error("Error: " + dataDirectoryFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }
            else {
                // For user supplied path like [path_exists_but_is_not_readable_or_writable]/[paths_do_not_exist], the hadoop file system won't
                // be able to create directory for it. e.g. "/aaa/bbb" is not creatable because path "/" is not writable.
                while (!dataDirectoryFile.exists()) {
                    dataDirectoryFile = dataDirectoryFile.getParentFile();
                }
                if (!dataDirectoryFile.canRead() || !dataDirectoryFile.canWrite()) {
                    log.error("Error: The ancestor directory " + dataDirectoryFile.getAbsolutePath() + " is not readable/writable.");
                    System.exit(1);
                }
            }

            dataDirectory = Optional.of(dataDirectoryFile.toPath());
        }

        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        DistributedQueryRunner queryRunner = null;
        try {
            queryRunner = createIcebergQueryRunner(properties, dataDirectory);
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
