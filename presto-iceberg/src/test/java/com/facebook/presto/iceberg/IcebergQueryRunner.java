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
import com.facebook.presto.common.RuntimeStats;
import com.facebook.presto.connector.jmx.JmxPlugin;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveColumnConverterProvider;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpcds.TpcdsPlugin;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.function.BiFunction;

import static com.facebook.airlift.log.Level.ERROR;
import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.hive.HiveTestUtils.getDataDirectoryPath;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
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
    public static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext("test_user", "test_queryId", Optional.empty(), Collections.emptySet(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, WarningCollector.NOOP, new RuntimeStats());

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
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, createTpchTables, addJmxPlugin, nodeCount, Optional.empty(), dataDirectory);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables,
            boolean addJmxPlugin,
            OptionalInt nodeCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher,
            Optional<Path> dataDirectory)
            throws Exception
    {
        return createIcebergQueryRunner(extraProperties, extraConnectorProperties, format, createTpchTables, addJmxPlugin, nodeCount, externalWorkerLauncher, dataDirectory, false);
    }

    public static DistributedQueryRunner createIcebergQueryRunner(
            Map<String, String> extraProperties,
            Map<String, String> extraConnectorProperties,
            FileFormat format,
            boolean createTpchTables,
            boolean addJmxPlugin,
            OptionalInt nodeCount,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher,
            Optional<Path> dataDirectory,
            boolean addStorageFormatToPath)
            throws Exception
    {
        setupLogging();

        Session session = testSessionBuilder()
                .setCatalog(ICEBERG_CATALOG)
                .setSchema("tpch")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(session)
                .setExtraProperties(extraProperties)
                .setDataDirectory(dataDirectory)
                .setNodeCount(nodeCount.orElse(4))
                .setExternalWorkerLauncher(externalWorkerLauncher)
                .build();

        queryRunner.installPlugin(new TpchPlugin());
        queryRunner.createCatalog("tpch", "tpch");

        queryRunner.installPlugin(new TpcdsPlugin());
        queryRunner.createCatalog("tpcds", "tpcds");

        queryRunner.installPlugin(new IcebergPlugin());

        String catalogType = extraConnectorProperties.getOrDefault("iceberg.catalog.type", HIVE.name());
        Path icebergDataDirectory = getIcebergDataDirectoryPath(queryRunner.getCoordinator().getDataDirectory(), catalogType, format, addStorageFormatToPath);

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

        if (catalogType == HIVE.name()) {
            ExtendedHiveMetastore metastore = getFileHiveMetastore(icebergDataDirectory);
            if (!metastore.getDatabase(METASTORE_CONTEXT, "tpch").isPresent()) {
                queryRunner.execute("CREATE SCHEMA tpch");
            }
            if (!metastore.getDatabase(METASTORE_CONTEXT, "tpcds").isPresent()) {
                queryRunner.execute("CREATE SCHEMA tpcds");
            }
        }
        else {
            queryRunner.execute("CREATE SCHEMA tpch");
            queryRunner.execute("CREATE SCHEMA tpcds");
        }

        if (createTpchTables) {
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, session, TpchTable.getTables(), true);
        }

        return queryRunner;
    }

    private static ExtendedHiveMetastore getFileHiveMetastore(Path dataDirectory)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, dataDirectory.toFile().toURI().toString(), "test");
    }

    public static Path getIcebergDataDirectoryPath(Path dataDirectory, String catalogType, FileFormat format, boolean addStorageFormatToPath)
    {
        Path icebergDataDirectory = addStorageFormatToPath ? dataDirectory.resolve(TEST_DATA_DIRECTORY).resolve(format.name())
                : dataDirectory.resolve(TEST_DATA_DIRECTORY);
        Path icebergCatalogDirectory = icebergDataDirectory.resolve(catalogType);
        return icebergCatalogDirectory;
    }

    private static Map<String, String> getConnectorProperties(CatalogType icebergCatalogType, Path icebergDataDirectory)
    {
        switch (icebergCatalogType) {
            case HADOOP:
            case REST:
            case NESSIE:
                try {
                    if (!Files.exists(icebergDataDirectory)) {
                        Files.createDirectories(icebergDataDirectory);
                    }
                }
                catch (IOException e) {
                    throw new PrestoException(GENERIC_INTERNAL_ERROR, "cannot create Iceberg catalog directory " + icebergDataDirectory, e);
                }
                return ImmutableMap.of("iceberg.catalog.warehouse", icebergDataDirectory.toFile().toURI().toString());
            case HIVE:
                return ImmutableMap.of(
                        "hive.metastore", "file",
                        "hive.metastore.catalog.dir", icebergDataDirectory.toFile().toURI().toString());
        }
        throw new PrestoException(NOT_SUPPORTED, "Unsupported Presto Iceberg catalog type " + icebergCatalogType);
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("com.facebook.presto.event", WARN);
        logging.setLevel("com.facebook.presto.security.AccessControlManager", WARN);
        logging.setLevel("com.facebook.presto.server.PluginManager", WARN);
        logging.setLevel("com.facebook.airlift.bootstrap.LifeCycleManager", WARN);
        logging.setLevel("org.apache.parquet.hadoop", WARN);
        logging.setLevel("org.eclipse.jetty.server.handler.ContextHandler", WARN);
        logging.setLevel("org.eclipse.jetty.server.AbstractConnector", WARN);
        logging.setLevel("org.glassfish.jersey.internal.inject.Providers", ERROR);
        logging.setLevel("parquet.hadoop", WARN);
        logging.setLevel("org.apache.iceberg", WARN);
        logging.setLevel("com.facebook.airlift.bootstrap", WARN);
        logging.setLevel("org.apache.hadoop.io.compress", WARN);
    }

    public static void main(String[] args)
            throws Exception
    {
        setupLogging();
        Optional<Path> dataDirectory;
        if (args.length > 0) {
            if (args.length != 1) {
                log.error("usage: IcebergQueryRunner [dataDirectory]\n");
                log.error("       [dataDirectory] is a local directory under which you want the iceberg_data directory to be created.]\n");
                System.exit(1);
            }
            dataDirectory = getDataDirectoryPath(Optional.of(args[0]));
        }
        else {
            dataDirectory = getDataDirectoryPath(Optional.empty());
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
