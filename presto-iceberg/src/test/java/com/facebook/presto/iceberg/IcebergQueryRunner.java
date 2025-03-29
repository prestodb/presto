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
import com.facebook.presto.iceberg.hive.IcebergFileHiveMetastore;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tpcds.TpcdsPlugin;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;

import javax.management.MBeanServer;
import javax.management.MBeanServerFactory;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

import static com.facebook.airlift.log.Level.ERROR;
import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.hive.HiveTestUtils.getDataDirectoryPath;
import static com.facebook.presto.iceberg.CatalogType.HIVE;
import static com.facebook.presto.iceberg.FileFormat.PARQUET;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class IcebergQueryRunner
{
    private static final Logger log = Logger.get(IcebergQueryRunner.class);

    public static final String ICEBERG_CATALOG = "iceberg";
    public static final String TEST_DATA_DIRECTORY = "iceberg_data";
    public static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext("test_user", "test_queryId", Optional.empty(), Collections.emptySet(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER, WarningCollector.NOOP, new RuntimeStats());

    private DistributedQueryRunner queryRunner;
    private Map<String, Map<String, String>> icebergCatalogs;

    private IcebergQueryRunner(DistributedQueryRunner queryRunner, Map<String, Map<String, String>> icebergCatalogs)
    {
        this.queryRunner = requireNonNull(queryRunner, "queryRunner is null");
        this.icebergCatalogs = new ConcurrentHashMap<>(requireNonNull(icebergCatalogs, "icebergCatalogs is null"));
    }

    public DistributedQueryRunner getQueryRunner()
    {
        return queryRunner;
    }

    public Map<String, Map<String, String>> getIcebergCatalogs()
    {
        return icebergCatalogs;
    }

    public void addCatalog(String name, Map<String, String> properties)
    {
        queryRunner.createCatalog(name, "iceberg", properties);
        icebergCatalogs.put(name, properties);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private Builder() {}

        private CatalogType catalogType = HIVE;
        private Map<String, Map<String, String>> icebergCatalogs = new HashMap<>();
        private Map<String, String> extraProperties = new HashMap<>();
        private Map<String, String> extraConnectorProperties = new HashMap<>();
        private Map<String, String> tpcdsProperties = new HashMap<>();
        private FileFormat format = PARQUET;
        private boolean createTpchTables = true;
        private boolean addJmxPlugin = true;
        private OptionalInt nodeCount = OptionalInt.of(4);
        private Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();
        private Optional<Path> dataDirectory = Optional.empty();
        private boolean addStorageFormatToPath;
        private Optional<String> schemaName = Optional.empty();

        public Builder setFormat(FileFormat format)
        {
            this.format = format;
            return this;
        }

        public Builder setExternalWorkerLauncher(Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
        {
            this.externalWorkerLauncher = externalWorkerLauncher;
            return this;
        }

        public Builder setSchemaName(String schemaName)
        {
            this.schemaName = Optional.of(schemaName);
            return this;
        }

        public Builder setCreateTpchTables(boolean createTpchTables)
        {
            this.createTpchTables = createTpchTables;
            return this;
        }

        public Builder setAddJmxPlugin(boolean addJmxPlugin)
        {
            this.addJmxPlugin = addJmxPlugin;
            return this;
        }

        public Builder setNodeCount(OptionalInt nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = extraProperties;
            return this;
        }

        public Builder setCatalogType(CatalogType catalogType)
        {
            this.catalogType = catalogType;
            return this;
        }

        public Builder setDataDirectory(Optional<Path> dataDirectory)
        {
            this.dataDirectory = dataDirectory;
            return this;
        }

        public Builder setExtraConnectorProperties(Map<String, String> extraConnectorProperties)
        {
            this.extraConnectorProperties = extraConnectorProperties;
            return this;
        }

        public Builder setAddStorageFormatToPath(boolean addStorageFormatToPath)
        {
            this.addStorageFormatToPath = addStorageFormatToPath;
            return this;
        }

        public Builder setTpcdsProperties(Map<String, String> tpcdsProperties)
        {
            this.tpcdsProperties = tpcdsProperties;
            return this;
        }

        public IcebergQueryRunner build()
                throws Exception
        {
            setupLogging();

            checkArgument(!extraConnectorProperties.containsKey("iceberg.catalog.type"), "extraConnectorProperties cannot contain iceberg.catalog.type");
            checkArgument(!extraConnectorProperties.containsKey("iceberg.file-format"), "extraConnectorProperties cannot contain iceberg.file-format");

            ImmutableMap.Builder<String, Map<String, String>> icebergCatalogs = ImmutableMap.builder();

            Session session = testSessionBuilder()
                    .setCatalog(ICEBERG_CATALOG)
                    .setSchema(schemaName.orElse("tpch"))
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
            queryRunner.createCatalog("tpcds", "tpcds", tpcdsProperties);

            queryRunner.getServers().forEach(server -> {
                MBeanServer mBeanServer = MBeanServerFactory.newMBeanServer();
                server.installPlugin(new IcebergPlugin(mBeanServer));
                if (addJmxPlugin) {
                    server.installPlugin(new JmxPlugin(mBeanServer));
                }
            });

            Path icebergDataDirectory = getIcebergDataDirectoryPath(queryRunner.getCoordinator().getDataDirectory(), catalogType.name(), format, addStorageFormatToPath);

            Map<String, String> icebergProperties = new HashMap<>();
            icebergProperties.put("iceberg.file-format", format.name());
            icebergProperties.put("iceberg.catalog.type", catalogType.name());
            icebergProperties.putAll(getConnectorProperties(catalogType, icebergDataDirectory));
            icebergProperties.putAll(extraConnectorProperties);

            queryRunner.createCatalog(ICEBERG_CATALOG, "iceberg", ImmutableMap.copyOf(icebergProperties));
            icebergCatalogs.put(ICEBERG_CATALOG, ImmutableMap.copyOf(icebergProperties));

            if (addJmxPlugin) {
                queryRunner.createCatalog("jmx", "jmx");
            }

            if (catalogType == HIVE) {
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

            return new IcebergQueryRunner(queryRunner, icebergCatalogs.build());
        }
    }

    private static ExtendedHiveMetastore getFileHiveMetastore(Path dataDirectory)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new IcebergFileHiveMetastore(hdfsEnvironment, dataDirectory.toFile().toURI().toString(), "test");
    }

    public static Path getIcebergDataDirectoryPath(Path dataDirectory, String catalogType, FileFormat format, boolean addStorageFormatToPath)
    {
        Path icebergDataDirectory = addStorageFormatToPath ? dataDirectory.resolve(TEST_DATA_DIRECTORY).resolve(format.name())
                : dataDirectory.resolve(TEST_DATA_DIRECTORY);
        Path icebergCatalogDirectory = icebergDataDirectory.resolve(catalogType);
        return icebergCatalogDirectory;
    }

    public static Map<String, String> getConnectorProperties(CatalogType icebergCatalogType, Path icebergDataDirectory)
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
        logging.setLevel("Bootstrap", WARN);
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
            queryRunner = builder()
                    .setExtraProperties(properties)
                    .setDataDirectory(dataDirectory)
                    .build()
                    .getQueryRunner();
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
