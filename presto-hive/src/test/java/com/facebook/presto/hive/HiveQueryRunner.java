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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryManagerConfig.ExchangeMaterializationStrategy;
import com.facebook.presto.hive.TestHiveEventListenerPlugin.TestingHiveEventListenerPlugin;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.MetastoreContext;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.spi.security.Identity;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.tests.tpcds.TpcdsTableName;
import com.facebook.presto.tpcds.TpcdsPlugin;
import com.facebook.presto.tpch.TpchPlugin;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.tpch.TpchTable;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.net.URI;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;

import static com.facebook.airlift.log.Level.ERROR;
import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.SystemSessionProperties.COLOCATED_JOIN;
import static com.facebook.presto.SystemSessionProperties.EXCHANGE_MATERIALIZATION_STRATEGY;
import static com.facebook.presto.SystemSessionProperties.GROUPED_EXECUTION;
import static com.facebook.presto.SystemSessionProperties.HASH_PARTITION_COUNT;
import static com.facebook.presto.SystemSessionProperties.PARTITIONING_PROVIDER_CATALOG;
import static com.facebook.presto.spi.security.SelectedRole.Type.ROLE;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.QueryAssertions.copyTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static java.util.Locale.ENGLISH;
import static org.testng.Assert.assertEquals;

public final class HiveQueryRunner
{
    private static final Logger log = Logger.get(HiveQueryRunner.class);

    private HiveQueryRunner()
    {
    }

    public static final String HIVE_CATALOG = "hive";
    public static final String HIVE_BUCKETED_CATALOG = "hive_bucketed";
    public static final String TPCH_SCHEMA = "tpch";
    public static final String TPCH_BUCKETED_SCHEMA = "tpch_bucketed";
    public static final String TPCDS_SCHEMA = "tpcds";
    public static final String TPCDS_BUCKETED_SCHEMA = "tpcds_bucketed";
    public static final MetastoreContext METASTORE_CONTEXT = new MetastoreContext("test_user", "test_queryId", Optional.empty(), Optional.empty(), Optional.empty(), false, HiveColumnConverterProvider.DEFAULT_COLUMN_CONVERTER_PROVIDER);
    private static final String TEMPORARY_TABLE_SCHEMA = "__temporary_tables__";
    private static final DateTimeZone TIME_ZONE = DateTimeZone.forID("America/Bahia_Banderas");

    public static DistributedQueryRunner createQueryRunner(TpchTable<?>... tables)
            throws Exception
    {
        return createQueryRunner(ImmutableList.copyOf(tables));
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(tables, ImmutableMap.of(), Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(
            Iterable<TpchTable<?>> tpchTables,
            Map<String, String> extraProperties,
            Map<String, String> extraCoordinatorProperties,
            Optional<Path> dataDirectory)
            throws Exception
    {
        return createQueryRunner(tpchTables, ImmutableList.of(), extraProperties, extraCoordinatorProperties, "sql-standard", ImmutableMap.of(), Optional.empty(), dataDirectory, Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tpchTables, Map<String, String> extraProperties, Optional<Path> dataDirectory)
            throws Exception
    {
        return createQueryRunner(tpchTables, ImmutableList.of(), extraProperties, ImmutableMap.of(), "sql-standard", ImmutableMap.of(), Optional.empty(), dataDirectory, Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(Iterable<TpchTable<?>> tpchTables, List<String> tpcdsTableNames, Map<String, String> extraProperties, Optional<Path> dataDirectory)
            throws Exception
    {
        return createQueryRunner(tpchTables, tpcdsTableNames, extraProperties, ImmutableMap.of(), "sql-standard", ImmutableMap.of(), Optional.empty(), dataDirectory, Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(
            Iterable<TpchTable<?>> tpchTables,
            Map<String, String> extraProperties,
            String security,
            Map<String, String> extraHiveProperties,
            Optional<Path> dataDirectory)
            throws Exception
    {
        return createQueryRunner(tpchTables, ImmutableList.of(), extraProperties, ImmutableMap.of(), security, extraHiveProperties, Optional.empty(), dataDirectory, Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(
            Iterable<TpchTable<?>> tpchTables,
            Iterable<String> tpcdsTableNames,
            Map<String, String> extraProperties,
            Map<String, String> extraCoordinatorProperties,
            String security,
            Map<String, String> extraHiveProperties,
            Optional<Integer> workerCount,
            Optional<Path> dataDirectory,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
            throws Exception
    {
        return createQueryRunner(tpchTables, tpcdsTableNames, extraProperties, extraCoordinatorProperties, security, extraHiveProperties, workerCount, dataDirectory, externalWorkerLauncher, Optional.empty());
    }

    public static DistributedQueryRunner createQueryRunner(
            Iterable<TpchTable<?>> tpchTables,
            Iterable<String> tpcdsTableNames,
            Map<String, String> extraProperties,
            Map<String, String> extraCoordinatorProperties,
            String security,
            Map<String, String> extraHiveProperties,
            Optional<Integer> workerCount,
            Optional<Path> dataDirectory,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher,
            Optional<ExtendedHiveMetastore> externalMetastore)
            throws Exception
    {
        assertEquals(DateTimeZone.getDefault(), TIME_ZONE, "Timezone not configured correctly. Add -Duser.timezone=America/Bahia_Banderas to your JVM arguments");
        setupLogging();

        Map<String, String> systemProperties = ImmutableMap.<String, String>builder()
                .put("task.writer-count", "2")
                .put("task.partitioned-writer-count", "4")
                .put("tracing.tracer-type", "simple")
                .put("tracing.enable-distributed-tracing", "simple")
                .putAll(extraProperties)
                .build();

        DistributedQueryRunner queryRunner =
                DistributedQueryRunner.builder(createSession(Optional.of(new SelectedRole(ROLE, Optional.of("admin")))))
                        .setNodeCount(workerCount.orElse(4))
                        .setExtraProperties(systemProperties)
                        .setCoordinatorProperties(extraCoordinatorProperties)
                        .setDataDirectory(dataDirectory)
                        .setExternalWorkerLauncher(externalWorkerLauncher)
                        .build();
        try {
            queryRunner.installPlugin(new TpchPlugin());
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.installPlugin(new TestingHiveEventListenerPlugin());
            queryRunner.createCatalog("tpch", "tpch");
            queryRunner.createCatalog("tpcds", "tpcds");
            Map<String, String> tpchProperties = ImmutableMap.<String, String>builder()
                    .put("tpch.column-naming", "standard")
                    .build();
            queryRunner.createCatalog("tpchstandard", "tpch", tpchProperties);

            ExtendedHiveMetastore metastore;
            metastore = externalMetastore.orElse(getFileHiveMetastore(queryRunner));

            queryRunner.installPlugin(new HivePlugin(HIVE_CATALOG, Optional.of(metastore)));

            Map<String, String> hiveProperties = ImmutableMap.<String, String>builder()
                    .putAll(extraHiveProperties)
                    .put("hive.time-zone", TIME_ZONE.getID())
                    .put("hive.security", security)
                    .put("hive.max-partitions-per-scan", "1000")
                    .put("hive.assume-canonical-partition-keys", "true")
                    .put("hive.collect-column-statistics-on-write", "true")
                    .put("hive.temporary-table-schema", TEMPORARY_TABLE_SCHEMA)
                    .build();

            Map<String, String> storageProperties = extraHiveProperties.containsKey("hive.storage-format") ?
                    ImmutableMap.copyOf(hiveProperties) :
                    ImmutableMap.<String, String>builder()
                            .putAll(hiveProperties)
                            .put("hive.storage-format", "TEXTFILE")
                            .put("hive.compression-codec", "NONE")
                            .build();

            Map<String, String> hiveBucketedProperties = ImmutableMap.<String, String>builder()
                    .putAll(storageProperties)
                    .put("hive.max-initial-split-size", "10kB") // so that each bucket has multiple splits
                    .put("hive.max-split-size", "10kB") // so that each bucket has multiple splits
                    .build();
            queryRunner.createCatalog(HIVE_CATALOG, HIVE_CATALOG, hiveProperties);
            queryRunner.createCatalog(HIVE_BUCKETED_CATALOG, HIVE_CATALOG, hiveBucketedProperties);

            List<String> tpchTableNames = getTpchTableNames(tpchTables);
            if (!metastore.getDatabase(METASTORE_CONTEXT, TPCH_SCHEMA).isPresent()) {
                metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(TPCH_SCHEMA));
                copyTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createSession(Optional.empty()), tpchTableNames, true, false);
            }

            if (!metastore.getDatabase(METASTORE_CONTEXT, TPCH_BUCKETED_SCHEMA).isPresent()) {
                metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(TPCH_BUCKETED_SCHEMA));
                copyTables(queryRunner, "tpch", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty()), tpchTableNames, true, true);
            }

//            if (!metastore.getDatabase(METASTORE_CONTEXT, TEMPORARY_TABLE_SCHEMA).isPresent()) {
//                metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(TEMPORARY_TABLE_SCHEMA));
//            }
//
//            if (!metastore.getDatabase(METASTORE_CONTEXT, TPCDS_SCHEMA).isPresent()) {
//                metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(TPCDS_SCHEMA));
//                copyTables(queryRunner, "tpcds", TINY_SCHEMA_NAME, createSession(Optional.empty(), TPCDS_SCHEMA), tpcdsTableNames, true, false);
//            }
//
//            if (!metastore.getDatabase(METASTORE_CONTEXT, TPCDS_BUCKETED_SCHEMA).isPresent()) {
//                metastore.createDatabase(METASTORE_CONTEXT, createDatabaseMetastoreObject(TPCDS_BUCKETED_SCHEMA));
//                copyTables(queryRunner, "tpcds", TINY_SCHEMA_NAME, createBucketedSession(Optional.empty(), TPCDS_BUCKETED_SCHEMA), tpcdsTableNames, true, true);
//            }

            return queryRunner;
        }
        catch (Exception e) {
            queryRunner.close();
            throw e;
        }
    }

    private static List<String> getTpchTableNames(Iterable<TpchTable<?>> tables)
    {
        ImmutableList.Builder<String> tableNames = ImmutableList.builder();
        tables.forEach(table -> tableNames.add(table.getTableName().toLowerCase(ENGLISH)));
        return tableNames.build();
    }

    public static List<String> getAllTpcdsTableNames()
    {
        ImmutableList.Builder<String> tables = ImmutableList.builder();
        for (TpcdsTableName tpcdsTable : TpcdsTableName.getBaseTables()) {
            tables.add(tpcdsTable.getTableName().toLowerCase(ENGLISH));
        }
        return tables.build();
    }

    private static ExtendedHiveMetastore getFileHiveMetastore(DistributedQueryRunner queryRunner)
    {
        File dataDirectory = queryRunner.getCoordinator().getDataDirectory().resolve("hive_data").toFile();
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of(), hiveClientConfig);
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, dataDirectory.toURI().toString(), "test");
    }

    public static DistributedQueryRunner createMaterializingQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(
                tables,
                ImmutableMap.of(
                        "query.partitioning-provider-catalog", "hive",
                        "query.exchange-materialization-strategy", "ALL",
                        "query.hash-partition-count", "11",
                        "colocated-joins-enabled", "true",
                        "grouped-execution-enabled", "true"),
                "sql-standard",
                ImmutableMap.of("hive.create-empty-bucket-files-for-temporary-table", "false"),
                Optional.empty());
    }

    public static DistributedQueryRunner createMaterializingAndSpillingQueryRunner(Iterable<TpchTable<?>> tables)
            throws Exception
    {
        return createQueryRunner(
                tables,
                ImmutableMap.<String, String>builder()
                        .put("query.partitioning-provider-catalog", "hive")
                        .put("query.exchange-materialization-strategy", "ALL")
                        .put("query.hash-partition-count", "11")
                        .put("colocated-joins-enabled", "true")
                        .put("grouped-execution-enabled", "true")
                        .put("experimental.spill-enabled", "true")
                        .put("experimental.spiller-spill-path", Paths.get(System.getProperty("java.io.tmpdir"), "presto", "spills").toString())
                        .put("experimental.spiller-max-used-space-threshold", "1.0")
                        .put("experimental.memory-revoking-threshold", "0.0") // revoke always
                        .put("experimental.memory-revoking-target", "0.0")
                        .build(),
                Optional.empty());
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
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    public static Session createSession(Optional<SelectedRole> role)
    {
        return createSession(role, TPCH_SCHEMA);
    }

    public static Session createSession(Optional<SelectedRole> role, String schema)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of(HIVE_CATALOG, selectedRole))
                                .orElse(ImmutableMap.of()),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .setCatalog(HIVE_CATALOG)
                .setSchema(schema)
                .build();
    }

    public static Session createBucketedSession(Optional<SelectedRole> role)
    {
        return createBucketedSession(role, TPCH_BUCKETED_SCHEMA);
    }

    public static Session createBucketedSession(Optional<SelectedRole> role, String schema)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of(HIVE_BUCKETED_CATALOG, selectedRole))
                                .orElse(ImmutableMap.of()),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .setCatalog(HIVE_BUCKETED_CATALOG)
                .setSchema(schema)
                .build();
    }

    public static Session createMaterializeExchangesSession(Optional<SelectedRole> role)
    {
        return testSessionBuilder()
                .setIdentity(new Identity(
                        "hive",
                        Optional.empty(),
                        role.map(selectedRole -> ImmutableMap.of("hive", selectedRole))
                                .orElse(ImmutableMap.of()),
                        ImmutableMap.of(),
                        ImmutableMap.of(),
                        Optional.empty(),
                        Optional.empty()))
                .setSystemProperty(PARTITIONING_PROVIDER_CATALOG, HIVE_CATALOG)
                .setSystemProperty(EXCHANGE_MATERIALIZATION_STRATEGY, ExchangeMaterializationStrategy.ALL.name())
                .setSystemProperty(HASH_PARTITION_COUNT, "13")
                .setSystemProperty(COLOCATED_JOIN, "true")
                .setSystemProperty(GROUPED_EXECUTION, "true")
                .setCatalog(HIVE_CATALOG)
                .setSchema(TPCH_SCHEMA)
                .build();
    }

    public static void main(String[] args)
            throws Exception
    {
        // You need to add "--user user" to your CLI for your queries to work
        Logging.initialize();

        Optional<Path> dataDirectory = Optional.empty();
        if (args.length > 0) {
            if (args.length != 1) {
                log.error("usage: HiveQueryRunner [dataDirectory]\n");
                log.error("       [dataDirectory] is a local directory under which you want the hive_data directory to be created.]\n");
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

        DistributedQueryRunner queryRunner = createQueryRunner(TpchTable.getTables(), getAllTpcdsTableNames(), ImmutableMap.of("http-server.http.port", "8080"), dataDirectory);
        Thread.sleep(10);
        Logger log = Logger.get(DistributedQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }
}
