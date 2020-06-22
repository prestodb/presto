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
package com.facebook.presto.spark;

import com.facebook.airlift.bootstrap.LifeCycleManager;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.log.Logging;
import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorManager;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.hive.HdfsConfiguration;
import com.facebook.presto.hive.HdfsConfigurationInitializer;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.HiveHdfsConfiguration;
import com.facebook.presto.hive.HivePlugin;
import com.facebook.presto.hive.MetastoreClientConfig;
import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.CatalogManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.security.AccessControlManager;
import com.facebook.presto.server.PluginManager;
import com.facebook.presto.spark.PrestoSparkQueryExecutionFactory.PrestoSparkQueryExecution;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkQueryExecutionFactory;
import com.facebook.presto.spark.classloader_interface.IPrestoSparkTaskExecutorFactory;
import com.facebook.presto.spark.classloader_interface.PrestoSparkConfInitializer;
import com.facebook.presto.spark.classloader_interface.PrestoSparkSession;
import com.facebook.presto.spark.classloader_interface.PrestoSparkTaskExecutorFactoryProvider;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.MaterializedRow;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.AbstractTestQueries;
import com.facebook.presto.tpch.TpchPlugin;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Binder;
import com.google.inject.Injector;
import com.google.inject.Module;
import com.google.inject.Scopes;
import io.airlift.tpch.TpchTable;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.facebook.airlift.log.Level.ERROR;
import static com.facebook.airlift.log.Level.WARN;
import static com.facebook.presto.spark.PrestoSparkSettingsRequirements.SPARK_EXECUTOR_CORES_PROPERTY;
import static com.facebook.presto.spark.PrestoSparkSettingsRequirements.SPARK_TASK_CPUS_PROPERTY;
import static com.facebook.presto.spark.classloader_interface.SparkProcessType.DRIVER;
import static com.facebook.presto.testing.MaterializedResult.DEFAULT_PRECISION;
import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
import static com.facebook.presto.tests.QueryAssertions.copyTpchTables;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCHEMA_NAME;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.tpch.TpchTable.getTables;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.String.format;
import static java.nio.file.Files.createTempDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.TimeUnit.SECONDS;

public class PrestoSparkQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(PrestoSparkQueryRunner.class);

    private static final int NODE_COUNT = 4;

    private static final Map<String, PrestoSparkQueryRunner> instances = new ConcurrentHashMap<>();
    private static final SparkContextHolder sparkContextHolder = new SparkContextHolder();

    private final Session defaultSession;

    private final TransactionManager transactionManager;
    private final Metadata metadata;
    private final SplitManager splitManager;
    private final PageSourceManager pageSourceManager;
    private final NodePartitioningManager nodePartitioningManager;
    private final ConnectorPlanOptimizerManager connectorPlanOptimizerManager;
    private final StatsCalculator statsCalculator;
    private final PluginManager pluginManager;
    private final ConnectorManager connectorManager;

    private final LifeCycleManager lifeCycleManager;

    private final SparkContext sparkContext;
    private final PrestoSparkService prestoSparkService;

    private final TestingAccessControlManager testingAccessControlManager;

    private final FileHiveMetastore metastore;

    private final String instanceId;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public static PrestoSparkQueryRunner createHivePrestoSparkQueryRunner()
    {
        return createHivePrestoSparkQueryRunner(getTables());
    }

    public static PrestoSparkQueryRunner createHivePrestoSparkQueryRunner(Iterable<TpchTable<?>> tables)
    {
        PrestoSparkQueryRunner queryRunner = new PrestoSparkQueryRunner("hive");
        ExtendedHiveMetastore metastore = queryRunner.getMetastore();
        if (!metastore.getDatabase("tpch").isPresent()) {
            metastore.createDatabase(createDatabaseMetastoreObject("tpch"));
            copyTpchTables(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), tables);
            copyTpchTablesBucketed(queryRunner, "tpch", TINY_SCHEMA_NAME, queryRunner.getDefaultSession(), tables);
        }
        return queryRunner;
    }

    public static void copyTpchTablesBucketed(
            QueryRunner queryRunner,
            String sourceCatalog,
            String sourceSchema,
            Session session,
            Iterable<TpchTable<?>> tables)
    {
        log.info("Loading data from %s.%s...", sourceCatalog, sourceSchema);
        long startTime = System.nanoTime();
        for (TpchTable<?> table : tables) {
            copyTableBucketed(queryRunner, new QualifiedObjectName(sourceCatalog, sourceSchema, table.getTableName().toLowerCase(ENGLISH)), session);
        }
        log.info("Loading from %s.%s complete in %s", sourceCatalog, sourceSchema, nanosSince(startTime).toString(SECONDS));
    }

    private static void copyTableBucketed(QueryRunner queryRunner, QualifiedObjectName table, Session session)
    {
        long start = System.nanoTime();
        String tableName = table.getObjectName() + "_bucketed";
        log.info("Running import for %s", tableName);
        String sql;
        switch (tableName) {
            case "lineitem_bucketed":
            case "orders_bucketed":
                sql = format("CREATE TABLE %s WITH (bucketed_by=array['orderkey'], bucket_count=11) AS SELECT * FROM %s", tableName, table);
                break;
            default:
                log.info("Skipping %s", tableName);
                return;
        }
        long rows = (Long) queryRunner.execute(session, sql).getMaterializedRows().get(0).getField(0);
        log.info("Imported %s rows for %s in %s", rows, tableName, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public PrestoSparkQueryRunner(String defaultCatalog)
    {
        setupLogging();

        PrestoSparkInjectorFactory injectorFactory = new PrestoSparkInjectorFactory(
                DRIVER,
                ImmutableMap.of(
                        "presto.version", "testversion",
                        "query.hash-partition-count", Integer.toString(NODE_COUNT * 2),
                        "prefer-distributed-union", "false"),
                ImmutableMap.of(),
                ImmutableList.of(),
                Optional.of(new TestingAccessControlModule()));

        Injector injector = injectorFactory.create();

        defaultSession = testSessionBuilder(injector.getInstance(SessionPropertyManager.class))
                .setCatalog(defaultCatalog)
                .setSchema("tpch")
                .build();

        transactionManager = injector.getInstance(TransactionManager.class);
        metadata = injector.getInstance(Metadata.class);
        splitManager = injector.getInstance(SplitManager.class);
        pageSourceManager = injector.getInstance(PageSourceManager.class);
        nodePartitioningManager = injector.getInstance(NodePartitioningManager.class);
        connectorPlanOptimizerManager = injector.getInstance(ConnectorPlanOptimizerManager.class);
        statsCalculator = injector.getInstance(StatsCalculator.class);
        pluginManager = injector.getInstance(PluginManager.class);
        connectorManager = injector.getInstance(ConnectorManager.class);

        lifeCycleManager = injector.getInstance(LifeCycleManager.class);

        sparkContext = sparkContextHolder.get();
        prestoSparkService = injector.getInstance(PrestoSparkService.class);
        testingAccessControlManager = injector.getInstance(TestingAccessControlManager.class);

        // Install tpch Plugin
        pluginManager.installPlugin(new TpchPlugin());
        connectorManager.createConnection("tpch", "tpch", ImmutableMap.of());

        // Install Hive Plugin
        File baseDir;
        try {
            baseDir = createTempDirectory("PrestoTest").toFile();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());

        this.metastore = new FileHiveMetastore(hdfsEnvironment, baseDir.toURI().toString(), "test");
        metastore.createDatabase(createDatabaseMetastoreObject("hive_test"));
        pluginManager.installPlugin(new HivePlugin("hive", Optional.of(metastore)));

        connectorManager.createConnection("hive", "hive", ImmutableMap.of());

        metadata.registerBuiltInFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);

        // add bogus catalog for testing procedures and session properties
        CatalogManager catalogManager = injector.getInstance(CatalogManager.class);
        Catalog bogusTestingCatalog = createBogusTestingCatalog(TESTING_CATALOG);
        catalogManager.registerCatalog(bogusTestingCatalog);

        SessionPropertyManager sessionPropertyManager = metadata.getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), TEST_CATALOG_PROPERTIES);

        // register the instance
        instanceId = randomUUID().toString();
        instances.put(instanceId, this);
    }

    private static void setupLogging()
    {
        Logging logging = Logging.initialize();
        logging.setLevel("org.apache.spark", WARN);
        logging.setLevel("org.spark_project", WARN);
        logging.setLevel("com.facebook.presto.spark", WARN);
        logging.setLevel("com.facebook.presto.spark", WARN);
        logging.setLevel("org.apache.spark.util.ClosureCleaner", ERROR);
        logging.setLevel("com.facebook.presto.security.AccessControlManager", WARN);
        logging.setLevel("com.facebook.presto.server.PluginManager", WARN);
        logging.setLevel("com.facebook.airlift.bootstrap.LifeCycleManager", WARN);
        logging.setLevel("org.apache.parquet.hadoop", WARN);
        logging.setLevel("parquet.hadoop", WARN);
    }

    @Override
    public int getNodeCount()
    {
        return NODE_COUNT;
    }

    @Override
    public Session getDefaultSession()
    {
        return defaultSession;
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return transactionManager;
    }

    @Override
    public Metadata getMetadata()
    {
        return metadata;
    }

    @Override
    public SplitManager getSplitManager()
    {
        return splitManager;
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return pageSourceManager;
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return nodePartitioningManager;
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return connectorPlanOptimizerManager;
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return statsCalculator;
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return testingAccessControlManager;
    }

    @Override
    public MaterializedResult execute(String sql)
    {
        return execute(defaultSession, sql);
    }

    @Override
    public MaterializedResult execute(Session session, String sql)
    {
        IPrestoSparkQueryExecutionFactory executionFactory = prestoSparkService.getQueryExecutionFactory();
        PrestoSparkQueryExecution execution = (PrestoSparkQueryExecution) executionFactory.create(
                sparkContext,
                createSessionInfo(session),
                sql,
                new TestingPrestoSparkTaskExecutorFactoryProvider(instanceId));
        List<List<Object>> results = execution.execute();
        List<MaterializedRow> rows = results.stream()
                .map(result -> new MaterializedRow(DEFAULT_PRECISION, result))
                .collect(toImmutableList());

        if (!execution.getUpdateType().isPresent()) {
            return new MaterializedResult(rows, execution.getOutputTypes());
        }
        else {
            return new MaterializedResult(
                    rows,
                    execution.getOutputTypes(),
                    ImmutableMap.of(),
                    ImmutableSet.of(),
                    execution.getUpdateType(),
                    OptionalLong.of((Long) getOnlyElement(getOnlyElement(rows).getFields())),
                    ImmutableList.of());
        }
    }

    private static PrestoSparkSession createSessionInfo(Session session)
    {
        ImmutableMap.Builder<String, Map<String, String>> catalogSessionProperties = ImmutableMap.builder();
        catalogSessionProperties.putAll(session.getConnectorProperties().entrySet().stream()
                .collect(toImmutableMap(entry -> entry.getKey().getCatalogName(), Map.Entry::getValue)));
        catalogSessionProperties.putAll(session.getUnprocessedCatalogProperties());
        return new PrestoSparkSession(
                session.getIdentity().getUser(),
                session.getIdentity().getPrincipal(),
                session.getIdentity().getExtraCredentials(),
                session.getCatalog(),
                session.getSchema(),
                session.getSource(),
                session.getClientInfo(),
                session.getClientTags(),
                Optional.of(session.getTimeZoneKey().getId()),
                Optional.empty(),
                session.getSystemProperties(),
                catalogSessionProperties.build(),
                session.getTraceToken());
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        pluginManager.installPlugin(plugin);
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        connectorManager.createConnection(catalogName, connectorName, properties);
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        metadata.getFunctionManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    public PrestoSparkService getPrestoSparkService()
    {
        return prestoSparkService;
    }

    public FileHiveMetastore getMetastore()
    {
        return metastore;
    }

    @Override
    public void close()
    {
        sparkContextHolder.release(sparkContext);

        try {
            if (lifeCycleManager != null) {
                lifeCycleManager.stop();
            }
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }

        if (instanceId != null) {
            instances.remove(instanceId);
        }
    }

    private static class TestingPrestoSparkTaskExecutorFactoryProvider
            implements PrestoSparkTaskExecutorFactoryProvider
    {
        private final String instanceId;

        private TestingPrestoSparkTaskExecutorFactoryProvider(String instanceId)
        {
            this.instanceId = requireNonNull(instanceId, "instanceId is null");
        }

        @Override
        public IPrestoSparkTaskExecutorFactory get()
        {
            return instances.get(instanceId).getPrestoSparkService().getTaskExecutorFactory();
        }
    }

    private static Database createDatabaseMetastoreObject(String name)
    {
        return Database.builder()
                .setDatabaseName(name)
                .setOwnerName("public")
                .setOwnerType(PrincipalType.ROLE)
                .build();
    }

    private static class TestingAccessControlModule
            implements Module
    {
        @Override
        public void configure(Binder binder)
        {
            binder.bind(TestingAccessControlManager.class).in(Scopes.SINGLETON);
            binder.bind(AccessControlManager.class).to(TestingAccessControlManager.class).in(Scopes.SINGLETON);
            binder.bind(AccessControl.class).to(AccessControlManager.class).in(Scopes.SINGLETON);
        }
    }

    private static class SparkContextHolder
    {
        private static SparkContext sparkContext;
        private static int referenceCount;

        public SparkContext get()
        {
            synchronized (SparkContextHolder.class) {
                if (sparkContext == null) {
                    SparkConf sparkConfiguration = new SparkConf()
                            .setMaster(format("local[%s]", NODE_COUNT))
                            .setAppName("presto")
                            .set("spark.driver.host", "localhost")
                            .set(SPARK_EXECUTOR_CORES_PROPERTY, "4")
                            .set(SPARK_TASK_CPUS_PROPERTY, "4");
                    PrestoSparkConfInitializer.initialize(sparkConfiguration);
                    sparkContext = new SparkContext(sparkConfiguration);
                }
                referenceCount++;
                return sparkContext;
            }
        }

        public void release(SparkContext sparkContext)
        {
            synchronized (SparkContextHolder.class) {
                checkState(SparkContextHolder.sparkContext == sparkContext, "unexpected spark context");
                referenceCount--;
                if (referenceCount == 0) {
                    sparkContext.cancelAllJobs();
                    sparkContext.stop();
                    SparkContextHolder.sparkContext = null;
                }
            }
        }
    }
}
