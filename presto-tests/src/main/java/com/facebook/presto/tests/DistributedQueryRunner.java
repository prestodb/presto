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
package com.facebook.presto.tests;

import com.facebook.airlift.discovery.server.testing.TestingDiscoveryServer;
import com.facebook.airlift.log.Logger;
import com.facebook.airlift.testing.Assertions;
import com.facebook.presto.Session;
import com.facebook.presto.Session.SessionBuilder;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.BasicQueryInfo;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;
import org.jdbi.v3.core.Handle;
import org.jdbi.v3.core.Jdbi;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.BiFunction;
import java.util.function.Function;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.Duration.nanosSince;
import static java.lang.System.nanoTime;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunner.class);
    private static final String ENVIRONMENT = "testing";
    private static final SqlParserOptions DEFAULT_SQL_PARSER_OPTIONS = new SqlParserOptions();

    private final TestingDiscoveryServer discoveryServer;
    private final TestingPrestoServer coordinator;
    private final List<TestingPrestoServer> servers;
    private final List<Process> externalWorkers;

    private final Closer closer = Closer.create();

    private final TestingPrestoClient prestoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final AtomicReference<Handle> testFunctionNamespacesHandle = new AtomicReference<>();

    @Deprecated
    public DistributedQueryRunner(Session defaultSession, int nodeCount)
            throws Exception
    {
        this(defaultSession, nodeCount, ImmutableMap.of());
    }

    @Deprecated
    public DistributedQueryRunner(Session defaultSession, int nodeCount, Map<String, String> extraProperties)
            throws Exception
    {
        this(defaultSession, nodeCount, extraProperties, ImmutableMap.of(), DEFAULT_SQL_PARSER_OPTIONS, ENVIRONMENT, Optional.empty(), Optional.empty());
    }

    public static Builder builder(Session defaultSession)
    {
        return new Builder(defaultSession);
    }

    private DistributedQueryRunner(
            Session defaultSession,
            int nodeCount,
            Map<String, String> extraProperties,
            Map<String, String> coordinatorProperties,
            SqlParserOptions parserOptions,
            String environment,
            Optional<Path> baseDataDir,
            Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        try {
            long start = nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            closer.register(() -> closeUnchecked(discoveryServer));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());
            URI discoveryUrl = discoveryServer.getBaseUrl();
            log.info("Discovery URL %s", discoveryUrl);

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();
            Map<String, String> extraCoordinatorProperties = new HashMap<>();

            if (externalWorkerLauncher.isPresent()) {
                ImmutableList.Builder<Process> externalWorkersBuilder = ImmutableList.builder();
                for (int i = 0; i < nodeCount; i++) {
                    externalWorkersBuilder.add(externalWorkerLauncher.get().apply(i, discoveryUrl));
                }
                externalWorkers = externalWorkersBuilder.build();
                closer.register(() -> {
                    for (Process worker : externalWorkers) {
                        worker.destroyForcibly();
                    }
                });

                // Don't use coordinator as worker
                extraCoordinatorProperties.put("node-scheduler.include-coordinator", "false");
            }
            else {
                externalWorkers = ImmutableList.of();

                for (int i = 1; i < nodeCount; i++) {
                    TestingPrestoServer worker = closer.register(createTestingPrestoServer(discoveryUrl, false, extraProperties, parserOptions, environment, baseDataDir));
                    servers.add(worker);
                }
            }

            extraCoordinatorProperties.put("experimental.iterative-optimizer-enabled", "true");
            extraCoordinatorProperties.putAll(extraProperties);
            extraCoordinatorProperties.putAll(coordinatorProperties);
            coordinator = closer.register(createTestingPrestoServer(discoveryUrl, true, extraCoordinatorProperties, parserOptions, environment, baseDataDir));
            servers.add(coordinator);

            this.servers = servers.build();
        }
        catch (Exception e) {
            try {
                throw closer.rethrow(e, Exception.class);
            }
            finally {
                closer.close();
            }
        }

        // copy session using property manager in coordinator
        defaultSession = defaultSession.toSessionRepresentation().toSession(coordinator.getMetadata().getSessionPropertyManager());
        this.prestoClient = closer.register(new TestingPrestoClient(coordinator, defaultSession));

        long start = nanoTime();
        while (!allNodesGloballyVisible()) {
            Assertions.assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
        log.info("Announced servers in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

        start = nanoTime();
        for (TestingPrestoServer server : servers) {
            server.getMetadata().registerBuiltInFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
        }
        log.info("Added functions in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

        for (TestingPrestoServer server : servers) {
            // add bogus catalog for testing procedures and session properties
            Catalog bogusTestingCatalog = createBogusTestingCatalog(TESTING_CATALOG);
            server.getCatalogManager().registerCatalog(bogusTestingCatalog);

            SessionPropertyManager sessionPropertyManager = server.getMetadata().getSessionPropertyManager();
            sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
            sessionPropertyManager.addConnectorSessionProperties(bogusTestingCatalog.getConnectorId(), TEST_CATALOG_PROPERTIES);
        }
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator, Map<String, String> extraProperties, SqlParserOptions parserOptions, String environment, Optional<Path> baseDataDir)
            throws Exception
    {
        long start = nanoTime();
        ImmutableMap.Builder<String, String> propertiesBuilder = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.idle-timeout", "1h")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("datasources", "system")
                .put("distributed-index-joins-enabled", "true");
        if (coordinator) {
            propertiesBuilder.put("node-scheduler.include-coordinator", "true");
            propertiesBuilder.put("join-distribution-type", "PARTITIONED");
        }
        HashMap<String, String> properties = new HashMap<>(propertiesBuilder.build());
        properties.putAll(extraProperties);

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties, environment, discoveryUri, parserOptions, ImmutableList.of(), baseDataDir);

        String nodeRole = coordinator ? "coordinator" : "worker";
        log.info("Created %s TestingPrestoServer in %s: %s", nodeRole, nanosSince(start).convertToMostSuccinctTimeUnit(), server.getBaseUrl());

        return server;
    }

    private boolean allNodesGloballyVisible()
    {
        int expectedActiveNodes = externalWorkers.size() + servers.size();
        for (TestingPrestoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != expectedActiveNodes)) {
                return false;
            }
        }
        return true;
    }

    public TestingPrestoClient getClient()
    {
        return prestoClient;
    }

    @Override
    public int getNodeCount()
    {
        return servers.size();
    }

    @Override
    public Session getDefaultSession()
    {
        return prestoClient.getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return coordinator.getTransactionManager();
    }

    @Override
    public Metadata getMetadata()
    {
        return coordinator.getMetadata();
    }

    @Override
    public SplitManager getSplitManager()
    {
        return coordinator.getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return coordinator.getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return coordinator.getNodePartitioningManager();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return coordinator.getPlanOptimizerManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return coordinator.getStatsCalculator();
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        return coordinator.getEventListener();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return coordinator.getAccessControl();
    }

    public TestingPrestoServer getCoordinator()
    {
        return coordinator;
    }

    public List<TestingPrestoServer> getServers()
    {
        return ImmutableList.copyOf(servers);
    }

    @Override
    public void installPlugin(Plugin plugin)
    {
        long start = nanoTime();
        for (TestingPrestoServer server : servers) {
            server.installPlugin(plugin);
        }
        log.info("Installed plugin %s in %s", plugin.getClass().getSimpleName(), nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = nanoTime();
        Set<ConnectorId> connectorIds = new HashSet<>();
        for (TestingPrestoServer server : servers) {
            connectorIds.add(server.createCatalog(catalogName, connectorName, properties));
        }
        ConnectorId connectorId = getOnlyElement(connectorIds);
        log.info("Created catalog %s (%s) in %s", catalogName, connectorId, nanosSince(start));

        // wait for all nodes to announce the new catalog
        start = nanoTime();
        while (!isConnectorVisibleToAllNodes(connectorId)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting for connector " + connectorId + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        log.info("Announced catalog %s (%s) in %s", catalogName, connectorId, nanosSince(start));
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        for (TestingPrestoServer server : servers) {
            server.getMetadata().getFunctionAndTypeManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
        }
    }

    /**
     * This method exists only because it is currently impossible to create a function namespace from the query engine,
     * and therefore the query runner needs to be aware of the H2 handle in order to create function namespaces when
     * required by the tests.
     * <p>
     * TODO: Remove when there is a generic way of creating function namespaces as if creating schemas.
     */
    public void enableTestFunctionNamespaces(List<String> catalogNames, Map<String, String> additionalProperties)
    {
        checkState(testFunctionNamespacesHandle.get() == null, "Test function namespaces already enabled");

        String databaseName = String.valueOf(nanoTime());
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("database-name", databaseName)
                .putAll(additionalProperties)
                .build();
        installPlugin(new H2FunctionNamespaceManagerPlugin());
        for (String catalogName : catalogNames) {
            loadFunctionNamespaceManager("h2", catalogName, properties);
        }

        Handle handle = Jdbi.open(H2ConnectionModule.getJdbcUrl(databaseName));
        testFunctionNamespacesHandle.set(handle);
        closer.register(handle);
    }

    public void createTestFunctionNamespace(String catalogName, String schemaName)
    {
        checkState(testFunctionNamespacesHandle.get() != null, "Test function namespaces not enabled");

        testFunctionNamespacesHandle.get().execute("INSERT INTO function_namespaces SELECT ?, ?", catalogName, schemaName);
    }

    private boolean isConnectorVisibleToAllNodes(ConnectorId connectorId)
    {
        if (!externalWorkers.isEmpty()) {
            return true;
        }

        for (TestingPrestoServer server : servers) {
            server.refreshNodes();
            Set<InternalNode> activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
            if (activeNodesWithConnector.size() != servers.size()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
    {
        lock.readLock().lock();
        try {
            return prestoClient.listTables(session, catalog, schema);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        lock.readLock().lock();
        try {
            return prestoClient.tableExists(session, table);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return prestoClient.execute(sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return prestoClient.execute(session, sql).getResult();
        }
        finally {
            lock.readLock().unlock();
        }
    }

    public ResultWithQueryId<MaterializedResult> executeWithQueryId(Session session, @Language("SQL") String sql)
    {
        lock.readLock().lock();
        try {
            return prestoClient.execute(session, sql);
        }
        finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public MaterializedResultWithPlan executeWithPlan(Session session, String sql, WarningCollector warningCollector)
    {
        ResultWithQueryId<MaterializedResult> resultWithQueryId = executeWithQueryId(session, sql);
        return new MaterializedResultWithPlan(resultWithQueryId.getResult().toTestTypes(), getQueryPlan(resultWithQueryId.getQueryId()));
    }

    @Override
    public Plan createPlan(Session session, String sql, WarningCollector warningCollector)
    {
        QueryId queryId = executeWithQueryId(session, sql).getQueryId();
        Plan queryPlan = getQueryPlan(queryId);
        coordinator.getQueryManager().cancelQuery(queryId);
        return queryPlan;
    }

    public List<BasicQueryInfo> getQueries()
    {
        return coordinator.getQueryManager().getQueries();
    }

    public QueryInfo getQueryInfo(QueryId queryId)
    {
        return coordinator.getQueryManager().getFullQueryInfo(queryId);
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return coordinator.getQueryPlan(queryId);
    }

    @Override
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    @Override
    public final void close()
    {
        cancelAllQueries();
        try {
            closer.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void cancelAllQueries()
    {
        QueryManager queryManager = coordinator.getQueryManager();
        for (BasicQueryInfo queryInfo : queryManager.getQueries()) {
            if (!queryInfo.getState().isDone()) {
                queryManager.cancelQuery(queryInfo.getQueryId());
            }
        }
    }

    private static void closeUnchecked(AutoCloseable closeable)
    {
        try {
            closeable.close();
        }
        catch (Exception e) {
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    public static class Builder
    {
        private Session defaultSession;
        private int nodeCount = 4;
        private Map<String, String> extraProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private SqlParserOptions parserOptions = DEFAULT_SQL_PARSER_OPTIONS;
        private String environment = ENVIRONMENT;
        private Optional<Path> baseDataDir = Optional.empty();
        private Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher = Optional.empty();

        protected Builder(Session defaultSession)
        {
            this.defaultSession = requireNonNull(defaultSession, "defaultSession is null");
        }

        public Builder amendSession(Function<SessionBuilder, SessionBuilder> amendSession)
        {
            SessionBuilder builder = Session.builder(defaultSession);
            this.defaultSession = amendSession.apply(builder).build();
            return this;
        }

        public Builder setNodeCount(int nodeCount)
        {
            this.nodeCount = nodeCount;
            return this;
        }

        public Builder setExtraProperties(Map<String, String> extraProperties)
        {
            this.extraProperties = extraProperties;
            return this;
        }

        /**
         * Sets extra properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one extra property needs to be set.
         */
        public Builder setSingleExtraProperty(String key, String value)
        {
            return setExtraProperties(ImmutableMap.of(key, value));
        }

        public Builder setCoordinatorProperties(Map<String, String> coordinatorProperties)
        {
            this.coordinatorProperties = coordinatorProperties;
            return this;
        }

        /**
         * Sets coordinator properties being equal to a map containing given key and value.
         * Note, that calling this method OVERWRITES previously set property values.
         * As a result, it should only be used when only one coordinator property needs to be set.
         */
        public Builder setSingleCoordinatorProperty(String key, String value)
        {
            return setCoordinatorProperties(ImmutableMap.of(key, value));
        }

        public Builder setParserOptions(SqlParserOptions parserOptions)
        {
            this.parserOptions = parserOptions;
            return this;
        }

        public Builder setEnvironment(String environment)
        {
            this.environment = environment;
            return this;
        }

        public Builder setBaseDataDir(Optional<Path> baseDataDir)
        {
            this.baseDataDir = requireNonNull(baseDataDir, "baseDataDir is null");
            return this;
        }

        public Builder setExternalWorkerLauncher(Optional<BiFunction<Integer, URI, Process>> externalWorkerLauncher)
        {
            this.externalWorkerLauncher = requireNonNull(externalWorkerLauncher, "externalWorkerLauncher is null");
            return this;
        }

        public DistributedQueryRunner build()
                throws Exception
        {
            return new DistributedQueryRunner(defaultSession, nodeCount, extraProperties, coordinatorProperties, parserOptions, environment, baseDataDir, externalWorkerLauncher);
        }
    }
}
