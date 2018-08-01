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

import com.facebook.presto.Session;
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.execution.QueryInfo;
import com.facebook.presto.execution.QueryManager;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.Catalog;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import io.airlift.discovery.server.testing.TestingDiscoveryServer;
import io.airlift.log.Logger;
import io.airlift.testing.Assertions;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.facebook.presto.testing.TestingSession.TESTING_CATALOG;
import static com.facebook.presto.testing.TestingSession.createBogusTestingCatalog;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.units.Duration.nanosSince;
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

    private final Closer closer = Closer.create();

    private final TestingPrestoClient prestoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

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
        this(defaultSession, nodeCount, extraProperties, ImmutableMap.of(), DEFAULT_SQL_PARSER_OPTIONS, ENVIRONMENT);
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
            String environment)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        try {
            long start = System.nanoTime();
            discoveryServer = new TestingDiscoveryServer(environment);
            closer.register(() -> closeUnchecked(discoveryServer));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();

            for (int i = 1; i < nodeCount; i++) {
                TestingPrestoServer worker = closer.register(createTestingPrestoServer(discoveryServer.getBaseUrl(), false, extraProperties, parserOptions, environment));
                servers.add(worker);
            }

            Map<String, String> extraCoordinatorProperties = new HashMap<>();
            extraCoordinatorProperties.put("experimental.iterative-optimizer-enabled", "true");
            extraCoordinatorProperties.putAll(extraProperties);
            extraCoordinatorProperties.putAll(coordinatorProperties);
            coordinator = closer.register(createTestingPrestoServer(discoveryServer.getBaseUrl(), true, extraCoordinatorProperties, parserOptions, environment));
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

        long start = System.nanoTime();
        while (!allNodesGloballyVisible()) {
            Assertions.assertLessThan(nanosSince(start), new Duration(10, SECONDS));
            MILLISECONDS.sleep(10);
        }
        log.info("Announced servers in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

        start = System.nanoTime();
        for (TestingPrestoServer server : servers) {
            server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);
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

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator, Map<String, String> extraProperties, SqlParserOptions parserOptions, String environment)
            throws Exception
    {
        long start = System.nanoTime();
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

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties, environment, discoveryUri, parserOptions, ImmutableList.of());

        log.info("Created TestingPrestoServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

        return server;
    }

    private boolean allNodesGloballyVisible()
    {
        for (TestingPrestoServer server : servers) {
            AllNodes allNodes = server.refreshNodes();
            if (!allNodes.getInactiveNodes().isEmpty() ||
                    (allNodes.getActiveNodes().size() != servers.size())) {
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
    public NodePartitioningManager getNodePartitioningManager()
    {
        return coordinator.getNodePartitioningManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return coordinator.getStatsCalculator();
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
        long start = System.nanoTime();
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
        long start = System.nanoTime();
        Set<ConnectorId> connectorIds = new HashSet<>();
        for (TestingPrestoServer server : servers) {
            connectorIds.add(server.createCatalog(catalogName, connectorName, properties));
        }
        ConnectorId connectorId = getOnlyElement(connectorIds);
        log.info("Created catalog %s (%s) in %s", catalogName, connectorId, nanosSince(start));

        // wait for all nodes to announce the new catalog
        start = System.nanoTime();
        while (!isConnectionVisibleToAllNodes(connectorId)) {
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

    private boolean isConnectionVisibleToAllNodes(ConnectorId connectorId)
    {
        for (TestingPrestoServer server : servers) {
            server.refreshNodes();
            Set<Node> activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
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
    public Plan createPlan(Session session, String sql)
    {
        QueryId queryId = executeWithQueryId(session, sql).getQueryId();
        Plan queryPlan = getQueryPlan(queryId);
        coordinator.getQueryManager().cancelQuery(queryId);
        return queryPlan;
    }

    public QueryInfo getQueryInfo(QueryId queryId)
    {
        return coordinator.getQueryManager().getQueryInfo(queryId);
    }

    public Plan getQueryPlan(QueryId queryId)
    {
        return coordinator.getQueryManager().getQueryPlan(queryId);
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
        for (QueryInfo queryInfo : queryManager.getAllQueryInfo()) {
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
        private final Session defaultSession;
        private int nodeCount = 4;
        private Map<String, String> extraProperties = ImmutableMap.of();
        private Map<String, String> coordinatorProperties = ImmutableMap.of();
        private SqlParserOptions parserOptions = DEFAULT_SQL_PARSER_OPTIONS;
        private String environment = ENVIRONMENT;

        protected Builder(Session defaultSession)
        {
            this.defaultSession = defaultSession;
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

        public DistributedQueryRunner build()
                throws Exception
        {
            return new DistributedQueryRunner(defaultSession, nodeCount, extraProperties, coordinatorProperties, parserOptions, environment);
        }
    }
}
