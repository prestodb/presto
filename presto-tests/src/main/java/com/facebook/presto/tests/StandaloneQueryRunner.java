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

import com.facebook.airlift.testing.Closeables;
import com.facebook.presto.Session;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.spi.eventlistener.EventListener;
import com.facebook.presto.split.PageSourceManager;
import com.facebook.presto.split.SplitManager;
import com.facebook.presto.sql.parser.SqlParserOptions;
import com.facebook.presto.sql.planner.ConnectorPlanOptimizerManager;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static com.facebook.presto.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static com.facebook.presto.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class StandaloneQueryRunner
        implements QueryRunner
{
    private final TestingPrestoServer server;

    private final TestingPrestoClient prestoClient;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public StandaloneQueryRunner(Session defaultSession)
            throws Exception
    {
        requireNonNull(defaultSession, "defaultSession is null");

        try {
            server = createTestingPrestoServer();
        }
        catch (Exception e) {
            close();
            throw e;
        }

        this.prestoClient = new TestingPrestoClient(server, defaultSession);

        refreshNodes();

        server.getMetadata().registerBuiltInFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);

        SessionPropertyManager sessionPropertyManager = server.getMetadata().getSessionPropertyManager();
        sessionPropertyManager.addSystemSessionProperties(TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties(new ConnectorId("catalog"), TEST_CATALOG_PROPERTIES);
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

    @Override
    public void close()
    {
        Closeables.closeQuietly(prestoClient);
        Closeables.closeQuietly(server);
    }

    @Override
    public int getNodeCount()
    {
        return 1;
    }

    @Override
    public Session getDefaultSession()
    {
        return prestoClient.getDefaultSession();
    }

    @Override
    public TransactionManager getTransactionManager()
    {
        return server.getTransactionManager();
    }

    @Override
    public Metadata getMetadata()
    {
        return server.getMetadata();
    }

    @Override
    public SplitManager getSplitManager()
    {
        return server.getSplitManager();
    }

    @Override
    public PageSourceManager getPageSourceManager()
    {
        return server.getPageSourceManager();
    }

    @Override
    public NodePartitioningManager getNodePartitioningManager()
    {
        return server.getNodePartitioningManager();
    }

    @Override
    public ConnectorPlanOptimizerManager getPlanOptimizerManager()
    {
        return server.getPlanOptimizerManager();
    }

    @Override
    public StatsCalculator getStatsCalculator()
    {
        return server.getStatsCalculator();
    }

    @Override
    public Optional<EventListener> getEventListener()
    {
        return server.getEventListener();
    }

    @Override
    public TestingAccessControlManager getAccessControl()
    {
        return server.getAccessControl();
    }

    public TestingPrestoServer getServer()
    {
        return server;
    }

    public void refreshNodes()
    {
        AllNodes allNodes;

        do {
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            allNodes = server.refreshNodes();
        }
        while (allNodes.getActiveNodes().isEmpty());
    }

    private void refreshNodes(ConnectorId connectorId)
    {
        Set<InternalNode> activeNodesWithConnector;

        do {
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
            activeNodesWithConnector = server.getActiveNodesWithConnector(connectorId);
        }
        while (activeNodesWithConnector.isEmpty());
    }

    public void installPlugin(Plugin plugin)
    {
        server.installPlugin(plugin);
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        ConnectorId connectorId = server.createCatalog(catalogName, connectorName, properties);

        refreshNodes(connectorId);
    }

    @Override
    public void loadFunctionNamespaceManager(String functionNamespaceManagerName, String catalogName, Map<String, String> properties)
    {
        server.getMetadata().getFunctionAndTypeManager().loadFunctionNamespaceManager(functionNamespaceManagerName, catalogName, properties);
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
    public Lock getExclusiveLock()
    {
        return lock.writeLock();
    }

    private static TestingPrestoServer createTestingPrestoServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.idle-timeout", "1h")
                .put("node-scheduler.min-candidates", "1")
                .put("datasources", "system");

        return new TestingPrestoServer(true, properties.build(), null, null, new SqlParserOptions(), ImmutableList.of());
    }
}
