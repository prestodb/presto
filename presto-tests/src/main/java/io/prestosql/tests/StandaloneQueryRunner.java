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
package io.prestosql.tests;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.testing.Closeables;
import io.prestosql.Session;
import io.prestosql.connector.ConnectorId;
import io.prestosql.cost.StatsCalculator;
import io.prestosql.metadata.AllNodes;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.SessionPropertyManager;
import io.prestosql.server.testing.TestingPrestoServer;
import io.prestosql.spi.Node;
import io.prestosql.spi.Plugin;
import io.prestosql.split.PageSourceManager;
import io.prestosql.split.SplitManager;
import io.prestosql.sql.parser.SqlParserOptions;
import io.prestosql.sql.planner.NodePartitioningManager;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.QueryRunner;
import io.prestosql.testing.TestingAccessControlManager;
import io.prestosql.transaction.TransactionManager;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static io.prestosql.tests.AbstractTestQueries.TEST_CATALOG_PROPERTIES;
import static io.prestosql.tests.AbstractTestQueries.TEST_SYSTEM_PROPERTIES;
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

        server.getMetadata().addFunctions(AbstractTestQueries.CUSTOM_FUNCTIONS);

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
    public StatsCalculator getStatsCalculator()
    {
        return server.getStatsCalculator();
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
        Set<Node> activeNodesWithConnector;

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
