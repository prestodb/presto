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
import com.facebook.presto.metadata.AllNodes;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Closer;
import com.google.inject.Module;
import io.airlift.log.Logger;
import io.airlift.testing.Assertions;
import io.airlift.units.Duration;
import org.intellij.lang.annotations.Language;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.units.Duration.nanosSince;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class DistributedQueryRunner
        implements QueryRunner
{
    private static final Logger log = Logger.get(DistributedQueryRunner.class);
    private static final String ENVIRONMENT = "testing";

    private final TestingDiscoveryServer discoveryServer;
    private final TestingPrestoServer coordinator;
    private final List<TestingPrestoServer> servers;

    private final Closer closer = Closer.create();

    private final TestingPrestoClient prestoClient;

    public DistributedQueryRunner(Session defaultSession, int workersCount)
            throws Exception
    {
        this(defaultSession, workersCount, ImmutableMap.of());
    }

    public DistributedQueryRunner(Session defaultSession, int workersCount, Map<String, String> extraProperties)
            throws Exception
    {
        checkNotNull(defaultSession, "defaultSession is null");

        try {
            long start = System.nanoTime();
            discoveryServer = closer.register(new TestingDiscoveryServer(ENVIRONMENT));
            log.info("Created TestingDiscoveryServer in %s", nanosSince(start).convertToMostSuccinctTimeUnit());

            ImmutableList.Builder<TestingPrestoServer> servers = ImmutableList.builder();
            coordinator = closer.register(createTestingPrestoServer(discoveryServer.getBaseUrl(), true, extraProperties));
            servers.add(coordinator);

            for (int i = 1; i < workersCount; i++) {
                TestingPrestoServer worker = closer.register(createTestingPrestoServer(discoveryServer.getBaseUrl(), false, extraProperties));
                servers.add(worker);
            }
            this.servers = servers.build();
        }
        catch (Exception e) {
            close();
            throw e;
        }

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
    }

    private static TestingPrestoServer createTestingPrestoServer(URI discoveryUri, boolean coordinator, Map<String, String> extraProperties)
            throws Exception
    {
        long start = System.nanoTime();
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("compiler.interpreter-enabled", "false")
                .put("task.max-index-memory", "16kB") // causes index joins to fault load
                .put("datasources", "system")
                .put("distributed-index-joins-enabled", "true")
                .put("optimizer.optimize-hash-generation", "true");
        properties.putAll(extraProperties);
        if (coordinator) {
            properties.put("node-scheduler.include-coordinator", "false");
            properties.put("distributed-joins-enabled", "true");
            properties.put("node-scheduler.multiple-tasks-per-node-enabled", "true");
        }

        TestingPrestoServer server = new TestingPrestoServer(coordinator, properties.build(), ENVIRONMENT, discoveryUri, ImmutableList.<Module>of());

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
        createCatalog(catalogName, connectorName, ImmutableMap.<String, String>of());
    }

    @Override
    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        long start = System.nanoTime();
        for (TestingPrestoServer server : servers) {
            server.createCatalog(catalogName, connectorName, properties);
        }
        log.info("Created catalog %s in %s", catalogName, nanosSince(start).convertToMostSuccinctTimeUnit());

        // wait for all nodes to announce the new catalog
        start = System.nanoTime();
        while (!isConnectionVisibleToAllNodes(catalogName)) {
            Assertions.assertLessThan(nanosSince(start), new Duration(100, SECONDS), "waiting form connector " + connectorName + " to be initialized in every node");
            try {
                MILLISECONDS.sleep(10);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
        }
        log.info("Announced catalog %s in %s", catalogName, nanosSince(start).convertToMostSuccinctTimeUnit());
    }

    private boolean isConnectionVisibleToAllNodes(String connectorId)
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
    public List<QualifiedTableName> listTables(Session session, String catalog, String schema)
    {
        return prestoClient.listTables(session, catalog, schema);
    }

    @Override
    public boolean tableExists(Session session, String table)
    {
        return prestoClient.tableExists(session, table);
    }

    @Override
    public MaterializedResult execute(@Language("SQL") String sql)
    {
        return prestoClient.execute(sql);
    }

    @Override
    public MaterializedResult execute(Session session, @Language("SQL") String sql)
    {
        return prestoClient.execute(session, sql);
    }

    @Override
    public final void close()
    {
        try {
            closer.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}
