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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedTableName;
import com.facebook.presto.metadata.SessionPropertyManager;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Module;
import io.airlift.testing.Closeables;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public final class StandaloneQueryRunner
        implements QueryRunner
{
    private final TestingPrestoServer server;

    private final TestingPrestoClient prestoClient;

    public StandaloneQueryRunner(Session defaultSession)
            throws Exception
    {
        checkNotNull(defaultSession, "defaultSession is null");

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
        sessionPropertyManager.addSystemSessionProperties(AbstractTestQueries.TEST_SYSTEM_PROPERTIES);
        sessionPropertyManager.addConnectorSessionProperties("catalog", AbstractTestQueries.TEST_CATALOG_PROPERTIES);
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
    public Metadata getMetadata()
    {
        return server.getMetadata();
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

    private void refreshNodes(String catalogName)
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
            activeNodesWithConnector = server.getActiveNodesWithConnector(catalogName);
        }
        while (activeNodesWithConnector.isEmpty());
    }

    public void installPlugin(Plugin plugin)
    {
        server.installPlugin(plugin);
    }

    public void createCatalog(String catalogName, String connectorName)
    {
        createCatalog(catalogName, connectorName, ImmutableMap.<String, String>of());
    }

    public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
    {
        server.createCatalog(catalogName, connectorName, properties);

        refreshNodes(catalogName);
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

    private static TestingPrestoServer createTestingPrestoServer()
            throws Exception
    {
        ImmutableMap.Builder<String, String> properties = ImmutableMap.<String, String>builder()
                .put("query.client.timeout", "10m")
                .put("exchange.http-client.read-timeout", "1h")
                .put("compiler.interpreter-enabled", "false")
                .put("node-scheduler.min-candidates", "1")
                .put("datasources", "system");

        return new TestingPrestoServer(true, properties.build(), null, null, ImmutableList.<Module>of());
    }
}
