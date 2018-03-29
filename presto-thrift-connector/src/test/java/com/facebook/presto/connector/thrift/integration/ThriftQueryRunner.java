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
package com.facebook.presto.connector.thrift.integration;

import com.facebook.presto.Session;
import com.facebook.presto.connector.thrift.ThriftPlugin;
import com.facebook.presto.connector.thrift.server.ThriftIndexedTpchService;
import com.facebook.presto.connector.thrift.server.ThriftTpchService;
import com.facebook.presto.cost.StatsCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.sql.planner.NodePartitioningManager;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.transaction.TransactionManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.drift.codec.ThriftCodecManager;
import io.airlift.drift.server.DriftServer;
import io.airlift.drift.server.DriftService;
import io.airlift.drift.server.stats.NullMethodInvocationStatsFactory;
import io.airlift.drift.transport.netty.server.DriftNettyServerConfig;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransport;
import io.airlift.drift.transport.netty.server.DriftNettyServerTransportFactory;
import io.airlift.log.Logger;
import io.airlift.log.Logging;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static io.airlift.testing.Closeables.closeQuietly;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

public final class ThriftQueryRunner
{
    public static final ThriftCodecManager CODEC_MANAGER = new ThriftCodecManager();

    private ThriftQueryRunner() {}

    public static QueryRunner createThriftQueryRunner(int thriftServers, int nodeCount, boolean enableIndexJoin, Map<String, String> properties)
            throws Exception
    {
        List<DriftServer> servers = null;
        DistributedQueryRunner runner = null;
        try {
            servers = startThriftServers(thriftServers, enableIndexJoin);
            runner = createThriftQueryRunnerInternal(servers, nodeCount, properties);
            return new ThriftQueryRunnerWithServers(runner, servers);
        }
        catch (Throwable t) {
            closeQuietly(runner);
            // runner might be null, so closing servers explicitly
            if (servers != null) {
                for (DriftServer server : servers) {
                    server.shutdown();
                }
            }
            throw t;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        Logging.initialize();
        Map<String, String> properties = ImmutableMap.of("http-server.http.port", "8080");
        ThriftQueryRunnerWithServers queryRunner = (ThriftQueryRunnerWithServers) createThriftQueryRunner(3, 3, true, properties);
        Thread.sleep(10);
        Logger log = Logger.get(ThriftQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    private static List<DriftServer> startThriftServers(int thriftServers, boolean enableIndexJoin)
    {
        List<DriftServer> servers = new ArrayList<>(thriftServers);
        for (int i = 0; i < thriftServers; i++) {
            ThriftTpchService service = enableIndexJoin ? new ThriftIndexedTpchService() : new ThriftTpchService();
            DriftServer server = new DriftServer(
                    new DriftNettyServerTransportFactory(new DriftNettyServerConfig()),
                    CODEC_MANAGER,
                    new NullMethodInvocationStatsFactory(),
                    ImmutableSet.of(new DriftService(service)),
                    ImmutableSet.of());
            server.start();
            servers.add(server);
        }
        return servers;
    }

    private static DistributedQueryRunner createThriftQueryRunnerInternal(List<DriftServer> servers, int nodeCount, Map<String, String> properties)
            throws Exception
    {
        String addresses = servers.stream()
                .map(server -> "localhost:" + driftServerPort(server))
                .collect(joining(","));

        Session defaultSession = testSessionBuilder()
                .setCatalog("thrift")
                .setSchema("tiny")
                .build();

        DistributedQueryRunner queryRunner = DistributedQueryRunner.builder(defaultSession)
                .setNodeCount(nodeCount)
                .setExtraProperties(properties)
                .build();

        queryRunner.installPlugin(new ThriftPlugin());
        Map<String, String> connectorProperties = ImmutableMap.<String, String>builder()
                .put("presto.thrift.client.addresses", addresses)
                .put("presto.thrift.client.connect-timeout", "30s")
                .put("presto-thrift.lookup-requests-concurrency", "2")
                .build();
        queryRunner.createCatalog("thrift", "presto-thrift", connectorProperties);

        return queryRunner;
    }

    private static int driftServerPort(DriftServer server)
    {
        return ((DriftNettyServerTransport) server.getServerTransport()).getPort();
    }

    /**
     * Wraps QueryRunner and a list of ThriftServers to clean them up together.
     */
    private static class ThriftQueryRunnerWithServers
            implements QueryRunner
    {
        private DistributedQueryRunner source;
        private List<DriftServer> thriftServers;

        private ThriftQueryRunnerWithServers(DistributedQueryRunner source, List<DriftServer> thriftServers)
        {
            this.source = requireNonNull(source, "source is null");
            this.thriftServers = ImmutableList.copyOf(requireNonNull(thriftServers, "thriftServers is null"));
        }

        public TestingPrestoServer getCoordinator()
        {
            return source.getCoordinator();
        }

        @Override
        public void close()
        {
            if (source != null) {
                closeQuietly(source);
                source = null;
            }
            if (thriftServers != null) {
                for (DriftServer server : thriftServers) {
                    server.shutdown();
                }
                thriftServers = null;
            }
        }

        @Override
        public int getNodeCount()
        {
            return source.getNodeCount();
        }

        @Override
        public Session getDefaultSession()
        {
            return source.getDefaultSession();
        }

        @Override
        public TransactionManager getTransactionManager()
        {
            return source.getTransactionManager();
        }

        @Override
        public Metadata getMetadata()
        {
            return source.getMetadata();
        }

        @Override
        public NodePartitioningManager getNodePartitioningManager()
        {
            return source.getNodePartitioningManager();
        }

        @Override
        public StatsCalculator getStatsCalculator()
        {
            return source.getStatsCalculator();
        }

        @Override
        public TestingAccessControlManager getAccessControl()
        {
            return source.getAccessControl();
        }

        @Override
        public MaterializedResult execute(String sql)
        {
            return source.execute(sql);
        }

        @Override
        public MaterializedResult execute(Session session, String sql)
        {
            return source.execute(session, sql);
        }

        @Override
        public List<QualifiedObjectName> listTables(Session session, String catalog, String schema)
        {
            return source.listTables(session, catalog, schema);
        }

        @Override
        public boolean tableExists(Session session, String table)
        {
            return source.tableExists(session, table);
        }

        @Override
        public void installPlugin(Plugin plugin)
        {
            source.installPlugin(plugin);
        }

        @Override
        public void createCatalog(String catalogName, String connectorName, Map<String, String> properties)
        {
            source.createCatalog(catalogName, connectorName, properties);
        }

        @Override
        public Lock getExclusiveLock()
        {
            return source.getExclusiveLock();
        }
    }
}
