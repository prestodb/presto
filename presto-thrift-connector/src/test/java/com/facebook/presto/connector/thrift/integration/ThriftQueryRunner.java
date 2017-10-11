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
import com.facebook.presto.connector.thrift.location.HostList;
import com.facebook.presto.connector.thrift.server.ThriftIndexedTpchService;
import com.facebook.presto.connector.thrift.server.ThriftTpchService;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.server.testing.TestingPrestoServer;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Plugin;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.facebook.presto.tests.DistributedQueryRunner;
import com.facebook.presto.transaction.TransactionManager;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.log.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.testing.Closeables.closeQuietly;
import static java.util.Objects.requireNonNull;

public final class ThriftQueryRunner
{
    private ThriftQueryRunner() {}

    public static QueryRunner createThriftQueryRunner(int thriftServers, int workers, boolean enableIndexJoin)
            throws Exception
    {
        List<ThriftServer> servers = null;
        DistributedQueryRunner runner = null;
        try {
            servers = startThriftServers(thriftServers, enableIndexJoin);
            runner = createThriftQueryRunnerInternal(servers, workers);
            return new ThriftQueryRunnerWithServers(runner, servers);
        }
        catch (Throwable t) {
            closeQuietly(runner);
            // runner might be null, so closing servers explicitly
            if (servers != null) {
                for (ThriftServer server : servers) {
                    closeQuietly(server);
                }
            }
            throw t;
        }
    }

    public static void main(String[] args)
            throws Exception
    {
        ThriftQueryRunnerWithServers queryRunner = (ThriftQueryRunnerWithServers) createThriftQueryRunner(3, 3, true);
        Thread.sleep(10);
        Logger log = Logger.get(ThriftQueryRunner.class);
        log.info("======== SERVER STARTED ========");
        log.info("\n====\n%s\n====", queryRunner.getCoordinator().getBaseUrl());
    }

    private static List<ThriftServer> startThriftServers(int thriftServers, boolean enableIndexJoin)
    {
        List<ThriftServer> servers = new ArrayList<>(thriftServers);
        for (int i = 0; i < thriftServers; i++) {
            ThriftTpchService service = enableIndexJoin ? new ThriftIndexedTpchService() : new ThriftTpchService();
            ThriftServiceProcessor processor = new ThriftServiceProcessor(new ThriftCodecManager(), ImmutableList.of(), service);
            servers.add(new ThriftServer(processor).start());
        }
        return servers;
    }

    private static DistributedQueryRunner createThriftQueryRunnerInternal(List<ThriftServer> servers, int workers)
            throws Exception
    {
        List<HostAddress> addresses = servers.stream()
                .map(server -> HostAddress.fromParts("localhost", server.getPort()))
                .collect(toImmutableList());
        HostList hosts = HostList.fromList(addresses);

        Session defaultSession = testSessionBuilder()
                .setCatalog("thrift")
                .setSchema("tiny")
                .build();
        DistributedQueryRunner queryRunner = new DistributedQueryRunner(defaultSession, workers);
        queryRunner.installPlugin(new ThriftPlugin());
        Map<String, String> connectorProperties = ImmutableMap.of(
                "static-location.hosts", hosts.stringValue(),
                "PrestoThriftService.thrift.client.connect-timeout", "30s",
                "presto-thrift.lookup-requests-concurrency", "2");
        queryRunner.createCatalog("thrift", "presto-thrift", connectorProperties);
        return queryRunner;
    }

    /**
     * Wraps QueryRunner and a list of ThriftServers to clean them up together.
     */
    private static class ThriftQueryRunnerWithServers
            implements QueryRunner
    {
        private DistributedQueryRunner source;
        private List<ThriftServer> thriftServers;

        private ThriftQueryRunnerWithServers(DistributedQueryRunner source, List<ThriftServer> thriftServers)
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
                for (ThriftServer server : thriftServers) {
                    closeQuietly(server);
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
        public CostCalculator getCostCalculator()
        {
            return source.getCostCalculator();
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
