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
package com.facebook.presto.benchmark;

import com.facebook.presto.connector.StaticConnector;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.NullOutputOperator.NullOutputFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Connector;
import com.facebook.presto.spi.ConnectorFactory;
import com.facebook.presto.spi.ConnectorHandleResolver;
import com.facebook.presto.spi.ConnectorMetadata;
import com.facebook.presto.spi.ConnectorSplitManager;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.split.ConnectorDataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchDataStreamProvider;
import com.facebook.presto.tpch.TpchHandleResolver;
import com.facebook.presto.tpch.TpchMetadata;
import com.facebook.presto.tpch.TpchSplitManager;
import com.facebook.presto.util.LocalQueryRunner;
import com.google.common.collect.ImmutableClassToInstanceMap;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

public abstract class AbstractSqlBenchmark
        extends AbstractOperatorBenchmark
{
    @Language("SQL")
    private final String query;
    private final LocalQueryRunner tpchLocalQueryRunner;

    protected AbstractSqlBenchmark(
            ExecutorService executor,
            TpchBlocksProvider tpchBlocksProvider,
            String benchmarkName,
            int warmupIterations,
            int measuredIterations,
            @Language("SQL") String query)
    {
        super(executor, tpchBlocksProvider, benchmarkName, warmupIterations, measuredIterations);
        this.query = query;
        this.tpchLocalQueryRunner = createTpchLocalQueryRunner(getTpchBlocksProvider(), executor);
    }

    @Override
    protected List<Driver> createDrivers(TaskContext taskContext)
    {
        return tpchLocalQueryRunner.createDrivers(query, new NullOutputFactory(), taskContext);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(TpchBlocksProvider tpchBlocksProvider, ExecutorService executor)
    {
        return createTpchLocalQueryRunner(new Session("user", "test", "tpch", "tiny", null, null), tpchBlocksProvider, executor);
    }

    public static LocalQueryRunner createTpchLocalQueryRunner(Session session, TpchBlocksProvider tpchBlocksProvider, ExecutorService executor)
    {
        LocalQueryRunner localQueryRunner = new LocalQueryRunner(session, executor);
        BenchmarkConnectorFactory connectorFactory = new BenchmarkConnectorFactory(localQueryRunner.getNodeManager(), tpchBlocksProvider);
        localQueryRunner.createCatalog(session.getCatalog(), connectorFactory, ImmutableMap.<String, String>of());
        return localQueryRunner;
    }

    public static class BenchmarkConnectorFactory
            implements ConnectorFactory
    {
        private final NodeManager nodeManager;
        private final TpchBlocksProvider tpchBlocksProvider;

        public BenchmarkConnectorFactory(NodeManager nodeManager, TpchBlocksProvider tpchBlocksProvider)
        {
            this.nodeManager = nodeManager;
            this.tpchBlocksProvider = tpchBlocksProvider;
        }

        @Override
        public String getName()
        {
            return "benchmark";
        }

        @Override
        public Connector create(String connectorId, Map<String, String> config)
        {
            ImmutableClassToInstanceMap.Builder<Object> builder = ImmutableClassToInstanceMap.builder();
            builder.put(ConnectorMetadata.class, new TpchMetadata());
            builder.put(ConnectorSplitManager.class, new TpchSplitManager(connectorId, nodeManager, 1));
            builder.put(ConnectorDataStreamProvider.class, new TpchDataStreamProvider(tpchBlocksProvider));
            builder.put(ConnectorHandleResolver.class, new TpchHandleResolver());

            return new StaticConnector(builder.build());
        }
    }
}
