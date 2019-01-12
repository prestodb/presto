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
package io.prestosql.benchmark;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.stats.TestingGcMonitor;
import io.airlift.units.DataSize;
import io.prestosql.Session;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStateMachine;
import io.prestosql.memory.DefaultQueryContext;
import io.prestosql.memory.MemoryPool;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.metadata.TableHandle;
import io.prestosql.operator.Driver;
import io.prestosql.operator.TaskContext;
import io.prestosql.plugin.memory.MemoryConnectorFactory;
import io.prestosql.plugin.tpch.TpchConnectorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.Plugin;
import io.prestosql.spi.QueryId;
import io.prestosql.spi.memory.MemoryPoolId;
import io.prestosql.spiller.SpillSpaceTracker;
import io.prestosql.testing.LocalQueryRunner;
import io.prestosql.testing.PageConsumerOperator;
import org.intellij.lang.annotations.Language;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;

import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static org.testng.Assert.assertTrue;

public class MemoryLocalQueryRunner
        implements AutoCloseable
{
    protected final LocalQueryRunner localQueryRunner;

    public MemoryLocalQueryRunner()
    {
        this(ImmutableMap.of());
    }

    public MemoryLocalQueryRunner(Map<String, String> properties)
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("memory")
                .setSchema("default");
        properties.forEach(sessionBuilder::setSystemProperty);

        localQueryRunner = createMemoryLocalQueryRunner(sessionBuilder.build());
    }

    public void installPlugin(Plugin plugin)
    {
        localQueryRunner.installPlugin(plugin);
    }

    public List<Page> execute(@Language("SQL") String query)
    {
        MemoryPool memoryPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(2, GIGABYTE));
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
        DefaultQueryContext queryContext = new DefaultQueryContext(
                new QueryId("test"),
                new DataSize(1, GIGABYTE),
                new DataSize(2, GIGABYTE),
                memoryPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                new DataSize(4, GIGABYTE),
                spillSpaceTracker);

        TaskContext taskContext = queryContext
                .addTaskContext(new TaskStateMachine(new TaskId("query", 0, 0), localQueryRunner.getExecutor()),
                        localQueryRunner.getDefaultSession(),
                        false,
                        false,
                        OptionalInt.empty());

        // Use NullOutputFactory to avoid coping out results to avoid affecting benchmark results
        ImmutableList.Builder<Page> output = ImmutableList.builder();
        List<Driver> drivers = localQueryRunner.createDrivers(
                query,
                new PageConsumerOperator.PageConsumerOutputFactory(types -> output::add),
                taskContext);

        boolean done = false;
        while (!done) {
            boolean processed = false;
            for (Driver driver : drivers) {
                if (!driver.isFinished()) {
                    driver.process();
                    processed = true;
                }
            }
            done = !processed;
        }

        return output.build();
    }

    private static LocalQueryRunner createMemoryLocalQueryRunner(Session session)
    {
        LocalQueryRunner localQueryRunner = LocalQueryRunner.queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());
        localQueryRunner.createCatalog(
                "memory",
                new MemoryConnectorFactory(),
                ImmutableMap.of("memory.max-data-per-node", "4GB"));

        return localQueryRunner;
    }

    public void dropTable(String tableName)
    {
        Session session = localQueryRunner.getDefaultSession();
        Metadata metadata = localQueryRunner.getMetadata();
        Optional<TableHandle> tableHandle = metadata.getTableHandle(session, QualifiedObjectName.valueOf(tableName));
        assertTrue(tableHandle.isPresent(), "Table " + tableName + " does not exist");
        metadata.dropTable(session, tableHandle.get());
    }

    @Override
    public void close()
    {
        localQueryRunner.close();
    }
}
