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
package com.facebook.presto.memory;

import com.facebook.presto.Session;
import com.facebook.presto.execution.QueryId;
import com.facebook.presto.metadata.InMemoryNodeManager;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithInitialTransaction;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryPools
{
    private static final long TEN_MEGABYTES = new DataSize(10, MEGABYTE).toBytes();

    @Test
    public void testBlocking()
            throws Exception
    {
        Session session = TEST_SESSION
                .withSystemProperty("task_default_concurrency", "1");

        LocalQueryRunner localQueryRunner = queryRunnerWithInitialTransaction(session);

        // add tpch
        InMemoryNodeManager nodeManager = localQueryRunner.getNodeManager();
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(nodeManager, 1), ImmutableMap.<String, String>of());

        // reserve all the memory in the pool
        MemoryPool pool = new MemoryPool(new MemoryPoolId("test"), new DataSize(10, MEGABYTE));
        QueryId fakeQueryId = new QueryId("fake");
        assertTrue(pool.tryReserve(fakeQueryId, TEN_MEGABYTES));
        MemoryPool systemPool = new MemoryPool(new MemoryPoolId("testSystem"), new DataSize(10, MEGABYTE));

        QueryContext queryContext = new QueryContext(new QueryId("query"), new DataSize(10, MEGABYTE), pool, systemPool, localQueryRunner.getExecutor());
        // discard all output
        OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> { }));
        TaskContext taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), session, new DataSize(0, BYTE));
        List<Driver> drivers = localQueryRunner.createDrivers("SELECT COUNT(*), clerk FROM orders GROUP BY clerk", outputFactory, taskContext);

        // run driver, until it blocks
        while (!isWaitingForMemory(drivers)) {
            for (Driver driver : drivers) {
                driver.process();
            }
        }

        // driver should be blocked waiting for memory
        for (Driver driver : drivers) {
            assertFalse(driver.isFinished());
        }
        assertTrue(pool.getFreeBytes() <= 0);

        pool.free(fakeQueryId, TEN_MEGABYTES);

        do {
            assertFalse(isWaitingForMemory(drivers));
            boolean progress = false;
            for (Driver driver : drivers) {
                ListenableFuture<?> blocked = driver.process();
                progress = progress | blocked.isDone();
            }
            // query should not block
            assertTrue(progress);
        } while (!drivers.stream().allMatch(Driver::isFinished));
    }

    public static boolean isWaitingForMemory(List<Driver> drivers)
    {
        for (Driver driver : drivers) {
            for (OperatorContext operatorContext : driver.getDriverContext().getOperatorContexts()) {
                if (!operatorContext.isWaitingForMemory().isDone()) {
                    return true;
                }
            }
        }
        return false;
    }
}
