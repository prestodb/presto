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
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithInitialTransaction;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryPools
{
    private static final DataSize TEN_MEGABYTES = new DataSize(10, MEGABYTE);

    private QueryId fakeQueryId;
    private MemoryPool userPool;
    private List<Driver> drivers;
    private LocalQueryRunner localQueryRunner;

    @BeforeTest
    public void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("task_default_concurrency", "1")
                .build();

        localQueryRunner = queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        userPool = new MemoryPool(new MemoryPoolId("test"), TEN_MEGABYTES);
        MemoryPool systemPool = new MemoryPool(new MemoryPoolId("testSystem"), TEN_MEGABYTES);
        fakeQueryId = new QueryId("fake");
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(new QueryId("query"), TEN_MEGABYTES, userPool, systemPool, localQueryRunner.getExecutor(), TEN_MEGABYTES, spillSpaceTracker);

        // query will reserve all memory in the user pool and discard the output
        OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> { }));
        TaskContext taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), session);
        drivers = localQueryRunner.createDrivers("SELECT COUNT(*) FROM orders JOIN lineitem USING (orderkey)", outputFactory, taskContext);
    }

    @AfterTest
    public void tearDown()
            throws Exception
    {
        localQueryRunner.close();
    }

    @Test
    public void testBlockingOnUserMemory()
            throws Exception
    {
        assertTrue(userPool.tryReserve(fakeQueryId, TEN_MEGABYTES.toBytes()));
        runDriversUntilBlocked();
        assertTrue(userPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));
        userPool.free(fakeQueryId, TEN_MEGABYTES.toBytes());
        assertDriversProgress();
    }

    private void runDriversUntilBlocked()
    {
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
    }

    private void assertDriversProgress()
    {
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
