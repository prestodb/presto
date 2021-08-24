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

import com.facebook.airlift.stats.TestingGcMonitor;
import com.facebook.presto.Session;
import com.facebook.presto.common.Page;
import com.facebook.presto.execution.buffer.TestingPagesSerdeFactory;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.TaskMemoryReservationSummary;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillSpaceTracker;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.PageConsumerOperator.PageConsumerOutputFactory;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.airlift.json.JsonCodec.listJsonCodec;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithInitialTransaction;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.GIGABYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestMemoryPools
{
    private static final DataSize TEN_MEGABYTES = new DataSize(10, MEGABYTE);
    private static final DataSize TEN_MEGABYTES_WITHOUT_TWO_BYTES = new DataSize(TEN_MEGABYTES.toBytes() - 2, BYTE);
    private static final DataSize ONE_BYTE = new DataSize(1, BYTE);

    private QueryId fakeQueryId;
    private LocalQueryRunner localQueryRunner;
    private MemoryPool userPool;
    private List<Driver> drivers;
    private TaskContext taskContext;

    private void setUp(Supplier<List<Driver>> driversSupplier)
    {
        checkState(localQueryRunner == null, "Already set up");

        Session session = testSessionBuilder()
                .setCatalog("tpch")
                .setSchema("tiny")
                .setSystemProperty("task_default_concurrency", "1")
                .build();

        localQueryRunner = queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.of());

        userPool = new MemoryPool(new MemoryPoolId("test"), TEN_MEGABYTES);
        fakeQueryId = new QueryId("fake");
        SpillSpaceTracker spillSpaceTracker = new SpillSpaceTracker(new DataSize(1, GIGABYTE));
        QueryContext queryContext = new QueryContext(new QueryId("query"),
                TEN_MEGABYTES,
                new DataSize(20, MEGABYTE),
                TEN_MEGABYTES,
                new DataSize(1, GIGABYTE),
                userPool,
                new TestingGcMonitor(),
                localQueryRunner.getExecutor(),
                localQueryRunner.getScheduler(),
                TEN_MEGABYTES,
                spillSpaceTracker,
                listJsonCodec(TaskMemoryReservationSummary.class));
        taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), session);
        drivers = driversSupplier.get();
    }

    private void setUpCountStarFromOrdersWithJoin()
    {
        // query will reserve all memory in the user pool and discard the output
        setUp(() -> {
            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> {}));
            return localQueryRunner.createDrivers("SELECT COUNT(*) FROM orders JOIN lineitem ON CAST(orders.orderkey AS VARCHAR) = CAST(lineitem.orderkey AS VARCHAR)", outputFactory, taskContext);
        });
    }

    private RevocableMemoryOperator setupConsumeRevocableMemory(DataSize reservedPerPage, long numberOfPages)
    {
        AtomicReference<RevocableMemoryOperator> createOperator = new AtomicReference<>();
        setUp(() -> {
            DriverContext driverContext = taskContext.addPipelineContext(0, false, false, false).addDriverContext();
            OperatorContext revokableOperatorContext = driverContext.addOperatorContext(
                    Integer.MAX_VALUE,
                    new PlanNodeId("revokable_operator"),
                    TableScanOperator.class.getSimpleName());

            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> {}));
            Operator outputOperator = outputFactory.createOutputOperator(2, new PlanNodeId("output"), ImmutableList.of(), Function.identity(), Optional.empty(), new TestingPagesSerdeFactory())
                    .createOperator(driverContext);
            RevocableMemoryOperator revocableMemoryOperator = new RevocableMemoryOperator(revokableOperatorContext, reservedPerPage, numberOfPages);
            createOperator.set(revocableMemoryOperator);

            Driver driver = Driver.createDriver(driverContext, revocableMemoryOperator, outputOperator);
            return ImmutableList.of(driver);
        });
        return createOperator.get();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        if (localQueryRunner != null) {
            localQueryRunner.close();
            localQueryRunner = null;
        }
    }

    @Test
    public void testBlockingOnUserMemory()
    {
        setUpCountStarFromOrdersWithJoin();
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES.toBytes()));
        runDriversUntilBlocked(waitingForUserMemory());
        assertTrue(userPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));
        userPool.free(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertDriversProgress(waitingForUserMemory());
    }

    @Test
    public void testNotifyListenerOnMemoryReserved()
    {
        setupConsumeRevocableMemory(ONE_BYTE, 10);
        AtomicReference<MemoryPool> notifiedPool = new AtomicReference<>();
        AtomicLong notifiedBytes = new AtomicLong();
        userPool.addListener((pool, queryId, memoryReservation) -> {
            notifiedPool.set(pool);
            notifiedBytes.set(pool.getReservedBytes());
        });

        userPool.reserve(fakeQueryId, "test", 3);
        assertEquals(notifiedPool.get(), userPool);
        assertEquals(notifiedBytes.get(), 3L);
    }

    @Test
    public void testMemoryFutureCancellation()
    {
        setUpCountStarFromOrdersWithJoin();
        ListenableFuture future = userPool.reserve(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertTrue(!future.isDone());
        try {
            future.cancel(true);
            fail("cancel should fail");
        }
        catch (UnsupportedOperationException e) {
            assertEquals(e.getMessage(), "cancellation is not supported");
        }
        userPool.free(fakeQueryId, "test", TEN_MEGABYTES.toBytes());
        assertTrue(future.isDone());
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeUser()
    {
        setupConsumeRevocableMemory(ONE_BYTE, 10);
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        assertTrue(userPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // lets free 5 bytes
        userPool.free(fakeQueryId, "test", 5);
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 5);
        assertTrue(userPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // 3 more bytes is enough for driver to finish
        userPool.free(fakeQueryId, "test", 3);
        assertDriversProgress(waitingForRevocableSystemMemory());
        assertEquals(userPool.getFreeBytes(), 10);
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeViaRevoke()
    {
        RevocableMemoryOperator revocableMemoryOperator = setupConsumeRevocableMemory(ONE_BYTE, 5);
        assertTrue(userPool.tryReserve(fakeQueryId, "test", TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in memory pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestMemoryRevoking();

        // 2 more iterations
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestMemoryRevoking();

        // 3 more bytes is enough for driver to finish
        assertDriversProgress(waitingForRevocableSystemMemory());
        assertEquals(userPool.getFreeBytes(), 2);
    }

    @Test
    public void testTaggedAllocations()
    {
        QueryId testQuery = new QueryId("test_query");
        MemoryPool testPool = new MemoryPool(new MemoryPoolId("test"), new DataSize(1000, BYTE));

        testPool.reserve(testQuery, "test_tag", 10);

        Map<String, Long> allocations = testPool.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 10L));

        // free 5 bytes for test_tag
        testPool.free(testQuery, "test_tag", 5);
        allocations = testPool.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 5L));

        testPool.reserve(testQuery, "test_tag2", 20);
        allocations = testPool.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 5L, "test_tag2", 20L));

        // free the remaining 5 bytes for test_tag
        testPool.free(testQuery, "test_tag", 5);
        allocations = testPool.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag2", 20L));

        // free all for test_tag2
        testPool.free(testQuery, "test_tag2", 20);
        assertEquals(testPool.getTaggedMemoryAllocations().size(), 0);
    }

    @Test
    public void testMoveQuery()
    {
        QueryId testQuery = new QueryId("test_query");
        MemoryPool pool1 = new MemoryPool(new MemoryPoolId("test"), new DataSize(1000, BYTE));
        MemoryPool pool2 = new MemoryPool(new MemoryPoolId("test"), new DataSize(1000, BYTE));
        pool1.reserve(testQuery, "test_tag", 10);

        Map<String, Long> allocations = pool1.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 10L));

        pool1.moveQuery(testQuery, pool2);
        assertNull(pool1.getTaggedMemoryAllocations(testQuery));
        allocations = pool2.getTaggedMemoryAllocations(testQuery);
        assertEquals(allocations, ImmutableMap.of("test_tag", 10L));

        assertEquals(pool1.getFreeBytes(), 1000);
        assertEquals(pool2.getFreeBytes(), 990);

        pool2.free(testQuery, "test", 10);
        assertEquals(pool2.getFreeBytes(), 1000);
    }

    private long runDriversUntilBlocked(Predicate<OperatorContext> reason)
    {
        long iterationsCount = 0;

        // run driver, until it blocks
        while (!isOperatorBlocked(drivers, reason)) {
            for (Driver driver : drivers) {
                driver.process();
            }
            iterationsCount++;
        }

        // driver should be blocked waiting for memory
        for (Driver driver : drivers) {
            assertFalse(driver.isFinished());
        }
        return iterationsCount;
    }

    private void assertDriversProgress(Predicate<OperatorContext> reason)
    {
        do {
            assertFalse(isOperatorBlocked(drivers, reason));
            boolean progress = false;
            for (Driver driver : drivers) {
                ListenableFuture<?> blocked = driver.process();
                progress = progress | blocked.isDone();
            }
            // query should not block
            assertTrue(progress);
        }
        while (!drivers.stream().allMatch(Driver::isFinished));
    }

    private Predicate<OperatorContext> waitingForUserMemory()
    {
        return (OperatorContext operatorContext) -> !operatorContext.isWaitingForMemory().isDone();
    }

    private Predicate<OperatorContext> waitingForRevocableSystemMemory()
    {
        return (OperatorContext operatorContext) ->
                !operatorContext.isWaitingForRevocableMemory().isDone() &&
                        !operatorContext.isMemoryRevokingRequested();
    }

    private static boolean isOperatorBlocked(List<Driver> drivers, Predicate<OperatorContext> reason)
    {
        for (Driver driver : drivers) {
            for (OperatorContext operatorContext : driver.getDriverContext().getOperatorContexts()) {
                if (reason.apply(operatorContext)) {
                    return true;
                }
            }
        }
        return false;
    }

    private class RevocableMemoryOperator
            implements Operator
    {
        private final DataSize reservedPerPage;
        private final long numberOfPages;
        private final OperatorContext operatorContext;
        private long producedPagesCount;
        private final LocalMemoryContext revocableMemoryContext;

        public RevocableMemoryOperator(OperatorContext operatorContext, DataSize reservedPerPage, long numberOfPages)
        {
            this.operatorContext = operatorContext;
            this.reservedPerPage = reservedPerPage;
            this.numberOfPages = numberOfPages;
            this.revocableMemoryContext = operatorContext.localRevocableMemoryContext();
        }

        @Override
        public ListenableFuture<?> startMemoryRevoke()
        {
            return Futures.immediateFuture(null);
        }

        @Override
        public void finishMemoryRevoke()
        {
            revocableMemoryContext.setBytes(0);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public void finish()
        {
            revocableMemoryContext.setBytes(0);
        }

        @Override
        public boolean isFinished()
        {
            return producedPagesCount >= numberOfPages;
        }

        @Override
        public boolean needsInput()
        {
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page getOutput()
        {
            revocableMemoryContext.setBytes(revocableMemoryContext.getBytes() + reservedPerPage.toBytes());
            producedPagesCount++;
            if (producedPagesCount == numberOfPages) {
                finish();
            }
            return new Page(10);
        }
    }
}
