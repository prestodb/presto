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
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OutputFactory;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.QueryId;
import com.facebook.presto.spi.memory.MemoryPoolId;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
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
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.testing.LocalQueryRunner.queryRunnerWithInitialTransaction;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestMemoryPools
{
    private static final DataSize TEN_MEGABYTES = new DataSize(10, MEGABYTE);
    private static final DataSize TEN_MEGABYTES_WITHOUT_TWO_BYTES = new DataSize(TEN_MEGABYTES.toBytes() - 2, BYTE);
    private static final DataSize ONE_BYTE = new DataSize(1, BYTE);
    private static final DataSize ZERO_BYTES = new DataSize(0, BYTE);

    private QueryId fakeQueryId;
    private LocalQueryRunner localQueryRunner;
    private MemoryPool userPool;
    private MemoryPool systemPool;
    private List<Driver> drivers;
    private TaskContext taskContext;

    private void setUp(Supplier<List<Driver>> driversSupplier)
    {
        Session session = TEST_SESSION
                .withSystemProperty("task_default_concurrency", "1");

        localQueryRunner = queryRunnerWithInitialTransaction(session);

        // add tpch
        localQueryRunner.createCatalog("tpch", new TpchConnectorFactory(1), ImmutableMap.<String, String>of());

        userPool = new MemoryPool(new MemoryPoolId("test"), TEN_MEGABYTES);
        systemPool = new MemoryPool(new MemoryPoolId("testSystem"), TEN_MEGABYTES);
        fakeQueryId = new QueryId("fake");
        QueryContext queryContext = new QueryContext(new QueryId("query"), new DataSize(10, MEGABYTE), userPool, systemPool, localQueryRunner.getExecutor());
        taskContext = createTaskContext(queryContext, localQueryRunner.getExecutor(), session, ZERO_BYTES);
        drivers = driversSupplier.get();
    }

    private void setUpCountStarFromOrders()
    {
        // query will reserve all memory in the user pool and discard the output
        setUp(() -> {
            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> { }));
            return localQueryRunner.createDrivers("SELECT COUNT(*), clerk FROM orders GROUP BY clerk", outputFactory, taskContext);
        });
    }

    private RevocableMemoryOperator setupConsumeRevocableMemory(DataSize reservedPerPage, long numberOfPages)
    {
        final AtomicReference<RevocableMemoryOperator> createOperator = new AtomicReference<>();
        setUp(() -> {
            DriverContext driverContext = taskContext.addPipelineContext(false, false).addDriverContext();
            OperatorContext revokableOperatorContext = driverContext.addOperatorContext(
                    Integer.MAX_VALUE,
                    new PlanNodeId("revokable_operator"),
                    TableScanOperator.class.getSimpleName());

            OutputFactory outputFactory = new PageConsumerOutputFactory(types -> (page -> { }));
            Operator outputOperator = outputFactory.createOutputOperator(2, new PlanNodeId("output"), ImmutableList.of(), Function.identity()).createOperator(driverContext);
            RevocableMemoryOperator revocableMemoryOperator = new RevocableMemoryOperator(revokableOperatorContext, reservedPerPage, numberOfPages);
            createOperator.set(revocableMemoryOperator);

            return ImmutableList.of(new Driver(driverContext, revocableMemoryOperator, outputOperator));
        });
        return createOperator.get();
    }

    @Test
    public void testBlockingOnUserMemory()
            throws Exception
    {
        setUpCountStarFromOrders();
        assertTrue(userPool.tryReserve(fakeQueryId, TEN_MEGABYTES.toBytes()));
        runDriversUntilBlocked(waitingForUserMemory());
        assertTrue(userPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));
        userPool.free(fakeQueryId, TEN_MEGABYTES.toBytes());
        assertDriversProgress(waitingForUserMemory());
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeSystem()
            throws Exception
    {
        setupConsumeRevocableMemory(ONE_BYTE, 10);
        assertTrue(systemPool.tryReserve(fakeQueryId, TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in system pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        assertTrue(systemPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // lets free 5 bytes
        systemPool.free(fakeQueryId, 5);
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 5);
        assertTrue(systemPool.getFreeBytes() <= 0, String.format("Expected empty pool but got [%d]", userPool.getFreeBytes()));

        // 3 more bytes is enough for driver to finish
        systemPool.free(fakeQueryId, 3);
        assertDriversProgress(waitingForRevocableSystemMemory());
        assertEquals(systemPool.getFreeBytes(), 10);
    }

    @Test
    public void testBlockingOnRevocableMemoryFreeViaRevoke()
            throws Exception
    {
        RevocableMemoryOperator revocableMemoryOperator = setupConsumeRevocableMemory(ONE_BYTE, 5);
        assertTrue(systemPool.tryReserve(fakeQueryId, TEN_MEGABYTES_WITHOUT_TWO_BYTES.toBytes()));

        // we expect 2 iterations as we have 2 bytes remaining in system pool and we allocate 1 byte per page
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestSystemMemoryRevoking();

        // 2 more iterations
        assertEquals(runDriversUntilBlocked(waitingForRevocableSystemMemory()), 2);
        revocableMemoryOperator.getOperatorContext().requestSystemMemoryRevoking();

        // 3 more bytes is enough for driver to finish
        assertDriversProgress(waitingForRevocableSystemMemory());
        assertEquals(systemPool.getFreeBytes(), 2);
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
                !operatorContext.isWaitingForSystemRevocableMemory().isDone() &&
                        !operatorContext.isSystemMemoryRevokingRequested();
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
        private final LocalMemoryContext memoryContext;
        private long producedPagesCount = 0;

        public RevocableMemoryOperator(OperatorContext operatorContext, DataSize reservedPerPage, long numberOfPages)
        {
            this.operatorContext = operatorContext;
            this.reservedPerPage = reservedPerPage;
            this.numberOfPages = numberOfPages;
            this.memoryContext = operatorContext.getSystemMemoryContext().newLocalMemoryContext();
        }

        @Override
        public ListenableFuture<?> revokeMemory()
        {
            memoryContext.setRevocableBytes(0);
            return Futures.immediateFuture(null);
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        @Override
        public List<Type> getTypes()
        {
            return ImmutableList.of();
        }

        @Override
        public void finish()
        {
            memoryContext.setRevocableBytes(0);
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
            memoryContext.setRevocableBytes(memoryContext.getRevocableBytes() + reservedPerPage.toBytes());
            producedPagesCount++;
            return new Page(10);
        }
    }
}
