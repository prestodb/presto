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
package com.facebook.presto.operator;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.FragmentResultCacheContext;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.ScheduledSplit;
import com.facebook.presto.execution.TaskSource;
import com.facebook.presto.memory.context.LocalMemoryContext;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.operator.FileFragmentResultCacheManager.CacheKey;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.FixedPageSource;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.schedule.NodeSelectionStrategy;
import com.facebook.presto.split.PageSourceProvider;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.PageConsumerOperator;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import io.airlift.units.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.FRAGMENT_RESULT_CACHING_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.FragmentResultCacheContext.createFragmentResultCacheContext;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.plan.AggregationNode.singleGroupingSet;
import static com.facebook.presto.spi.schedule.NodeSelectionStrategy.HARD_AFFINITY;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestDriver
{
    private static final TableHandle TESTING_TABLE_HANDLE = new TableHandle(
            new ConnectorId("test"),
            new TestingTableHandle(),
            new TestingTransactionHandle(UUID.randomUUID()),
            Optional.empty());

    private static final FragmentResultCacheContext TESTING_FRAGMENT_RESULT_CACHE_CONTEXT = createFragmentResultCacheContext(
            new TestingFragmentResultCacheManager(),
            new AggregationNode(new PlanNodeId("test-agg"),
                    new TableScanNode(
                            new PlanNodeId("test-scan"),
                            TESTING_TABLE_HANDLE,
                            ImmutableList.of(),
                            ImmutableMap.of(),
                            TupleDomain.all(),
                            TupleDomain.all()),
                    ImmutableMap.of(),
                    singleGroupingSet(ImmutableList.of()),
                    ImmutableList.of(),
                    AggregationNode.Step.PARTIAL,
                    Optional.empty(),
                    Optional.empty()),
            new PartitioningScheme(Partitioning.create(FIXED_HASH_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
            testSessionBuilder().setSystemProperty(FRAGMENT_RESULT_CACHING_ENABLED, "true").build(),
            new ObjectMapper()).get();

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DriverContext driverContext;
    private DriverContext driverContextWithFragmentResultCacheContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

        driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        driverContextWithFragmentResultCacheContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNormalFinish()
    {
        testNormalFinish(driverContext);
        testNormalFinish(driverContextWithFragmentResultCacheContext);
    }

    private void testNormalFinish(DriverContext driverContext)
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator sink = createSinkOperator(driverContext, types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        ListenableFuture<?> blocked = driver.processFor(new Duration(1, TimeUnit.SECONDS));
        assertTrue(blocked.isDone());
        assertTrue(driver.isFinished());

        assertTrue(sink.isFinished());
        assertTrue(source.isFinished());
    }

    // The race can be reproduced somewhat reliably when the invocationCount is 10K, but we use 1K iterations to cap the test runtime.
    @Test(invocationCount = 1_000, timeOut = 10_000)
    public void testConcurrentClose()
    {
        testConcurrentClose(driverContext);
        testConcurrentClose(driverContextWithFragmentResultCacheContext);
    }

    private void testConcurrentClose(DriverContext driverContext)
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        OperatorContext operatorContext = driverContext.addOperatorContext(0, new PlanNodeId("test"), "values");
        ValuesOperator source = new ValuesOperator(operatorContext, rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        Operator sink = createSinkOperator(driverContext, types);
        Driver driver = Driver.createDriver(driverContext, source, sink);
        // let these threads race
        scheduledExecutor.submit(() -> driver.processFor(new Duration(1, TimeUnit.NANOSECONDS))); // don't want to call isFinishedInternal in processFor
        scheduledExecutor.submit(driver::close);
        while (!driverContext.isDone()) {
            sleepUninterruptibly(1, TimeUnit.MILLISECONDS);
        }
    }

    @Test
    public void testAbruptFinish()
    {
        testAbruptFinish(driverContext);
        testAbruptFinish(driverContextWithFragmentResultCacheContext);
    }

    private void testAbruptFinish(DriverContext driverContext)
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        ValuesOperator source = new ValuesOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "values"), rowPagesBuilder(types)
                .addSequencePage(10, 20, 30, 40)
                .build());

        PageConsumerOperator sink = createSinkOperator(driverContext, types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        driver.close();
        assertTrue(driver.isFinished());

        // finish is only called in normal operations
        assertFalse(source.isFinished());
        assertFalse(sink.isFinished());

        // close is always called (values operator doesn't have a closed state)
        assertTrue(sink.isClosed());
    }

    @Test
    public void testAddSourceFinish()
    {
        testAddSourceFinish(driverContext);
        testAddSourceFinish(driverContextWithFragmentResultCacheContext);
    }

    private void testAddSourceFinish(DriverContext driverContext)
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        final List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new TableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TESTING_TABLE_HANDLE,
                ImmutableList.of());

        PageConsumerOperator sink = createSinkOperator(driverContext, types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        assertFalse(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.updateSource(new TaskSource(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertFalse(driver.isFinished());
        assertTrue(driver.processFor(new Duration(1, TimeUnit.SECONDS)).isDone());
        assertTrue(driver.isFinished());

        assertTrue(sink.isFinished());
        assertTrue(source.isFinished());
    }

    @Test
    public void testBrokenOperatorCloseWhileProcessing()
            throws Exception
    {
        testBrokenOperatorCloseWhileProcessing(driverContext);
        testBrokenOperatorCloseWhileProcessing(driverContextWithFragmentResultCacheContext);
    }

    private void testBrokenOperatorCloseWhileProcessing(DriverContext driverContext)
            throws Exception
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), false);
        final Driver driver = Driver.createDriver(driverContext, brokenOperator, createSinkOperator(driverContext, ImmutableList.of()));

        assertSame(driver.getDriverContext(), driverContext);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(() -> driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        brokenOperator.waitForLocked();

        driver.close();
        assertTrue(driver.isFinished());

        try {
            driverProcessFor.get(1, TimeUnit.SECONDS);
            fail("Expected InterruptedException");
        }
        catch (ExecutionException e) {
            assertDriverInterrupted(e.getCause());
        }
    }

    @Test
    public void testBrokenOperatorProcessWhileClosing()
            throws Exception
    {
        testBrokenOperatorProcessWhileClosing(driverContext);
        testBrokenOperatorProcessWhileClosing(driverContextWithFragmentResultCacheContext);
    }

    private void testBrokenOperatorProcessWhileClosing(DriverContext driverContext)
            throws Exception
    {
        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"), true);
        final Driver driver = Driver.createDriver(driverContext, brokenOperator, createSinkOperator(driverContext, ImmutableList.of()));

        assertSame(driver.getDriverContext(), driverContext);

        // block thread in operator close
        Future<Boolean> driverClose = executor.submit(() -> {
            driver.close();
            return true;
        });
        brokenOperator.waitForLocked();

        assertTrue(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertTrue(driver.isFinished());

        brokenOperator.unlock();

        assertTrue(driverClose.get());
    }

    @Test
    public void testMemoryRevocationRace()
    {
        testMemoryRevocationRace(driverContext);
        testMemoryRevocationRace(driverContextWithFragmentResultCacheContext);
    }

    private void testMemoryRevocationRace(DriverContext driverContext)
    {
        List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new AlwaysBlockedMemoryRevokingTableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "scan"),
                new PlanNodeId("source"),
                (session, split, table, columns) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TESTING_TABLE_HANDLE,
                ImmutableList.of());

        Driver driver = Driver.createDriver(driverContext, source, createSinkOperator(driverContext, types));
        // the table scan operator will request memory revocation with requestMemoryRevoking()
        // while the driver is still not done with the processFor() method and before it moves to
        // updateDriverBlockedFuture() method.
        assertTrue(driver.processFor(new Duration(100, TimeUnit.MILLISECONDS)).isDone());
    }

    @Test
    public void testBrokenOperatorAddSource()
            throws Exception
    {
        testBrokenOperatorAddSource(driverContext);
        testBrokenOperatorAddSource(driverContextWithFragmentResultCacheContext);
    }

    private void testBrokenOperatorAddSource(DriverContext driverContext)
            throws Exception
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        final List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        // create a table scan operator that does not block, which will cause the driver loop to busy wait
        TableScanOperator source = new NotBlockedTableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TESTING_TABLE_HANDLE,
                ImmutableList.of());

        BrokenOperator brokenOperator = new BrokenOperator(driverContext.addOperatorContext(0, new PlanNodeId("test"), "source"));
        final Driver driver = Driver.createDriver(driverContext, source, brokenOperator);

        // block thread in operator processing
        Future<Boolean> driverProcessFor = executor.submit(() -> driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        brokenOperator.waitForLocked();

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertTrue(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.updateSource(new TaskSource(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertFalse(driver.isFinished());
        // processFor always returns NOT_BLOCKED, because DriveLockResult was not acquired
        assertTrue(driver.processFor(new Duration(1, TimeUnit.SECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.close();
        assertTrue(driver.isFinished());

        try {
            driverProcessFor.get(1, TimeUnit.SECONDS);
            fail("Expected InterruptedException");
        }
        catch (ExecutionException e) {
            assertDriverInterrupted(e.getCause());
        }
    }

    @Test
    public void testFragmentResultCache()
    {
        processSourceDriver(driverContextWithFragmentResultCacheContext);

        // Create a new driver and test cache hit
        driverContextWithFragmentResultCacheContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext(Lifespan.taskWide(), Optional.of(TESTING_FRAGMENT_RESULT_CACHE_CONTEXT));
        processSourceDriver(driverContextWithFragmentResultCacheContext);
    }

    private void processSourceDriver(DriverContext driverContext)
    {
        PlanNodeId sourceId = new PlanNodeId("source");
        final List<Type> types = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        TableScanOperator source = new TableScanOperator(driverContext.addOperatorContext(99, new PlanNodeId("test"), "values"),
                sourceId,
                (session, split, table, columns) -> new FixedPageSource(rowPagesBuilder(types)
                        .addSequencePage(10, 20, 30, 40)
                        .build()),
                TESTING_TABLE_HANDLE,
                ImmutableList.of());

        PageConsumerOperator sink = createSinkOperator(driverContext, types);
        Driver driver = Driver.createDriver(driverContext, source, sink);

        assertSame(driver.getDriverContext(), driverContext);

        assertFalse(driver.isFinished());
        assertFalse(driver.processFor(new Duration(1, TimeUnit.MILLISECONDS)).isDone());
        assertFalse(driver.isFinished());

        driver.updateSource(new TaskSource(sourceId, ImmutableSet.of(new ScheduledSplit(0, sourceId, newMockSplit())), true));

        assertFalse(driver.isFinished());
        assertTrue(driver.processFor(new Duration(1, TimeUnit.SECONDS)).isDone());
        assertTrue(driver.isFinished());

        assertTrue(sink.isFinished());
        assertTrue(source.isFinished());
    }

    private void assertDriverInterrupted(Throwable cause)
    {
        checkArgument(cause instanceof PrestoException, "Expected root cause exception to be an instance of PrestoException");
        assertEquals(((PrestoException) cause).getErrorCode(), GENERIC_INTERNAL_ERROR.toErrorCode());
        assertEquals(cause.getMessage(), "Driver was interrupted");
    }

    private static Split newMockSplit()
    {
        return new Split(new ConnectorId("test"), TestingTransactionHandle.create(), new MockSplit(1));
    }

    private PageConsumerOperator createSinkOperator(DriverContext driverContext, List<Type> types)
    {
        // materialize the output to catch some type errors
        MaterializedResult.Builder resultBuilder = MaterializedResult.resultBuilder(driverContext.getSession(), types);
        return new PageConsumerOperator(driverContext.addOperatorContext(1, new PlanNodeId("test"), "sink"), resultBuilder::page, Function.identity());
    }

    private static class BrokenOperator
            implements Operator, Closeable
    {
        private final OperatorContext operatorContext;
        private final ReentrantLock lock = new ReentrantLock();
        private final CountDownLatch lockedLatch = new CountDownLatch(1);
        private final CountDownLatch unlockLatch = new CountDownLatch(1);
        private final boolean lockForClose;

        private BrokenOperator(OperatorContext operatorContext)
        {
            this(operatorContext, false);
        }

        private BrokenOperator(OperatorContext operatorContext, boolean lockForClose)
        {
            this.operatorContext = operatorContext;
            this.lockForClose = lockForClose;
        }

        @Override
        public OperatorContext getOperatorContext()
        {
            return operatorContext;
        }

        public void unlock()
        {
            unlockLatch.countDown();
        }

        private void waitForLocked()
        {
            try {
                assertTrue(lockedLatch.await(10, TimeUnit.SECONDS));
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        private void waitForUnlock()
        {
            try {
                assertTrue(lock.tryLock(1, TimeUnit.SECONDS));
                try {
                    lockedLatch.countDown();
                    assertTrue(unlockLatch.await(5, TimeUnit.SECONDS));
                }
                finally {
                    lock.unlock();
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException("Interrupted", e);
            }
        }

        @Override
        public void finish()
        {
            waitForUnlock();
        }

        @Override
        public boolean isFinished()
        {
            waitForUnlock();
            return true;
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            waitForUnlock();
            return NOT_BLOCKED;
        }

        @Override
        public boolean needsInput()
        {
            waitForUnlock();
            return false;
        }

        @Override
        public void addInput(Page page)
        {
            waitForUnlock();
        }

        @Override
        public Page getOutput()
        {
            waitForUnlock();
            return null;
        }

        @Override
        public void close()
        {
            if (lockForClose) {
                waitForUnlock();
            }
        }
    }

    private static class AlwaysBlockedMemoryRevokingTableScanOperator
            extends TableScanOperator
    {
        public AlwaysBlockedMemoryRevokingTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, table, columns);
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            // this operator is always blocked and when queried by the driver
            // it triggers memory revocation so that the driver gets unblocked
            LocalMemoryContext revocableMemoryContext = getOperatorContext().localRevocableMemoryContext();
            revocableMemoryContext.setBytes(100);
            getOperatorContext().requestMemoryRevoking();
            return SettableFuture.create();
        }
    }

    private static class NotBlockedTableScanOperator
            extends TableScanOperator
    {
        public NotBlockedTableScanOperator(
                OperatorContext operatorContext,
                PlanNodeId planNodeId,
                PageSourceProvider pageSourceProvider,
                TableHandle table,
                Iterable<ColumnHandle> columns)
        {
            super(operatorContext, planNodeId, pageSourceProvider, table, columns);
        }

        @Override
        public ListenableFuture<?> isBlocked()
        {
            return NOT_BLOCKED;
        }
    }

    private static class MockSplit
            implements ConnectorSplit
    {
        private final int identifier;

        public MockSplit(int identifier)
        {
            this.identifier = identifier;
        }

        @Override
        public NodeSelectionStrategy getNodeSelectionStrategy()
        {
            return HARD_AFFINITY;
        }

        @Override
        public List<HostAddress> getPreferredNodes(List<HostAddress> sortedCandidates)
        {
            return ImmutableList.of();
        }

        @Override
        public Object getInfo()
        {
            return null;
        }

        @Override
        public Object getSplitIdentifier()
        {
            return identifier;
        }
    }

    private static class TestingFragmentResultCacheManager
            implements FragmentResultCacheManager
    {
        private final Map<CacheKey, List<Page>> cache = new HashMap<>();

        @Override
        public Future<?> put(String plan, Split split, List<Page> result)
        {
            cache.put(new CacheKey(plan, split.getSplitIdentifier()), result);
            return immediateFuture(null);
        }

        @Override
        public Optional<Iterator<Page>> get(String plan, Split split)
        {
            CacheKey key = new CacheKey(plan, split.getSplitIdentifier());
            if (cache.containsKey(key)) {
                return Optional.of(cache.get(key).iterator());
            }
            return Optional.empty();
        }
    }
}
