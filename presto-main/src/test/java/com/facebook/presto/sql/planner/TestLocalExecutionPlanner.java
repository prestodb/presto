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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.FragmentResultCacheContext;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.StateMachine;
import com.facebook.presto.execution.buffer.BufferResult;
import com.facebook.presto.execution.buffer.BufferState;
import com.facebook.presto.execution.buffer.OutputBuffer;
import com.facebook.presto.execution.buffer.OutputBufferInfo;
import com.facebook.presto.execution.buffer.OutputBuffers;
import com.facebook.presto.execution.scheduler.TableWriteInfo;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Closeables.closeAllRuntimeException;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.ENABLE_INTERMEDIATE_AGGREGATIONS;
import static com.facebook.presto.SystemSessionProperties.FRAGMENT_RESULT_CACHING_ENABLED;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.Collections.nCopies;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestLocalExecutionPlanner
{
    private static final ExecutorService EXECUTOR = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    private static final ScheduledExecutorService SCHEDULED_EXECUTOR = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));

    private LocalQueryRunner runner;

    @BeforeClass
    public void setUp()
    {
        runner = new LocalQueryRunner(TEST_SESSION);
        runner.createCatalog(runner.getDefaultSession().getCatalog().get(),
                new TpchConnectorFactory(1),
                ImmutableMap.of());
    }

    @AfterClass(alwaysRun = true)
    public void cleanup()
    {
        closeAllRuntimeException(runner);
        runner = null;
    }

    @Test
    public void testCompilerFailure()
    {
        // structure the query this way to avoid stack overflow when parsing
        String inner = "(" + Joiner.on(" + ").join(nCopies(100, "rand()")) + ")";
        String outer = Joiner.on(" + ").join(nCopies(100, inner));
        assertFails("SELECT " + outer, COMPILER_ERROR);
    }

    private void assertFails(@Language("SQL") String sql, ErrorCodeSupplier supplier)
    {
        try {
            runner.execute(sql);
            fail("expected exception");
        }
        catch (PrestoException e) {
            assertEquals(e.getErrorCode(), supplier.toErrorCode());
        }
    }

    @Test
    public void testCreatingFragmentResultCacheContext()
    {
        Session session = Session.builder(runner.getDefaultSession())
                .setSystemProperty(FRAGMENT_RESULT_CACHING_ENABLED, "true")
                .build();
        LocalExecutionPlan planWithoutIntermediateAggregation = getLocalExecutionPlan(session);
        // Expect one driver factory: partial aggregation
        assertEquals(planWithoutIntermediateAggregation.getDriverFactories().size(), 1);
        Optional<FragmentResultCacheContext> contextWithoutIntermediateAggregation = planWithoutIntermediateAggregation.getDriverFactories().get(0).getFragmentResultCacheContext();
        assertTrue(contextWithoutIntermediateAggregation.isPresent());

        session = Session.builder(runner.getDefaultSession())
                .setSystemProperty(FRAGMENT_RESULT_CACHING_ENABLED, "true")
                .setSystemProperty(ENABLE_INTERMEDIATE_AGGREGATIONS, "true")
                .build();
        LocalExecutionPlan planWithIntermediateAggregation = getLocalExecutionPlan(session);
        // Expect twp driver factories: partial aggregation and intermediate aggregation
        assertEquals(planWithIntermediateAggregation.getDriverFactories().size(), 2);
        Optional<FragmentResultCacheContext> contextWithIntermediateAggregation = planWithIntermediateAggregation.getDriverFactories().get(0).getFragmentResultCacheContext();
        assertTrue(contextWithIntermediateAggregation.isPresent());

        assertEquals(contextWithIntermediateAggregation.get().getHashedCanonicalPlanFragment(), contextWithoutIntermediateAggregation.get().getHashedCanonicalPlanFragment());
    }

    private LocalExecutionPlan getLocalExecutionPlan(Session session)
    {
        SubPlan subPlan = runner.inTransaction(session, transactionSession -> {
            Plan plan = runner.createPlan(transactionSession, "SELECT avg(totalprice) FROM orders", OPTIMIZED_AND_VALIDATED, false, WarningCollector.NOOP);
            return runner.createSubPlans(transactionSession, plan, false);
        });
        // Expect only one child sub plan doing partial aggregation.
        assertEquals(subPlan.getChildren().size(), 1);

        PlanFragment leafFragment = subPlan.getChildren().get(0).getFragment();
        return createTestingPlanner().plan(
                createTaskContext(EXECUTOR, SCHEDULED_EXECUTOR, session),
                leafFragment.getRoot(),
                leafFragment.getPartitioningScheme(),
                leafFragment.getStageExecutionDescriptor(),
                leafFragment.getTableScanSchedulingOrder(),
                new TestingOutputBuffer(),
                new TestingRemoteSourceFactory(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()));
    }

    private static class TestingOutputBuffer
            implements OutputBuffer
    {
        @Override
        public OutputBufferInfo getInfo()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFinished()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public double getUtilization()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isOverutilized()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addStateChangeListener(StateMachine.StateChangeListener<BufferState> stateChangeListener)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setOutputBuffers(OutputBuffers newOutputBuffers)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<BufferResult> get(OutputBuffers.OutputBufferId bufferId, long token, DataSize maxSize)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void acknowledge(OutputBuffers.OutputBufferId bufferId, long token)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void abort(OutputBuffers.OutputBufferId bufferId)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public ListenableFuture<?> isFull()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(Lifespan lifespan, List<SerializedPage> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void enqueue(Lifespan lifespan, int partition, List<SerializedPage> pages)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNoMorePages()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void destroy()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void fail()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void setNoMorePagesForLifespan(Lifespan lifespan)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void registerLifespanCompletionCallback(Consumer<Lifespan> callback)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isFinishedForLifespan(Lifespan lifespan)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getPeakMemoryUsage()
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingRemoteSourceFactory
            implements RemoteSourceFactory
    {
        @Override
        public SourceOperatorFactory createRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types)
        {
            return new TestingSourceOperatorFactory();
        }

        @Override
        public SourceOperatorFactory createMergeRemoteSource(Session session, int operatorId, PlanNodeId planNodeId, List<Type> types, List<Integer> outputChannels, List<Integer> sortChannels, List<SortOrder> sortOrder)
        {
            throw new UnsupportedOperationException();
        }
    }

    private static class TestingSourceOperatorFactory
            implements SourceOperatorFactory
    {
        @Override
        public PlanNodeId getSourceId()
        {
            return new PlanNodeId("test");
        }

        @Override
        public SourceOperator createOperator(DriverContext driverContext)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators()
        {
            throw new UnsupportedOperationException();
        }
    }
}
