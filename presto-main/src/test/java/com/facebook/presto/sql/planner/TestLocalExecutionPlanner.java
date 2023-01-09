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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.cost.StatsAndCosts;
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
import com.facebook.presto.operator.DriverFactory;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceOperatorFactory;
import com.facebook.presto.operator.StageExecutionDescriptor;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TaskOutputOperator;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.page.SerializedPage;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LocalExecutionPlanner.LocalExecutionPlan;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingHandle;
import com.facebook.presto.testing.TestingMetadata;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchConnectorFactory;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.execution.TaskTestUtils.createTestingPlanner;
import static com.facebook.presto.spi.StandardErrorCode.COMPILER_ERROR;
import static com.facebook.presto.sql.planner.LogicalPlanner.Stage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
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

    @Test
    public void testCustomPlanTranslator()
    {
        VariableReferenceExpression variable = new VariableReferenceExpression(Optional.empty(), "column", VARCHAR);
        PlanNode scan = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("sourceId"),
                new TableHandle(new ConnectorId("test"), new TestingMetadata.TestingTableHandle(), TestingTransactionHandle.create(), Optional.of(TestingHandle.INSTANCE)),
                ImmutableList.of(variable),
                ImmutableMap.of(variable, new TestingMetadata.TestingColumnHandle("column")),
                TupleDomain.all(),
                TupleDomain.all());
        PlanNode node1 = new CustomNodeA(new PlanNodeId("node1"), scan);
        PlanNode node2 = new CustomNodeB(new PlanNodeId("node2"), node1);

        LocalExecutionPlan plan = getLocalExecutionPlan(
                runner.getDefaultSession(),
                node2,
                ImmutableList.of(new CustomOperatorAFactory.PlanTranslator(), new CustomOperatorBFactory.PlanTranslator()));

        List<DriverFactory> driverFactories = plan.getDriverFactories();
        assertEquals(driverFactories.size(), 1);
        List<OperatorFactory> operatorFactories = driverFactories.get(0).getOperatorFactories();
        assertEquals(operatorFactories.size(), 4);
        assertTrue(operatorFactories.get(0) instanceof TableScanOperator.TableScanOperatorFactory);
        assertTrue(operatorFactories.get(1) instanceof CustomOperatorAFactory);
        assertTrue(operatorFactories.get(2) instanceof CustomOperatorBFactory);
        assertTrue(operatorFactories.get(3) instanceof TaskOutputOperator.TaskOutputOperatorFactory);
    }

    private LocalExecutionPlan getLocalExecutionPlan(Session session, PlanNode plan, List<LocalExecutionPlanner.CustomPlanTranslator> customPlanTranslators)
    {
        PlanFragment testFragment = new PlanFragment(
                new PlanFragmentId(0),
                plan,
                ImmutableSet.of(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(new PlanNodeId("sourceId")),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                StageExecutionDescriptor.ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
        return createTestingPlanner().plan(
                createTaskContext(EXECUTOR, SCHEDULED_EXECUTOR, session),
                testFragment,
                new TestingOutputBuffer(),
                new TestingRemoteSourceFactory(),
                new TableWriteInfo(Optional.empty(), Optional.empty(), Optional.empty()),
                customPlanTranslators);
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
                leafFragment,
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

    private static class CustomNodeA
            extends CustomNode
    {
        protected CustomNodeA(PlanNodeId id, PlanNode source)
        {
            super(Optional.empty(), id, Optional.empty(), source);
        }
    }

    private static class CustomNodeB
            extends CustomNode
    {
        protected CustomNodeB(PlanNodeId id, PlanNode source)
        {
            super(Optional.empty(), id, Optional.empty(), source);
        }
    }

    private static class CustomNode
            extends PlanNode
    {
        private final PlanNode source;

        protected CustomNode(Optional<SourceLocation> sourceLocation, PlanNodeId id, Optional<PlanNode> statsEquivalentPlanNode, PlanNode source)
        {
            super(sourceLocation, id, statsEquivalentPlanNode);
            this.source = source;
        }

        public PlanNode getSource()
        {
            return source;
        }

        @Override
        public List<PlanNode> getSources()
        {
            return singletonList(source);
        }

        @Override
        public List<VariableReferenceExpression> getOutputVariables()
        {
            return ImmutableList.of();
        }

        @Override
        public PlanNode replaceChildren(List<PlanNode> newChildren)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public PlanNode assignStatsEquivalentPlanNode(Optional<PlanNode> statsEquivalentPlanNode)
        {
            throw new UnsupportedOperationException();
        }
    }

    public static class CustomOperatorAFactory
            extends CustomOperatorFactory
    {
        public CustomOperatorAFactory(int operatorId, PlanNodeId sourceId)
        {
            super(operatorId, sourceId);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, CustomOperatorA.class.getSimpleName());
            return new CustomOperatorA(operatorContext, sourceId);
        }

        public static class PlanTranslator
                extends LocalExecutionPlanner.CustomPlanTranslator
        {
            @Override
            public Optional<LocalExecutionPlanner.PhysicalOperation> translate(
                    PlanNode node,
                    LocalExecutionPlanner.LocalExecutionPlanContext context,
                    InternalPlanVisitor<LocalExecutionPlanner.PhysicalOperation, LocalExecutionPlanner.LocalExecutionPlanContext> visitor)
            {
                if (node instanceof CustomNodeA) {
                    OperatorFactory operatorFactory = new CustomOperatorAFactory(
                            context.getNextOperatorId(),
                            node.getId());
                    LocalExecutionPlanner.PhysicalOperation sourceOperator = ((CustomNodeA) node).getSource().accept(visitor, context);
                    return Optional.of(
                            new LocalExecutionPlanner.PhysicalOperation(operatorFactory, makeLayout(node), context, sourceOperator));
                }
                return Optional.empty();
            }
        }

        public static class CustomOperatorA
                extends CustomOperator
        {
            public CustomOperatorA(OperatorContext operatorContext, PlanNodeId planNodeId)
            {
                super(operatorContext, planNodeId);
            }
        }
    }

    public static class CustomOperatorBFactory
            extends CustomOperatorFactory
    {
        public CustomOperatorBFactory(int operatorId, PlanNodeId sourceId)
        {
            super(operatorId, sourceId);
        }

        @Override
        public Operator createOperator(DriverContext driverContext)
        {
            OperatorContext operatorContext = driverContext.addOperatorContext(operatorId, sourceId, CustomOperatorB.class.getSimpleName());
            return new CustomOperatorB(operatorContext, sourceId);
        }

        public static class PlanTranslator
                extends LocalExecutionPlanner.CustomPlanTranslator
        {
            @Override
            public Optional<LocalExecutionPlanner.PhysicalOperation> translate(
                    PlanNode node,
                    LocalExecutionPlanner.LocalExecutionPlanContext context,
                    InternalPlanVisitor<LocalExecutionPlanner.PhysicalOperation, LocalExecutionPlanner.LocalExecutionPlanContext> visitor)
            {
                if (node instanceof CustomNodeB) {
                    OperatorFactory operatorFactory = new CustomOperatorBFactory(
                            context.getNextOperatorId(),
                            node.getId());
                    LocalExecutionPlanner.PhysicalOperation sourceOperator = ((CustomNodeB) node).getSource().accept(visitor, context);
                    return Optional.of(
                            new LocalExecutionPlanner.PhysicalOperation(operatorFactory, makeLayout(node), context, sourceOperator));
                }
                return Optional.empty();
            }
        }

        public class CustomOperatorB
                extends CustomOperator
        {
            public CustomOperatorB(OperatorContext operatorContext, PlanNodeId planNodeId)
            {
                super(operatorContext, planNodeId);
            }
        }
    }

    public abstract static class CustomOperatorFactory
            implements OperatorFactory
    {
        protected final int operatorId;
        protected final PlanNodeId sourceId;

        public CustomOperatorFactory(
                int operatorId,
                PlanNodeId sourceId)
        {
            this.operatorId = operatorId;
            this.sourceId = requireNonNull(sourceId, "sourceId is null");
        }

        public abstract Operator createOperator(DriverContext driverContext);

        @Override
        public synchronized void noMoreOperators(Lifespan lifespan)
        {
        }

        @Override
        public OperatorFactory duplicate()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void noMoreOperators()
        {
        }

        public static class CustomOperator
                implements Operator
        {
            private final OperatorContext operatorContext;
            private final PlanNodeId planNodeId;

            private boolean finished;

            public CustomOperator(
                    OperatorContext operatorContext,
                    PlanNodeId planNodeId)
            {
                this.operatorContext = requireNonNull(operatorContext, "operatorContext is null");
                this.planNodeId = requireNonNull(planNodeId, "planNodeId is null");
            }

            @Override
            public OperatorContext getOperatorContext()
            {
                return operatorContext;
            }

            @Override
            public void close()
            {
                finish();
            }

            @Override
            public void finish()
            {
                finished = true;
            }

            @Override
            public boolean isFinished()
            {
                return finished;
            }

            @Override
            public boolean needsInput()
            {
                return false;
            }

            @Override
            public void addInput(Page page)
            {
                throw new UnsupportedOperationException(getClass().getName() + " can not take input");
            }

            @Override
            public Page getOutput()
            {
                return null;
            }
        }
    }
}
