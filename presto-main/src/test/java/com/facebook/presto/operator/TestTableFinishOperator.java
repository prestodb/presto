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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.Session;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlockBuilder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.execution.Lifespan;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.TableFinishOperator.PageSinkCommitter;
import com.facebook.presto.operator.TableFinishOperator.TableFinishOperatorFactory;
import com.facebook.presto.operator.TableFinishOperator.TableFinisher;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.connector.ConnectorOutputMetadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.statistics.ColumnStatisticMetadata;
import com.facebook.presto.spi.statistics.ComputedStatistics;
import com.facebook.presto.sql.planner.plan.StatisticAggregationsDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.json.JsonCodec.jsonCodec;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.PageSinkCommitStrategy.LIFESPAN_COMMIT;
import static com.facebook.presto.operator.PageSinkCommitStrategy.NO_COMMIT;
import static com.facebook.presto.operator.TableWriterUtils.STATS_START_CHANNEL;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTableFinishOperator
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = createTestMetadataManager().getFunctionAndTypeManager();
    private static final InternalAggregationFunction LONG_MAX = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
            FUNCTION_AND_TYPE_MANAGER.lookupFunction("max", fromTypes(BIGINT)));
    private static final JsonCodec<TableCommitContext> TABLE_COMMIT_CONTEXT_CODEC = jsonCodec(TableCommitContext.class);

    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        scheduledExecutor.shutdownNow();
        scheduledExecutor = null;
    }

    @Test
    public void testStatisticsAggregation()
            throws Exception
    {
        TestingTableFinisher tableFinisher = new TestingTableFinisher();
        TestingPageSinkCommitter pageSinkCommitter = new TestingPageSinkCommitter();
        ColumnStatisticMetadata statisticMetadata = new ColumnStatisticMetadata("column", MAX_VALUE);
        StatisticAggregationsDescriptor<Integer> descriptor = new StatisticAggregationsDescriptor<>(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(statisticMetadata, 0));
        Session session = testSessionBuilder()
                .setSystemProperty("statistics_cpu_timer_enabled", "true")
                .build();
        TableFinishOperatorFactory operatorFactory = new TableFinishOperatorFactory(
                0,
                new PlanNodeId("node"),
                tableFinisher,
                pageSinkCommitter,
                new AggregationOperator.AggregationOperatorFactory(
                        1,
                        new PlanNodeId("test"),
                        AggregationNode.Step.SINGLE,
                        ImmutableList.of(LONG_MAX.bind(ImmutableList.of(STATS_START_CHANNEL), Optional.empty())),
                        true),
                descriptor,
                session,
                TABLE_COMMIT_CONTEXT_CODEC,
                false);
        DriverContext driverContext = createTaskContext(scheduledExecutor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        TableFinishOperator operator = (TableFinishOperator) operatorFactory.createOperator(driverContext);

        List<Type> inputTypes = ImmutableList.of(BIGINT, VARBINARY, VARBINARY, BIGINT);

        byte[] tableCommitContextForStatsPage = getTableCommitContextBytes(Lifespan.taskWide(), 0, 0, NO_COMMIT, false);
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, tableCommitContextForStatsPage, 6).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, tableCommitContextForStatsPage, 7).build().get(0));
        byte[] tableCommitContextForFragmentsPage = getTableCommitContextBytes(Lifespan.taskWide(), 0, 0, NO_COMMIT, true);
        operator.addInput(rowPagesBuilder(inputTypes).row(4, new byte[] {1}, tableCommitContextForFragmentsPage, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(5, new byte[] {2}, tableCommitContextForFragmentsPage, null).build().get(0));

        assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
        assertEquals(driverContext.getMemoryUsage(), 0);

        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.needsInput());

        operator.finish();
        assertFalse(operator.isFinished());

        assertNull(operator.getOutput());
        List<Type> outputTypes = ImmutableList.of(BIGINT);
        assertPageEquals(outputTypes, operator.getOutput(), rowPagesBuilder(outputTypes).row(9).build().get(0));

        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.needsInput());
        assertTrue(operator.isFinished());

        operator.close();

        assertEquals(tableFinisher.getFragments(), ImmutableList.of(Slices.wrappedBuffer(new byte[] {1}), Slices.wrappedBuffer(new byte[] {2})));
        assertEquals(tableFinisher.getComputedStatistics().size(), 1);
        assertEquals(getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics().size(), 1);
        Block expectedStatisticsBlock = new LongArrayBlockBuilder(null, 1)
                .writeLong(7)
                .closeEntry()
                .build();
        assertBlockEquals(BIGINT, getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics().get(statisticMetadata), expectedStatisticsBlock);

        TableFinishInfo tableFinishInfo = operator.getInfo();
        assertThat(tableFinishInfo.getStatisticsWallTime().getValue(NANOSECONDS)).isGreaterThan(0);
        assertThat(tableFinishInfo.getStatisticsCpuTime().getValue(NANOSECONDS)).isGreaterThan(0);

        assertTrue(pageSinkCommitter.getCommittedFragments().isEmpty());

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testTableWriteCommit()
            throws Exception
    {
        TestingTableFinisher tableFinisher = new TestingTableFinisher();
        TestingPageSinkCommitter pageSinkCommitter = new TestingPageSinkCommitter();
        ColumnStatisticMetadata statisticMetadata = new ColumnStatisticMetadata("column", MAX_VALUE);
        StatisticAggregationsDescriptor<Integer> descriptor = new StatisticAggregationsDescriptor<>(
                ImmutableMap.of(),
                ImmutableMap.of(),
                ImmutableMap.of(statisticMetadata, 0));
        Session session = testSessionBuilder()
                .setSystemProperty("statistics_cpu_timer_enabled", "true")
                .build();
        TableFinishOperatorFactory operatorFactory = new TableFinishOperatorFactory(
                0,
                new PlanNodeId("node"),
                tableFinisher,
                pageSinkCommitter,
                new AggregationOperator.AggregationOperatorFactory(
                        1,
                        new PlanNodeId("test"),
                        AggregationNode.Step.SINGLE,
                        ImmutableList.of(LONG_MAX.bind(ImmutableList.of(STATS_START_CHANNEL), Optional.empty())),
                        true),
                descriptor,
                session,
                TABLE_COMMIT_CONTEXT_CODEC,
                false);
        DriverContext driverContext = createTaskContext(scheduledExecutor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        TableFinishOperator operator = (TableFinishOperator) operatorFactory.createOperator(driverContext);

        List<Type> inputTypes = ImmutableList.of(BIGINT, VARBINARY, VARBINARY, BIGINT);

        // pages for non-grouped execution
        // expect lifespan committer not to be called and stats
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, getTableCommitContextBytes(Lifespan.taskWide(), 0, 0, NO_COMMIT, false), 1).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(3, new byte[] {2}, getTableCommitContextBytes(Lifespan.taskWide(), 0, 0, NO_COMMIT, true), null).build().get(0));
        assertTrue(pageSinkCommitter.getCommittedFragments().isEmpty());

        // pages for unrecoverable grouped execution
        // expect lifespan committer not to be called
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, getTableCommitContextBytes(Lifespan.driverGroup(1), 1, 1, NO_COMMIT, false), 4).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(6, new byte[] {5}, getTableCommitContextBytes(Lifespan.driverGroup(1), 1, 1, NO_COMMIT, true), null).build().get(0));
        assertTrue(pageSinkCommitter.getCommittedFragments().isEmpty());

        // pages for failed recoverable grouped execution
        // expect lifespan committer not to be called and page ignored
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, getTableCommitContextBytes(Lifespan.driverGroup(2), 2, 2, LIFESPAN_COMMIT, false), 100).build().get(0));
        assertTrue(pageSinkCommitter.getCommittedFragments().isEmpty());

        // pages for successful recoverable grouped execution
        // expect lifespan committer to be called and pages published
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, getTableCommitContextBytes(Lifespan.driverGroup(2), 2, 3, LIFESPAN_COMMIT, false), 9).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(11, new byte[] {10}, getTableCommitContextBytes(Lifespan.driverGroup(2), 2, 3, LIFESPAN_COMMIT, true), null).build().get(0));
        assertEquals(getOnlyElement(pageSinkCommitter.getCommittedFragments()), ImmutableList.of(Slices.wrappedBuffer(new byte[] {10})));

        assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
        assertEquals(driverContext.getMemoryUsage(), 0);

        assertTrue(operator.isBlocked().isDone());
        assertTrue(operator.needsInput());

        operator.finish();
        assertFalse(operator.isFinished());

        assertNull(operator.getOutput());
        List<Type> outputTypes = ImmutableList.of(BIGINT);
        assertPageEquals(outputTypes, operator.getOutput(), rowPagesBuilder(outputTypes).row(20).build().get(0));

        assertTrue(operator.isBlocked().isDone());
        assertFalse(operator.needsInput());
        assertTrue(operator.isFinished());

        operator.close();

        assertEquals(tableFinisher.getFragments(), ImmutableList.of(Slices.wrappedBuffer(new byte[] {2}), Slices.wrappedBuffer(new byte[] {5}), Slices.wrappedBuffer(new byte[] {
                10})));
        assertEquals(tableFinisher.getComputedStatistics().size(), 1);
        assertEquals(getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics().size(), 1);
        Block expectedStatisticsBlock = new LongArrayBlockBuilder(null, 1)
                .writeLong(9)
                .closeEntry()
                .build();
        assertBlockEquals(BIGINT, getOnlyElement(tableFinisher.getComputedStatistics()).getColumnStatistics().get(statisticMetadata), expectedStatisticsBlock);

        TableFinishInfo tableFinishInfo = operator.getInfo();
        assertThat(tableFinishInfo.getStatisticsWallTime().getValue(NANOSECONDS)).isGreaterThan(0);
        assertThat(tableFinishInfo.getStatisticsCpuTime().getValue(NANOSECONDS)).isGreaterThan(0);

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    private static byte[] getTableCommitContextBytes(Lifespan lifespan, int stageId, int taskId, PageSinkCommitStrategy pageSinkCommitStrategy, boolean lastPage)
    {
        return TABLE_COMMIT_CONTEXT_CODEC.toJsonBytes(
                new TableCommitContext(
                        lifespan,
                        new TaskId("query", stageId, 0, taskId),
                        pageSinkCommitStrategy,
                        lastPage));
    }

    private static class TestingTableFinisher
            implements TableFinisher
    {
        private boolean finished;
        private Collection<Slice> fragments;
        private Collection<ComputedStatistics> computedStatistics;

        @Override
        public Optional<ConnectorOutputMetadata> finishTable(Collection<Slice> fragments, Collection<ComputedStatistics> computedStatistics)
        {
            checkState(!finished, "already finished");
            finished = true;
            this.fragments = fragments;
            this.computedStatistics = computedStatistics;
            return Optional.empty();
        }

        public Collection<Slice> getFragments()
        {
            return fragments;
        }

        public Collection<ComputedStatistics> getComputedStatistics()
        {
            return computedStatistics;
        }
    }

    private static class TestingPageSinkCommitter
            implements PageSinkCommitter
    {
        private List<Collection<Slice>> fragmentsList = new ArrayList<>();

        @Override
        public ListenableFuture<Void> commitAsync(Collection<Slice> fragments)
        {
            fragmentsList.add(fragments);
            return immediateFuture(null);
        }

        public List<Collection<Slice>> getCommittedFragments()
        {
            return fragmentsList;
        }
    }
}
