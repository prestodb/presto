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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.prestosql.Session;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.TableFinishOperator.TableFinishOperatorFactory;
import io.prestosql.operator.TableFinishOperator.TableFinisher;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.LongArrayBlockBuilder;
import io.prestosql.spi.connector.ConnectorOutputMetadata;
import io.prestosql.spi.statistics.ColumnStatisticMetadata;
import io.prestosql.spi.statistics.ComputedStatistics;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.sql.planner.plan.StatisticAggregationsDescriptor;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ScheduledExecutorService;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.block.BlockAssertions.assertBlockEquals;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.metadata.MetadataManager.createTestMetadataManager;
import static io.prestosql.operator.PageAssertions.assertPageEquals;
import static io.prestosql.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarbinaryType.VARBINARY;
import static io.prestosql.testing.TestingSession.testSessionBuilder;
import static io.prestosql.testing.TestingTaskContext.createTaskContext;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

public class TestTableFinishOperator
{
    private static final InternalAggregationFunction LONG_MAX = createTestMetadataManager().getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("max", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));

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
        TestTableFinisher tableFinisher = new TestTableFinisher();
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
                new AggregationOperator.AggregationOperatorFactory(
                        1,
                        new PlanNodeId("test"),
                        AggregationNode.Step.SINGLE,
                        ImmutableList.of(LONG_MAX.bind(ImmutableList.of(2), Optional.empty())),
                        true),
                descriptor,
                session);
        DriverContext driverContext = createTaskContext(scheduledExecutor, scheduledExecutor, session)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
        TableFinishOperator operator = (TableFinishOperator) operatorFactory.createOperator(driverContext);

        List<Type> inputTypes = ImmutableList.of(BIGINT, VARBINARY, BIGINT);

        operator.addInput(rowPagesBuilder(inputTypes).row(4, null, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(5, null, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, new byte[] {1}, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, new byte[] {2}, null).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, 6).build().get(0));
        operator.addInput(rowPagesBuilder(inputTypes).row(null, null, 7).build().get(0));

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

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    private static class TestTableFinisher
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
}
