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
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.AccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestAggregationOperator
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    private static final InternalAggregationFunction LONG_AVERAGE = getAggregation("avg", BIGINT);
    private static final InternalAggregationFunction DOUBLE_SUM = getAggregation("sum", DOUBLE);
    private static final InternalAggregationFunction LONG_SUM = getAggregation("sum", BIGINT);
    private static final InternalAggregationFunction REAL_SUM = getAggregation("sum", REAL);
    private static final InternalAggregationFunction COUNT = getAggregation("count", BIGINT);

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testAggregation()
    {
        InternalAggregationFunction countVarcharColumn = getAggregation("count", VARCHAR);
        InternalAggregationFunction maxVarcharColumn = getAggregation("max", VARCHAR);
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, VARCHAR, BIGINT, REAL, DOUBLE, VARCHAR)
                .addSequencePage(100, 0, 0, 300, 500, 400, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(1), Optional.empty()),
                        LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty()),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty()),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty()),
                        REAL_SUM.bind(ImmutableList.of(4), Optional.empty()),
                        DOUBLE_SUM.bind(ImmutableList.of(5), Optional.empty()),
                        maxVarcharColumn.bind(ImmutableList.of(6), Optional.empty())),
                false);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, REAL, DOUBLE, VARCHAR)
                .row(100L, 4950L, 49.5, "399", 100L, 54950L, 44950.0f, 54950.0, "599")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testDistinctMaskWithNull()
    {
        AccumulatorFactory distinctFactory = COUNT.bind(
                ImmutableList.of(0),
                Optional.of(1),
                ImmutableList.of(BIGINT, BOOLEAN),
                ImmutableList.of(),
                ImmutableList.of(),
                null,
                true, // distinct
                new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig()),
                ImmutableList.of(),
                false,
                TEST_SESSION);

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(distinctFactory),
                false);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        ByteArrayBlock trueMaskAllNull = new ByteArrayBlock(
                4,
                Optional.of(new boolean[] {true, true, true, true}), /* all positions are null */
                new byte[] {1, 1, 1, 1}); /* non-zero value is true, all masks are true */

        Block trueNullRleMask = new RunLengthEncodedBlock(trueMaskAllNull.getSingleValueBlock(0), 4);

        List<Page> input = ImmutableList.of(
                new Page(4, createLongsBlock(1, 2, 3, 4), trueMaskAllNull),
                new Page(4, createLongsBlock(5, 6, 7, 8), trueNullRleMask));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT)
                .row(0L) // all rows should be filtered by nulls
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMemoryTracking()
            throws Exception
    {
        testMemoryTracking(false);
        testMemoryTracking(true);
    }

    private void testMemoryTracking(boolean useSystemMemory)
            throws Exception
    {
        Page input = getOnlyElement(rowPagesBuilder(BIGINT).addSequencePage(100, 0).build());

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty())),
                useSystemMemory);

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            assertTrue(operator.needsInput());
            operator.addInput(input);

            if (useSystemMemory) {
                assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
                assertEquals(driverContext.getMemoryUsage(), 0);
            }
            else {
                assertEquals(driverContext.getSystemMemoryUsage(), 0);
                assertThat(driverContext.getMemoryUsage()).isGreaterThan(0);
            }

            toPages(operator, emptyIterator());
        }

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    private static InternalAggregationFunction getAggregation(String name, Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction(name, fromTypes(arguments)));
    }
}
