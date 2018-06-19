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

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.AggregationOperator.AggregationInputChannel;
import com.facebook.presto.operator.AggregationOperator.AggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.DistinctInternalAccumulatorFactory;
import com.facebook.presto.operator.aggregation.GeneralInternalAccumulatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.assertBlockEquals;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Streams.mapWithIndex;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestAggregationOperator
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();

    private static final InternalAggregationFunction LONG_AVERAGE = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("avg", AGGREGATE, DOUBLE.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction DOUBLE_SUM = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, DOUBLE.getTypeSignature(), DOUBLE.getTypeSignature()));
    private static final InternalAggregationFunction LONG_SUM = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, BIGINT.getTypeSignature(), BIGINT.getTypeSignature()));
    private static final InternalAggregationFunction REAL_SUM = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("sum", AGGREGATE, REAL.getTypeSignature(), REAL.getTypeSignature()));
    private static final InternalAggregationFunction COUNT = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
            new Signature("count", AGGREGATE, BIGINT.getTypeSignature()));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private final JoinCompiler joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

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
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        InternalAggregationFunction countVarcharColumn = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT), parseTypeSignature(StandardTypes.VARCHAR)));
        InternalAggregationFunction maxVarcharColumn = metadata.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("max", AGGREGATE, parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR)));
        List<Type> inputTypes = ImmutableList.of(VARCHAR, BIGINT, VARCHAR, BIGINT, REAL, DOUBLE, VARCHAR);
        List<Page> input = rowPagesBuilder(inputTypes)
                .addSequencePage(100, 0, 0, 300, 500, 400, 500, 500)
                .build();

        OperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.SINGLE,
                ImmutableList.of(
                        new GeneralInternalAccumulatorFactory(COUNT.bind(ImmutableList.of(0), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(LONG_SUM.bind(ImmutableList.of(1), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(countVarcharColumn.bind(ImmutableList.of(0), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(LONG_SUM.bind(ImmutableList.of(3), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(REAL_SUM.bind(ImmutableList.of(4), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(DOUBLE_SUM.bind(ImmutableList.of(5), Optional.empty())),
                        new GeneralInternalAccumulatorFactory(maxVarcharColumn.bind(ImmutableList.of(6), Optional.empty()))),
                mapWithIndex(inputTypes.stream(), (type, channel) -> new AggregationInputChannel(new Symbol("i" + channel), (int) channel, type))
                    .collect(toImmutableList()),
                new DataSize(16, MEGABYTE));

        DriverContext driverContext = createDriverContext(Integer.MAX_VALUE);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT, REAL, DOUBLE, VARCHAR)
                .row(100L, 4950L, 49.5, "399", 100L, 54950L, 44950.0f, 54950.0, "599")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testMultiplePartialFlushes()
            throws Exception
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(500, 0)
                .addSequencePage(500, 500)
                .addSequencePage(500, 1000)
                .addSequencePage(500, 1500)
                .addSequencePage(500, 2000)
                .addSequencePage(500, 2500)
                .build();

        DataSize maxPartialMemory = new DataSize(100, KILOBYTE);
        AggregationOperatorFactory operatorFactory = new AggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                Step.PARTIAL,
                ImmutableList.of(new DistinctInternalAccumulatorFactory(
                        COUNT.bind(ImmutableList.of(0), Optional.empty()),
                        ImmutableList.of(BIGINT),
                        ImmutableList.of(0),
                        Optional.empty(),
                        TEST_SESSION,
                        joinCompiler)),
                ImmutableList.of(new AggregationInputChannel(new Symbol("x"), 0, BIGINT)),
                maxPartialMemory);

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            Iterator<Page> inputIterator = input.iterator();

            // Fill up the aggregation
            long lastMemoryUsage = driverContext.getSystemMemoryUsage();
            assertLessThan(lastMemoryUsage, maxPartialMemory.toBytes());
            boolean memoryIncreased = false;
            boolean memoryDecreased = false;
            while (inputIterator.hasNext()) {
                Page inputPage = inputIterator.next();
                operator.addInput(inputPage);

                long currentMemoryUsage = driverContext.getSystemMemoryUsage();
                // memory should always be less than the max
                assertLessThan(currentMemoryUsage, maxPartialMemory.toBytes());
                if (currentMemoryUsage > lastMemoryUsage) {
                    memoryIncreased = true;
                }
                if (currentMemoryUsage < lastMemoryUsage) {
                    memoryDecreased = true;
                }
                lastMemoryUsage = currentMemoryUsage;

                // all values are unique, so output should be the same size as input
                Page output = operator.getOutput();
                assertBlockEquals(BIGINT, output.getBlock(2), inputPage.getBlock(0));
                assertEquals(output.getBlock(0).getPositionCount(), inputPage.getPositionCount());
            }
            // we expect memory to increase and then decrease at least once
            assertTrue(memoryIncreased);
            assertTrue(memoryDecreased);

            // finish the operator
            operator.finish();
            // there should be one output page
            Page output = operator.getOutput();
            assertNotNull(output);
            assertTrue(operator.isFinished());
            assertFalse(operator.needsInput());

            // there should be one output position and it should be null
            assertEquals(output.getPositionCount(), 1);
            assertTrue(output.getBlock(1).isNull(0));
        }
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true)
                .addDriverContext();
    }
}
