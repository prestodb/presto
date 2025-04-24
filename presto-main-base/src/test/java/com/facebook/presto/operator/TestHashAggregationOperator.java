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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.DataSize.Unit;
import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ByteArrayBlock;
import com.facebook.presto.common.block.PageBuilderStatus;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.memory.context.AggregatedMemoryContext;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.builder.HashAggregationBuilder;
import com.facebook.presto.operator.aggregation.builder.InMemoryHashAggregationBuilder;
import com.facebook.presto.operator.aggregation.partial.PartialAggregationController;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.Spiller;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.airlift.units.DataSize.succinctBytes;
import static com.facebook.airlift.units.DataSize.succinctDataSize;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.GroupByHashYieldResult;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.createPagesWithDistinctHashKeys;
import static com.facebook.presto.operator.GroupByHashYieldAssertion.finishOperatorWithYieldingGroupByHash;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.dropChannel;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.aggregation.GenericAccumulatorFactory.generateAccumulatorFactory;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.lang.String.format;
import static java.util.Collections.emptyIterator;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestHashAggregationOperator
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    private static final JavaAggregationFunctionImplementation LONG_AVERAGE = getAggregation("avg", BIGINT);
    private static final JavaAggregationFunctionImplementation LONG_SUM = getAggregation("sum", BIGINT);
    private static final JavaAggregationFunctionImplementation COUNT = FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(
            FUNCTION_AND_TYPE_MANAGER.lookupFunction("count", ImmutableList.of()));

    private static final int MAX_BLOCK_SIZE_IN_BYTES = 64 * 1024;

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager());
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @DataProvider(name = "hashEnabled")
    public static Object[][] hashEnabled()
    {
        return new Object[][] {{true}, {false}};
    }

    @DataProvider(name = "hashEnabledAndMemoryLimitForMergeValues")
    public static Object[][] hashEnabledAndMemoryLimitForMergeValuesProvider()
    {
        return new Object[][] {
                {true, true, true, 8, Integer.MAX_VALUE},
                {true, true, false, 8, Integer.MAX_VALUE},
                {false, false, false, 0, 0},
                {false, true, true, 0, 0},
                {false, true, false, 0, 0},
                {false, true, true, 8, 0},
                {false, true, false, 8, 0},
                {false, true, true, 8, Integer.MAX_VALUE},
                {false, true, false, 8, Integer.MAX_VALUE}};
    }

    @DataProvider
    public Object[][] dataType()
    {
        return new Object[][] {{VARCHAR}, {BIGINT}};
    }

    @AfterMethod
    public void tearDown()
    {
        spillerFactory = null;
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 40_000;
        JavaAggregationFunctionImplementation countVarcharColumn = getAggregation("count", VARCHAR);
        JavaAggregationFunctionImplementation countBooleanColumn = getAggregation("count", BOOLEAN);
        JavaAggregationFunctionImplementation maxVarcharColumn = getAggregation("max", VARCHAR);
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(numberOfRows, 100, 0, 100_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 200_000, 0, 500)
                .addSequencePage(numberOfRows, 100, 0, 300_000, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(LONG_SUM, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(LONG_AVERAGE, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(maxVarcharColumn, ImmutableList.of(2), Optional.empty()),
                        generateAccumulatorFactory(countVarcharColumn, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(countBooleanColumn, ImmutableList.of(4), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                Optional.empty(),
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false);

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row(Integer.toString(i), 3L, 3L * i, (double) i, Integer.toString(300_000 + i), 3L, 3L);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");
        assertPagesEqualIgnoreOrder(driverContext, pages, expected, hashEnabled, Optional.of(hashChannels.size()));

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationWithGlobals(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        JavaAggregationFunctionImplementation countVarcharColumn = getAggregation("count", VARCHAR);
        JavaAggregationFunctionImplementation countBooleanColumn = getAggregation("count", BOOLEAN);
        JavaAggregationFunctionImplementation maxVarcharColumn = getAggregation("max", VARCHAR);

        Optional<Integer> groupIdChannel = Optional.of(1);
        List<Integer> groupByChannels = Ints.asList(1, 2);
        List<Integer> globalAggregationGroupIds = Ints.asList(42, 49);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, groupByChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder.build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR, BIGINT),
                groupByChannels,
                ImmutableList.of(),
                globalAggregationGroupIds,
                Step.SINGLE,
                true,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(LONG_SUM, ImmutableList.of(4), Optional.empty()),
                        generateAccumulatorFactory(LONG_AVERAGE, ImmutableList.of(4), Optional.empty()),
                        generateAccumulatorFactory(maxVarcharColumn, ImmutableList.of(2), Optional.empty()),
                        generateAccumulatorFactory(countVarcharColumn, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(countBooleanColumn, ImmutableList.of(5), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                groupIdChannel,
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                Optional.empty(),
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false);

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row(null, 42L, 0L, null, null, null, 0L, 0L)
                .row(null, 49L, 0L, null, null, null, 0L, 0L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, hashEnabled, Optional.of(groupByChannels.size()), revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashAggregationMemoryReservation(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        JavaAggregationFunctionImplementation arrayAggColumn = getAggregation("array_agg", BIGINT);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0)
                .addSequencePage(10, 200, 0)
                .addSequencePage(10, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.MEGABYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                true,
                ImmutableList.of(generateAccumulatorFactory(arrayAggColumn, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                Optional.empty(),
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false);

        Operator operator = operatorFactory.createOperator(driverContext);
        toPages(operator, input.iterator(), revokeMemoryWhenAddingPages);
        assertEquals(operator.getOperatorContext().getOperatorStats().getUserMemoryReservationInBytes(), 0);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 10B.*")
    public void testMemoryLimit(boolean hashEnabled)
    {
        JavaAggregationFunctionImplementation maxVarcharColumn = getAggregation("max", VARCHAR);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, BIGINT, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(LONG_SUM, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(LONG_AVERAGE, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(maxVarcharColumn, ImmutableList.of(2), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabledAndMemoryLimitForMergeValues")
    public void testHashBuilderResize(boolean hashEnabled, boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimitForMerge, long memoryLimitForMergeWithMemory)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(200_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createDriverContext(memoryLimitForMerge);

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                spillEnabled,
                Optional.empty(),
                succinctBytes(memoryLimitForMerge),
                succinctBytes(memoryLimitForMergeWithMemory),
                spillerFactory,
                joinCompiler,
                false);

        toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "dataType")
    public void testMemoryReservationYield(Type type)
    {
        List<Page> input = createPagesWithDistinctHashKeys(type, 6_000, 600);
        OperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(type),
                ImmutableList.of(0),
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty())),
                Optional.of(1),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false);

        // get result with yield; pick a relatively small buffer for aggregator's memory usage
        GroupByHashYieldResult result;
        result = finishOperatorWithYieldingGroupByHash(input, type, operatorFactory, this::getHashCapacity, 1_400_000);
        assertGreaterThan(result.getYieldCount(), 5);
        assertGreaterThan(result.getMaxReservedBytes(), 20L << 20);

        int count = 0;
        for (Page page : result.getOutput()) {
            // value + hash + aggregation result
            assertEquals(page.getChannelCount(), 3);
            for (int i = 0; i < page.getPositionCount(); i++) {
                assertEquals(page.getBlock(2).getLong(i), 1);
                count++;
            }
        }
        assertEquals(count, 6_000 * 600);
    }

    @Test(dataProvider = "hashEnabled", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 3MB.*")
    public void testHashBuilderResizeLimit(boolean hashEnabled)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(null, 1, MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(5_000_000)); // this must be larger than MAX_BLOCK_SIZE_IN_BYTES, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(3, MEGABYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(VARCHAR),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiSliceAggregationOutput(boolean hashEnabled)
    {
        // estimate the number of entries required to create 1.5 pages of results
        // See InMemoryHashAggregationBuilder.buildTypes()
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_LONG +  // Used by BigintGroupByHash, see BigintGroupByHash.TYPES_WITH_RAW_HASH
                SIZE_OF_LONG + SIZE_OF_DOUBLE;              // Used by COUNT and LONG_AVERAGE aggregators;
        int multiSlicePositionCount = (int) (1.5 * PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(LONG_AVERAGE, ImmutableList.of(1), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                false);

        assertEquals(toPages(operatorFactory, createDriverContext(), input).size(), 2);
    }

    @Test(dataProvider = "hashEnabled")
    public void testMultiplePartialFlushes(boolean hashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(500, 0)
                .addSequencePage(500, 500)
                .addSequencePage(500, 1000)
                .addSequencePage(500, 1500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.PARTIAL,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(1, KILOBYTE)),
                joinCompiler,
                true);

        DriverContext driverContext = createDriverContext(1024);

        try (Operator operator = operatorFactory.createOperator(driverContext)) {
            List<Page> expectedPages = rowPagesBuilder(BIGINT, BIGINT)
                    .addSequencePage(2000, 0, 0)
                    .build();
            MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                    .pages(expectedPages)
                    .build();

            Iterator<Page> inputIterator = input.iterator();

            // Fill up the aggregation
            while (operator.needsInput() && inputIterator.hasNext()) {
                operator.addInput(inputIterator.next());
            }

            assertThat(driverContext.getSystemMemoryUsage()).isGreaterThan(0);
            assertEquals(driverContext.getMemoryUsage(), 0);

            // Drain the output (partial flush)
            List<Page> outputPages = new ArrayList<>();
            while (true) {
                Page output = operator.getOutput();
                if (output == null) {
                    break;
                }
                outputPages.add(output);
            }

            // There should be some pages that were drained
            assertTrue(!outputPages.isEmpty());

            // The operator need input again since this was a partial flush
            assertTrue(operator.needsInput());

            // Now, drive the operator to completion
            outputPages.addAll(toPages(operator, inputIterator));

            MaterializedResult actual;
            if (hashEnabled) {
                // Drop the hashChannel for all pages
                outputPages = dropChannel(outputPages, ImmutableList.of(1));
            }
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expected.getTypes(), outputPages);

            assertEquals(actual.getTypes(), expected.getTypes());
            assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
        }

        assertEquals(driverContext.getSystemMemoryUsage(), 0);
        assertEquals(driverContext.getMemoryUsage(), 0);
    }

    @Test
    public void testMergeWithMemorySpill()
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);

        int smallPagesSpillThresholdSize = 150000;

        List<Page> input = rowPagesBuilder
                .addSequencePage(smallPagesSpillThresholdSize, 0)
                .addSequencePage(10, smallPagesSpillThresholdSize)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                true,
                Optional.empty(),
                new DataSize(smallPagesSpillThresholdSize, Unit.BYTE),
                succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                joinCompiler,
                false);

        DriverContext driverContext = createDriverContext(smallPagesSpillThresholdSize);

        MaterializedResult.Builder resultBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT);
        for (int i = 0; i < smallPagesSpillThresholdSize + 10; ++i) {
            resultBuilder.row((long) i, (long) i);
        }

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, resultBuilder.build());
    }

    @Test
    public void testMemoryLimitInSpillWhenTriggerRehash()
    {
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(BIGINT);

        int smallPagesSpillThresholdSize = 100000;

        List<Page> input = rowPagesBuilder
                .addSequencePage(smallPagesSpillThresholdSize, 0)
                .addSequencePage(smallPagesSpillThresholdSize, smallPagesSpillThresholdSize)
                .addSequencePage(smallPagesSpillThresholdSize, 2 * smallPagesSpillThresholdSize)
                .addSequencePage(smallPagesSpillThresholdSize, 3 * smallPagesSpillThresholdSize)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                true,
                Optional.empty(),
                new DataSize(smallPagesSpillThresholdSize, Unit.BYTE),
                succinctBytes(Integer.MAX_VALUE),
                spillerFactory,
                joinCompiler,
                false);

        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION,
                new DataSize(10, MEGABYTE), new DataSize(20, MEGABYTE));
        DriverContext driverContext = taskContext
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        MaterializedResult.Builder resultBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT);
        for (int i = 0; i < 4 * smallPagesSpillThresholdSize; ++i) {
            resultBuilder.row((long) i, (long) i);
        }
        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, resultBuilder.build());
    }

    @Test
    public void testSpillerFailure()
    {
        JavaAggregationFunctionImplementation maxVarcharColumn = getAggregation("max", VARCHAR);

        List<Integer> hashChannels = Ints.asList(1);
        ImmutableList<Type> types = ImmutableList.of(VARCHAR, BIGINT, VARCHAR, BIGINT);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, types);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setQueryMaxMemory(DataSize.valueOf("7MB"))
                .setMemoryPoolSize(DataSize.valueOf("1GB"))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(0), Optional.empty()),
                        generateAccumulatorFactory(LONG_SUM, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(LONG_AVERAGE, ImmutableList.of(3), Optional.empty()),
                        generateAccumulatorFactory(maxVarcharColumn, ImmutableList.of(2), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                true,
                Optional.empty(),
                succinctBytes(8),
                succinctBytes(Integer.MAX_VALUE),
                new FailingSpillerFactory(),
                joinCompiler,
                false);

        try {
            toPages(operatorFactory, driverContext, input);
            fail("An exception was expected");
        }
        catch (RuntimeException expected) {
            if (!nullToEmpty(expected.getMessage()).matches(".* Failed to spill")) {
                fail("Exception other than expected was thrown", expected);
            }
        }
    }

    @Test
    public void testMask()
    {
        int positions = 4;
        Block groupingBlock = RunLengthEncodedBlock.create(BIGINT, 1L, positions);
        Block countBlock = RunLengthEncodedBlock.create(BIGINT, 1L, positions);
        Block maskBlock = new ByteArrayBlock(positions, Optional.of(new boolean[] {false, false, true, true}), new byte[] {(byte) 0, (byte) 1, (byte) 0, (byte) 1});
        Page page = new Page(groupingBlock, countBlock, maskBlock);

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                ImmutableList.of(0),
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                false,
                ImmutableList.of(generateAccumulatorFactory(COUNT, ImmutableList.of(1), Optional.of(2))),
                Optional.empty(),
                Optional.empty(),
                1,
                Optional.of(new DataSize(16, MEGABYTE)),
                false,
                Optional.empty(),
                new DataSize(16, MEGABYTE),
                new DataSize(16, MEGABYTE),
                new FailingSpillerFactory(),
                joinCompiler,
                false);

        List<Page> outputPages = toPages(operatorFactory, createDriverContext(), ImmutableList.of(page)).stream()
                .filter(p -> p.getPositionCount() > 0)
                .collect(toImmutableList());
        assertEquals(outputPages.size(), 1);
        Page outputPage = outputPages.get(0);
        assertEquals(outputPage.getBlock(0).getLong(0), 1L);
        assertEquals(outputPage.getBlock(1).getLong(0), 1L);
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
        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(false, hashChannels, BIGINT);
        Page input = getOnlyElement(rowPagesBuilder.addSequencePage(500, 0).build());

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.SINGLE,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                rowPagesBuilder.getHashChannel(),
                Optional.empty(),
                100_000,
                Optional.of(new DataSize(16, MEGABYTE)),
                joinCompiler,
                useSystemMemory);

        DriverContext driverContext = createDriverContext(1024);

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

    @Test
    public void testAdaptivePartialAggregation()
    {
        List<Integer> hashChannels = Ints.asList(0);
        DataSize maxPartialMemory = succinctBytes(1);
        PartialAggregationController partialAggregationController = new PartialAggregationController(maxPartialMemory, 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.PARTIAL,
                false,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100,
                Optional.of(maxPartialMemory), // We set partial agg buffer to be 1 byte to force it to flush after every page
                false,
                Optional.of(partialAggregationController),
                new DataSize(0, MEGABYTE),
                new DataSize(0, MEGABYTE),
                new FailingSpillerFactory(),
                joinCompiler,
                false);

        // Partial Aggregation should be enabled at the start
        assertFalse(partialAggregationController.isPartialAggregationDisabled());

        // After the first input page, since the values are mostly distinct, adaptive partial agg should kick in and disable partial aggregation for the second page
        List<Page> input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8, 8))
                .addBlocksPage(createLongRepeatBlock(1, 10))
                .build();
        List<Page> expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8), createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 16)) // first page should be aggregated
                .addBlocksPage(createLongRepeatBlock(1, 10), createLongRepeatBlock(1, 10)) // second page should NOT be aggregated
                .build();
        assertOperatorEquals(operatorFactory, input, expected);
        // The first flush should have triggered adaptivity and disabled partial aggregation. Now it is disabled for subsequent flushes.
        assertTrue(partialAggregationController.isPartialAggregationDisabled());

        // Now we create a second operator, but we since we re-use the same factory, the PartialAggregationController should ensure we are NOT aggregating still
        input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createLongRepeatBlock(1, 10))
                .addBlocksPage(createLongRepeatBlock(2, 10))
                .build();
        expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongRepeatBlock(1, 10), createLongRepeatBlock(1, 10)) // output page should not be aggregated
                .addBlocksPage(createLongRepeatBlock(2, 10), createLongRepeatBlock(2, 10)) // output page should not be aggregated
                .build();
        assertOperatorEquals(operatorFactory, input, expected);

        // By default, we re-enable partial agg every partial agg buffer * 1.5 * 200 bytes.
        // Since we've set our partial agg buffer to be 1 byte, this means we should re-enable partial aggregation every 300 bytes.
        // At this point, we have processed 4 long blocks of 10 rows each. Each value in the long block is 8 bytes (for the long) + 1 byte (for the null flag) = 9 bytes.
        // So 4 long blocks of 10 rows each = 90 * 4 = 360 bytes, which is over our threshold of 300 bytes. Thus, partial aggregation should be re-enabled at this point.
        assertFalse(partialAggregationController.isPartialAggregationDisabled());

        input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addBlocksPage(createLongRepeatBlock(1, 100))
                .addBlocksPage(createLongRepeatBlock(2, 100))
                .build();
        expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongsBlock(1), createLongsBlock(100))
                .addBlocksPage(createLongsBlock(2), createLongsBlock(200))
                .build();
        // Partial aggregation should show good efficiency since the values are repeating in the input. So we should keep partial aggregation on.
        assertOperatorEquals(operatorFactory, input, expected);
        assertFalse(partialAggregationController.isPartialAggregationDisabled());
    }

    @Test
    public void testAdaptivePartialAggregationIsTriggeredOnlyOnFlush()
    {
        List<Integer> hashChannels = Ints.asList(0);
        // We make partial aggregation controller to trigger after page flush by setting to 1 byte
        PartialAggregationController partialAggregationController = new PartialAggregationController(succinctBytes(1), 0.8);
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT),
                hashChannels,
                ImmutableList.of(),
                ImmutableList.of(),
                Step.PARTIAL,
                false,
                ImmutableList.of(generateAccumulatorFactory(LONG_SUM, ImmutableList.of(0), Optional.empty())),
                Optional.empty(),
                Optional.empty(),
                100,
                Optional.of(succinctDataSize(16, MEGABYTE)), // We set partial agg buffer to be 16 MB, so that we will only flush after processing all pages
                false,
                Optional.of(partialAggregationController),
                new DataSize(0, MEGABYTE),
                new DataSize(0, MEGABYTE),
                new FailingSpillerFactory(),
                joinCompiler,
                false);

        List<Page> input = rowPagesBuilder(false, hashChannels, BIGINT)
                .addSequencePage(10, 0)
                .addBlocksPage(createLongRepeatBlock(1, 2))
                .build();
        List<Page> expected = rowPagesBuilder(BIGINT, BIGINT)
                .addBlocksPage(createLongsBlock(0, 1, 2, 3, 4, 5, 6, 7, 8, 9), createLongsBlock(0, 3, 2, 3, 4, 5, 6, 7, 8, 9))
                .build();
        // Since first input page is unique values, partial agg would've been disabled for the second page.
        // But because we wait for the flush, partial agg remains enabled for the second page.
        assertOperatorEquals(operatorFactory, input, expected);
        // After the flush, partial agg should be disabled because 10 / 12 values are unique, which is > 0.8 default uniqueness row ratio threshold.
        assertTrue(partialAggregationController.isPartialAggregationDisabled());
    }

    private void assertOperatorEquals(OperatorFactory operatorFactory, List<Page> inputPages, List<Page> expectedPages)
    {
        DriverContext driverContext = createDriverContext(1024);
        MaterializedResult expected = MaterializedResult.resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .pages(expectedPages)
                .build();
        OperatorAssertion.assertOperatorEquals(operatorFactory, driverContext, inputPages, expected, false, ImmutableList.of(), false);
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Integer.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }

    private int getHashCapacity(Operator operator)
    {
        assertTrue(operator instanceof HashAggregationOperator);
        HashAggregationBuilder aggregationBuilder = ((HashAggregationOperator) operator).getAggregationBuilder();
        if (aggregationBuilder == null) {
            return 0;
        }
        assertTrue(aggregationBuilder instanceof InMemoryHashAggregationBuilder);
        return ((InMemoryHashAggregationBuilder) aggregationBuilder).getCapacity();
    }

    private static JavaAggregationFunctionImplementation getAggregation(String name, Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction(name, fromTypes(arguments)));
    }

    private static class FailingSpillerFactory
            implements SpillerFactory
    {
        @Override
        public Spiller create(List<Type> types, SpillContext spillContext, AggregatedMemoryContext memoryContext)
        {
            return new Spiller()
            {
                @Override
                public ListenableFuture<?> spill(Iterator<Page> pageIterator)
                {
                    return immediateFailedFuture(new IOException("Failed to spill"));
                }

                @Override
                public List<Iterator<Page>> getSpills()
                {
                    return ImmutableList.of();
                }

                @Override
                public void commit()
                {
                }

                @Override
                public void close()
                {
                }
            };
        }
    }
}
