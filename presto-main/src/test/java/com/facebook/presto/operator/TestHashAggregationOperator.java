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

import com.facebook.presto.ExceededMemoryLimitException;
import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.PageBuilderStatus;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.dropChannel;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.OperatorAssertion.without;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.spi.block.BlockBuilderStatus.DEFAULT_MAX_BLOCK_SIZE_IN_BYTES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHashAggregationOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        driverContext = createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testHashAggregation(boolean hashEnabled)
            throws Exception
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        InternalAggregationFunction countVarcharColumn = metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)), false).getAggregationFunction();
        InternalAggregationFunction countBooleanColumn = metadata.resolveFunction(QualifiedName.of("count"), ImmutableList.of(parseTypeSignature(StandardTypes.BOOLEAN)), false).getAggregationFunction();
        InternalAggregationFunction maxVarcharColumn = metadata.resolveFunction(QualifiedName.of("max"), ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)), false).getAggregationFunction();
        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0, 500)
                .addSequencePage(10, 100, 0, 200, 0, 500)
                .addSequencePage(10, 100, 0, 300, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                hashChannels,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty(), Optional.empty(), 1.0),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty(), Optional.empty(), 1.0),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty(), Optional.empty(), 1.0),
                        countVarcharColumn.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        countBooleanColumn.bind(ImmutableList.of(4), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row("0", 3, 0, 0.0, "300", 3, 3)
                .row("1", 3, 3, 1.0, "301", 3, 3)
                .row("2", 3, 6, 2.0, "302", 3, 3)
                .row("3", 3, 9, 3.0, "303", 3, 3)
                .row("4", 3, 12, 4.0, "304", 3, 3)
                .row("5", 3, 15, 5.0, "305", 3, 3)
                .row("6", 3, 18, 6.0, "306", 3, 3)
                .row("7", 3, 21, 7.0, "307", 3, 3)
                .row("8", 3, 24, 8.0, "308", 3, 3)
                .row("9", 3, 27, 9.0, "309", 3, 3)
                .build();

        assertOperatorEqualsIgnoreOrder(operator, input, expected, hashEnabled, Optional.of(hashChannels.size()));
    }

    @Test(dataProvider = "hashEnabledValues", expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit(boolean hashEnabled)
    {
        MetadataManager metadata = MetadataManager.createTestMetadataManager();
        InternalAggregationFunction maxVarcharColumn = metadata.resolveFunction(QualifiedName.of("max"), ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)), false).getAggregationFunction();

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR, VARCHAR, VARCHAR, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                hashChannels,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        LONG_SUM.bind(ImmutableList.of(3), Optional.empty(), Optional.empty(), 1.0),
                        LONG_AVERAGE.bind(ImmutableList.of(3), Optional.empty(), Optional.empty(), 1.0),
                        maxVarcharColumn.bind(ImmutableList.of(2), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testHashBuilderResize(boolean hashEnabled)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1, DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(200_000)); // this must be larger than DEFAULT_MAX_BLOCK_SIZE, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(10, MEGABYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                hashChannels,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test(dataProvider = "hashEnabledValues", expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 3MB")
    public void testHashBuilderResizeLimit(boolean hashEnabled)
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus(), 1, DEFAULT_MAX_BLOCK_SIZE_IN_BYTES);
        VARCHAR.writeSlice(builder, Slices.allocate(5_000_000)); // this must be larger than DEFAULT_MAX_BLOCK_SIZE, 64K
        builder.build();

        List<Integer> hashChannels = Ints.asList(0);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, VARCHAR);
        List<Page> input = rowPagesBuilder
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(3, MEGABYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                hashChannels,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testMultiSliceAggregationOutput(boolean hashEnabled)
    {
        // estimate the number of entries required to create 1.5 pages of results
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_DOUBLE + SIZE_OF_DOUBLE;
        int multiSlicePositionCount = (int) (1.5 * PageBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);
        multiSlicePositionCount = Math.min((int) (1.5 * DEFAULT_MAX_BLOCK_SIZE_IN_BYTES / SIZE_OF_DOUBLE), multiSlicePositionCount);

        List<Integer> hashChannels = Ints.asList(1);
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, hashChannels, BIGINT, BIGINT);
        List<Page> input = rowPagesBuilder
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(BIGINT),
                hashChannels,
                Step.SINGLE,
                ImmutableList.of(COUNT.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0),
                        LONG_AVERAGE.bind(ImmutableList.of(1), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        Operator operator = operatorFactory.createOperator(driverContext);

        assertEquals(toPages(operator, input).size(), 2);
    }

    @Test(dataProvider = "hashEnabledValues")
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
                ImmutableList.of(BIGINT),
                hashChannels,
                Step.PARTIAL,
                ImmutableList.of(LONG_SUM.bind(ImmutableList.of(0), Optional.empty(), Optional.empty(), 1.0)),
                Optional.<Integer>empty(),
                rowPagesBuilder.getHashChannel(),
                100_000,
                new DataSize(16, MEGABYTE));

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(1, Unit.KILOBYTE))
                .addPipelineContext(true, true)
                .addDriverContext();
        Operator operator = operatorFactory.createOperator(driverContext);

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
            List<Page> actualPages = dropChannel(outputPages, hashChannels);
            List<Type> expectedTypes = without(operator.getTypes(), hashChannels);
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), expectedTypes, actualPages);
        }
        else {
            actual = toMaterializedResult(operator.getOperatorContext().getSession(), operator.getTypes(), outputPages);
        }

        assertEquals(actual.getTypes(), expected.getTypes());
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expected.getMaterializedRows());
    }
}
