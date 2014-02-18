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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.Session;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.sql.planner.plan.AggregationNode.Step;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.util.MaterializedResult;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.operator.AggregationFunctionDefinition.aggregation;
import static com.facebook.presto.operator.OperatorAssertion.appendSampleWeight;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_BOOLEAN_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_STRING_COLUMN;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeZoneKey.UTC_KEY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.MaterializedResult.resultBuilder;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.slice.SizeOf.SIZE_OF_DOUBLE;
import static io.airlift.slice.SizeOf.SIZE_OF_LONG;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHashAggregationOperator
{
    private ExecutorService executor;
    private DriverContext driverContext;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test"));
        Session session = new Session("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testSampledHashAggregation()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .addSequencePage(10, 100, 0, 100, 0, 500)
                .addSequencePage(10, 100, 0, 200, 0, 500)
                .addSequencePage(10, 100, 0, 300, 0, 500)
                .build();
        input = appendSampleWeight(input, 2);

        Optional<Input> sampleWeightInput = Optional.of(new Input(input.get(0).getChannelCount() - 1));
        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                Ints.asList(1),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), sampleWeightInput, 1.0),
                        aggregation(LONG_SUM, ImmutableList.of(new Input(3)), Optional.<Input>absent(), sampleWeightInput, 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(new Input(3)), Optional.<Input>absent(), sampleWeightInput, 1.0),
                        aggregation(VAR_BINARY_MAX, ImmutableList.of(new Input(2)), Optional.<Input>absent(), sampleWeightInput, 1.0),
                        aggregation(COUNT_STRING_COLUMN, ImmutableList.of(new Input(0)), Optional.<Input>absent(), sampleWeightInput, 1.0),
                        aggregation(COUNT_BOOLEAN_COLUMN, ImmutableList.of(new Input(4)), Optional.<Input>absent(), sampleWeightInput, 1.0)),
                100_000);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, BIGINT, DOUBLE, VARCHAR, BIGINT, BIGINT)
                .row("0", 6, 2 * 0, 0.0, "300", 6, 6)
                .row("1", 6, 2 * 3, 1.0, "301", 6, 6)
                .row("2", 6, 2 * 6, 2.0, "302", 6, 6)
                .row("3", 6, 2 * 9, 3.0, "303", 6, 6)
                .row("4", 6, 2 * 12, 4.0, "304", 6, 6)
                .row("5", 6, 2 * 15, 5.0, "305", 6, 6)
                .row("6", 6, 2 * 18, 6.0, "306", 6, 6)
                .row("7", 6, 2 * 21, 7.0, "307", 6, 6)
                .row("8", 6, 2 * 24, 8.0, "308", 6, 6)
                .row("9", 6, 2 * 27, 9.0, "309", 6, 6)
                .build();

        assertOperatorEqualsIgnoreOrder(operator, input, expected);
    }

    @Test
    public void testHashAggregation()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, VARCHAR, BIGINT, BOOLEAN)
                .addSequencePage(10, 100, 0, 100, 0, 500)
                .addSequencePage(10, 100, 0, 200, 0, 500)
                .addSequencePage(10, 100, 0, 300, 0, 500)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                Ints.asList(1),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_SUM, ImmutableList.of(new Input(3)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(new Input(3)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(VAR_BINARY_MAX, ImmutableList.of(new Input(2)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(COUNT_STRING_COLUMN, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(COUNT_BOOLEAN_COLUMN, ImmutableList.of(new Input(4)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)),
                100_000);

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

        assertOperatorEqualsIgnoreOrder(operator, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 10B")
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, VARCHAR, BIGINT)
                .addSequencePage(10, 100, 0, 100, 0)
                .addSequencePage(10, 100, 0, 200, 0)
                .addSequencePage(10, 100, 0, 300, 0)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                Ints.asList(1),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_SUM, ImmutableList.of(new Input(3)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(new Input(3)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(VAR_BINARY_MAX, ImmutableList.of(new Input(2)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)),
                100_000);

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    public void testHashBuilderResize()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        builder.append(new String(new byte[200_000])); // this must be larger than DEFAULT_MAX_BLOCK_SIZE, 64K
        builder.build();

        List<Page> input = rowPagesBuilder(VARCHAR)
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(3, Unit.MEGABYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                Ints.asList(0),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)),
                100_000);

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test(expectedExceptions = PrestoException.class, expectedExceptionsMessageRegExp = "Task exceeded max memory size of 3MB")
    public void testHashBuilderResizeLimit()
    {
        BlockBuilder builder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        builder.append(new String(new byte[5_000_000])); // this must be larger than DEFAULT_MAX_BLOCK_SIZE, 64K
        builder.build();

        List<Page> input = rowPagesBuilder(VARCHAR)
                .addSequencePage(10, 100)
                .addBlocksPage(builder.build())
                .addSequencePage(10, 100)
                .build();

        Session session = new Session("user", "source", "catalog", "schema", UTC_KEY, Locale.ENGLISH, "address", "agent");
        DriverContext driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session, new DataSize(3, Unit.MEGABYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(VARCHAR),
                Ints.asList(0),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)),
                100_000);

        Operator operator = operatorFactory.createOperator(driverContext);

        toPages(operator, input);
    }

    @Test
    public void testMultiSliceAggregationOutput()
    {
        // estimate the number of entries required to create 1.5 pages of results
        int fixedWidthSize = SIZE_OF_LONG + SIZE_OF_DOUBLE + SIZE_OF_DOUBLE;
        int multiSlicePositionCount = (int) (1.5 * BlockBuilderStatus.DEFAULT_MAX_PAGE_SIZE_IN_BYTES / fixedWidthSize);

        List<Page> input = rowPagesBuilder(BIGINT, BIGINT)
                .addSequencePage(multiSlicePositionCount, 0, 0)
                .build();

        HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
                0,
                ImmutableList.of(BIGINT),
                Ints.asList(1),
                Step.SINGLE,
                ImmutableList.of(aggregation(COUNT, ImmutableList.of(new Input(0)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0),
                        aggregation(LONG_AVERAGE, ImmutableList.of(new Input(1)), Optional.<Input>absent(), Optional.<Input>absent(), 1.0)),
                100_000);

        Operator operator = operatorFactory.createOperator(driverContext);

        assertEquals(toPages(operator, input).size(), 2);
    }
}
