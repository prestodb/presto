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
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.block.BlockAssertions.createBlockOfReals;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createColorSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoubleRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createSequenceBlockOfReal;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.predicate.Range.range;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.TypeUtils.readNativeValue;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.google.common.base.Strings.repeat;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.Float.floatToRawIntBits;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestDynamicFilterSourceOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private PipelineContext pipelineContext;

    private ImmutableList.Builder<TupleDomain<String>> partitions;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        pipelineContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION)
                .addPipelineContext(0, true, true, false);

        partitions = ImmutableList.builder();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    private void verifyPassthrough(Operator operator, List<Type> types, Page... pages)
    {
        verifyPassthrough(operator, types, Arrays.asList(pages));
    }

    private void verifyPassthrough(Operator operator, List<Type> types, List<Page> pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private OperatorFactory createOperatorFactory(DynamicFilterSourceOperator.Channel... buildChannels)
    {
        return createOperatorFactory(100, new DataSize(10, KILOBYTE), 1_000_000, Arrays.asList(buildChannels));
    }

    private OperatorFactory createOperatorFactory(
            int maxFilterPositionsCount,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            Iterable<DynamicFilterSourceOperator.Channel> buildChannels)
    {
        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                this::consumePredicate,
                ImmutableList.copyOf(buildChannels),
                maxFilterPositionsCount,
                maxFilterSize,
                minMaxCollectionLimit);
    }

    private void consumePredicate(TupleDomain<String> partitionPredicate)
    {
        partitions.add(partitionPredicate);
    }

    private Operator createOperator(OperatorFactory operatorFactory)
    {
        return operatorFactory.createOperator(pipelineContext.addDriverContext());
    }

    private static DynamicFilterSourceOperator.Channel channel(int index, Type type)
    {
        return new DynamicFilterSourceOperator.Channel(Integer.toString(index), type, index);
    }

    private void assertDynamicFilters(int maxFilterPositionsCount, List<Type> types, List<Page> pages, List<TupleDomain<String>> expectedTupleDomains)
    {
        assertDynamicFilters(maxFilterPositionsCount, new DataSize(10, KILOBYTE), 1_000_000, types, pages, expectedTupleDomains);
    }

    private void assertDynamicFilters(
            int maxFilterPositionsCount,
            DataSize maxFilterSize,
            int minMaxCollectionLimit,
            List<Type> types,
            List<Page> pages,
            List<TupleDomain<String>> expectedTupleDomains)
    {
        List<DynamicFilterSourceOperator.Channel> buildChannels = IntStream.range(0, types.size())
                .mapToObj(i -> channel(i, types.get(i)))
                .collect(toImmutableList());
        OperatorFactory operatorFactory = createOperatorFactory(maxFilterPositionsCount, maxFilterSize, minMaxCollectionLimit, buildChannels);
        verifyPassthrough(createOperator(operatorFactory), types, pages);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), expectedTupleDomains);
    }

    @Test
    public void testCollectMultipleOperators()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));

        Operator op1 = createOperator(operatorFactory); // will finish before noMoreOperators()
        verifyPassthrough(op1,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2)),
                new Page(createLongsBlock(3, 5)));

        Operator op2 = createOperator(operatorFactory); // will finish after noMoreOperators()
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L))))));

        verifyPassthrough(op2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L)))),
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 4L))))));
    }

    @Test
    public void testCollectMultipleColumns()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN), channel(1, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false)),
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false))))));
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(1, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectWithNulls()
    {
        Block blockWithNulls = INTEGER
                .createFixedSizeBlockBuilder(0)
                .writeInt(3)
                .appendNull()
                .writeInt(4)
                .build();

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, INTEGER));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L, 4L, 5L), false)))));
    }

    @Test
    public void testCollectWithDoubleNaN()
    {
        BlockBuilder input = DOUBLE.createBlockBuilder(null, 10);
        DOUBLE.writeDouble(input, 42.0);
        DOUBLE.writeDouble(input, Double.NaN);

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, DOUBLE));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(DOUBLE),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(DOUBLE, ImmutableList.of(42.0))))));
    }

    @Test
    public void testCollectWithRealNaN()
    {
        BlockBuilder input = REAL.createBlockBuilder(null, 10);
        REAL.writeLong(input, floatToRawIntBits(42.0f));
        REAL.writeLong(input, floatToRawIntBits(Float.NaN));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, REAL));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(REAL),
                new Page(input.build()));
        operatorFactory.noMoreOperators();

        assertEquals(partitions.build(), ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(REAL, ImmutableList.of((long) floatToRawIntBits(42.0f)))))));
    }

    @Test
    public void testCollectTooMuchRowsDouble()
    {
        int maxPositionsCount = 100;
        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(DOUBLE),
                ImmutableList.of(
                        new Page(createDoubleSequenceBlock(0, maxPositionsCount + 1)),
                        new Page(createDoubleRepeatBlock(Double.NaN, maxPositionsCount + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchRowsReal()
    {
        int maxPositionsCount = 100;
        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(REAL),
                ImmutableList.of(
                        new Page(createSequenceBlockOfReal(0, maxPositionsCount + 1)),
                        new Page(createBlockOfReals(Collections.nCopies(maxPositionsCount + 1, Float.NaN)))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooMuchRowsNonOrderable()
    {
        int maxPositionsCount = 100;
        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(COLOR),
                ImmutableList.of(new Page(createColorSequenceBlock(0, maxPositionsCount + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectRowsNonOrderable()
    {
        int maxPositionsCount = 100;
        Block block = createColorSequenceBlock(0, maxPositionsCount / 2);
        ImmutableList.Builder<Object> values = ImmutableList.builder();
        for (int position = 0; position < block.getPositionCount(); ++position) {
            values.add(readNativeValue(COLOR, block, position));
        }

        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(COLOR),
                ImmutableList.of(new Page(block)),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0",
                        Domain.create(ValueSet.copyOf(COLOR, values.build()), false)))));
    }

    @Test
    public void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory();
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassthrough(createOperator(operatorFactory),
                ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testSingleColumnCollectMinMaxRangeWhenTooManyPositions()
    {
        int maxPositionsCount = 100;
        Page largePage = new Page(createLongSequenceBlock(0, maxPositionsCount + 1));

        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0",
                        Domain.create(
                                ValueSet.ofRanges(range(BIGINT, 0L, true, (long) maxPositionsCount, true)),
                                false)))));
    }

    @Test
    public void testMultipleColumnsCollectMinMaxRangeWhenTooManyPositions()
    {
        int maxPositionsCount = 300;
        Page largePage = new Page(
                createLongSequenceBlock(0, 101),
                createColorSequenceBlock(100, 201),
                createLongSequenceBlock(200, 301));

        List<TupleDomain<String>> expectedTupleDomains = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.ofRanges(
                                range(BIGINT, 0L, true, 100L, true)), false),
                        "2", Domain.create(ValueSet.ofRanges(
                                range(BIGINT, 200L, true, 300L, true)), false))));
        assertDynamicFilters(maxPositionsCount, ImmutableList.of(BIGINT, COLOR, BIGINT), ImmutableList.of(largePage), expectedTupleDomains);
    }

    @Test
    public void testMultipleColumnsCollectMinMaxWithNulls()
    {
        int maxPositionsCount = 100;
        Page largePage = new Page(
                createLongsBlock(Collections.nCopies(100, null)),
                createLongSequenceBlock(200, 301));

        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(BIGINT, BIGINT),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testSingleColumnCollectMinMaxRangeWhenTooManyBytes()
    {
        DataSize maxSize = new DataSize(10, KILOBYTE);
        long maxByteSize = maxSize.toBytes();
        String largeText = repeat("A", (int) maxByteSize + 1);
        Page largePage = new Page(createStringsBlock(largeText));

        assertDynamicFilters(
                100,
                maxSize,
                100,
                ImmutableList.of(VARCHAR),
                ImmutableList.of(largePage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0",
                        Domain.create(
                                ValueSet.ofRanges(range(VARCHAR, utf8Slice(largeText), true, utf8Slice(largeText), true)),
                                false)))));
    }

    @Test
    public void testMultipleColumnsCollectMinMaxRangeWhenTooManyBytes()
    {
        DataSize maxSize = new DataSize(10, KILOBYTE);
        long maxByteSize = maxSize.toBytes();
        String largeTextA = repeat("A", (int) (maxByteSize / 2) + 1);
        String largeTextB = repeat("B", (int) (maxByteSize / 2) + 1);
        Page largePage = new Page(createStringsBlock(largeTextA), createStringsBlock(largeTextB));

        List<TupleDomain<String>> expectedTupleDomains = ImmutableList.of(
                TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.ofRanges(
                                range(VARCHAR, utf8Slice(largeTextA), true, utf8Slice(largeTextA), true)), false),
                        "1", Domain.create(ValueSet.ofRanges(
                                range(VARCHAR, utf8Slice(largeTextB), true, utf8Slice(largeTextB), true)), false))));
        assertDynamicFilters(
                100,
                maxSize,
                100,
                ImmutableList.of(VARCHAR, VARCHAR),
                ImmutableList.of(largePage),
                expectedTupleDomains);
    }

    @Test
    public void testCollectMultipleLargePages()
    {
        int maxPositionsCount = 100;
        Page page1 = new Page(createLongSequenceBlock(50, 151));
        Page page2 = new Page(createLongSequenceBlock(0, 101));
        Page page3 = new Page(createLongSequenceBlock(100, 201));

        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(BIGINT),
                ImmutableList.of(page1, page2, page3),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0",
                        Domain.create(
                                ValueSet.ofRanges(range(BIGINT, 0L, true, 200L, true)),
                                false)))));
    }

    @Test
    public void testCollectDeduplication()
    {
        int maxPositionsCount = 100;
        Page largePage = new Page(createLongRepeatBlock(7, maxPositionsCount * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxPositionsCount * 10]))); // lots of nulls

        assertDynamicFilters(
                maxPositionsCount,
                ImmutableList.of(BIGINT),
                ImmutableList.of(largePage, nullsPage),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0",
                        Domain.create(ValueSet.of(BIGINT, 7L), false)))));
    }

    @Test
    public void testCollectMinMaxLimitSinglePage()
    {
        int maxPositionsCount = 100;
        assertDynamicFilters(
                maxPositionsCount,
                new DataSize(10, KILOBYTE),
                2 * maxPositionsCount,
                ImmutableList.of(BIGINT),
                ImmutableList.of(new Page(createLongSequenceBlock(0, (2 * maxPositionsCount) + 1))),
                ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectMinMaxLimitMultiplePages()
    {
        int maxPositionsCount = 100;
        assertDynamicFilters(
                maxPositionsCount,
                new DataSize(10, KILOBYTE),
                (2 * maxPositionsCount) + 1,
                ImmutableList.of(BIGINT),
                ImmutableList.of(
                        new Page(createLongSequenceBlock(0, maxPositionsCount + 1)),
                        new Page(createLongSequenceBlock(0, maxPositionsCount + 1))),
                ImmutableList.of(TupleDomain.all()));
    }
}
