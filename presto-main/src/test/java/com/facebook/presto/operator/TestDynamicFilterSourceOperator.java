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
import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.predicate.ValueSet;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.SystemSessionProperties.getDynamicFilteringMaxPerDriverRowCount;
import static com.facebook.presto.SystemSessionProperties.getDynamicFilteringMaxPerDriverSize;
import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.base.Strings.repeat;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.stream.Collectors.toList;

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

    private void verifyPassThrough(Operator operator, List<Type> types, Page... pages)
    {
        List<Page> inputPages = ImmutableList.copyOf(pages);
        List<Page> outputPages = toPages(operator, inputPages.iterator());
        MaterializedResult actual = toMaterializedResult(pipelineContext.getSession(), types, outputPages);
        MaterializedResult expected = toMaterializedResult(pipelineContext.getSession(), types, inputPages);
        assertEquals(actual, expected);
    }

    private OperatorFactory createOperatorFactory(DynamicFilterSourceOperator.Channel... buildChannels)
    {
        return new DynamicFilterSourceOperator.DynamicFilterSourceOperatorFactory(
                0,
                new PlanNodeId("PLAN_NODE_ID"),
                this::consumePredicate,
                Arrays.stream(buildChannels).collect(toList()),
                getDynamicFilteringMaxPerDriverRowCount(TEST_SESSION),
                getDynamicFilteringMaxPerDriverSize(TEST_SESSION));
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

    @Test
    public void testCollectMultipleOperators()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));

        Operator operator1 = createOperator(operatorFactory); // will finish before noMoreOperators()
        verifyPassThrough(operator1,
                          ImmutableList.of(BIGINT),
                          new Page(createLongsBlock(1, 2)),
                          new Page(createLongsBlock(3, 5)));

        Operator operator2 = createOperator(operatorFactory); // will finish after noMoreOperators()
        operatorFactory.noMoreOperators();
        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of("0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L))))));

        verifyPassThrough(
                operator2,
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(2, 3)),
                new Page(createLongsBlock(1, 4)));

        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of("0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 5L)))),
                TupleDomain.withColumnDomains(ImmutableMap.of("0", Domain.multipleValues(BIGINT, ImmutableList.of(1L, 2L, 3L, 4L))))));
    }

    @Test
    public void testCollectMultipleColumns()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN), channel(1, DOUBLE));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false)),
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectOnlyFirstColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BOOLEAN));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of("0", Domain.multipleValues(BOOLEAN, ImmutableList.of(true, false))))));
    }

    @Test
    public void testCollectOnlyLastColumn()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(1, DOUBLE));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(BOOLEAN, DOUBLE),
                new Page(createBooleansBlock(true, 2), createDoublesBlock(1.5, 3.0)),
                new Page(createBooleansBlock(false, 1), createDoublesBlock(4.5)));
        operatorFactory.noMoreOperators();

        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "1", Domain.multipleValues(DOUBLE, ImmutableList.of(1.5, 3.0, 4.5))))));
    }

    @Test
    public void testCollectWithNulls()
    {
        Block blockWithNulls = INTEGER.createFixedSizeBlockBuilder(0)
                .writeInt(3)
                .appendNull()
                .writeInt(4)
                .build();

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, INTEGER));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(INTEGER),
                new Page(createLongsBlock(1, 2, 3)),
                new Page(blockWithNulls),
                new Page(createLongsBlock(4, 5)));
        operatorFactory.noMoreOperators();

        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.of(INTEGER, 1L, 2L, 3L, 4L, 5L), false)))));
    }

    @Test
    public void testCollectNoFilters()
    {
        OperatorFactory operatorFactory = createOperatorFactory();
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                new Page(createLongsBlock(1, 2, 3)));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectEmptyBuildSide()
    {
        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassThrough(createOperator(operatorFactory), ImmutableList.of(BIGINT));
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.none()));
    }

    @Test
    public void testCollectTooManyRows()
    {
        int maxRowCount = getDynamicFilteringMaxPerDriverRowCount(pipelineContext.getSession());
        Page largePage = createSequencePage(ImmutableList.of(BIGINT), maxRowCount + 1);

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassThrough(createOperator(operatorFactory), ImmutableList.of(BIGINT), largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooManyBytesSingleColumn()
    {
        long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(createStringsBlock(repeat("A", (int) maxByteSize + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, VARCHAR));
        verifyPassThrough(createOperator(operatorFactory), ImmutableList.of(VARCHAR), largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectTooManyBytesMultipleColumns()
    {
        long maxByteSize = getDynamicFilteringMaxPerDriverSize(pipelineContext.getSession()).toBytes();
        Page largePage = new Page(
                createStringsBlock(repeat("A", (int) (maxByteSize / 2) + 1)),
                createStringsBlock(repeat("B", (int) (maxByteSize / 2) + 1)));

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, VARCHAR), channel(1, VARCHAR));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(VARCHAR, VARCHAR),
                largePage);
        operatorFactory.noMoreOperators();
        assertEquals(partitions.build(), ImmutableList.of(TupleDomain.all()));
    }

    @Test
    public void testCollectDeduplication()
    {
        int maxRowCount = getDynamicFilteringMaxPerDriverRowCount(pipelineContext.getSession());
        Page largePage = new Page(createLongRepeatBlock(7, maxRowCount * 10)); // lots of zeros
        Page nullsPage = new Page(createLongsBlock(Arrays.asList(new Long[maxRowCount * 10]))); // lots of nulls

        OperatorFactory operatorFactory = createOperatorFactory(channel(0, BIGINT));
        verifyPassThrough(
                createOperator(operatorFactory),
                ImmutableList.of(BIGINT),
                largePage,
                nullsPage);
        operatorFactory.noMoreOperators();
        assertEquals(
                partitions.build(),
                ImmutableList.of(TupleDomain.withColumnDomains(ImmutableMap.of(
                        "0", Domain.create(ValueSet.of(BIGINT, 7L), false)))));
    }
}
