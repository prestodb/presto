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
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class TestRowNumberOperator
{
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @DataProvider(name = "hashEnabledValues")
    public static Object[][] hashEnabledValuesProvider()
    {
        return new Object[][] { { true }, { false } };
    }

    private DriverContext getDriverContext()
    {
        return createTaskContext(executor, TEST_SESSION)
                .addPipelineContext(true, true)
                .addDriverContext();
    }

    @Test
    public void testRowNumberUnpartitioned()
            throws Exception
    {
        DriverContext driverContext = getDriverContext();
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.19)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .row(2L, 0.9)
                .build();

        RowNumberOperator.RowNumberOperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.<Type>of(),
                Optional.empty(),
                Optional.empty(),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expectedResult = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.3, 1L)
                .row(0.4, 1L)
                .row(0.5, 1L)
                .row(0.6, 1L)
                .row(0.2, 2L)
                .row(0.7, 2L)
                .row(0.8, 2L)
                .row(0.9, 2L)
                .row(0.1, 3L)
                .row(0.19, 3L)
                .build();

        List<Page> pages = toPages(operator, input);
        Block rowNumberColumn = getRowNumberColumn(pages);
        assertEquals(rowNumberColumn.getPositionCount(), 10);

        pages = stripRowNumberColumn(pages);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), ImmutableList.<Type>of(DOUBLE, BIGINT), pages);
        assertEqualsIgnoreOrder(actual.getMaterializedRows(), expectedResult.getMaterializedRows());
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testRowNumberPartitioned(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = getDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.19)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .row(2L, 0.9)
                .build();

        RowNumberOperator.RowNumberOperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Optional.of(10),
                rowPagesBuilder.getHashChannel(),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expectedPartition1 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.3, 1L)
                .row(0.4, 1L)
                .row(0.5, 1L)
                .row(0.6, 1L)
                .build();

        MaterializedResult expectedPartition2 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.2, 2L)
                .row(0.7, 2L)
                .row(0.8, 2L)
                .row(0.9, 2L)
                .build();

        MaterializedResult expectedPartition3 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.1, 3L)
                .row(0.19, 3L)
                .build();

        List<Page> pages = toPages(operator, input);
        Block rowNumberColumn = getRowNumberColumn(pages);
        assertEquals(rowNumberColumn.getPositionCount(), 10);

        pages = stripRowNumberColumn(pages);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), ImmutableList.<Type>of(DOUBLE, BIGINT), pages);
        ImmutableSet<?> actualSet = ImmutableSet.copyOf(actual.getMaterializedRows());
        ImmutableSet<?> expectedPartition1Set = ImmutableSet.copyOf(expectedPartition1.getMaterializedRows());
        ImmutableSet<?> expectedPartition2Set = ImmutableSet.copyOf(expectedPartition2.getMaterializedRows());
        ImmutableSet<?> expectedPartition3Set = ImmutableSet.copyOf(expectedPartition3.getMaterializedRows());
        assertEquals(Sets.intersection(expectedPartition1Set, actualSet).size(), 4);
        assertEquals(Sets.intersection(expectedPartition2Set, actualSet).size(), 4);
        assertEquals(Sets.intersection(expectedPartition3Set, actualSet).size(), 2);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testRowNumberPartitionedLimit(boolean hashEnabled)
            throws Exception
    {
        DriverContext driverContext = getDriverContext();
        RowPagesBuilder rowPagesBuilder = rowPagesBuilder(hashEnabled, Ints.asList(0), BIGINT, DOUBLE);
        List<Page> input = rowPagesBuilder
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.19)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .row(2L, 0.9)
                .build();

        RowNumberOperator.RowNumberOperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(0),
                ImmutableList.of(BIGINT),
                Optional.of(3),
                Optional.empty(),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expectedPartition1 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.3, 1L)
                .row(0.4, 1L)
                .row(0.5, 1L)
                .row(0.6, 1L)
                .build();

        MaterializedResult expectedPartition2 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.2, 2L)
                .row(0.7, 2L)
                .row(0.8, 2L)
                .row(0.9, 2L)
                .build();

        MaterializedResult expectedPartition3 = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT)
                .row(0.1, 3L)
                .row(0.19, 3L)
                .build();

        List<Page> pages = toPages(operator, input);
        Block rowNumberColumn = getRowNumberColumn(pages);
        assertEquals(rowNumberColumn.getPositionCount(), 8);
        // Check that all row numbers generated are <= 3
        for (int i = 0; i < rowNumberColumn.getPositionCount(); i++) {
            assertTrue(rowNumberColumn.getLong(i, 0) <= 3);
        }

        pages = stripRowNumberColumn(pages);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), ImmutableList.<Type>of(DOUBLE, BIGINT), pages);
        ImmutableSet<?> actualSet = ImmutableSet.copyOf(actual.getMaterializedRows());
        ImmutableSet<?> expectedPartition1Set = ImmutableSet.copyOf(expectedPartition1.getMaterializedRows());
        ImmutableSet<?> expectedPartition2Set = ImmutableSet.copyOf(expectedPartition2.getMaterializedRows());
        ImmutableSet<?> expectedPartition3Set = ImmutableSet.copyOf(expectedPartition3.getMaterializedRows());
        assertEquals(Sets.intersection(expectedPartition1Set, actualSet).size(), 3);
        assertEquals(Sets.intersection(expectedPartition2Set, actualSet).size(), 3);
        assertEquals(Sets.intersection(expectedPartition3Set, actualSet).size(), 2);
    }

    @Test
    public void testRowNumberUnpartitionedLimit()
            throws Exception
    {
        DriverContext driverContext = getDriverContext();
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.3)
                .row(2L, 0.2)
                .row(3L, 0.1)
                .row(3L, 0.19)
                .pageBreak()
                .row(1L, 0.4)
                .pageBreak()
                .row(1L, 0.5)
                .row(1L, 0.6)
                .row(2L, 0.7)
                .row(2L, 0.8)
                .row(2L, 0.9)
                .build();

        RowNumberOperator.RowNumberOperatorFactory operatorFactory = new RowNumberOperator.RowNumberOperatorFactory(
                0,
                new PlanNodeId("test"),
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                Ints.asList(),
                ImmutableList.<Type>of(),
                Optional.of(3),
                Optional.empty(),
                10);

        Operator operator = operatorFactory.createOperator(driverContext);

        MaterializedResult expectedRows = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(0.3, 1L)
                .row(0.2, 2L)
                .row(0.1, 3L)
                .row(0.19, 3L)
                .row(0.4, 1L)
                .row(0.5, 1L)
                .row(0.6, 1L)
                .row(0.7, 2L)
                .row(0.8, 2L)
                .row(0.9, 2L)
                .build();

        List<Page> pages = toPages(operator, input);
        Block rowNumberColumn = getRowNumberColumn(pages);
        assertEquals(rowNumberColumn.getPositionCount(), 3);

        pages = stripRowNumberColumn(pages);
        MaterializedResult actual = toMaterializedResult(operator.getOperatorContext().getSession(), ImmutableList.<Type>of(DOUBLE, BIGINT), pages);
        assertEquals(actual.getMaterializedRows().size(), 3);
        ImmutableSet<?> actualSet = ImmutableSet.copyOf(actual.getMaterializedRows());
        ImmutableSet<?> expectedRowsSet = ImmutableSet.copyOf(expectedRows.getMaterializedRows());
        assertEquals(Sets.intersection(expectedRowsSet, actualSet).size(), 3);
    }

    private static Block getRowNumberColumn(List<Page> pages)
    {
        BlockBuilder builder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), pages.size() * 100);
        for (Page page : pages) {
            int rowNumberChannel = page.getChannelCount() - 1;
            for (int i = 0; i < page.getPositionCount(); i++) {
                BIGINT.writeLong(builder, page.getBlock(rowNumberChannel).getLong(i, 0));
            }
        }
        return builder.build();
    }

    private static List<Page> stripRowNumberColumn(List<Page> input)
    {
        return input.stream()
                .map(page -> {
                    Block[] blocks = Arrays.copyOf(page.getBlocks(), page.getChannelCount() - 1);
                    return new Page(blocks);
                })
                .collect(toImmutableList());
    }
}
