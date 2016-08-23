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
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.FirstValueFunction;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.LagFunction;
import com.facebook.presto.operator.window.LastValueFunction;
import com.facebook.presto.operator.window.LeadFunction;
import com.facebook.presto.operator.window.NthValueFunction;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.tree.FrameBound.Type.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.tree.WindowFrame.Type.RANGE;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestWindowOperator
{
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty());

    private static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.<Type>of(), RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME)
    );

    private static final List<WindowFunctionDefinition> FIRST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("first_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), FirstValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1)
    );

    private static final List<WindowFunctionDefinition> LAST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("last_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), LastValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1)
    );

    private static final List<WindowFunctionDefinition> NTH_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("nth_value", VARCHAR, ImmutableList.of(VARCHAR, BIGINT), NthValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3)
    );

    private static final List<WindowFunctionDefinition> LAG = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lag", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LagFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4)
    );

    private static final List<WindowFunctionDefinition> LEAD = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lead", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LeadFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4)
    );

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

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
    }

    @Test
    public void testRowNumber()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(2L, 0.3)
                .row(4L, 0.2)
                .row(6L, 0.1)
                .pageBreak()
                .row(-1L, -0.1)
                .row(5L, 0.4)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.2, 4L, 3L)
                .row(0.4, 5L, 4L)
                .row(0.1, 6L, 5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testRowNumberPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, BIGINT, DOUBLE, BOOLEAN)
                .row("b", -1L, -0.1, true)
                .row("a", 2L, 0.3, false)
                .row("a", 4L, 0.2, true)
                .pageBreak()
                .row("b", 5L, 0.4, false)
                .row("a", 6L, 0.1, true)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, BIGINT, DOUBLE, BOOLEAN),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0),
                Ints.asList(1),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testRowNumberArbitrary()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT)
                .row(1L)
                .row(3L)
                .row(5L)
                .row(7L)
                .pageBreak()
                .row(2L)
                .row(4L)
                .row(6L)
                .row(8L)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT),
                Ints.asList(0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(),
                ImmutableList.copyOf(new SortOrder[] {}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(3L, 2L)
                .row(5L, 3L)
                .row(7L, 4L)
                .row(2L, 5L)
                .row(4L, 6L)
                .row(6L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of 10B")
    public void testMemoryLimit()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(true, true)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        toPages(operatorFactory, driverContext, input);
    }

    @Test
    public void testFirstValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                FIRST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "A2")
                .row("a", "B1", 2L, true, "A2")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "A1")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "A3")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLastValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, true, "")
                .row("a", "A2", 1L, false, "")
                .row("a", "B1", 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, false, "")
                .row("a", "C2", 3L, true, "")
                .row("c", "A3", 1L, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                LAST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "C2")
                .row("a", "C2", 3L, true, "C2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "C1")
                .row("c", "A3", 1L, true, "A3")
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testNthValuePartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 2L, true, "")
                .row("a", "A2", 1L, 3L, false, "")
                .row("a", "B1", 2L, 2L, true, "")
                .pageBreak()
                .row("b", "C1", 2L, 3L, false, "")
                .row("a", "C2", 3L, 1L, true, "")
                .row("c", "A3", 1L, null, true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 4),
                NTH_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "B1")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, null)
                .row("c", "A3", 1L, true, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLagPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LAG,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "D")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "D")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testLeadPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR)
                .row("b", "A1", 1L, 1L, "D", true, "")
                .row("a", "A2", 1L, 2L, "D", false, "")
                .row("a", "B1", 2L, 2L, "D", true, "")
                .pageBreak()
                .row("b", "C1", 2L, 1L, "D", false, "")
                .row("a", "C2", 3L, 2L, "D", true, "")
                .row("c", "A3", 1L, 1L, "D", true, "")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BIGINT, VARCHAR, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 5),
                LEAD,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}));

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "D")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "D")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testPartiallyPreGroupedPartitionWithEmptyInput()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testPartiallyPreGroupedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(3L, "b", 102L, "E")
                .row(1L, "b", 103L, "D")
                .pageBreak()
                .row(3L, "b", 104L, "C")
                .row(1L, "c", 105L, "F")
                .pageBreak()
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(0, 1),
                Ints.asList(1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(3L, "b", 104L, "C", 1L)
                .row(3L, "b", 102L, "E", 2L)
                .row(1L, "b", 103L, "D", 1L)
                .row(1L, "c", 105L, "F", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testFullyPreGroupedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "B")
                .pageBreak()
                .row(2L, "b", 102L, "D")
                .row(2L, "b", 103L, "C")
                .row(1L, "b", 104L, "E")
                .pageBreak()
                .row(1L, "b", 105L, "F")
                .row(3L, "c", 106L, "G")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                0);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(2L, "b", 103L, "C", 1L)
                .row(2L, "b", 102L, "D", 2L)
                .row(1L, "b", 104L, "E", 1L)
                .row(1L, "b", 105L, "F", 2L)
                .row(3L, "c", 106L, "G", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testFullyPreGroupedAndPartiallySortedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 101L, "A")
                .row(2L, "b", 100L, "B")
                .row(1L, "b", 101L, "A")
                .pageBreak()
                .row(1L, "b", 100L, "A")
                .row(3L, "c", 100L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3, 2),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST, SortOrder.ASC_NULLS_LAST),
                1);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 100L, "A", 1L)
                .row(2L, "b", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 2L)
                .row(2L, "b", 100L, "B", 3L)
                .row(1L, "b", 100L, "A", 1L)
                .row(1L, "b", 101L, "A", 2L)
                .row(3L, "c", 100L, "A", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected);
    }

    @Test
    public void testFullyPreGroupedAndFullySortedPartition()
            throws Exception
    {
        List<Page> input = rowPagesBuilder(BIGINT, VARCHAR, BIGINT, VARCHAR)
                .pageBreak()
                .row(1L, "a", 100L, "A")
                .pageBreak()
                .row(2L, "a", 101L, "A")
                .pageBreak()
                .row(2L, "b", 102L, "A")
                .row(2L, "b", 103L, "A")
                .row(2L, "b", 104L, "B")
                .row(1L, "b", 105L, "A")
                .pageBreak()
                .row(1L, "b", 106L, "A")
                .row(3L, "c", 107L, "A")
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, VARCHAR, BIGINT, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                ROW_NUMBER,
                Ints.asList(1, 0),
                Ints.asList(0, 1),
                Ints.asList(3),
                ImmutableList.of(SortOrder.ASC_NULLS_LAST),
                1);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "A", 1L)
                .row(2L, "b", 102L, "A", 1L)
                .row(2L, "b", 103L, "A", 2L)
                .row(2L, "b", 104L, "B", 3L)
                .row(1L, "b", 105L, "A", 1L)
                .row(1L, "b", 106L, "A", 2L)
                .row(3L, "c", 107L, "A", 1L)
                .build();

        // Since fully grouped and sorted already, should respect original input order
        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    private static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder)
    {
        return createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                ImmutableList.of(),
                sortChannels,
                sortOrder,
                0);
    }

    private static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix)
    {
        return new WindowOperatorFactory(
                0,
                new PlanNodeId("test"),
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                preGroupedChannels,
                sortChannels,
                sortOrder,
                preSortedChannelPrefix,
                10);
    }
}
