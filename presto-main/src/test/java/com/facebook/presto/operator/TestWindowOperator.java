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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.WindowOperator.WindowOperatorFactory;
import com.facebook.presto.operator.window.FirstValueFunction;
import com.facebook.presto.operator.window.FrameInfo;
import com.facebook.presto.operator.window.LagFunction;
import com.facebook.presto.operator.window.LastValueFunction;
import com.facebook.presto.operator.window.LeadFunction;
import com.facebook.presto.operator.window.NthValueFunction;
import com.facebook.presto.operator.window.ReflectionWindowFunctionSupplier;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SpillerFactory;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.airlift.testing.Assertions.assertGreaterThan;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toMaterializedResult;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.operator.WindowFunctionDefinition.window;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_FOLLOWING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.BoundType.UNBOUNDED_PRECEDING;
import static com.facebook.presto.sql.planner.plan.WindowNode.Frame.WindowType.RANGE;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestWindowOperator
{
    private static final FrameInfo UNBOUNDED_FRAME = new FrameInfo(RANGE, UNBOUNDED_PRECEDING, Optional.empty(), UNBOUNDED_FOLLOWING, Optional.empty());

    public static final List<WindowFunctionDefinition> ROW_NUMBER = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("row_number", BIGINT, ImmutableList.of(), RowNumberFunction.class), BIGINT, UNBOUNDED_FRAME));

    private static final List<WindowFunctionDefinition> FIRST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("first_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), FirstValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1));

    private static final List<WindowFunctionDefinition> LAST_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("last_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), LastValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1));

    private static final List<WindowFunctionDefinition> NTH_VALUE = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("nth_value", VARCHAR, ImmutableList.of(VARCHAR, BIGINT), NthValueFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3));

    private static final List<WindowFunctionDefinition> LAG = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lag", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LagFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4));

    private static final List<WindowFunctionDefinition> LEAD = ImmutableList.of(
            window(new ReflectionWindowFunctionSupplier<>("lead", VARCHAR, ImmutableList.of(VARCHAR, BIGINT, VARCHAR), LeadFunction.class), VARCHAR, UNBOUNDED_FRAME, 1, 3, 4));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private DummySpillerFactory spillerFactory;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
        spillerFactory = new DummySpillerFactory();
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        spillerFactory = null;
    }

    @DataProvider
    public static Object[][] spillEnabled()
    {
        return new Object[][] {
                {false, false, 0},
                {true, false, 8},
                {true, true, 8},
                {true, false, 0},
                {true, true, 0}};
    }

    @Test(dataProvider = "spillEnabled")
    public void testMultipleOutputPages(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
    {
        // make operator produce multiple pages during finish phase
        int numberOfRows = 80_000;
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .addSequencePage(numberOfRows, 0, 0)
                .build();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1, 0),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.DESC_NULLS_FIRST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT);
        for (int i = 0; i < numberOfRows; ++i) {
            expectedBuilder.row((double) numberOfRows - i - 1, (long) numberOfRows - i - 1, (long) i + 1);
        }
        MaterializedResult expected = expectedBuilder.build();

        List<Page> pages = toPages(operatorFactory, driverContext, input, revokeMemoryWhenAddingPages);
        assertGreaterThan(pages.size(), 1, "Expected more than one output page");

        MaterializedResult actual = toMaterializedResult(driverContext.getSession(), expected.getTypes(), pages);
        assertEquals(actual.getMaterializedRows(), expected.getMaterializedRows());

        assertTrue(spillEnabled == (spillerFactory.getSpillsCount() > 0), format("Spill state mismatch. Expected spill: %s, spill count: %s", spillEnabled, spillerFactory.getSpillsCount()));
    }

    @Test(dataProvider = "spillEnabled")
    public void testRowNumber(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), DOUBLE, BIGINT, BIGINT)
                .row(-0.1, -1L, 1L)
                .row(0.3, 2L, 2L)
                .row(0.2, 4L, 3L)
                .row(0.4, 5L, 4L)
                .row(0.1, 6L, 5L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testRowNumberPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, BIGINT, DOUBLE, BOOLEAN, BIGINT)
                .row("a", 2L, 0.3, false, 1L)
                .row("a", 4L, 0.2, true, 2L)
                .row("a", 6L, 0.1, true, 3L)
                .row("b", -1L, -0.1, true, 1L)
                .row("b", 5L, 0.4, false, 2L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testRowNumberArbitrary()
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
                ImmutableList.copyOf(new SortOrder[] {}),
                false);

        DriverContext driverContext = createDriverContext();
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

    @Test
    public void testRowNumberArbitraryWithSpill()
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
                ImmutableList.copyOf(new SortOrder[] {}),
                true);

        DriverContext driverContext = createDriverContext();
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, BIGINT)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, 3L)
                .row(4L, 4L)
                .row(5L, 5L)
                .row(6L, 6L)
                .row(7L, 7L)
                .row(8L, 8L)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected);
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of 10B.*")
    public void testMemoryLimit()
    {
        List<Page> input = rowPagesBuilder(BIGINT, DOUBLE)
                .row(1L, 0.1)
                .row(2L, 0.2)
                .pageBreak()
                .row(-1L, -0.1)
                .row(4L, 0.4)
                .build();

        DriverContext driverContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(10, Unit.BYTE))
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(BIGINT, DOUBLE),
                Ints.asList(1),
                ROW_NUMBER,
                Ints.asList(),
                Ints.asList(0),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                false);

        toPages(operatorFactory, driverContext, input);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFirstValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "A2")
                .row("a", "B1", 2L, true, "A2")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "A1")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "A3")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLastValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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

        DriverContext driverContext = createDriverContext(memoryLimit);
        WindowOperatorFactory operatorFactory = createFactoryUnbounded(
                ImmutableList.of(VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR),
                Ints.asList(0, 1, 2, 3),
                LAST_VALUE,
                Ints.asList(0),
                Ints.asList(2),
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "C2")
                .row("a", "C2", 3L, true, "C2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "C1")
                .row("c", "A3", 1L, true, "A3")
                .build();
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testNthValuePartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "B1")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, null)
                .row("c", "A3", 1L, true, null)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLagPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "D")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "A2")
                .row("b", "A1", 1L, true, "D")
                .row("b", "C1", 2L, false, "A1")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testLeadPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                ImmutableList.copyOf(new SortOrder[] {SortOrder.ASC_NULLS_LAST}),
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), VARCHAR, VARCHAR, BIGINT, BOOLEAN, VARCHAR)
                .row("a", "A2", 1L, false, "C2")
                .row("a", "B1", 2L, true, "D")
                .row("a", "C2", 3L, true, "D")
                .row("b", "A1", 1L, true, "C1")
                .row("b", "C1", 2L, false, "D")
                .row("c", "A3", 1L, true, "D")
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testPartiallyPreGroupedPartitionWithEmptyInput(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .build();

        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testPartiallyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(3L, "b", 104L, "C", 1L)
                .row(3L, "b", 102L, "E", 2L)
                .row(1L, "b", 103L, "D", 1L)
                .row(1L, "c", 105L, "F", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                0,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
        MaterializedResult expected = resultBuilder(driverContext.getSession(), BIGINT, VARCHAR, BIGINT, VARCHAR, BIGINT)
                .row(1L, "a", 100L, "A", 1L)
                .row(2L, "a", 101L, "B", 1L)
                .row(2L, "b", 103L, "C", 1L)
                .row(2L, "b", 102L, "D", 2L)
                .row(1L, "b", 104L, "E", 1L)
                .row(1L, "b", 105L, "F", 2L)
                .row(3L, "c", 106L, "G", 1L)
                .build();

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedAndPartiallySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                1,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
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

        assertOperatorEqualsIgnoreOrder(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test(dataProvider = "spillEnabled")
    public void testFullyPreGroupedAndFullySortedPartition(boolean spillEnabled, boolean revokeMemoryWhenAddingPages, long memoryLimit)
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
                1,
                spillEnabled);

        DriverContext driverContext = createDriverContext(memoryLimit);
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
        assertOperatorEquals(operatorFactory, driverContext, input, expected, revokeMemoryWhenAddingPages);
    }

    @Test
    public void testFindEndPosition()
    {
        assertFindEndPosition("0", 1);
        assertFindEndPosition("11", 2);
        assertFindEndPosition("1111111111", 10);

        assertFindEndPosition("01", 1);
        assertFindEndPosition("011", 1);
        assertFindEndPosition("0111", 1);
        assertFindEndPosition("0111111111", 1);

        assertFindEndPosition("012", 1);
        assertFindEndPosition("01234", 1);
        assertFindEndPosition("0123456789", 1);

        assertFindEndPosition("001", 2);
        assertFindEndPosition("0001", 3);
        assertFindEndPosition("0000000001", 9);

        assertFindEndPosition("000111", 3);
        assertFindEndPosition("0001111", 3);
        assertFindEndPosition("0000111", 4);
        assertFindEndPosition("000000000000001111111111", 14);
    }

    private static void assertFindEndPosition(String values, int expected)
    {
        char[] array = values.toCharArray();
        assertEquals(WindowOperator.findEndPosition(0, array.length, (first, second) -> array[first] == array[second]), expected);
    }

    private WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            boolean spillEnabled)
    {
        return createFactoryUnbounded(
                sourceTypes,
                outputChannels,
                functions,
                partitionChannels,
                ImmutableList.of(),
                sortChannels,
                sortOrder,
                0,
                spillEnabled);
    }

    public WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            boolean spillEnabled)
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
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler());
    }

    public static WindowOperatorFactory createFactoryUnbounded(
            List<? extends Type> sourceTypes,
            List<Integer> outputChannels,
            List<WindowFunctionDefinition> functions,
            List<Integer> partitionChannels,
            List<Integer> preGroupedChannels,
            List<Integer> sortChannels,
            List<SortOrder> sortOrder,
            int preSortedChannelPrefix,
            SpillerFactory spillerFactory,
            boolean spillEnabled)
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
                10,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                spillerFactory,
                new OrderingCompiler());
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Long.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
