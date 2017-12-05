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
import com.facebook.presto.operator.NestedLoopBuildOperator.NestedLoopBuildOperatorFactory;
import com.facebook.presto.operator.NestedLoopJoinOperator.NestedLoopJoinOperatorFactory;
import com.facebook.presto.operator.NestedLoopJoinOperator.NestedLoopPageBuilder;
import com.facebook.presto.operator.project.PageProcessor;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.PageFunctionCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.relational.RowExpression;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.MetadataManager.createTestMetadataManager;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import static com.facebook.presto.spi.function.OperatorType.EQUAL;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.relational.Expressions.call;
import static com.facebook.presto.sql.relational.Expressions.constant;
import static com.facebook.presto.sql.relational.Expressions.field;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestNestedLoopJoinOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testNestedLoopJoin()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();
        // build
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(3, 20, 30, 40);
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(2, 0, 1000, 2000)
                .build();

        MetadataManager metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, VARCHAR), field(1, BIGINT), field(2, BIGINT), field(3, VARCHAR), field(4, BIGINT), field(5, BIGINT)));

        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR, BIGINT, BIGINT),
                processor, new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("0", 1000L, 2000L, "20", 30L, 40L)
                .row("0", 1000L, 2000L, "21", 31L, 41L)
                .row("0", 1000L, 2000L, "22", 32L, 42L)
                .row("1", 1001L, 2001L, "20", 30L, 40L)
                .row("1", 1001L, 2001L, "21", 31L, 41L)
                .row("1", 1001L, 2001L, "22", 32L, 42L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);

        // Test probe pages has more rows
        buildPages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(2, 20, 30, 40);
        nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);
        joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR, BIGINT, BIGINT), processor, new DataSize(0, BYTE), 0);

        // probe
        probePages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        probeInput = probePages
                .addSequencePage(3, 0, 1000, 2000)
                .build();

        // expected
        expected = resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("0", 1000L, 2000L, "20", 30L, 40L)
                .row("1", 1001L, 2001L, "20", 30L, 40L)
                .row("2", 1002L, 2002L, "20", 30L, 40L)
                .row("0", 1000L, 2000L, "21", 31L, 41L)
                .row("1", 1001L, 2001L, "21", 31L, 41L)
                .row("2", 1002L, 2002L, "21", 31L, 41L)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    private Supplier<PageProcessor> getPageProcessor()
    {
        MetadataManager metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        return compiler.compilePageProcessor(Optional.empty(), ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));
    }

    @Test
    public void testCrossJoinWithNullProbe()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("a")
                .row("b");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("A")
                .row((String) null)
                .row((String) null)
                .row("A")
                .row("B")
                .build();

        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("A", "a")
                .row(null, "a")
                .row(null, "a")
                .row("A", "a")
                .row("B", "a")
                .row("A", "b")
                .row(null, "b")
                .row(null, "b")
                .row("A", "b")
                .row("B", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testCrossJoinWithNullBuild()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("A")
                .row("B")
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("A", "a")
                .row("A", null)
                .row("A", null)
                .row("A", "a")
                .row("A", "b")
                .row("B", "a")
                .row("B", null)
                .row("B", null)
                .row("B", "a")
                .row("B", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testCrossJoinWithNullOnBothSides()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("a")
                .row((String) null)
                .row("b")
                .row("c")
                .row((String) null);
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("A")
                .row("B")
                .row((String) null)
                .row("C")
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("A", "a")
                .row("A", null)
                .row("A", "b")
                .row("A", "c")
                .row("A", null)
                .row("B", "a")
                .row("B", null)
                .row("B", "b")
                .row("B", "c")
                .row("B", null)
                .row(null, "a")
                .row(null, null)
                .row(null, "b")
                .row(null, "c")
                .row(null, null)
                .row("C", "a")
                .row("C", null)
                .row("C", "b")
                .row("C", "c")
                .row("C", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testBuildMultiplePages()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("a")
                .pageBreak()
                .row((String) null)
                .row("b")
                .row("c")
                .pageBreak()
                .row("d");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("A")
                .row("B")
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("A", "a")
                .row("B", "a")
                .row("A", null)
                .row("A", "b")
                .row("A", "c")
                .row("B", null)
                .row("B", "b")
                .row("B", "c")
                .row("A", "d")
                .row("B", "d")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testProbeMultiplePages()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("A")
                .row("B");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .pageBreak()
                .row((String) null)
                .row("b")
                .row("c")
                .pageBreak()
                .row("d")
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "A")
                .row("a", "B")
                .row(null, "A")
                .row("b", "A")
                .row("c", "A")
                .row(null, "B")
                .row("b", "B")
                .row("c", "B")
                .row("d", "A")
                .row("d", "B")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testProbeAndBuildMultiplePages()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("A")
                .row("B")
                .pageBreak()
                .row("C");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .pageBreak()
                .row((String) null)
                .row("b")
                .row("c")
                .pageBreak()
                .row("d")
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "A")
                .row("a", "B")
                .row("a", "C")
                .row(null, "A")
                .row("b", "A")
                .row("c", "A")
                .row(null, "B")
                .row("b", "B")
                .row("c", "B")
                .row(null, "C")
                .row("b", "C")
                .row("c", "C")
                .row("d", "A")
                .row("d", "B")
                .row("d", "C")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testFilter()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("a")
                .row("b")
                .pageBreak()
                .row("c");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("apple")
                .pageBreak()
                .row("banana")
                .row("carrot")
                .pageBreak()
                .row("date")
                .build();

        RowExpression filter = call(
                Signature.internalOperator(EQUAL, BOOLEAN.getTypeSignature(), ImmutableList.of(BIGINT.getTypeSignature(), BIGINT.getTypeSignature())),
                BOOLEAN,
                call(
                        Signature.internalScalarFunction("strpos", BIGINT.getTypeSignature(), ImmutableList.of(VARCHAR.getTypeSignature(), VARCHAR.getTypeSignature())),
                        BIGINT,
                        field(0, VARCHAR),
                        field(1, VARCHAR)),
                constant(1L, BIGINT));

        MetadataManager metadata = createTestMetadataManager();
        ExpressionCompiler compiler = new ExpressionCompiler(metadata, new PageFunctionCompiler(metadata, 0));
        Supplier<PageProcessor> processor = compiler.compilePageProcessor(Optional.of(filter), ImmutableList.of(field(0, VARCHAR), field(1, VARCHAR)));

        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR),
                processor, new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("apple", "a")
                .row("banana", "b")
                .row("carrot", "c")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testEmptyProbePage()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .row("A")
                .row("B")
                .pageBreak()
                .row("C");
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .pageBreak()
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testEmptyBuildPage()
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildTypes)
                .pageBreak();
        NestedLoopJoinPagesSupplier nestedLoopJoinPagesSupplier = buildPageSource(taskContext, buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeTypes);
        List<Page> probeInput = probePages
                .row("A")
                .row("B")
                .pageBreak()
                .build();
        NestedLoopJoinOperatorFactory joinOperatorFactory = new NestedLoopJoinOperatorFactory(3, new PlanNodeId("test"), nestedLoopJoinPagesSupplier, ImmutableList.of(VARCHAR), getPageProcessor(), new DataSize(0, BYTE), 0);

        // expected
        MaterializedResult expected = resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected);
    }

    @Test
    public void testCount()
            throws Exception
    {
        // normal case
        Page buildPage = new Page(100);
        Page probePage = new Page(45);

        NestedLoopPageBuilder resultPageBuilder = new NestedLoopPageBuilder(probePage, buildPage);
        assertTrue(resultPageBuilder.hasNext(), "There should be at least one page.");

        long result = 0;
        while (resultPageBuilder.hasNext()) {
            result += resultPageBuilder.next().getPositionCount();
        }
        assertEquals(result, 4500);

        // force the product to be bigger than Integer.MAX_VALUE
        buildPage = new Page(Integer.MAX_VALUE - 10);
        resultPageBuilder = new NestedLoopPageBuilder(probePage, buildPage);

        result = 0;
        while (resultPageBuilder.hasNext()) {
            result += resultPageBuilder.next().getPositionCount();
        }
        assertEquals((Integer.MAX_VALUE - 10) * 45L, result);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private static NestedLoopJoinPagesSupplier buildPageSource(TaskContext taskContext, RowPagesBuilder buildPages)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();

        ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("test"), buildPages.getTypes(), buildPages.build());
        NestedLoopBuildOperatorFactory nestedLoopBuildOperatorFactory = new NestedLoopBuildOperatorFactory(1, new PlanNodeId("test"), buildPages.getTypes());

        Driver driver = Driver.createDriver(driverContext,
                valuesOperatorFactory.createOperator(driverContext),
                nestedLoopBuildOperatorFactory.createOperator(driverContext));

        valuesOperatorFactory.noMoreOperators();
        nestedLoopBuildOperatorFactory.noMoreOperators();

        while (!driver.isFinished()) {
            driver.process();
        }
        return nestedLoopBuildOperatorFactory.getNestedLoopJoinPagesSupplier();
    }
}
