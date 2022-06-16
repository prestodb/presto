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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.internal.collections.Ints;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.Executors.newScheduledThreadPool;

@Test(singleThreaded = true)
public class TestMergeJoinOperator
{
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                daemonThreadsNamed("test-executor-%s"),
                new ThreadPoolExecutor.DiscardPolicy());
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @Test
    public void testSimpleJoin()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a", "10")
                .row("b", "20")
                .row("f", "60")
                .row("g", "70");

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a", "1")
                .row("b", "2")
                .row("c", "3")
                .row("d", "4")
                .row("e", "5")
                .row("f", "6");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftPages.getTypesWithoutHash(), rightPages.getTypesWithoutHash())))
                .row("a", "1", "a", "10")
                .row("b", "2", "b", "20")
                .row("f", "6", "f", "60")
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoin()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .addSequencePage(2, 20, 30, 40)
                .addSequencePage(2, 22, 32, 42)
                .addSequencePage(2, 24, 34, 44)
                .addSequencePage(2, 26, 36, 46)
                .addSequencePage(2, 28, 38, 48);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .addSequencePage(1000, 0, 1000, 2000);
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftPages.getTypesWithoutHash(), rightPages.getTypesWithoutHash())))
                .row("20", 1020L, 2020L, "20", 30L, 40L)
                .row("21", 1021L, 2021L, "21", 31L, 41L)
                .row("22", 1022L, 2022L, "22", 32L, 42L)
                .row("23", 1023L, 2023L, "23", 33L, 43L)
                .row("24", 1024L, 2024L, "24", 34L, 44L)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testLeftJoin()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a", 1)
                .row("b", 2)
                .row("d", 5);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("2", 11)
                .row("20", 11)
                .row("3", 11)
                .row("a", 11)
                .row("a", 22)
                .pageBreak()
                .row("b", 22)
                .row("c", 33)
                .pageBreak()
                .row("d", 55)
                .pageBreak()
                .row("e", 60)
                .row("f", 70);
        List<Page> leftInput = leftPages.build();

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.LEFT);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("2", 11L, null, null)
                .row("20", 11L, null, null)
                .row("3", 11L, null, null)
                .row("a", 11L, "a", 1L)
                .row("a", 22L, "a", 1L)
                .row("b", 22L, "b", 2L)
                .row("c", 33L, null, null)
                .row("d", 55L, "d", 5L)
                .row("e", 60L, null, null)
                .row("f", 70L, null, null)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testRightJoin()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("2", 11)
                .row("20", 11)
                .row("3", 11)
                .row("a", 11)
                .pageBreak()
                .row("b", 22)
                .row("c", 33)
                .pageBreak()
                .row("d", 55)
                .pageBreak()
                .row("e", 60)
                .row("f", 70);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, false);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a", 1)
                .row("a", 30)
                .row("b", 2)
                .row("d", 5);

        List<Page> leftInput = leftPages.build();

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.RIGHT);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row(null, null, "2", 11L)
                .row(null, null, "20", 11L)
                .row(null, null, "3", 11L)
                .row("a", 1L, "a", 11L)
                .row("a", 30L, "a", 11L)
                .row("b", 2L, "b", 22L)
                .row(null, null, "c", 33L)
                .row("d", 5L, "d", 55L)
                .row(null, null, "e", 60L)
                .row(null, null, "f", 70L)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testOuterJoin()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("1", 0L)
                .row("a", 100L)
                .row("c", 3L)
                .row("d", 4L)
                .row("e", 5L)
                .row("g", 200L)
                .row("h", 300L)
                .row("j", 10L)
                .pageBreak()
                .row("p", 12L);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, false);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a", 1L)
                .row("b", 2L)
                .row("f", 6L)
                .row("g", 7L)
                .row("h", 8L)
                .row("i", 9L)
                .pageBreak()
                .row("k", 11L);

        List<Page> leftInput = leftPages.build();

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.FULL);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row(null, null, "1", 0L)
                .row("a", 1L, "a", 100L)
                .row("b", 2L, null, null)
                .row(null, null, "c", 3L)
                .row(null, null, "d", 4L)
                .row(null, null, "e", 5L)
                .row("f", 6L, null, null)
                .row("g", 7L, "g", 200L)
                .row("h", 8L, "h", 300L)
                .row("i", 9L, null, null)
                .row(null, null, "j", 10L)
                .row("k", 11L, null, null)
                .row(null, null, "p", 12L)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithMultiplePages()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a", 1)
                .pageBreak()
                .row("b", 2)
                .pageBreak()
                .row("d", 5);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        // disable sorting so that the pages do not get merged by sort operator
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/false);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR, BIGINT);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a", 11)
                .pageBreak()
                .row("b", 22)
                .row("c", 33)
                .pageBreak()
                .row("d", 55);
        List<Page> leftInput = leftPages.build();

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("a", 11L, "a", 1L)
                .row("b", 22L, "b", 2L)
                .row("d", 55L, "d", 5L)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithNullLeft()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a")
                .row("b")
                .row("c");

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(true);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithNullRight()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a")
                .row("b")
                .row("c");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithNullOnBothSides()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithEmptyRight()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testInnerJoinWithNonEmptyRightAndEmptyLeft()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes);
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testLeftJoinWithEmptyRight()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/true);

        // left side
        List<Type> leftTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .row("a");
        List<Page> leftInput = sortPages(leftPages);

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.LEFT);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .row("a", null)
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    @Test
    public void testLargeInput()
    {
        TaskContext taskContext = createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        // right side
        List<Type> rightTypes = ImmutableList.of(BIGINT);
        RowPagesBuilder rightPages = rowPagesBuilder(false, Ints.asList(0), rightTypes)
                .addSequencePage(100000, 1);

        MergeJoinSourceManager mergeJoinSourceManager = new MergeJoinSourceManager(false);
        setupAndInitializeRightDriver(taskContext, rightPages, mergeJoinSourceManager, /*sortPages=*/false);

        // left side
        List<Type> leftTypes = ImmutableList.of(BIGINT, DOUBLE);
        RowPagesBuilder leftPages = rowPagesBuilder(false, Ints.asList(0), leftTypes)
                .addSequencePage(100000, 1, 1501);

        List<Page> leftInput = leftPages.build();

        OperatorFactory mergeJoinOperatorFactory = mergeJoinOperatorFactory(leftTypes, rightTypes, mergeJoinSourceManager, JoinNode.Type.INNER);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), ImmutableList.copyOf(Iterables.concat(leftTypes, rightTypes)))
                .pages(rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT, DOUBLE, BIGINT)).addSequencePage(100000, 1, 1501, 1).build())
                .build();

        assertOperatorEquals(mergeJoinOperatorFactory, taskContext.addPipelineContext(1, true, true, false).addDriverContext(), leftInput, expected, false);
    }

    private List<Page> sortPages(RowPagesBuilder pages)
    {
        DriverContext driverContext = TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(1024))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();

        OrderByOperator.OrderByOperatorFactory operatorFactory = new OrderByOperator.OrderByOperatorFactory(
                0,
                new PlanNodeId("test"),
                pages.getTypes(),
                IntStream.range(0, pages.getTypes().size()).boxed().collect(Collectors.toList()),
                1000,
                ImmutableList.of(0),
                ImmutableList.of(SortOrder.ASC_NULLS_FIRST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler());
        List<Page> sortedPages = toPages(operatorFactory, driverContext, pages.build(), false);
        return sortedPages;
    }

    private void setupAndInitializeRightDriver(TaskContext taskContext, RowPagesBuilder rightPages, MergeJoinSourceManager mergeJoinSourceManager, boolean sortPages)
    {
        DriverContext rightDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        Operator valuesOperator = new ValuesOperator.ValuesOperatorFactory(1, new PlanNodeId("values"), rightPages.build()).createOperator(rightDriverContext);
        Operator orderByOperator = new OrderByOperator.OrderByOperatorFactory(
                2,
                new PlanNodeId("orderby"),
                rightPages.getTypes(),
                IntStream.range(0, rightPages.getTypes().size()).boxed().collect(Collectors.toList()), 1000, ImmutableList.of(0), ImmutableList.of(SortOrder.ASC_NULLS_FIRST),
                new PagesIndex.TestingFactory(false),
                false,
                Optional.empty(),
                new OrderingCompiler())
                .createOperator(rightDriverContext);
        Operator mergeJoinSinkOperator = new MergeJoinSinkOperator.MergeJoinSinkOperatorFactory(
                3,
                new PlanNodeId("mergeJoinSink"),
                mergeJoinSourceManager)
                .createOperator(rightDriverContext);

        Driver rightDriver;
        if (sortPages) {
            rightDriver = Driver.createDriver(
                    rightDriverContext,
                    valuesOperator,
                    orderByOperator,
                    mergeJoinSinkOperator);
        }
        else {
            rightDriver = Driver.createDriver(
                    rightDriverContext,
                    valuesOperator,
                    mergeJoinSinkOperator);
        }

        // run the right side driver in a separate thread
        runDriverInThread(executor, rightDriver);
    }

    private OperatorFactory mergeJoinOperatorFactory(List<Type> leftTypes, List<Type> rightTypes, MergeJoinSourceManager mergeJoinSourceManager, JoinNode.Type joinType)
    {
        MergeJoinOperators.MergeJoiner mergeJoiner = null;
        switch (joinType) {
            case LEFT:
                mergeJoiner = new MergeJoinOperators.LeftJoiner();
                break;
            case INNER:
                mergeJoiner = new MergeJoinOperators.InnerJoiner();
                break;
            case RIGHT:
                mergeJoiner = new MergeJoinOperators.RightJoiner();
                break;
            case FULL:
                mergeJoiner = new MergeJoinOperators.FullJoiner();
                break;
            default:
                throw new UnsupportedOperationException("Unsupported join type: " + joinType);
        }
        OperatorFactory mergeInnerJoinOperatorFactory = new MergeJoinOperatorFactory(
                3,
                new PlanNodeId("mergeJoinNode"),
                mergeJoinSourceManager,
                leftTypes,
                rangeList(leftTypes.size()),
                rightTypes,
                rangeList(rightTypes.size()),
                ImmutableList.of(0),
                ImmutableList.of(0),
                mergeJoiner);
        return mergeInnerJoinOperatorFactory;
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    // run driver in another thread until it is finished
    private static void runDriverInThread(ExecutorService executor, Driver driver)
    {
        executor.execute(() -> {
            if (!driver.isFinished()) {
                try {
                    driver.process();
                }
                catch (PrestoException e) {
                    driver.getDriverContext().failed(e);
                    throw e;
                }
                runDriverInThread(executor, driver);
            }
        });
    }
}
