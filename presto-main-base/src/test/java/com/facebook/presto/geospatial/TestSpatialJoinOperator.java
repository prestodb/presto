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
package com.facebook.presto.geospatial;

import com.facebook.presto.RowPagesBuilder;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.operator.Driver;
import com.facebook.presto.operator.DriverContext;
import com.facebook.presto.operator.InternalJoinFilterFunction;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorFactory;
import com.facebook.presto.operator.PagesIndex.TestingFactory;
import com.facebook.presto.operator.PagesSpatialIndex;
import com.facebook.presto.operator.PagesSpatialIndexFactory;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialIndexBuilderOperatorFactory;
import com.facebook.presto.operator.SpatialIndexBuilderOperator.SpatialPredicate;
import com.facebook.presto.operator.SpatialJoinOperator.SpatialJoinOperatorFactory;
import com.facebook.presto.operator.StandardJoinFilterFunction;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.operator.ValuesOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.SpatialJoinNode.SpatialJoinType;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.geospatial.GeoFunctions.spatialPartitions;
import static com.facebook.presto.geospatial.GeoFunctions.stGeometryFromText;
import static com.facebook.presto.geospatial.GeoFunctions.stPoint;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEqualsIgnoreOrder;
import static com.facebook.presto.spi.plan.SpatialJoinNode.SpatialJoinType.INNER;
import static com.facebook.presto.spi.plan.SpatialJoinNode.SpatialJoinType.LEFT;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestSpatialJoinOperator
{
    private static final KdbTree KDB_TREE = KdbTree.buildKdbTree(
            2,
            ImmutableList.of(
                    new Rectangle(-2, -2, -2, -2),
                    new Rectangle(0, 0, 0, 0),
                    new Rectangle(-1, -2, 4, 3),
                    new Rectangle(6, 1, 6, 1),
                    new Rectangle(3, 9, 3, 9),
                    new Rectangle(15, 15, 15, 15)));
    private static final String KDB_TREE_JSON = KdbTreeUtils.toJson(KDB_TREE);

    //  2 intersecting polygons: A and B
    private static final Slice POLYGON_A = stGeometryFromText(Slices.utf8Slice("POLYGON ((0 0, -0.5 2.5, 0 5, 2.5 5.5, 5 5, 5.5 2.5, 5 0, 2.5 -0.5, 0 0))"));
    private static final Slice POLYGON_B = stGeometryFromText(Slices.utf8Slice("POLYGON ((4 4, 3.5 7, 4 10, 7 10.5, 10 10, 10.5 7, 10 4, 7 3.5, 4 4))"));
    private static final Slice POLYGON_C = stGeometryFromText(Slices.utf8Slice("POLYGON ((15 15, 15 14, 14 14, 14 15, 15 15))"));
    private static final Slice POLYGON_D = stGeometryFromText(Slices.utf8Slice("POLYGON ((18 18, 18 19, 19 19, 19 18, 18 18))"));

    // A set of points: X in A, Y in A and B, Z in B, W outside of A and B
    private static final Slice POINT_X = stPoint(1, 1);
    private static final Slice POINT_Y = stPoint(4.5, 4.5);
    private static final Slice POINT_Z = stPoint(6, 6);
    private static final Slice POINT_W = stPoint(20, 20);
    private static final Slice POINT_V = stPoint(15, 15);
    private static final Slice MULTIPOINT_U = stGeometryFromText(Slices.utf8Slice("MULTIPOINT (15 15)"));
    private static final Slice MULTIPOINT_T = stGeometryFromText(Slices.utf8Slice("MULTIPOINT (14.5 14.5, 16 16)"));
    private static final Slice POINT_S = stPoint(18, 18);
    private static final Slice MULTIPOINT_R = stGeometryFromText(Slices.utf8Slice("MULTIPOINT (15 15, 19 19)"));
    private static final Slice POINT_Q = stPoint(28, 28);

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        // Before/AfterMethod is chosen here because the executor needs to be shutdown
        // after every single test case to terminate outstanding threads, if any.

        // The line below is the same as newCachedThreadPool(daemonThreadsNamed(...)) except RejectionExecutionHandler.
        // RejectionExecutionHandler is set to DiscardPolicy (instead of the default AbortPolicy) here.
        // Otherwise, a large number of RejectedExecutionException will flood logging, resulting in Travis failure.
        executor = new ThreadPoolExecutor(
                0,
                Integer.MAX_VALUE,
                60L,
                SECONDS,
                new SynchronousQueue<>(),
                daemonThreadsNamed("test-executor-%s"),
                new ThreadPoolExecutor.DiscardPolicy());
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testSpatialJoin()
    {
        TaskContext taskContext = createTaskContext();
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POLYGON_A, "A")
                .row(null, "null")
                .pageBreak()
                .row(POLYGON_B, "B")
                .row(POLYGON_C, "C")
                .row(POLYGON_D, "D");

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POINT_X, "x")
                .row(null, "null")
                .row(POINT_Y, "y")
                .pageBreak()
                .row(POINT_Z, "z")
                .pageBreak()
                .row(POINT_W, "w")
                .row(POINT_V, "v")
                .row(MULTIPOINT_U, "u")
                .pageBreak()
                .row(MULTIPOINT_T, "t")
                .row(POINT_S, "s")
                .row(MULTIPOINT_R, "r")
                .row(POINT_Q, "q");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("x", "A")
                .row("y", "A")
                .row("y", "B")
                .row("z", "B")
                .row("v", "C")
                .row("u", "C")
                .row("t", "C")
                .row("s", "D")
                .row("r", "C")
                .row("r", "D")
                .build();

        assertSpatialJoin(taskContext, INNER, buildPages, probePages, expected);
    }

    @Test
    public void testSpatialLeftJoin()
    {
        TaskContext taskContext = createTaskContext();
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POLYGON_A, "A")
                .row(null, "null")
                .pageBreak()
                .row(POLYGON_B, "B")
                .row(POLYGON_C, "C")
                .row(POLYGON_D, "D");

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POINT_X, "x")
                .row(null, "null")
                .row(POINT_Y, "y")
                .pageBreak()
                .row(POINT_Z, "z")
                .pageBreak()
                .row(POINT_W, "w")
                .row(POINT_V, "v")
                .row(MULTIPOINT_U, "u")
                .pageBreak()
                .row(MULTIPOINT_T, "t")
                .row(POINT_S, "s")
                .row(MULTIPOINT_R, "r")
                .row(POINT_Q, "q");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("x", "A")
                .row("null", null)
                .row("y", "A")
                .row("y", "B")
                .row("z", "B")
                .row("w", null)
                .row("v", "C")
                .row("u", "C")
                .row("t", "C")
                .row("s", "D")
                .row("r", "C")
                .row("r", "D")
                .row("q", null)
                .build();

        assertSpatialJoin(taskContext, LEFT, buildPages, probePages, expected);
    }

    private void assertSpatialJoin(TaskContext taskContext, SpatialJoinType joinType, RowPagesBuilder buildPages, RowPagesBuilder probePages, MaterializedResult expected)
    {
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildIndex(driverContext, (build, probe, r) -> build.intersects(probe), Optional.empty(), Optional.empty(), buildPages);
        OperatorFactory joinOperatorFactory = new SpatialJoinOperatorFactory(2, new PlanNodeId("test"), joinType, probePages.getTypes(), Ints.asList(1), 0, Optional.empty(), pagesSpatialIndexFactory);
        assertOperatorEqualsIgnoreOrder(joinOperatorFactory, driverContext, probePages.build(), expected);
    }

    @Test
    public void testEmptyBuild()
    {
        TaskContext taskContext = createTaskContext();
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR));

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POINT_X, "x")
                .row(null, "null")
                .row(POINT_Y, "y")
                .pageBreak()
                .row(POINT_Z, "z")
                .pageBreak()
                .row(POINT_W, "w");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR)).build();

        assertSpatialJoin(taskContext, INNER, buildPages, probePages, expected);
    }

    @Test
    public void testEmptyBuildLeftJoin()
    {
        TaskContext taskContext = createTaskContext();
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR));

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POINT_X, "x")
                .row(null, "null")
                .row(POINT_Y, "y")
                .pageBreak()
                .row(POINT_Z, "z")
                .pageBreak()
                .row(POINT_W, "w");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("x", null)
                .row("null", null)
                .row("y", null)
                .row("z", null)
                .row("w", null)
                .build();

        assertSpatialJoin(taskContext, LEFT, buildPages, probePages, expected);
    }

    @Test
    public void testEmptyProbe()
    {
        TaskContext taskContext = createTaskContext();
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POLYGON_A, "A")
                .row(null, "null")
                .pageBreak()
                .row(POLYGON_B, "B");

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR));

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR)).build();

        assertSpatialJoin(taskContext, INNER, buildPages, probePages, expected);
    }

    @Test
    public void testYield()
    {
        // create a filter function that yields for every probe match
        // verify we will yield #match times totally

        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        // force a yield for every match
        AtomicInteger filterFunctionCalls = new AtomicInteger();
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    filterFunctionCalls.incrementAndGet();
                    driverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                }));

        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(POLYGON_A, "A")
                .pageBreak()
                .row(POLYGON_B, "B");
        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildIndex(driverContext, (build, probe, r) -> build.contains(probe), Optional.empty(), Optional.of(filterFunction), buildPages);

        // 10 points in polygon A (x0...x9)
        // 10 points in polygons A and B (y0...y9)
        // 10 points in polygon B (z0...z9)
        // 40 total matches
        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR));
        for (int i = 0; i < 10; i++) {
            probePages.row(stPoint(1 + 0.1 * i, 1 + 0.1 * i), "x" + i);
        }
        for (int i = 0; i < 10; i++) {
            probePages.row(stPoint(4.5 + 0.01 * i, 4.5 + 0.01 * i), "y" + i);
        }
        for (int i = 0; i < 10; i++) {
            probePages.row(stPoint(6 + 0.1 * i, 6 + 0.1 * i), "z" + i);
        }
        List<Page> probeInput = probePages.build();

        OperatorFactory joinOperatorFactory = new SpatialJoinOperatorFactory(2, new PlanNodeId("test"), INNER, probePages.getTypes(), Ints.asList(1), 0, Optional.empty(), pagesSpatialIndexFactory);

        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertTrue(operator.needsInput());
        operator.addInput(probeInput.get(0));
        operator.finish();

        // we will yield 40 times due to filterFunction
        for (int i = 0; i < 40; i++) {
            driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
            assertNull(operator.getOutput());
            assertEquals(filterFunctionCalls.get(), i + 1, "Expected join to stop processing (yield) after calling filter function once");
            driverContext.getYieldSignal().reset();
        }
        // delayed yield is not going to prevent operator from producing a page now (yield won't be forced because filter function won't be called anymore)
        driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
        Page output = operator.getOutput();
        assertNotNull(output);

        // make sure we have 40 matches
        assertEquals(output.getPositionCount(), 40);
    }

    @Test
    public void testDistanceQuery()
    {
        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();

        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR, DOUBLE))
                .row(stPoint(0, 0), "0_0", 1.5)
                .row(null, "null", 1.5)
                .row(stPoint(1, 0), "1_0", 1.5)
                .pageBreak()
                .row(stPoint(3, 0), "3_0", 1.5)
                .pageBreak()
                .row(stPoint(10, 0), "10_0", 1.5);
        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildIndex(driverContext, (build, probe, r) -> build.distance(probe) <= r.getAsDouble(), Optional.of(2), Optional.empty(), buildPages);

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR))
                .row(stPoint(0, 1), "0_1")
                .row(null, "null")
                .row(stPoint(1, 1), "1_1")
                .pageBreak()
                .row(stPoint(3, 1), "3_1")
                .pageBreak()
                .row(stPoint(10, 1), "10_1");
        OperatorFactory joinOperatorFactory = new SpatialJoinOperatorFactory(2, new PlanNodeId("test"), INNER, probePages.getTypes(), Ints.asList(1), 0, Optional.empty(), pagesSpatialIndexFactory);

        // Make sure that spatial index reference counting works with duplicate factories
        joinOperatorFactory.duplicate().noMoreOperators();

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("0_1", "0_0")
                .row("0_1", "1_0")
                .row("1_1", "0_0")
                .row("1_1", "1_0")
                .row("3_1", "3_0")
                .row("10_1", "10_0")
                .build();

        assertOperatorEqualsIgnoreOrder(joinOperatorFactory, driverContext, probePages.build(), expected);
    }

    @Test
    public void testDistributedSpatialJoin()
    {
        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, true).addDriverContext();

        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR, INTEGER));
        addGeometryPartitionRows(buildPages, POLYGON_A, "A");
        buildPages.row(null, "null", null);
        buildPages.pageBreak();
        addGeometryPartitionRows(buildPages, POLYGON_B, "B");
        addGeometryPartitionRows(buildPages, POLYGON_C, "C");
        addGeometryPartitionRows(buildPages, POLYGON_D, "D");

        RowPagesBuilder probePages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR, INTEGER));
        addGeometryPartitionRows(probePages, POINT_X, "x");
        probePages.row(null, "null", null);
        addGeometryPartitionRows(probePages, POINT_Y, "y");
        probePages.pageBreak();
        addGeometryPartitionRows(probePages, POINT_Z, "z");
        addGeometryPartitionRows(probePages, POINT_W, "w");
        addGeometryPartitionRows(probePages, POINT_V, "v");
        addGeometryPartitionRows(probePages, MULTIPOINT_U, "u");
        probePages.pageBreak();
        addGeometryPartitionRows(probePages, MULTIPOINT_T, "t");
        addGeometryPartitionRows(probePages, POINT_S, "s");
        addGeometryPartitionRows(probePages, MULTIPOINT_R, "r");
        addGeometryPartitionRows(probePages, POINT_Q, "q");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("x", "A")
                .row("y", "A")
                .row("y", "B")
                .row("z", "B")
                .row("v", "C")
                .row("u", "C")
                .row("t", "C")
                .row("s", "D")
                .row("r", "C")
                .row("r", "D")
                .build();

        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildIndex(driverContext, (build, probe, r) -> build.intersects(probe), Optional.empty(), Optional.of(2), Optional.of(KDB_TREE_JSON), Optional.empty(), buildPages);
        OperatorFactory joinOperatorFactory = new SpatialJoinOperatorFactory(2, new PlanNodeId("test"), INNER, probePages.getTypes(), Ints.asList(1), 0, Optional.of(2), pagesSpatialIndexFactory);
        assertOperatorEqualsIgnoreOrder(joinOperatorFactory, driverContext, probePages.build(), expected);
    }

    @Test
    public void testDistributedSpatialSelfJoin()
    {
        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true, true).addDriverContext();

        RowPagesBuilder pages = rowPagesBuilder(ImmutableList.of(GEOMETRY, VARCHAR, INTEGER));
        addGeometryPartitionRows(pages, POLYGON_A, "A");
        pages.row(null, "null", null);
        pages.pageBreak();
        addGeometryPartitionRows(pages, POLYGON_B, "B");

        MaterializedResult expected = resultBuilder(taskContext.getSession(), ImmutableList.of(VARCHAR, VARCHAR))
                .row("A", "A")
                .row("A", "B")
                .row("B", "A")
                .row("B", "B")
                .build();

        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildIndex(driverContext, (build, probe, r) -> build.intersects(probe), Optional.empty(), Optional.of(2), Optional.of(KDB_TREE_JSON), Optional.empty(), pages);
        OperatorFactory joinOperatorFactory = new SpatialJoinOperatorFactory(2, new PlanNodeId("test"), INNER, pages.getTypes(), Ints.asList(1), 0, Optional.of(2), pagesSpatialIndexFactory);
        assertOperatorEqualsIgnoreOrder(joinOperatorFactory, driverContext, pages.build(), expected);
    }

    private void addGeometryPartitionRows(RowPagesBuilder pageBuilder, Slice geometry, String geometryName)
    {
        Block partitionIndices = spatialPartitions(KDB_TREE, geometry);
        for (int position = 0; position < partitionIndices.getPositionCount(); position++) {
            int partitionIndex = partitionIndices.getInt(position);
            pageBuilder.row(geometry, geometryName, partitionIndex);
        }
    }

    private PagesSpatialIndexFactory buildIndex(DriverContext driverContext, SpatialPredicate spatialRelationshipTest, Optional<Integer> radiusChannel, Optional<InternalJoinFilterFunction> filterFunction, RowPagesBuilder buildPages)
    {
        return buildIndex(driverContext, spatialRelationshipTest, radiusChannel, Optional.empty(), Optional.empty(), filterFunction, buildPages);
    }

    private PagesSpatialIndexFactory buildIndex(DriverContext driverContext, SpatialPredicate spatialRelationshipTest, Optional<Integer> radiusChannel, Optional<Integer> partitionChannel, Optional<String> kdbTreeJson, Optional<InternalJoinFilterFunction> filterFunction, RowPagesBuilder buildPages)
    {
        Optional<JoinFilterFunctionCompiler.JoinFilterFunctionFactory> filterFunctionFactory = filterFunction
                .map(function -> (session, addresses, pages) -> new StandardJoinFilterFunction(function, addresses, pages));

        ValuesOperator.ValuesOperatorFactory valuesOperatorFactory = new ValuesOperator.ValuesOperatorFactory(0, new PlanNodeId("test"), buildPages.build());
        SpatialIndexBuilderOperatorFactory buildOperatorFactory = new SpatialIndexBuilderOperatorFactory(
                1,
                new PlanNodeId("test"),
                buildPages.getTypes(),
                Ints.asList(1),
                0,
                radiusChannel,
                partitionChannel,
                spatialRelationshipTest,
                kdbTreeJson,
                filterFunctionFactory,
                10_000,
                new TestingFactory(false));

        Driver driver = Driver.createDriver(
                driverContext,
                valuesOperatorFactory.createOperator(driverContext),
                buildOperatorFactory.createOperator(driverContext));

        PagesSpatialIndexFactory pagesSpatialIndexFactory = buildOperatorFactory.getPagesSpatialIndexFactory();
        ListenableFuture<PagesSpatialIndex> pagesSpatialIndex = pagesSpatialIndexFactory.createPagesSpatialIndex();

        while (!pagesSpatialIndex.isDone()) {
            driver.process();
        }

        // Release the spatial index reference
        pagesSpatialIndexFactory.probeOperatorFinished();

        runDriverInThread(executor, driver);
        return pagesSpatialIndexFactory;
    }

    /**
     * Runs Driver in another thread until it is finished
     */
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

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
    }

    private static class TestInternalJoinFilterFunction
            implements InternalJoinFilterFunction
    {
        public interface Lambda
        {
            boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage);
        }

        private final TestInternalJoinFilterFunction.Lambda lambda;

        private TestInternalJoinFilterFunction(TestInternalJoinFilterFunction.Lambda lambda)
        {
            this.lambda = lambda;
        }

        @Override
        public boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
        {
            return lambda.filter(leftPosition, leftPage, rightPosition, rightPage);
        }
    }
}
