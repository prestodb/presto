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
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHashJoinOperator
{
    private static final int PARTITION_COUNT = 4;

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
        return new Object[][] {
                {true, true, true},
                {true, true, false},
                {true, false, true},
                {true, false, false},
                {false, true, true},
                {false, true, false},
                {false, false, true},
                {false, false, false}};
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("20", 1020, 2020, "20", 30, 40)
                .row("21", 1021, 2021, "21", 31, 41)
                .row("22", 1022, 2022, "22", 32, 42)
                .row("23", 1023, 2023, "23", 33, 43)
                .row("24", 1024, 2024, "24", 34, 44)
                .row("25", 1025, 2025, "25", 35, 45)
                .row("26", 1026, 2026, "26", 36, 46)
                .row("27", 1027, 2027, "27", 37, 47)
                .row("28", 1028, 2028, "28", 38, 48)
                .row("29", 1029, 2029, "29", 39, 49)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020, 2020, "20", 30, 40)
                .row("21", 1021, 2021, "21", 31, 41)
                .row("22", 1022, 2022, "22", 32, 42)
                .row("23", 1023, 2023, "23", 33, 43)
                .row("24", 1024, 2024, "24", 34, 44)
                .row("25", 1025, 2025, "25", 35, 45)
                .row("26", 1026, 2026, "26", 36, 46)
                .row("27", 1027, 2027, "27", 37, 47)
                .row("28", 1028, 2028, "28", 38, 48)
                .row("29", 1029, 2029, "29", 39, 49)
                .row("30", 1030, 2030, null, null, null)
                .row("31", 1031, 2031, null, null, null)
                .row("32", 1032, 2032, null, null, null)
                .row("33", 1033, 2033, null, null, null)
                .row("34", 1034, 2034, null, null, null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);

        // probe
        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel());
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of.*", dataProvider = "hashEnabledValues")
    public void testMemoryLimit(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, TEST_SESSION, new DataSize(100, BYTE));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testMemoryReservation(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(1), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10000, 2000, 30, 40);
        buildHash(parallelBuild, taskContext, Ints.asList(1), buildPages);

        long buildPagesSize = 0;
        for (Page page : buildPages.build()) {
            buildPagesSize += page.getSizeInBytes();
        }
        // especially for parallel build, ensure that buildPages are not accounted more then once
        assertTrue(taskContext.getPeekMemoryReservation() < 2 * buildPagesSize);
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, TEST_SESSION);
    }

    private static List<Integer> getHashChannels(RowPagesBuilder probe, RowPagesBuilder build)
    {
        ImmutableList.Builder<Integer> hashChannels = ImmutableList.builder();
        if (probe.getHashChannel().isPresent()) {
            hashChannels.add(probe.getHashChannel().get());
        }
        if (build.getHashChannel().isPresent()) {
            hashChannels.add(probe.getTypes().size() + build.getHashChannel().get());
        }
        return hashChannels.build();
    }

    private static LookupSourceSupplier buildHash(boolean parallelBuild, TaskContext taskContext, List<Integer> hashChannels, RowPagesBuilder buildPages)
    {
        if (parallelBuild) {
            ParallelHashBuilder parallelHashBuilder = new ParallelHashBuilder(buildPages.getTypes(), hashChannels, buildPages.getHashChannel(), 100, PARTITION_COUNT);

            // collect input data
            DriverContext collectDriverContext = taskContext.addPipelineContext(true, true).addDriverContext();
            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("test"), buildPages.getTypes(), buildPages.build());
            OperatorFactory collectOperatorFactory = parallelHashBuilder.getCollectOperatorFactory(1, new PlanNodeId("test"));
            Driver driver = new Driver(collectDriverContext,
                    valuesOperatorFactory.createOperator(collectDriverContext),
                    collectOperatorFactory.createOperator(collectDriverContext));

            while (!driver.isFinished()) {
                driver.process();
            }

            // build hash tables
            PipelineContext buildPipeline = taskContext.addPipelineContext(true, true);
            OperatorFactory buildOperatorFactory = parallelHashBuilder.getBuildOperatorFactory(new PlanNodeId("test"));
            for (int i = 0; i < PARTITION_COUNT; i++) {
                DriverContext buildDriverContext = buildPipeline.addDriverContext();
                Driver buildDriver = new Driver(buildDriverContext,
                        buildOperatorFactory.createOperator(buildDriverContext));

                while (!buildDriver.isFinished()) {
                    buildDriver.process();
                }
            }

            return parallelHashBuilder.getLookupSourceSupplier();
        }
        else {
            DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("test"), buildPages.getTypes(), buildPages.build());
            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(1, new PlanNodeId("test"), buildPages.getTypes(), hashChannels, buildPages.getHashChannel(), 100);

            Driver driver = new Driver(driverContext,
                    valuesOperatorFactory.createOperator(driverContext),
                    hashBuilderOperatorFactory.createOperator(driverContext));

            while (!driver.isFinished()) {
                driver.process();
            }
            return hashBuilderOperatorFactory.getLookupSourceSupplier();
        }
    }
}
