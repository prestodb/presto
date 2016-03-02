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
import org.testng.annotations.BeforeMethod;
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
    private TaskContext taskContext;

    @BeforeClass
    public void setUpClass()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDownClass()
    {
        executor.shutdownNow();
    }

    @BeforeMethod
    public void setUp()
    {
        taskContext = createTaskContext();
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
        List<Integer> hashChannels = Ints.asList(0);

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);

        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(1000, 0, 1000, 2000);

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

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(1);

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 1030, 40);

        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(1000, 0, 1000, 2000);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
                .row("30", 1030, 2030, "20", 1030, 40)
                .row("31", 1031, 2031, "21", 1031, 41)
                .row("32", 1032, 2032, "22", 1032, 42)
                .row("33", 1033, 2033, "23", 1033, 43)
                .row("34", 1034, 2034, "24", 1034, 44)
                .row("35", 1035, 2035, "25", 1035, 45)
                .row("36", 1036, 2036, "26", 1036, 46)
                .row("37", 1037, 2037, "27", 1037, 47)
                .row("38", 1038, 2038, "28", 1038, 48)
                .row("39", 1039, 2039, "29", 1039, 49)
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row("a")
                .row("b")
                .row("c");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row(1)
                .row(2)
                .row(3);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1, 1)
                .row(1, 1)
                .row(2, 2)
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row("b")
                .row("c");

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row(2)
                .row(3);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1, 1)
                .row(1, 1)
                .row(2, 2)
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row(2)
                .row((Long) null)
                .row(3);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1, 1)
                .row(1, 1)
                .row(2, 2)
                .build();

        assertInnerJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .addSequencePage(15, 20, 1020, 2020);

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

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(1);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 30, 1020, 40);

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .addSequencePage(15, 20, 1020, 2020);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020, 2020, "30", 1020, 40)
                .row("21", 1021, 2021, "31", 1021, 41)
                .row("22", 1022, 2022, "32", 1022, 42)
                .row("23", 1023, 2023, "33", 1023, 43)
                .row("24", 1024, 2024, "34", 1024, 44)
                .row("25", 1025, 2025, "35", 1025, 45)
                .row("26", 1026, 2026, "36", 1026, 46)
                .row("27", 1027, 2027, "37", 1027, 47)
                .row("28", 1028, 2028, "38", 1028, 48)
                .row("29", 1029, 2029, "39", 1029, 49)
                .row("30", 1030, 2030, null, null, null)
                .row("31", 1031, 2031, null, null, null)
                .row("32", 1032, 2032, null, null, null)
                .row("33", 1033, 2033, null, null, null)
                .row("34", 1034, 2034, null, null, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row("a")
                .row("b")
                .row("c");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, buildTypes)
                .row(1)
                .row(2)
                .row(3);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1, 1)
                .row(null, null)
                .row(null, null)
                .row(1, 1)
                .row(2, 2)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row("b")
                .row("c");

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        List<Type> buildTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(BIGINT))
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row(2)
                .row(3);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1, 1)
                .row(1, 1)
                .row(2, 2)
                .row(3, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");

        List<Type> probeTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testBigintOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        List<Integer> hashChannels = Ints.asList(0);

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, hashChannels, ImmutableList.of(BIGINT))
                .row(1)
                .row((Long) null)
                .row((Long) null)
                .row(1)
                .row(2);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1)
                .row(2)
                .row((Long) null)
                .row(3);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1, 1)
                .row(1, 1)
                .row(2, 2)
                .row(null, null)
                .row(3, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
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

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of.*", dataProvider = "hashEnabledValues")
    public void testBigintMemoryLimit(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, TEST_SESSION, new DataSize(100, BYTE));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(1), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        buildHash(parallelBuild, taskContext, Ints.asList(1), buildPages);
    }

    private void assertInnerJoin(RowPagesBuilder probePages, RowPagesBuilder buildPages, List<Integer> hashChannels, boolean parallelBuild, MaterializedResult expected)
    {
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, hashChannels, buildPages);

        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                hashChannels,
                probePages.getHashChannel());

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        assertOperatorEquals(joinOperator, probePages.build(), expected, true, getHashChannels(probePages, buildPages));
    }

    private void assertProbeOuterJoin(RowPagesBuilder probePages, RowPagesBuilder buildPages, List<Integer> hashChannels, boolean parallelBuild, MaterializedResult expected)
    {
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, hashChannels, buildPages);

        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                hashChannels,
                probePages.getHashChannel());

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        assertOperatorEquals(joinOperator, probePages.build(), expected, true, getHashChannels(probePages, buildPages));
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
