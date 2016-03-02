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
import com.facebook.presto.operator.ParallelHashBuildOperator.ParallelHashBuildOperatorFactory;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchange;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.primitives.Ints;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

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

        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(1000, 0, 1000, 2000);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypes(), buildPages.getTypes()))
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
                .row("30", 1030L, 2030L, "20", 1030L, 40L)
                .row("31", 1031L, 2031L, "21", 1031L, 41L)
                .row("32", 1032L, 2032L, "22", 1032L, 42L)
                .row("33", 1033L, 2033L, "23", 1033L, 43L)
                .row("34", 1034L, 2034L, "24", 1034L, 44L)
                .row("35", 1035L, 2035L, "25", 1035L, 45L)
                .row("36", 1036L, 2036L, "26", 1036L, 46L)
                .row("37", 1037L, 2037L, "27", 1037L, 47L)
                .row("38", 1038L, 2038L, "28", 1038L, 48L)
                .row("39", 1039L, 2039L, "29", 1039L, 49L)
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
                .row(1L)
                .row(2L)
                .row(3L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
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
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row(2L)
                .row(3L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
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
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row(2L)
                .row((Long) null)
                .row(3L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
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
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeOuterJoinWithFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        JoinFilterFunction filterFunction = new TestJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> BIGINT.getLong(rightBlocks[1], rightPosition) >= 1025));

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

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
                probePages.getHashChannel(),
                true);

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("20", 1020L, 2020L, null, null, null)
                .row("21", 1021L, 2021L, null, null, null)
                .row("22", 1022L, 2022L, null, null, null)
                .row("23", 1023L, 2023L, null, null, null)
                .row("24", 1024L, 2024L, null, null, null)
                .row("25", 1025L, 2025L, "25", 35L, 45L)
                .row("26", 1026L, 2026L, "26", 36L, 46L)
                .row("27", 1027L, 2027L, "27", 37L, 47L)
                .row("28", 1028L, 2028L, "28", 38L, 48L)
                .row("29", 1029L, 2029L, "29", 39L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
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
                .row("20", 1020L, 2020L, "30", 1020L, 40L)
                .row("21", 1021L, 2021L, "31", 1021L, 41L)
                .row("22", 1022L, 2022L, "32", 1022L, 42L)
                .row("23", 1023L, 2023L, "33", 1023L, 43L)
                .row("24", 1024L, 2024L, "34", 1024L, 44L)
                .row("25", 1025L, 2025L, "35", 1025L, 45L)
                .row("26", 1026L, 2026L, "36", 1026L, 46L)
                .row("27", 1027L, 2027L, "37", 1027L, 47L)
                .row("28", 1028L, 2028L, "38", 1028L, 48L)
                .row("29", 1029L, 2029L, "39", 1029L, 49L)
                .row("30", 1030L, 2030L, null, null, null)
                .row("31", 1031L, 2031L, null, null, null)
                .row("32", 1032L, 2032L, null, null, null)
                .row("33", 1033L, 2033L, null, null, null)
                .row("34", 1034L, 2034L, null, null, null)
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
                .row(1L)
                .row(2L)
                .row(3L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(null, null)
                .row(null, null)
                .row(1L, 1L)
                .row(2L, 2L)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        JoinFilterFunction filterFunction = new TestJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii().equals("a")));

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

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
                probePages.getHashChannel(),
                true);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
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
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row(2L)
                .row(3L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(3L, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        JoinFilterFunction filterFunction = new TestJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii())));

        // build
        List<Type> buildTypes = ImmutableList.<Type>of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

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
                probePages.getHashChannel(),
                true);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperator, probeInput, expected, true, getHashChannels(probePages, buildPages));
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
                .row(1L)
                .row((Long) null)
                .row((Long) null)
                .row(1L)
                .row(2L);

        List<Type> probeTypes = ImmutableList.<Type>of(BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, hashChannels, probeTypes)
                .row(1L)
                .row(2L)
                .row((Long) null)
                .row(3L);

        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row(1L, 1L)
                .row(1L, 1L)
                .row(2L, 2L)
                .row(null, null)
                .row(3L, null)
                .build();

        assertProbeOuterJoin(probePages, buildPages, hashChannels, parallelBuild, expected);
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        JoinFilterFunction filterFunction = new TestJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii())));

        // build
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

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
                probePages.getHashChannel(),
                true);
        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
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
        buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of.*", dataProvider = "hashEnabledValues")
    public void testBigintMemoryLimit(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, TEST_SESSION, new DataSize(100, BYTE));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(1), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        buildHash(parallelBuild, taskContext, Ints.asList(1), buildPages, Optional.empty());
    }

    private void assertInnerJoin(RowPagesBuilder probePages, RowPagesBuilder buildPages, List<Integer> hashChannels, boolean parallelBuild, MaterializedResult expected)
    {
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, hashChannels, buildPages, Optional.empty());

        OperatorFactory joinOperatorFactory = LookupJoinOperators.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                hashChannels,
                probePages.getHashChannel(),
                false);

        Operator joinOperator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(true, true).addDriverContext());

        assertOperatorEquals(joinOperator, probePages.build(), expected, true, getHashChannels(probePages, buildPages));
    }

    private void assertProbeOuterJoin(RowPagesBuilder probePages, RowPagesBuilder buildPages, List<Integer> hashChannels, boolean parallelBuild, MaterializedResult expected)
    {
        LookupSourceSupplier lookupSourceSupplier = buildHash(parallelBuild, taskContext, hashChannels, buildPages, Optional.empty());

        OperatorFactory joinOperatorFactory = LookupJoinOperators.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceSupplier,
                probePages.getTypes(),
                hashChannels,
                probePages.getHashChannel(),
                false);

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

    private static LookupSourceSupplier buildHash(boolean parallelBuild, TaskContext taskContext, List<Integer> hashChannels, RowPagesBuilder buildPages, Optional<JoinFilterFunction> filterFunction)
    {
        if (parallelBuild) {
            LocalExchange localExchange = new LocalExchange(FIXED_HASH_DISTRIBUTION, PARTITION_COUNT, buildPages.getTypes(), hashChannels, buildPages.getHashChannel());
            LocalExchangeSinkFactory sinkFactory = localExchange.createSinkFactory();
            sinkFactory.noMoreSinkFactories();

            // collect input data into the partitioned exchange
            DriverContext collectDriverContext = taskContext.addPipelineContext(true, true).addDriverContext();
            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("values"), buildPages.getTypes(), buildPages.build());
            LocalExchangeSinkOperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperatorFactory(1, new PlanNodeId("sink"), sinkFactory, Function.identity());
            Driver driver = new Driver(collectDriverContext,
                    valuesOperatorFactory.createOperator(collectDriverContext),
                    sinkOperatorFactory.createOperator(collectDriverContext));
            valuesOperatorFactory.close();
            sinkOperatorFactory.close();

            while (!driver.isFinished()) {
                driver.process();
            }

            // build hash tables
            LocalExchangeSourceOperatorFactory sourceOperatorFactory = new LocalExchangeSourceOperatorFactory(0, new PlanNodeId("source"), localExchange);
            ParallelHashBuildOperatorFactory buildOperatorFactory = new ParallelHashBuildOperatorFactory(
                    1,
                    new PlanNodeId("build"),
                    buildPages.getTypes(),
                    ImmutableMap.of(),
                    hashChannels,
                    buildPages.getHashChannel(),
                    false,
                    filterFunction,
                    100,
                    PARTITION_COUNT);
            PipelineContext buildPipeline = taskContext.addPipelineContext(true, true);
            for (int i = 0; i < PARTITION_COUNT; i++) {
                DriverContext buildDriverContext = buildPipeline.addDriverContext();
                Driver buildDriver = new Driver(buildDriverContext,
                        sourceOperatorFactory.createOperator(buildDriverContext),
                        buildOperatorFactory.createOperator(buildDriverContext));

                while (!buildDriver.isFinished()) {
                    buildDriver.process();
                }
            }

            return buildOperatorFactory.getLookupSourceSupplier();
        }
        else {
            DriverContext driverContext = taskContext.addPipelineContext(true, true).addDriverContext();

            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("test"), buildPages.getTypes(), buildPages.build());
            HashBuilderOperatorFactory hashBuilderOperatorFactory = new HashBuilderOperatorFactory(
                    1,
                    new PlanNodeId("test"),
                    buildPages.getTypes(),
                    ImmutableMap.of(),
                    hashChannels,
                    buildPages.getHashChannel(),
                    false,
                    filterFunction,
                    100);

            Driver driver = new Driver(driverContext,
                    valuesOperatorFactory.createOperator(driverContext),
                    hashBuilderOperatorFactory.createOperator(driverContext));

            while (!driver.isFinished()) {
                driver.process();
            }
            return hashBuilderOperatorFactory.getLookupSourceSupplier();
        }
    }

    private static class TestJoinFilterFunction
            implements JoinFilterFunction
    {
        public interface Lambda
        {
            boolean filter(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks);
        }

        private final Lambda lambda;

        private TestJoinFilterFunction(Lambda lambda)
        {
            this.lambda = lambda;
        }

        @Override
        public boolean filter(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks)
        {
            return lambda.filter(leftPosition, leftBlocks, rightPosition, rightBlocks);
        }
    }
}
