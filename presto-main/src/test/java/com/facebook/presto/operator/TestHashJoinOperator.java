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
import com.facebook.presto.operator.exchange.LocalExchange;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
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
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.google.common.collect.Iterables.concat;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;

@Test(singleThreaded = true)
public class TestHashJoinOperator
{
    private static final int PARTITION_COUNT = 4;
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators(new JoinProbeCompiler());

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
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty()
        );

        // expected
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty()
        );

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty()
        );

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty()
        );

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        // expected
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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testProbeOuterJoinWithFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> BIGINT.getLong(rightBlocks[1], rightPosition) >= 1025));

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii().equals("a")));

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii())));

        // build
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
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
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashEnabledValues")
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightBlocks[0], rightPosition).toStringAscii())));

        // build
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        LookupSourceFactory lookupSourceFactory = buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

        // probe
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                probePages.getHashChannel(),
                Optional.empty());

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypes()))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
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

    private static LookupSourceFactory buildHash(boolean parallelBuild, TaskContext taskContext, List<Integer> hashChannels, RowPagesBuilder buildPages, Optional<InternalJoinFilterFunction> filterFunction)
    {
        Optional<JoinFilterFunctionFactory> filterFunctionFactory = filterFunction
                .map(function -> ((session, addresses, channels) -> new StandardJoinFilterFunction(function, addresses, channels)));

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;
        LocalExchange localExchange = new LocalExchange(FIXED_HASH_DISTRIBUTION, partitionCount, buildPages.getTypes(), hashChannels, buildPages.getHashChannel());
        LocalExchangeSinkFactory sinkFactory = localExchange.createSinkFactory();
        sinkFactory.noMoreSinkFactories();

        // collect input data into the partitioned exchange
        DriverContext collectDriverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();
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
        HashBuilderOperatorFactory buildOperatorFactory = new HashBuilderOperatorFactory(
                1,
                new PlanNodeId("build"),
                buildPages.getTypes(),
                rangeList(buildPages.getTypes().size()),
                ImmutableMap.of(),
                hashChannels,
                buildPages.getHashChannel(),
                false,
                filterFunctionFactory,
                100,
                partitionCount,
                new PagesIndex.TestingFactory());
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true);

        Driver[] buildDrivers = new Driver[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            DriverContext buildDriverContext = buildPipeline.addDriverContext();
            buildDrivers[i] = new Driver(
                    buildDriverContext,
                    sourceOperatorFactory.createOperator(buildDriverContext),
                    buildOperatorFactory.createOperator(buildDriverContext));
        }

        while (!buildOperatorFactory.getLookupSourceFactory().createLookupSource().isDone()) {
            for (Driver buildDriver : buildDrivers) {
                buildDriver.process();
            }
        }

        return buildOperatorFactory.getLookupSourceFactory();
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private static class TestInternalJoinFilterFunction
            implements InternalJoinFilterFunction
    {
        public interface Lambda
        {
            boolean filter(int leftPosition, Block[] leftBlocks, int rightPosition, Block[] rightBlocks);
        }

        private final Lambda lambda;

        private TestInternalJoinFilterFunction(Lambda lambda)
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
