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
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.TaskStateMachine;
import com.facebook.presto.memory.LocalMemoryContext;
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.operator.ValuesOperator.ValuesOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchange;
import com.facebook.presto.operator.exchange.LocalExchange.LocalExchangeSinkFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import com.facebook.presto.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import com.facebook.presto.operator.index.PageBuffer;
import com.facebook.presto.operator.index.PageBufferOperator.PageBufferOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spiller.GenericPartitioningSpillerFactory;
import com.facebook.presto.spiller.PartitioningSpillerFactory;
import com.facebook.presto.spiller.SingleStreamSpiller;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import com.facebook.presto.sql.gen.JoinProbeCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorEquals;
import static com.facebook.presto.operator.OperatorAssertion.dropChannel;
import static com.facebook.presto.operator.OperatorAssertion.without;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static com.facebook.presto.testing.assertions.Assert.assertEquals;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterators.unmodifiableIterator;
import static com.google.common.util.concurrent.Futures.immediateFailedFuture;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertEqualsIgnoreOrder;
import static io.airlift.units.DataSize.Unit.BYTE;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.stream.Collectors.toList;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(singleThreaded = true)
public class TestHashJoinOperator
{
    private static final int PARTITION_COUNT = 4;
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators(new JoinProbeCompiler());
    private static final SingleStreamSpillerFactory SINGLE_STREAM_SPILLER_FACTORY = new DummySpillerFactory();
    private static final PartitioningSpillerFactory PARTITIONING_SPILLER_FACTORY = new GenericPartitioningSpillerFactory(SINGLE_STREAM_SPILLER_FACTORY);

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
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
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
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

    @Test
    public void testYield()
            throws Exception
    {
        // create a filter function that yields for every probe match
        // verify we will yield #match times totally

        TaskContext taskContext = createTaskContext();
        DriverContext driverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();

        // force a yield for every match
        AtomicInteger filterFunctionCalls = new AtomicInteger();
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> {
                    filterFunctionCalls.incrementAndGet();
                    driverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                }));

        // build with 40 entries
        int entries = 40;
        RowPagesBuilder buildPages = rowPagesBuilder(true, Ints.asList(0), ImmutableList.of(BIGINT))
                .addSequencePage(entries, 42);
        LookupSourceFactory lookupSourceFactory = buildHash(true, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction));

        // probe matching the above 40 entries
        RowPagesBuilder probePages = rowPagesBuilder(false, Ints.asList(0), ImmutableList.of(BIGINT));
        List<Page> probeInput = probePages.addSequencePage(100, 0).build();
        OperatorFactory joinOperatorFactory = LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        Operator operator = joinOperatorFactory.createOperator(driverContext);
        assertTrue(operator.needsInput());
        operator.addInput(probeInput.get(0));
        operator.finish();

        // we will yield 40 times due to filterFunction
        for (int i = 0; i < entries; i++) {
            driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
            filterFunctionCalls.set(0);
            assertNull(operator.getOutput());
            assertEquals(filterFunctionCalls.get(), 1, "Expected join to stop processing (yield) after calling filter function once");
            driverContext.getYieldSignal().reset();
        }
        // delayed yield is not going to prevent operator from producing a page now (yield won't be forced because filter function won't be called anymore)
        driverContext.getYieldSignal().setWithDelay(5 * SECONDS.toNanos(1), driverContext.getYieldExecutor());
        // expect output page to be produced within few calls to getOutput(), e.g. to facilitate spill
        Page output = null;
        for (int i = 0; output == null && i < 5; i++) {
            output = operator.getOutput();
        }
        assertNotNull(output);
        driverContext.getYieldSignal().reset();

        // make sure we have all 4 entries
        assertEquals(output.getPositionCount(), entries);
    }

    private enum WhenSpill
    {
        DURING_BUILD, AFTER_BUILD, DURING_USAGE, NEVER
    }

    private enum WhenSpillFails
    {
        SPILL_BUILD, SPILL_JOIN, UNSPILL_BUILD, UNSPILL_JOIN
    }

    @DataProvider
    public Object[][] joinWithSpillValues()
    {
        return joinWithSpillParameters(true).stream()
                .map(List::toArray)
                .toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] joinWithFailingSpillValues()
    {
        List<List<Object>> spillFailValues = Arrays.stream(WhenSpillFails.values())
                .map(ImmutableList::<Object>of)
                .collect(toList());
        return product(joinWithSpillParameters(false), spillFailValues).stream()
                .map(List::toArray)
                .toArray(Object[][]::new);
    }

    private static List<List<Object>> joinWithSpillParameters(boolean allowNoSpill)
    {
        List<List<Object>> result = new ArrayList<>();
        for (boolean probeHashEnabled : ImmutableList.of(false, true)) {
            for (WhenSpill whenSpill : WhenSpill.values()) {
                // spill all
                if (allowNoSpill || whenSpill != WhenSpill.NEVER) {
                    result.add(ImmutableList.of(probeHashEnabled, nCopies(PARTITION_COUNT, whenSpill)));
                }

                if (whenSpill != WhenSpill.NEVER) {
                    // spill one
                    result.add(ImmutableList.of(probeHashEnabled, concat(singletonList(whenSpill), nCopies(PARTITION_COUNT - 1, WhenSpill.NEVER))));
                }
            }

            result.add(ImmutableList.of(probeHashEnabled, concat(asList(WhenSpill.DURING_BUILD, WhenSpill.AFTER_BUILD), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER))));
            result.add(ImmutableList.of(probeHashEnabled, concat(asList(WhenSpill.DURING_BUILD, WhenSpill.DURING_USAGE), nCopies(PARTITION_COUNT - 2, WhenSpill.NEVER))));
        }
        return result;
    }

    @Test(dataProvider = "joinWithSpillValues")
    public void testInnerJoinWithSpill(boolean probeHashEnabled, List<WhenSpill> whenSpill)
            throws Exception
    {
        innerJoinWithSpill(probeHashEnabled, whenSpill, SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
    }

    @Test(dataProvider = "joinWithFailingSpillValues")
    public void testInnerJoinWithFailingSpill(boolean probeHashEnabled, List<WhenSpill> whenSpill, WhenSpillFails whenSpillFails)
            throws Throwable
    {
        DummySpillerFactory buildSpillerFactory = new DummySpillerFactory();
        DummySpillerFactory joinSpillerFactory = new DummySpillerFactory();
        PartitioningSpillerFactory partitioningSpillerFactory = new GenericPartitioningSpillerFactory(joinSpillerFactory);

        String expectedMessage;
        switch (whenSpillFails) {
            case SPILL_BUILD:
                buildSpillerFactory.failSpill();
                expectedMessage = "Spill failed";
                break;
            case SPILL_JOIN:
                joinSpillerFactory.failSpill();
                expectedMessage = "Spill failed";
                break;
            case UNSPILL_BUILD:
                buildSpillerFactory.failUnspill();
                expectedMessage = "Unspill failed";
                break;
            case UNSPILL_JOIN:
                joinSpillerFactory.failUnspill();
                expectedMessage = "Unspill failed";
                break;
            default:
                throw new IllegalArgumentException(format("Unsupported option: %s", whenSpillFails));
        }
        try {
            innerJoinWithSpill(probeHashEnabled, whenSpill, buildSpillerFactory, partitioningSpillerFactory);
            fail("Exception not thrown");
        }
        catch (RuntimeException exception) {
            assertEquals(exception.getMessage(), expectedMessage);
        }
    }

    private void innerJoinWithSpill(boolean probeHashEnabled, List<WhenSpill> whenSpill, SingleStreamSpillerFactory buildSpillerFactory, PartitioningSpillerFactory joinSpillerFactory)
            throws Exception
    {
        TaskStateMachine taskStateMachine = new TaskStateMachine(new TaskId("query", 0, 0), executor);
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, taskStateMachine);

        DriverContext joinDriverContext = taskContext.addPipelineContext(2, true, true).addDriverContext();

        // force a yield for every match in LookupJoinOperator, set called to true after first
        AtomicBoolean called = new AtomicBoolean(false);
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftBlocks, rightPosition, rightBlocks) -> {
                    called.set(true);
                    joinDriverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                });

        // build side
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT))
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 30, 300)
                .addSequencePage(4, 40, 400);

        BuildSideSetup buildSideSetup = setupBuildSide(true, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), true, buildSpillerFactory);
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();
        int buildOperatorCount = buildDrivers.size();
        checkState(buildOperatorCount == whenSpill.size());
        LookupSourceFactory lookupSourceFactory = buildSideSetup.getLookupSourceFactory();

        // probe side
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 123_000L)
                .row("20", 123_000L)
                .pageBreak()
                .addSequencePage(20, 0, 123_000)
                .addSequencePage(10, 30, 123_000);

        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, joinSpillerFactory);
        try (Operator joinOperator = joinOperatorFactory.createOperator(joinDriverContext)) {
            // build lookup source
            ListenableFuture<LookupSourceProvider> lookupSourceProvider = buildSideSetup.getLookupSourceFactory().createLookupSourceProvider();
            List<Boolean> revoked = new ArrayList<>(nCopies(buildOperatorCount, false));
            while (!lookupSourceProvider.isDone()) {
                for (int i = 0; i < buildOperatorCount; i++) {
                    checkErrors(taskStateMachine);
                    buildDrivers.get(i).process();
                    HashBuilderOperator buildOperator = buildSideSetup.getBuildOperators().get(i);
                    if (whenSpill.get(i) == WhenSpill.DURING_BUILD && buildOperator.getOperatorContext().getReservedRevocableBytes() > 0) {
                        checkState(!lookupSourceProvider.isDone(), "Too late, LookupSource already done");
                        revokeMemory(buildOperator);
                        revoked.set(i, true);
                    }
                }
            }
            getFutureValue(lookupSourceProvider).close();
            assertEquals(revoked, whenSpill.stream().map(WhenSpill.DURING_BUILD::equals).collect(toImmutableList()), "Some operators not spilled before LookupSource built");

            for (int i = 0; i < buildOperatorCount; i++) {
                if (whenSpill.get(i) == WhenSpill.AFTER_BUILD) {
                    revokeMemory(buildSideSetup.getBuildOperators().get(i));
                }
            }

            for (Driver buildDriver : buildDrivers) {
                runDriverInThread(executor, buildDriver);
            }

            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(17, new PlanNodeId("values"), probePages.getTypes(), probePages.build());

            PageBuffer pageBuffer = new PageBuffer(10);
            PageBufferOperatorFactory pageBufferOperatorFactory = new PageBufferOperatorFactory(18, new PlanNodeId("pageBuffer"), pageBuffer);

            Driver joinDriver = new Driver(
                    joinDriverContext,
                    valuesOperatorFactory.createOperator(joinDriverContext),
                    joinOperator,
                    pageBufferOperatorFactory.createOperator(joinDriverContext));

            while (!called.get()) { // process first row of first page of LookupJoinOperator
                processRow(joinDriver, taskStateMachine);
            }

            for (int i = 0; i < buildOperatorCount; i++) {
                if (whenSpill.get(i) == WhenSpill.DURING_USAGE) {
                    triggerMemoryRevokingAndWait(buildSideSetup.getBuildOperators().get(i), taskStateMachine);
                }
            }

            // process remaining LookupJoinOperator pages
            while (!joinDriver.isFinished()) {
                checkErrors(taskStateMachine);
                processRow(joinDriver, taskStateMachine);
            }
            checkErrors(taskStateMachine);

            List<Page> actualPages = getPages(pageBuffer);

            MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probePages.getTypesWithoutHash(), buildPages.getTypesWithoutHash()))
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("20", 123_000L, "20", 200L)
                    .row("30", 123_000L, "30", 300L)
                    .row("31", 123_001L, "31", 301L)
                    .row("32", 123_002L, "32", 302L)
                    .row("33", 123_003L, "33", 303L)
                    .build();

            assertEqualsIgnoreOrder(getProperColumns(joinOperator, probePages, actualPages).getMaterializedRows(), expected.getMaterializedRows());
        }
        finally {
            joinOperatorFactory.noMoreOperators();
        }
    }

    private static void processRow(final Driver joinDriver, final TaskStateMachine taskStateMachine)
            throws Exception
    {
        joinDriver.getDriverContext().getYieldSignal().setWithDelay(TimeUnit.SECONDS.toNanos(1), joinDriver.getDriverContext().getYieldExecutor());
        joinDriver.process();
        joinDriver.getDriverContext().getYieldSignal().reset();
        checkErrors(taskStateMachine);
    }

    private static void checkErrors(TaskStateMachine taskStateMachine)
            throws Exception
    {
        if (taskStateMachine.getFailureCauses().size() > 0) {
            Throwable exception = requireNonNull(taskStateMachine.getFailureCauses().peek());
            throw new RuntimeException(exception.getMessage(), exception);
        }
    }

    private static void revokeMemory(HashBuilderOperator operator)
    {
        getFutureValue(operator.startMemoryRevoke());
        operator.finishMemoryRevoke();
        checkState(operator.getState() == HashBuilderOperator.State.SPILLING_INPUT || operator.getState() == HashBuilderOperator.State.INPUT_SPILLED);
    }

    private static void triggerMemoryRevokingAndWait(HashBuilderOperator operator, TaskStateMachine taskStateMachine)
            throws Exception
    {
        // When there is background thread running Driver, we must delegate memory revoking to that thread
        operator.getOperatorContext().requestMemoryRevoking();
        while (operator.getOperatorContext().isMemoryRevokingRequested()) {
            checkErrors(taskStateMachine);
            Thread.sleep(10);
        }
        checkErrors(taskStateMachine);
        checkState(operator.getState() == HashBuilderOperator.State.SPILLING_INPUT || operator.getState() == HashBuilderOperator.State.INPUT_SPILLED);
    }

    private static List<Page> getPages(PageBuffer pageBuffer)
    {
        List<Page> result = new ArrayList<>();

        Page page = pageBuffer.poll();
        while (page != null) {
            result.add(page);
            page = pageBuffer.poll();
        }
        return result;
    }

    private static MaterializedResult getProperColumns(Operator joinOperator, RowPagesBuilder probePages, List<Page> actualPages)
    {
        List<Type> types = joinOperator.getTypes();
        if (probePages.getHashChannel().isPresent()) {
            List<Integer> hashChannels = ImmutableList.of(probePages.getHashChannel().get());
            actualPages = dropChannel(actualPages, hashChannels);
            types = without(types, hashChannels);
        }
        return OperatorAssertion.toMaterializedResult(joinOperator.getOperatorContext().getSession(), types, actualPages);
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
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
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
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

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
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
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
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded local memory limit of.*", dataProvider = "testMemoryLimitProvider")
    public void testMemoryLimit(boolean parallelBuild, boolean buildHashEnabled)
            throws Exception
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(100, BYTE));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        buildHash(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty());
    }

    @DataProvider
    public static Object[][] testMemoryLimitProvider()
    {
        return new Object[][] {
                {true, true},
                {true, false},
                {false, true},
                {false, false}};
    }

    private TaskContext createTaskContext()
    {
        return TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);
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

    private OperatorFactory probeOuterJoinOperatorFactory(LookupSourceFactory lookupSourceFactory, RowPagesBuilder probePages)
    {
        return LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);
    }

    private OperatorFactory innerJoinOperatorFactory(LookupSourceFactory lookupSourceFactory, RowPagesBuilder probePages, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactory,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                partitioningSpillerFactory);
    }

    private LookupSourceFactory buildHash(
            boolean parallelBuild,
            TaskContext taskContext,
            List<Integer> hashChannels,
            RowPagesBuilder buildPages,
            Optional<InternalJoinFilterFunction> filterFunction)
    {
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, hashChannels, buildPages, filterFunction, false, SINGLE_STREAM_SPILLER_FACTORY);
        buildLookupSource(buildSideSetup);
        return buildSideSetup.getLookupSourceFactory();
    }

    private BuildSideSetup setupBuildSide(
            boolean parallelBuild,
            TaskContext taskContext,
            List<Integer> hashChannels,
            RowPagesBuilder buildPages,
            Optional<InternalJoinFilterFunction> filterFunction,
            boolean spillEnabled,
            SingleStreamSpillerFactory singleStreamSpillerFactory)
    {
        Optional<JoinFilterFunctionFactory> filterFunctionFactory = filterFunction
                .map(function -> (session, addresses, channels) -> new StandardJoinFilterFunction(function, addresses, channels));

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;
        LocalExchange localExchange = new LocalExchange(FIXED_HASH_DISTRIBUTION, partitionCount, buildPages.getTypes(), hashChannels, buildPages.getHashChannel());
        LocalExchangeSinkFactory sinkFactory = localExchange.createSinkFactory();
        sinkFactory.noMoreSinkFactories();

        // collect input data into the partitioned exchange
        DriverContext collectDriverContext = taskContext.addPipelineContext(0, true, true).addDriverContext();
        ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("values"), buildPages.getTypes(), buildPages.build());
        LocalExchangeSinkOperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperatorFactory(1, new PlanNodeId("sink"), sinkFactory, Function.identity());
        Driver sourceDriver = new Driver(collectDriverContext,
                valuesOperatorFactory.createOperator(collectDriverContext),
                sinkOperatorFactory.createOperator(collectDriverContext));
        valuesOperatorFactory.noMoreOperators();
        sinkOperatorFactory.noMoreOperators();

        while (!sourceDriver.isFinished()) {
            sourceDriver.process();
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
                buildPages.getHashChannel()
                        .map(OptionalInt::of).orElse(OptionalInt.empty()),
                false,
                filterFunctionFactory,
                Optional.empty(),
                ImmutableList.of(),
                100,
                partitionCount,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                singleStreamSpillerFactory);
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true);

        List<Driver> buildDrivers = new ArrayList<>();
        List<HashBuilderOperator> buildOperators = new ArrayList<>();
        for (int i = 0; i < partitionCount; i++) {
            DriverContext buildDriverContext = buildPipeline.addDriverContext();
            HashBuilderOperator buildOperator = buildOperatorFactory.createOperator(buildDriverContext);
            Driver driver = new Driver(
                    buildDriverContext,
                    sourceOperatorFactory.createOperator(buildDriverContext),
                    buildOperator);
            buildDrivers.add(driver);
            buildOperators.add(buildOperator);
        }

        return new BuildSideSetup(buildOperatorFactory.getLookupSourceFactory(), buildDrivers, buildOperators);
    }

    private void buildLookupSource(BuildSideSetup buildSideSetup)
    {
        requireNonNull(buildSideSetup, "buildSideSetup is null");

        Future<LookupSourceProvider> lookupSourceProvider = buildSideSetup.getLookupSourceFactory().createLookupSourceProvider();
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();

        while (!lookupSourceProvider.isDone()) {
            for (Driver buildDriver : buildDrivers) {
                buildDriver.process();
            }
        }
        getFutureValue(lookupSourceProvider).close();

        for (Driver buildDriver : buildDrivers) {
            runDriverInThread(executor, buildDriver);
        }
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

    private static OptionalInt getHashChannelAsInt(RowPagesBuilder probePages)
    {
        return probePages.getHashChannel()
                .map(OptionalInt::of).orElse(OptionalInt.empty());
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }

    private static <T> List<List<T>> product(List<List<T>> left, List<List<T>> right)
    {
        List<List<T>> result = new ArrayList<>();
        for (List<T> l : left) {
            for (List<T> r : right) {
                result.add(concat(l, r));
            }
        }
        return result;
    }

    private static <T> List<T> concat(List<T> initialElements, List<T> moreElements)
    {
        return ImmutableList.copyOf(Iterables.concat(initialElements, moreElements));
    }

    private static class BuildSideSetup
    {
        private final LookupSourceFactory lookupSourceFactory;
        private final List<Driver> buildDrivers;
        private final List<HashBuilderOperator> buildOperators;

        BuildSideSetup(LookupSourceFactory lookupSourceFactory, List<Driver> buildDrivers, List<HashBuilderOperator> buildOperators)
        {
            checkArgument(buildDrivers.size() == buildOperators.size());
            this.lookupSourceFactory = requireNonNull(lookupSourceFactory, "lookupSourceFactory is null");
            this.buildDrivers = ImmutableList.copyOf(buildDrivers);
            this.buildOperators = ImmutableList.copyOf(buildOperators);
        }

        LookupSourceFactory getLookupSourceFactory()
        {
            return lookupSourceFactory;
        }

        List<Driver> getBuildDrivers()
        {
            return buildDrivers;
        }

        List<HashBuilderOperator> getBuildOperators()
        {
            return buildOperators;
        }
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

    private static class DummySpillerFactory
            implements SingleStreamSpillerFactory
    {
        private volatile boolean failSpill;
        private volatile boolean failUnspill;

        void failSpill()
        {
            failSpill = true;
        }

        void failUnspill()
        {
            failUnspill = true;
        }

        @Override
        public SingleStreamSpiller create(List<Type> types, SpillContext spillContext, LocalMemoryContext memoryContext)
        {
            return new SingleStreamSpiller()
            {
                private boolean writing = true;
                private final List<Page> spills = new ArrayList<>();

                @Override
                public ListenableFuture<?> spill(Iterator<Page> pageIterator)
                {
                    checkState(writing, "writing already finished");
                    if (failSpill) {
                        return immediateFailedFuture(new PrestoException(GENERIC_INTERNAL_ERROR, "Spill failed"));
                    }
                    Iterators.addAll(spills, pageIterator);
                    return immediateFuture(null);
                }

                @Override
                public Iterator<Page> getSpilledPages()
                {
                    if (failUnspill) {
                        throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unspill failed");
                    }
                    writing = false;
                    return unmodifiableIterator(spills.iterator());
                }

                @Override
                public long getSpilledPagesInMemorySize()
                {
                    return spills.stream()
                            .mapToLong(Page::getSizeInBytes)
                            .sum();
                }

                @Override
                public ListenableFuture<List<Page>> getAllSpilledPages()
                {
                    if (failUnspill) {
                        return immediateFailedFuture(new PrestoException(GENERIC_INTERNAL_ERROR, "Unspill failed"));
                    }
                    writing = false;
                    return immediateFuture(ImmutableList.copyOf(spills));
                }

                @Override
                public void close()
                {
                    writing = false;
                }
            };
        }
    }
}
