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
package io.prestosql.operator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.DataSize;
import io.prestosql.ExceededMemoryLimitException;
import io.prestosql.RowPagesBuilder;
import io.prestosql.execution.Lifespan;
import io.prestosql.execution.TaskId;
import io.prestosql.execution.TaskStateMachine;
import io.prestosql.memory.context.LocalMemoryContext;
import io.prestosql.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import io.prestosql.operator.ValuesOperator.ValuesOperatorFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeFactory;
import io.prestosql.operator.exchange.LocalExchange.LocalExchangeSinkFactoryId;
import io.prestosql.operator.exchange.LocalExchangeSinkOperator.LocalExchangeSinkOperatorFactory;
import io.prestosql.operator.exchange.LocalExchangeSourceOperator.LocalExchangeSourceOperatorFactory;
import io.prestosql.operator.index.PageBuffer;
import io.prestosql.operator.index.PageBufferOperator.PageBufferOperatorFactory;
import io.prestosql.spi.Page;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.type.Type;
import io.prestosql.spiller.GenericPartitioningSpillerFactory;
import io.prestosql.spiller.PartitioningSpillerFactory;
import io.prestosql.spiller.SingleStreamSpiller;
import io.prestosql.spiller.SingleStreamSpillerFactory;
import io.prestosql.sql.gen.JoinFilterFunctionCompiler.JoinFilterFunctionFactory;
import io.prestosql.sql.planner.plan.PlanNodeId;
import io.prestosql.testing.MaterializedResult;
import io.prestosql.testing.TestingTaskContext;
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
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;

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
import static io.prestosql.RowPagesBuilder.rowPagesBuilder;
import static io.prestosql.SessionTestUtils.TEST_SESSION;
import static io.prestosql.operator.OperatorAssertion.assertOperatorEquals;
import static io.prestosql.operator.OperatorAssertion.dropChannel;
import static io.prestosql.operator.OperatorAssertion.without;
import static io.prestosql.operator.PipelineExecutionStrategy.UNGROUPED_EXECUTION;
import static io.prestosql.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.SystemPartitioningHandle.FIXED_HASH_DISTRIBUTION;
import static io.prestosql.testing.assertions.Assert.assertEquals;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.nCopies;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
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
    private static final LookupJoinOperators LOOKUP_JOIN_OPERATORS = new LookupJoinOperators();
    private static final SingleStreamSpillerFactory SINGLE_STREAM_SPILLER_FACTORY = new DummySpillerFactory();
    private static final PartitioningSpillerFactory PARTITIONING_SPILLER_FACTORY = new GenericPartitioningSpillerFactory(SINGLE_STREAM_SPILLER_FACTORY);

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
                new SynchronousQueue<Runnable>(),
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

    @DataProvider(name = "hashJoinTestValues")
    public static Object[][] hashJoinTestValuesProvider()
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

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT));
        List<Page> probeInput = probePages
                .addSequencePage(1000, 0, 1000, 2000)
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
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

        // build with 40 entries
        int entries = 40;
        RowPagesBuilder buildPages = rowPagesBuilder(true, Ints.asList(0), ImmutableList.of(BIGINT))
                .addSequencePage(entries, 42);
        BuildSideSetup buildSideSetup = setupBuildSide(true, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

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

        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);
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
        List<List<Object>> dictionaryProcessingValues = ImmutableList.of(ImmutableList.of(true), ImmutableList.of(false));
        return product(joinWithSpillParameters(true), dictionaryProcessingValues).stream()
                .map(List::toArray)
                .toArray(Object[][]::new);
    }

    @DataProvider
    public Object[][] joinWithFailingSpillValues()
    {
        List<List<Object>> dictionaryProcessingValues = ImmutableList.of(ImmutableList.of(true), ImmutableList.of(false));
        List<List<Object>> spillFailValues = Arrays.stream(WhenSpillFails.values())
                .map(ImmutableList::<Object>of)
                .collect(toList());
        return product(product(joinWithSpillParameters(false), spillFailValues), dictionaryProcessingValues).stream()
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
    public void testInnerJoinWithSpill(boolean probeHashEnabled, List<WhenSpill> whenSpill, boolean isDictionaryProcessingJoinEnabled)
            throws Exception
    {
        innerJoinWithSpill(probeHashEnabled, whenSpill, SINGLE_STREAM_SPILLER_FACTORY, PARTITIONING_SPILLER_FACTORY);
    }

    @Test(dataProvider = "joinWithFailingSpillValues")
    public void testInnerJoinWithFailingSpill(boolean probeHashEnabled, List<WhenSpill> whenSpill, WhenSpillFails whenSpillFails, boolean isDictionaryProcessingJoinEnabled)
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

        DriverContext joinDriverContext = taskContext.addPipelineContext(2, true, true, false).addDriverContext();

        // force a yield for every match in LookupJoinOperator, set called to true after first
        AtomicBoolean called = new AtomicBoolean(false);
        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction(
                (leftPosition, leftPage, rightPosition, rightPage) -> {
                    called.set(true);
                    joinDriverContext.getYieldSignal().forceYieldForTesting();
                    return true;
                });

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(ImmutableList.of(VARCHAR, BIGINT))
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 20, 200)
                .addSequencePage(4, 30, 300)
                .addSequencePage(4, 40, 400);

        BuildSideSetup buildSideSetup = setupBuildSide(true, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), true, buildSpillerFactory);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT))
                .row("20", 123_000L)
                .row("20", 123_000L)
                .pageBreak()
                .addSequencePage(20, 0, 123_000)
                .addSequencePage(10, 30, 123_000);
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactoryManager, probePages, joinSpillerFactory);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        List<Driver> buildDrivers = buildSideSetup.getBuildDrivers();
        int buildOperatorCount = buildDrivers.size();
        checkState(buildOperatorCount == whenSpill.size());
        LookupSourceFactory lookupSourceFactory = lookupSourceFactoryManager.getJoinBridge(Lifespan.taskWide());

        try (Operator joinOperator = joinOperatorFactory.createOperator(joinDriverContext)) {
            // build lookup source
            ListenableFuture<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
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

            ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(17, new PlanNodeId("values"), probePages.build());

            PageBuffer pageBuffer = new PageBuffer(10);
            PageBufferOperatorFactory pageBufferOperatorFactory = new PageBufferOperatorFactory(18, new PlanNodeId("pageBuffer"), pageBuffer);

            Driver joinDriver = Driver.createDriver(joinDriverContext,
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

            assertEqualsIgnoreOrder(getProperColumns(joinOperator, concat(probePages.getTypes(), buildPages.getTypes()), probePages, actualPages).getMaterializedRows(), expected.getMaterializedRows());
        }
        finally {
            joinOperatorFactory.noMoreOperators();
        }
    }

    private static void processRow(final Driver joinDriver, final TaskStateMachine taskStateMachine)
    {
        joinDriver.getDriverContext().getYieldSignal().setWithDelay(TimeUnit.SECONDS.toNanos(1), joinDriver.getDriverContext().getYieldExecutor());
        joinDriver.process();
        joinDriver.getDriverContext().getYieldSignal().reset();
        checkErrors(taskStateMachine);
    }

    private static void checkErrors(TaskStateMachine taskStateMachine)
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

    private static MaterializedResult getProperColumns(Operator joinOperator, List<Type> types, RowPagesBuilder probePages, List<Page> actualPages)
    {
        if (probePages.getHashChannel().isPresent()) {
            List<Integer> hashChannels = ImmutableList.of(probePages.getHashChannel().get());
            actualPages = dropChannel(actualPages, hashChannels);
            types = without(types, hashChannels);
        }
        return OperatorAssertion.toMaterializedResult(joinOperator.getOperatorContext().getSession(), types, actualPages);
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
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

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = innerJoinOperatorFactory(lookupSourceFactory, probePages, PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testProbeOuterJoin(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testProbeOuterJoinWithFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> BIGINT.getLong(rightPage.getBlock(1), rightPosition) >= 1025));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR, BIGINT, BIGINT);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .addSequencePage(15, 20, 1020, 2020)
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

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

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
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

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", "b")
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullProbeAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) -> VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii().equals("a")));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
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

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row(null, null)
                .row(null, null)
                .row("a", "a")
                .row("b", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullBuild(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullBuildAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii())));

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullOnBothSides(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row("a", "a")
                .row("a", "a")
                .row("b", "b")
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testOuterJoinWithNullOnBothSidesAndFilterFunction(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        InternalJoinFilterFunction filterFunction = new TestInternalJoinFilterFunction((
                (leftPosition, leftPage, rightPosition, rightPage) ->
                        ImmutableSet.of("a", "c").contains(VARCHAR.getSlice(rightPage.getBlock(0), rightPosition).toStringAscii())));

        // build factory
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR))
                .row("a")
                .row((String) null)
                .row((String) null)
                .row("a")
                .row("b");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.of(filterFunction), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = probeOuterJoinOperatorFactory(lookupSourceFactory, probePages);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildPages.getTypesWithoutHash()))
                .row("a", "a")
                .row("a", "a")
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();

        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(expectedExceptions = ExceededMemoryLimitException.class, expectedExceptionsMessageRegExp = "Query exceeded per-node user memory limit of.*", dataProvider = "testMemoryLimitProvider")
    public void testMemoryLimit(boolean parallelBuild, boolean buildHashEnabled)
    {
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION, new DataSize(100, BYTE));

        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(VARCHAR, BIGINT, BIGINT))
                .addSequencePage(10, 20, 30, 40);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = new LookupJoinOperators().innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row("test").build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertNull(outputPage);
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testLookupOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        OperatorFactory joinOperatorFactory = new LookupJoinOperators().lookupOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        // drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);
        Operator operator = joinOperatorFactory.createOperator(taskContext.addPipelineContext(0, true, true, false).addDriverContext());

        List<Page> pages = probePages.row("test").build();
        operator.addInput(pages.get(0));
        Page outputPage = operator.getOutput();
        assertNull(outputPage);
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testProbeOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = new LookupJoinOperators().probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", null)
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testFullOuterJoinWithEmptyLookupSource(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes);
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages
                .row("a")
                .row("b")
                .row((String) null)
                .row("c")
                .build();
        OperatorFactory joinOperatorFactory = new LookupJoinOperators().fullOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes))
                .row("a", null)
                .row("b", null)
                .row(null, null)
                .row("c", null)
                .build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
    }

    @Test(dataProvider = "hashJoinTestValues")
    public void testInnerJoinWithNonEmptyLookupSourceAndEmptyProbe(boolean parallelBuild, boolean probeHashEnabled, boolean buildHashEnabled)
    {
        TaskContext taskContext = createTaskContext();

        // build factory
        List<Type> buildTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder buildPages = rowPagesBuilder(buildHashEnabled, Ints.asList(0), buildTypes)
                .row("a")
                .row("b")
                .row((String) null)
                .row("c");
        BuildSideSetup buildSideSetup = setupBuildSide(parallelBuild, taskContext, Ints.asList(0), buildPages, Optional.empty(), false, SINGLE_STREAM_SPILLER_FACTORY);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = buildSideSetup.getLookupSourceFactoryManager();

        // probe factory
        List<Type> probeTypes = ImmutableList.of(VARCHAR);
        RowPagesBuilder probePages = rowPagesBuilder(probeHashEnabled, Ints.asList(0), probeTypes);
        List<Page> probeInput = probePages.build();
        OperatorFactory joinOperatorFactory = new LookupJoinOperators().innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);

        // build drivers and operators
        instantiateBuildDrivers(buildSideSetup, taskContext);
        buildLookupSource(buildSideSetup);

        // expected
        MaterializedResult expected = MaterializedResult.resultBuilder(taskContext.getSession(), concat(probeTypes, buildTypes)).build();
        assertOperatorEquals(joinOperatorFactory, taskContext.addPipelineContext(0, true, true, false).addDriverContext(), probeInput, expected, true, getHashChannels(probePages, buildPages));
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

    private OperatorFactory probeOuterJoinOperatorFactory(JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, RowPagesBuilder probePages)
    {
        return LOOKUP_JOIN_OPERATORS.probeOuterJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                PARTITIONING_SPILLER_FACTORY);
    }

    private OperatorFactory innerJoinOperatorFactory(JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, RowPagesBuilder probePages, PartitioningSpillerFactory partitioningSpillerFactory)
    {
        return LOOKUP_JOIN_OPERATORS.innerJoin(
                0,
                new PlanNodeId("test"),
                lookupSourceFactoryManager,
                probePages.getTypes(),
                Ints.asList(0),
                getHashChannelAsInt(probePages),
                Optional.empty(),
                OptionalInt.of(1),
                partitioningSpillerFactory);
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
                .map(function -> (session, addresses, pages) -> new StandardJoinFilterFunction(function, addresses, pages));

        int partitionCount = parallelBuild ? PARTITION_COUNT : 1;
        LocalExchangeFactory localExchangeFactory = new LocalExchangeFactory(
                FIXED_HASH_DISTRIBUTION,
                partitionCount,
                buildPages.getTypes(),
                hashChannels,
                buildPages.getHashChannel(),
                UNGROUPED_EXECUTION,
                new DataSize(32, DataSize.Unit.MEGABYTE));
        LocalExchangeSinkFactoryId localExchangeSinkFactoryId = localExchangeFactory.newSinkFactoryId();
        localExchangeFactory.noMoreSinkFactories();

        // collect input data into the partitioned exchange
        DriverContext collectDriverContext = taskContext.addPipelineContext(0, true, true, false).addDriverContext();
        ValuesOperatorFactory valuesOperatorFactory = new ValuesOperatorFactory(0, new PlanNodeId("values"), buildPages.build());
        LocalExchangeSinkOperatorFactory sinkOperatorFactory = new LocalExchangeSinkOperatorFactory(localExchangeFactory, 1, new PlanNodeId("sink"), localExchangeSinkFactoryId, Function.identity());
        Driver sourceDriver = Driver.createDriver(collectDriverContext,
                valuesOperatorFactory.createOperator(collectDriverContext),
                sinkOperatorFactory.createOperator(collectDriverContext));
        valuesOperatorFactory.noMoreOperators();
        sinkOperatorFactory.noMoreOperators();

        while (!sourceDriver.isFinished()) {
            sourceDriver.process();
        }

        // build side operator factories
        LocalExchangeSourceOperatorFactory sourceOperatorFactory = new LocalExchangeSourceOperatorFactory(0, new PlanNodeId("source"), localExchangeFactory);
        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                buildPages.getTypes(),
                rangeList(buildPages.getTypes().size()).stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                hashChannels.stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                partitionCount,
                requireNonNull(ImmutableMap.of(), "layout is null"),
                false));

        HashBuilderOperatorFactory buildOperatorFactory = new HashBuilderOperatorFactory(
                1,
                new PlanNodeId("build"),
                lookupSourceFactoryManager,
                rangeList(buildPages.getTypes().size()),
                hashChannels,
                buildPages.getHashChannel()
                        .map(OptionalInt::of).orElse(OptionalInt.empty()),
                filterFunctionFactory,
                Optional.empty(),
                ImmutableList.of(),
                100,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                singleStreamSpillerFactory);
        return new BuildSideSetup(lookupSourceFactoryManager, buildOperatorFactory, sourceOperatorFactory, partitionCount);
    }

    private void instantiateBuildDrivers(BuildSideSetup buildSideSetup, TaskContext taskContext)
    {
        PipelineContext buildPipeline = taskContext.addPipelineContext(1, true, true, false);
        List<Driver> buildDrivers = new ArrayList<>();
        List<HashBuilderOperator> buildOperators = new ArrayList<>();
        for (int i = 0; i < buildSideSetup.getPartitionCount(); i++) {
            DriverContext buildDriverContext = buildPipeline.addDriverContext();
            HashBuilderOperator buildOperator = buildSideSetup.getBuildOperatorFactory().createOperator(buildDriverContext);
            Driver driver = Driver.createDriver(
                    buildDriverContext,
                    buildSideSetup.getBuildSideSourceOperatorFactory().createOperator(buildDriverContext),
                    buildOperator);
            buildDrivers.add(driver);
            buildOperators.add(buildOperator);
        }

        buildSideSetup.setDriversAndOperators(buildDrivers, buildOperators);
    }

    private void buildLookupSource(BuildSideSetup buildSideSetup)
    {
        requireNonNull(buildSideSetup, "buildSideSetup is null");

        LookupSourceFactory lookupSourceFactory = buildSideSetup.getLookupSourceFactoryManager().getJoinBridge(Lifespan.taskWide());
        Future<LookupSourceProvider> lookupSourceProvider = lookupSourceFactory.createLookupSourceProvider();
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
        private final JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager;
        private final HashBuilderOperatorFactory buildOperatorFactory;
        private final LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory;
        private final int partitionCount;
        private List<Driver> buildDrivers;
        private List<HashBuilderOperator> buildOperators;

        BuildSideSetup(JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager, HashBuilderOperatorFactory buildOperatorFactory, LocalExchangeSourceOperatorFactory buildSideSourceOperatorFactory, int partitionCount)
        {
            this.lookupSourceFactoryManager = requireNonNull(lookupSourceFactoryManager, "lookupSourceFactoryManager is null");
            this.buildOperatorFactory = requireNonNull(buildOperatorFactory, "buildOperatorFactory is null");
            this.buildSideSourceOperatorFactory = buildSideSourceOperatorFactory;
            this.partitionCount = partitionCount;
        }

        void setDriversAndOperators(List<Driver> buildDrivers, List<HashBuilderOperator> buildOperators)
        {
            checkArgument(buildDrivers.size() == buildOperators.size());
            this.buildDrivers = ImmutableList.copyOf(buildDrivers);
            this.buildOperators = ImmutableList.copyOf(buildOperators);
        }

        JoinBridgeManager<PartitionedLookupSourceFactory> getLookupSourceFactoryManager()
        {
            return lookupSourceFactoryManager;
        }

        HashBuilderOperatorFactory getBuildOperatorFactory()
        {
            return buildOperatorFactory;
        }

        public LocalExchangeSourceOperatorFactory getBuildSideSourceOperatorFactory()
        {
            return buildSideSourceOperatorFactory;
        }

        public int getPartitionCount()
        {
            return partitionCount;
        }

        List<Driver> getBuildDrivers()
        {
            checkState(buildDrivers != null, "buildDrivers is not initialized yet");
            return buildDrivers;
        }

        List<HashBuilderOperator> getBuildOperators()
        {
            checkState(buildOperators != null, "buildDrivers is not initialized yet");
            return buildOperators;
        }
    }

    private static class TestInternalJoinFilterFunction
            implements InternalJoinFilterFunction
    {
        public interface Lambda
        {
            boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage);
        }

        private final Lambda lambda;

        private TestInternalJoinFilterFunction(Lambda lambda)
        {
            this.lambda = lambda;
        }

        @Override
        public boolean filter(int leftPosition, Page leftPage, int rightPosition, Page rightPage)
        {
            return lambda.filter(leftPosition, leftPage, rightPosition, rightPage);
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
