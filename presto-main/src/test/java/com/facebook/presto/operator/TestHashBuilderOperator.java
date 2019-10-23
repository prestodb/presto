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
import com.facebook.presto.operator.HashBuilderOperator.HashBuilderOperatorFactory;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spiller.SingleStreamSpillerFactory;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Ints;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.IntStream;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.HashBuilderOperator.State.CONSUMING_INPUT;
import static com.facebook.presto.operator.HashBuilderOperator.State.LOOKUP_SOURCE_BUILT;
import static com.facebook.presto.operator.TestHashJoinOperator.DummySpillerFactory;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestHashBuilderOperator
{
    private static final SingleStreamSpillerFactory SINGLE_STREAM_SPILLER_FACTORY = new DummySpillerFactory();
    private static final int NUM_PAGES = 1000;
    private static final int PAGE_SIZE = 1024;

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
            throws Exception
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
        assertTrue(executor.awaitTermination(10, SECONDS));
        assertTrue(scheduledExecutor.awaitTermination(10, SECONDS));
    }

    @DataProvider(name = "hashBuildTestValues")
    public static Object[][] hashJoinTestValuesProvider()
    {
        return new Object[][] {
                {true, true},
                {false, true},
                {true, false},
                {false, false},
        };
    }

    @Test(dataProvider = "hashBuildTestValues")
    public void testMemoryTracking(boolean buildHashEnabled, boolean spillEnabled)
    {
        // Build the resources you need
        RowPagesBuilder pagesBuilder = rowPagesBuilder(buildHashEnabled, Ints.asList(0), ImmutableList.of(BIGINT, BIGINT, BIGINT));

        int totalRows = NUM_PAGES * PAGE_SIZE;
        List<Integer> someNumbers = rangeList(totalRows + 1);
        TaskContext taskContext = TestingTaskContext.createTaskContext(executor, scheduledExecutor, TEST_SESSION);

        for (int i = 0; i < NUM_PAGES; i++) {
            pagesBuilder.addSequencePage(PAGE_SIZE, 1, someNumbers.get(i), someNumbers.get((i * i) % totalRows));
        }
        List<Page> inputPages = pagesBuilder.build();

        HashBuilderOperator buildOperator = createTestOperator(pagesBuilder, spillEnabled, taskContext);
        assertEquals(buildOperator.getState(), CONSUMING_INPUT);
        inputPages.stream().forEach(buildOperator::addInput);

        // Check our memory usage before we build
        long estimatedFootprintBefore;
        if (spillEnabled) {
            estimatedFootprintBefore = taskContext.getTaskMemoryContext().getRevocableMemory();
        }
        else {
            estimatedFootprintBefore = taskContext.getMemoryReservation().toBytes();
        }

        // Build
        buildOperator.finish();

        // Check our memory usage after we build
        long estimatedFootprintAfter;
        if (spillEnabled) {
            estimatedFootprintAfter = taskContext.getTaskMemoryContext().getRevocableMemory();
        }
        else {
            estimatedFootprintAfter = taskContext.getMemoryReservation().toBytes();
        }

        // Are we using the memory we should be using?
        assertEquals(buildOperator.getState(), LOOKUP_SOURCE_BUILT);
        if (buildHashEnabled) {
            assertEquals(estimatedFootprintBefore, 46147728);
            assertEquals(estimatedFootprintAfter, 59640024);
        }
        else {
            assertEquals(estimatedFootprintBefore, 37887616);
            assertEquals(estimatedFootprintAfter, 51384016);
        }
    }

    private HashBuilderOperator createTestOperator(RowPagesBuilder buildPages, boolean spillEnabled, TaskContext taskContext)
    {
        List<Integer> hashChannels = Ints.asList(0);

        JoinBridgeManager<PartitionedLookupSourceFactory> lookupSourceFactoryManager = JoinBridgeManager.lookupAllAtOnce(new PartitionedLookupSourceFactory(
                buildPages.getTypes(),
                rangeList(buildPages.getTypes().size()).stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                hashChannels.stream()
                        .map(buildPages.getTypes()::get)
                        .collect(toImmutableList()),
                1,
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
                Optional.empty(),
                Optional.empty(),
                ImmutableList.of(),
                100,
                new PagesIndex.TestingFactory(false),
                spillEnabled,
                SINGLE_STREAM_SPILLER_FACTORY);

        PipelineContext buildPipeline = taskContext.addPipelineContext(0, true, true, false);
        DriverContext buildDriverContext = buildPipeline.addDriverContext();
        HashBuilderOperator buildOperator = buildOperatorFactory.createOperator(buildDriverContext);
        return buildOperator;
    }

    private static List<Integer> rangeList(int endExclusive)
    {
        return IntStream.range(0, endExclusive)
                .boxed()
                .collect(toImmutableList());
    }
}
