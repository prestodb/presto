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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.LongArrayBlock;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.HashAggregationOperator.HashAggregationOperatorFactory;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.plan.AggregationNode.Step;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.gen.JoinCompiler;
import com.facebook.presto.testing.MaterializedResult;
import com.facebook.presto.testing.TestingTaskContext;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;

import static com.facebook.airlift.concurrent.Threads.daemonThreadsNamed;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.operator.OperatorAssertion.assertPagesEqualIgnoreOrder;
import static com.facebook.presto.operator.OperatorAssertion.toPages;
import static com.facebook.presto.testing.MaterializedResult.resultBuilder;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static io.airlift.units.DataSize.succinctBytes;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestHashAggregationOperatorInSegmentedAggregationMode
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    private static final InternalAggregationFunction COUNT = FUNCTION_AND_TYPE_MANAGER.getAggregateFunctionImplementation(
            FUNCTION_AND_TYPE_MANAGER.lookupFunction("count", ImmutableList.of()));

    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private JoinCompiler joinCompiler = new JoinCompiler(MetadataManager.createTestMetadataManager(), new FeaturesConfig());

    private HashAggregationOperatorFactory operatorFactory = new HashAggregationOperatorFactory(
            0,
            new PlanNodeId("test"),
            ImmutableList.of(BIGINT, BIGINT),
            ImmutableList.of(0, 1),
            ImmutableList.of(0),
            ImmutableList.of(),
            Step.SINGLE,
            false,
            ImmutableList.of(COUNT.bind(ImmutableList.of(2), Optional.empty())),
            Optional.empty(),
            Optional.empty(),
            4,
            Optional.of(new DataSize(16, MEGABYTE)),
            false,
            new DataSize(16, MEGABYTE),
            new DataSize(16, MEGABYTE),
            new DummySpillerFactory(),
            joinCompiler,
            false);

    @BeforeMethod
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-executor-%s"));
        scheduledExecutor = newScheduledThreadPool(2, daemonThreadsNamed("test-scheduledExecutor-%s"));
    }

    @AfterMethod
    public void tearDown()
    {
        executor.shutdownNow();
        scheduledExecutor.shutdownNow();
    }

    @Test
    public void testSegmentedAggregationSinglePage()
    {
        int numberOfRows = 10;
        Block sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 2, 2, 2, 3, 3, 3, 3});
        Block groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 2, 1, 2, 2, 1, 2, 1});
        Block countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5, 6, 7, 8, 9, 10});
        Page inputPage = new Page(sortedBlock, groupingBlock, countBlock);

        DriverContext driverContext = createDriverContext();
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT);
        expectedBuilder.row(1L, 1L, 3L);
        expectedBuilder.row(2L, 1L, 1L);
        expectedBuilder.row(2L, 2L, 2L);
        expectedBuilder.row(3L, 1L, 2L);
        expectedBuilder.row(3L, 2L, 2L);

        MaterializedResult expected = expectedBuilder.build();
        List<Page> outputPages = toPages(operatorFactory, driverContext, ImmutableList.of(inputPage));
        assertEquals(outputPages.size(), 2);
        assertPagesEqualIgnoreOrder(driverContext, outputPages, expected, true, Optional.empty());
    }

    @Test
    public void testSegmentedAggregationSingleSegment()
    {
        int numberOfRows = 5;

        Block sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 1, 1});
        Block groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        Block countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage1 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 1, 1});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage2 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 1, 1});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage3 = new Page(sortedBlock, groupingBlock, countBlock);

        DriverContext driverContext = createDriverContext();
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT);
        expectedBuilder.row(1L, 1L, 9L);
        expectedBuilder.row(1L, 2L, 6L);

        MaterializedResult expected = expectedBuilder.build();
        List<Page> outputPages = toPages(operatorFactory, driverContext, ImmutableList.of(inputPage1, inputPage2, inputPage3));
        assertEquals(outputPages.size(), 1);
        assertPagesEqualIgnoreOrder(driverContext, outputPages, expected, true, Optional.empty());
    }

    @Test
    public void testSegmentedAggregationMultiplePages()
    {
        int numberOfRows = 5;

        Block sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 1, 1, 2, 2});
        Block groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        Block countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage1 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{2, 2, 2, 2, 2});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage2 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{2, 2, 3, 3, 4});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage3 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{5, 5, 5, 5, 5});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage4 = new Page(sortedBlock, groupingBlock, countBlock);

        sortedBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{5, 6, 7, 8, 8});
        groupingBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 1, 2, 1});
        countBlock = new LongArrayBlock(numberOfRows, Optional.of(new boolean[numberOfRows]), new long[]{1, 2, 3, 4, 5});
        Page inputPage5 = new Page(sortedBlock, groupingBlock, countBlock);

        DriverContext driverContext = createDriverContext();
        MaterializedResult.Builder expectedBuilder = resultBuilder(driverContext.getSession(), BIGINT, BIGINT, BIGINT);
        expectedBuilder.row(1L, 1L, 2L);
        expectedBuilder.row(1L, 2L, 1L);
        expectedBuilder.row(2L, 1L, 5L);
        expectedBuilder.row(2L, 2L, 4L);
        expectedBuilder.row(3L, 1L, 1L);
        expectedBuilder.row(3L, 2L, 1L);
        expectedBuilder.row(4L, 1L, 1L);
        expectedBuilder.row(5L, 1L, 4L);
        expectedBuilder.row(5L, 2L, 2L);
        expectedBuilder.row(6L, 2L, 1L);
        expectedBuilder.row(7L, 1L, 1L);
        expectedBuilder.row(8L, 1L, 1L);
        expectedBuilder.row(8L, 2L, 1L);

        MaterializedResult expected = expectedBuilder.build();
        List<Page> outputPages = toPages(operatorFactory, driverContext, ImmutableList.of(inputPage1, inputPage2, inputPage3, inputPage4, inputPage5));
        // segment 1: [1] | segment 2: [2 - 3] | segment 3: [4] | segment 4: [5 - 7] | segment 5: [8]
        assertEquals(outputPages.size(), 5);
        assertPagesEqualIgnoreOrder(driverContext, outputPages, expected, true, Optional.empty());
    }

    private DriverContext createDriverContext()
    {
        return createDriverContext(Integer.MAX_VALUE);
    }

    private DriverContext createDriverContext(long memoryLimit)
    {
        return TestingTaskContext.builder(executor, scheduledExecutor, TEST_SESSION)
                .setMemoryPoolSize(succinctBytes(memoryLimit))
                .build()
                .addPipelineContext(0, true, true, false)
                .addDriverContext();
    }
}
