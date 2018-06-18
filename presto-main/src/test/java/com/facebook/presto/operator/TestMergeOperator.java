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

import com.facebook.presto.execution.buffer.PagesSerdeFactory;
import com.facebook.presto.execution.buffer.TestingPagesSerdeFactory;
import com.facebook.presto.metadata.RemoteTransactionHandle;
import com.facebook.presto.metadata.Split;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.SortOrder;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.gen.OrderingCompiler;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.testing.TestingHttpClient;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.RowPagesBuilder.rowPagesBuilder;
import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorIsBlocked;
import static com.facebook.presto.operator.OperatorAssertion.assertOperatorIsUnblocked;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.spi.block.SortOrder.ASC_NULLS_FIRST;
import static com.facebook.presto.spi.block.SortOrder.DESC_NULLS_FIRST;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.testing.TestingTaskContext.createTaskContext;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestMergeOperator
{
    private static final String TASK_1_ID = "task1";
    private static final String TASK_2_ID = "task2";
    private static final String TASK_3_ID = "task3";

    private AtomicInteger operatorId = new AtomicInteger();

    private ScheduledExecutorService executor;
    private PagesSerdeFactory serdeFactory;
    private HttpClient httpClient;
    private ExchangeClientFactory exchangeClientFactory;
    private OrderingCompiler orderingCompiler;

    private LoadingCache<String, TestingTaskBuffer> taskBuffers;

    @BeforeMethod
    public void setUp()
    {
        executor = newSingleThreadScheduledExecutor(daemonThreadsNamed("test-merge-operator-%s"));
        serdeFactory = new TestingPagesSerdeFactory();

        taskBuffers = CacheBuilder.newBuilder().build(CacheLoader.from(TestingTaskBuffer::new));
        httpClient = new TestingHttpClient(new TestingExchangeHttpClientHandler(taskBuffers), executor);
        exchangeClientFactory = new ExchangeClientFactory(new ExchangeClientConfig(), httpClient, executor);
        orderingCompiler = new OrderingCompiler();
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown()
    {
        serdeFactory = null;
        orderingCompiler = null;

        httpClient.close();
        httpClient = null;

        executor.shutdownNow();
        executor = null;

        exchangeClientFactory.stop();
        exchangeClientFactory = null;
    }

    @Test
    public void testSingleStream()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT);

        MergeOperator operator = createMergeOperator(types, ImmutableList.of(1), ImmutableList.of(0, 1), ImmutableList.of(ASC_NULLS_FIRST, ASC_NULLS_FIRST));
        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        operator.addSplit(createRemoteSplit(TASK_1_ID));
        assertFalse(operator.isFinished());
        assertFalse(operator.isBlocked().isDone());

        operator.noMoreSplits();

        List<Page> input = rowPagesBuilder(types)
                .row(1, 1)
                .row(2, 2)
                .pageBreak()
                .row(3, 3)
                .row(4, 4)
                .build();

        assertNull(operator.getOutput());
        assertFalse(operator.isFinished());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_1_ID).addPage(input.get(0), false);
        assertOperatorIsUnblocked(operator);

        assertNull(operator.getOutput());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_1_ID).addPage(input.get(1), true);
        assertOperatorIsUnblocked(operator);

        Page expected = rowPagesBuilder(BIGINT)
                .row(1)
                .row(2)
                .row(3)
                .row(4)
                .build()
                .get(0);
        assertPageEquals(ImmutableList.of(BIGINT), getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    @Test
    public void testMergeDifferentTypes()
            throws Exception
    {
        ImmutableList<Type> types = ImmutableList.of(BIGINT, INTEGER);
        MergeOperator operator = createMergeOperator(types, ImmutableList.of(1, 0), ImmutableList.of(1, 0), ImmutableList.of(DESC_NULLS_FIRST, ASC_NULLS_FIRST));
        operator.addSplit(createRemoteSplit(TASK_1_ID));
        operator.addSplit(createRemoteSplit(TASK_2_ID));
        operator.noMoreSplits();

        List<Page> task1Pages = rowPagesBuilder(types)
                .row(0, null)
                .row(1, 4)
                .row(2, 3)
                .build();

        List<Page> task2Pages = rowPagesBuilder(types)
                .row(null, 5)
                .row(2, 5)
                .row(4, 3)
                .build();

        // blocked on first data source
        assertNull(operator.getOutput());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_1_ID).addPages(task1Pages, true);
        assertOperatorIsUnblocked(operator);

        // blocked on second data source
        assertNull(operator.getOutput());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(task2Pages, true);
        assertOperatorIsUnblocked(operator);

        ImmutableList<Type> outputTypes = ImmutableList.of(INTEGER, BIGINT);
        Page expected = rowPagesBuilder(outputTypes)
                .row(null, 0)
                .row(5, null)
                .row(5, 2)
                .row(4, 1)
                .row(3, 2)
                .row(3, 4)
                .build()
                .get(0);

        assertPageEquals(outputTypes, getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    @Test
    public void testMultipleStreamsSameOutputColumns()
            throws Exception
    {
        List<Type> types = ImmutableList.of(BIGINT, BIGINT, BIGINT);

        MergeOperator operator = createMergeOperator(types, ImmutableList.of(0, 1, 2), ImmutableList.of(0), ImmutableList.of(ASC_NULLS_FIRST));
        operator.addSplit(createRemoteSplit(TASK_1_ID));
        operator.addSplit(createRemoteSplit(TASK_2_ID));
        operator.addSplit(createRemoteSplit(TASK_3_ID));
        operator.noMoreSplits();

        List<Page> source1Pages = rowPagesBuilder(types)
                .row(1, 1, 2)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .pageBreak()
                .row(55, 1, 2)
                .row(89, 1, 3)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .pageBreak()
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build();

        List<Page> source2Pages = rowPagesBuilder(types)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .pageBreak()
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(99, 2, 2)
                .pageBreak()
                .row(99, 1, 1)
                .row(100, 1, 7)
                .row(100, 1, 6)
                .build();

        List<Page> source3Pages = rowPagesBuilder(types)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .pageBreak()
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .build();

        // blocked on first data source
        assertNull(operator.getOutput());
        assertFalse(operator.isFinished());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_1_ID).addPage(source1Pages.get(0), false);
        assertOperatorIsUnblocked(operator);

        // blocked on second data source
        assertNull(operator.getOutput());
        assertOperatorIsBlocked(operator);
        taskBuffers.getUnchecked(TASK_2_ID).addPage(source2Pages.get(0), false);
        assertOperatorIsUnblocked(operator);

        // blocked on third data source
        assertNull(operator.getOutput());
        assertOperatorIsBlocked(operator);

        taskBuffers.getUnchecked(TASK_3_ID).addPage(source3Pages.get(0), false);
        assertOperatorIsUnblocked(operator);

        taskBuffers.getUnchecked(TASK_1_ID).addPage(source1Pages.get(1), false);
        taskBuffers.getUnchecked(TASK_2_ID).addPage(source2Pages.get(1), false);

        taskBuffers.getUnchecked(TASK_3_ID).addPage(source3Pages.get(1), true);
        taskBuffers.getUnchecked(TASK_2_ID).addPage(source2Pages.get(2), true);
        taskBuffers.getUnchecked(TASK_1_ID).addPage(source1Pages.get(2), true);
        Page expected = rowPagesBuilder(types)
                .row(1, 1, 2)
                .row(2, 1, 2)
                .row(8, 1, 1)
                .row(8, 1, 1)
                .row(19, 1, 3)
                .row(19, 1, 3)
                .row(25, 1, 4)
                .row(26, 2, 5)
                .row(27, 1, 4)
                .row(41, 2, 5)
                .row(55, 1, 2)
                .row(56, 1, 2)
                .row(66, 1, 3)
                .row(77, 1, 4)
                .row(88, 1, 3)
                .row(88, 1, 3)
                .row(89, 1, 3)
                .row(89, 1, 3)
                .row(90, 1, 3)
                .row(91, 1, 4)
                .row(92, 2, 5)
                .row(93, 1, 2)
                .row(94, 1, 3)
                .row(95, 1, 4)
                .row(97, 1, 3)
                .row(98, 2, 2)
                .row(99, 2, 2)
                .row(99, 1, 1)
                .row(100, 1, 7)
                .row(100, 1, 6)
                .row(101, 1, 4)
                .row(202, 1, 3)
                .row(399, 2, 2)
                .row(400, 1, 1)
                .row(401, 1, 7)
                .row(402, 1, 6)
                .build()
                .get(0);

        assertPageEquals(types, getOnlyElement(pullAvailablePages(operator)), expected);
        operator.close();
    }

    private MergeOperator createMergeOperator(List<Type> sourceTypes, List<Integer> outputChannels, List<Integer> sortChannels, List<SortOrder> sortOrder)
    {
        int mergeOperatorId = operatorId.getAndIncrement();
        MergeOperator.MergeOperatorFactory factory = new MergeOperator.MergeOperatorFactory(
                mergeOperatorId,
                new PlanNodeId("plan_node_id" + mergeOperatorId),
                exchangeClientFactory,
                serdeFactory,
                orderingCompiler,
                sourceTypes,
                outputChannels,
                sortChannels,
                sortOrder);
        DriverContext driverContext = createTaskContext(executor, executor, TEST_SESSION)
                .addPipelineContext(0, true, true)
                .addDriverContext();
        return (MergeOperator) factory.createOperator(driverContext);
    }

    private static Split createRemoteSplit(String taskId)
    {
        return new Split(ExchangeOperator.REMOTE_CONNECTOR_ID, new RemoteTransactionHandle(), new RemoteSplit(URI.create("http://localhost/" + taskId)));
    }

    private static List<Page> pullAvailablePages(Operator operator)
            throws InterruptedException
    {
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Page> outputPages = new ArrayList<>();

        assertOperatorIsUnblocked(operator);

        while (!operator.isFinished() && System.nanoTime() - endTime < 0) {
            assertFalse(operator.needsInput());
            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
            else {
                Thread.sleep(10);
            }
        }

        // verify state
        assertFalse(operator.needsInput(), "Operator still wants input");
        assertTrue(operator.isFinished(), "Expected operator to be finished");

        return outputPages;
    }
}
