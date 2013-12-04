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

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.ExchangeOperator.ExchangeOperatorFactory;
import com.facebook.presto.serde.PagesSerde;
import com.facebook.presto.split.RemoteSplit;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Function;
import com.google.common.base.Splitter;
import com.google.common.base.Supplier;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableListMultimap.Builder;
import com.google.common.collect.Iterables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.units.DataSize;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.Closeable;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.operator.PageAssertions.assertPageEquals;
import static com.facebook.presto.operator.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static javax.ws.rs.core.HttpHeaders.CONTENT_TYPE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

public class TestExchangeOperator
{
    private static final List<TupleInfo> TUPLE_INFOS = ImmutableList.of(SINGLE_VARBINARY);
    private static final Page PAGE = createSequencePage(TUPLE_INFOS, 10, 100);

    private static final String TASK_1_ID = "task1";
    private static final String TASK_2_ID = "task2";
    private static final String TASK_3_ID = "task3";

    private LoadingCache<String, TaskBuffer> taskBuffers;
    private ExecutorService executor;
    private AsyncHttpClient httpClient;

    private DriverContext driverContext;
    private Supplier<ExchangeClient> exchangeClientSupplier;

    @BeforeMethod
    public void setUp()
            throws Exception
    {
        taskBuffers = CacheBuilder.newBuilder().build(new CacheLoader<String, TaskBuffer>()
        {
            @Override
            public TaskBuffer load(String key)
                    throws Exception
            {
                return new TaskBuffer();
            }
        });
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));

        httpClient = new TestingHttpClient(new HttpClientHandler(), executor);

        Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
        driverContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                .addPipelineContext(true, true)
                .addDriverContext();

        exchangeClientSupplier = new Supplier<ExchangeClient>()
        {
            @Override
            public ExchangeClient get()
            {
                return new ExchangeClient(new DataSize(32, MEGABYTE), new DataSize(10, MEGABYTE), 3, httpClient, executor);
            }
        };
    }

    @AfterMethod
    public void tearDown()
            throws Exception
    {
        taskBuffers = null;

        httpClient.close();
        httpClient = null;

        executor.shutdownNow();
        executor = null;
    }

    @Test
    public void testSimple()
            throws Exception
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(0, new PlanNodeId("test"), exchangeClientSupplier, TUPLE_INFOS);
        SourceOperator operator = operatorFactory.createOperator(driverContext);

        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_1_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_2_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_3_ID), TUPLE_INFOS));
        operator.noMoreSplits();

        // add pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(10, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(10, true);

        // read the pages
        waitForPages(operator, 30);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testWaitForClose()
            throws Exception
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(0, new PlanNodeId("test"), exchangeClientSupplier, TUPLE_INFOS);
        SourceOperator operator = operatorFactory.createOperator(driverContext);

        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_1_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_2_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_3_ID), TUPLE_INFOS));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // add more pages and close the buffers
        taskBuffers.getUnchecked(TASK_1_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 6);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testWaitForNoMoreSplits()
            throws Exception
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(0, new PlanNodeId("test"), exchangeClientSupplier, TUPLE_INFOS);
        SourceOperator operator = operatorFactory.createOperator(driverContext);

        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_1_ID), TUPLE_INFOS));

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, true);

        // read page
        waitForPages(operator, 1);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // add more pages and close the buffers
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_2_ID), TUPLE_INFOS));
        operator.noMoreSplits();
        taskBuffers.getUnchecked(TASK_2_ID).addPages(2, true);

        // read all pages
        waitForPages(operator, 2);

        // wait for finished
        waitForFinished(operator);
    }

    @Test
    public void testFinish()
            throws Exception
    {
        ExchangeOperatorFactory operatorFactory = new ExchangeOperatorFactory(0, new PlanNodeId("test"), exchangeClientSupplier, TUPLE_INFOS);
        SourceOperator operator = operatorFactory.createOperator(driverContext);

        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_1_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_2_ID), TUPLE_INFOS));
        operator.addSplit(new RemoteSplit(URI.create("http://localhost/" + TASK_3_ID), TUPLE_INFOS));
        operator.noMoreSplits();

        // add pages and leave buffers open
        taskBuffers.getUnchecked(TASK_1_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_2_ID).addPages(1, false);
        taskBuffers.getUnchecked(TASK_3_ID).addPages(1, false);

        // read 3 pages
        waitForPages(operator, 3);

        // verify state
        assertEquals(operator.isFinished(), false);
        assertEquals(operator.needsInput(), false);
        assertEquals(operator.getOutput(), null);

        // finish without closing buffers
        operator.finish();

        // wait for finished
        waitForFinished(operator);
    }

    private List<Page> waitForPages(Operator operator, int expectedPageCount)
            throws InterruptedException
    {
        // read expected pages or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        List<Page> outputPages = new ArrayList<>();
        while (outputPages.size() < expectedPageCount && System.nanoTime() < endTime) {
            assertEquals(operator.needsInput(), false);
            if (operator.isFinished()) {
                break;
            }

            Page outputPage = operator.getOutput();
            if (outputPage != null) {
                outputPages.add(outputPage);
            }
            else {
                Thread.sleep(10);
            }
        }

        // sleep for a bit to make sure that there aren't extra pages on the way
        Thread.sleep(10);

        // verify state
        assertEquals(operator.needsInput(), false);
        assertNull(operator.getOutput());

        // verify pages
        assertEquals(outputPages.size(), expectedPageCount);
        for (Page page : outputPages) {
            assertPageEquals(page, PAGE);
        }

        return outputPages;
    }

    private void waitForFinished(Operator operator)
            throws InterruptedException
    {
        // wait for finished or until 10 seconds has passed
        long endTime = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < endTime) {
            assertEquals(operator.needsInput(), false);
            assertNull(operator.getOutput());
            if (operator.isFinished()) {
                break;
            }
            Thread.sleep(10);
        }

        // verify final state
        assertEquals(operator.isFinished(), true);
        assertEquals(operator.needsInput(), false);
        assertNull(operator.getOutput());
    }

    private class HttpClientHandler
            implements Function<Request, Response>
    {
        @Override
        public Response apply(Request request)
        {
            ImmutableList<String> parts = ImmutableList.copyOf(Splitter.on("/").omitEmptyStrings().split(request.getUri().getPath()));
            assertEquals(parts.size(), 2);
            String taskId = parts.get(0);
            int pageToken = Integer.parseInt(parts.get(1));

            Builder<String, String> headers = ImmutableListMultimap.builder();
            headers.put(PRESTO_PAGE_TOKEN, String.valueOf(pageToken));

            TaskBuffer taskBuffer = taskBuffers.getUnchecked(taskId);
            Page page = taskBuffer.getPage(pageToken);
            if (page != null) {
                headers.put(CONTENT_TYPE, PRESTO_PAGES);
                headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken + 1));
                DynamicSliceOutput output = new DynamicSliceOutput(256);
                PagesSerde.writePages(output, page);
                return new TestingResponse(HttpStatus.OK, headers.build(), output.slice().getInput());
            }
            else if (taskBuffer.isClosed()) {
                headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
                return new TestingResponse(HttpStatus.GONE, headers.build(), new byte[0]);
            }
            else {
                headers.put(PRESTO_PAGE_NEXT_TOKEN, String.valueOf(pageToken));
                return new TestingResponse(HttpStatus.NO_CONTENT, headers.build(), new byte[0]);
            }
        }
    }

    private static class TaskBuffer
            implements Closeable
    {
        private final List<Page> buffer = new ArrayList<>();
        private boolean closed;

        private void addPages(int pages, boolean close)
        {
            addPages(Collections.nCopies(pages, PAGE));
            if (close) {
                closed = true;
            }
        }

        public void addPages(Iterable<Page> pages)
        {
            Iterables.addAll(buffer, pages);
        }

        public Page getPage(int pageSequenceId)
        {
            if (pageSequenceId >= buffer.size()) {
                return null;
            }
            return buffer.get(pageSequenceId);
        }

        private boolean isClosed()
        {
            return closed;
        }

        @Override
        public void close()
        {
            closed = true;
        }
    }
}
