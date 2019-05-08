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

import com.facebook.presto.block.BlockAssertions;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.memory.context.SimpleLocalMemoryContext;
import com.facebook.presto.spi.Page;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static com.facebook.presto.memory.context.AggregatedMemoryContext.newSimpleAggregatedMemoryContext;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.MoreFutures.tryGetFutureValue;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;
    private ExecutorService testingHttpClientExecutor;

    private static final PagesSerde PAGES_SERDE = testingPagesSerde();

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
        testingHttpClientExecutor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        if (scheduler != null) {
            scheduler.shutdownNow();
            scheduler = null;
        }

        if (pageBufferClientCallbackExecutor != null) {
            pageBufferClientCallbackExecutor.shutdownNow();
            pageBufferClientCallbackExecutor = null;
        }

        if (testingHttpClientExecutor != null) {
            testingHttpClientExecutor.shutdownNow();
            testingHttpClientExecutor = null;
        }
    }

    @Test
    public void testHappyPath()
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                new TestingHttpClient(processor, scheduler),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location, TaskId.valueOf("taskid"));
        exchangeClient.noMoreLocations();

        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(3));
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.isClosed(), true);

        ExchangeClientStatus status = exchangeClient.getStatus();
        assertEquals(status.getBufferedPages(), 0);
        assertEquals(status.getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(status.getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(32, MEGABYTE),
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                new TestingHttpClient(processor, testingHttpClientExecutor),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(location1, TaskId.valueOf("foo"));

        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(2));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertFalse(exchangeClient.isClosed());

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(location2, TaskId.valueOf("bar"));

        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertFalse(exchangeClient.isClosed());

        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        ImmutableMap<URI, PageBufferClientStatus> statuses = uniqueIndex(exchangeClient.getStatus().getPageBufferClientStatuses(), PageBufferClientStatus::getUri);
        assertStatus(statuses.get(location1), location1, "closed", 3, 3, 3, "not scheduled");
        assertStatus(statuses.get(location2), location2, "closed", 3, 3, 3, "not scheduled");
    }

    @Test
    public void testBufferLimit()
    {
        DataSize maxResponseSize = new DataSize(1, BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, BYTE),
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                new TestingHttpClient(processor, testingHttpClientExecutor),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);

        exchangeClient.addLocation(location, TaskId.valueOf("taskid"));
        exchangeClient.noMoreLocations();
        assertFalse(exchangeClient.isClosed());

        long start = System.nanoTime();

        // start fetching pages
        exchangeClient.scheduleRequestIfNecessary();
        // wait for a page to be fetched
        do {
            // there is no thread coordination here, so sleep is the best we can do
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 1, 1, 1, "not scheduled");

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(1));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 2, 2, 2, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove the page and wait for the client to fetch another page
        assertPageEquals(exchangeClient.pollPage(), createPage(2));
        do {
            assertLessThan(Duration.nanosSince(start), new Duration(5, TimeUnit.SECONDS));
            sleepUninterruptibly(100, MILLISECONDS);
        }
        while (exchangeClient.getStatus().getBufferedPages() == 0);

        // client should have sent a single request for a single page
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "queued", 3, 3, 3, "not scheduled");
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() > 0);

        // remove last page
        assertPageEquals(getNextPage(exchangeClient), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(getNextPage(exchangeClient));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, BYTE),
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                new TestingHttpClient(processor, testingHttpClientExecutor),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);
        exchangeClient.addLocation(location, TaskId.valueOf("taskid"));
        exchangeClient.noMoreLocations();

        // fetch a page
        assertFalse(exchangeClient.isClosed());
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        waitUntilEquals(exchangeClient::isFinished, true, new Duration(5, SECONDS));
        assertEquals(exchangeClient.isClosed(), true);
        assertNull(exchangeClient.pollPage());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertEquals(exchangeClient.getStatus().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertStatus(clientStatus, location, "closed", "not scheduled");
    }

    @Test
    public void testRemoveRemoteSource()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location1 = URI.create("http://localhost:8081/foo");
        TaskId taskId1 = TaskId.valueOf("foo");
        URI location2 = URI.create("http://localhost:8082/bar");
        TaskId taskId2 = TaskId.valueOf("bar");

        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));

        ExchangeClient exchangeClient = new ExchangeClient(
                new DataSize(1, BYTE),
                maxResponseSize,
                1,
                new Duration(1, MINUTES),
                true,
                new TestingHttpClient(processor, testingHttpClientExecutor),
                scheduler,
                new SimpleLocalMemoryContext(newSimpleAggregatedMemoryContext(), "test"),
                pageBufferClientCallbackExecutor);
        exchangeClient.addLocation(location1, taskId1);
        exchangeClient.addLocation(location2, taskId2);

        assertEquals(exchangeClient.isClosed(), false);

        // Wait until exactly one page is buffered:
        //  * We cannot call ExchangeClient#pollPage() directly, since it will schedule the next request to buffer data,
        //    and this request to buffer data could win the race against the request to remove remote source.
        //  * Buffer capacity is set to 1 byte, so only one page can be buffered.
        waitUntilEquals(() -> exchangeClient.getStatus().getBufferedPages(), 1, new Duration(5, SECONDS));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 1);

        // remove remote source
        exchangeClient.removeRemoteSource(taskId1);

        // the previously buffered page will still be read out
        assertPageEquals(getNextPage(exchangeClient), createPage(1));

        // client should not receive any further pages from removed remote source
        assertNull(exchangeClient.pollPage());
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);

        // add pages to another source
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(getNextPage(exchangeClient), createPage(6));

        assertFalse(tryGetFutureValue(exchangeClient.isBlocked(), 10, MILLISECONDS).isPresent());
        assertEquals(exchangeClient.isClosed(), false);

        exchangeClient.noMoreLocations();
        // The transition to closed may happen asynchronously, since it requires that all the HTTP clients
        // receive a final GONE response, so just spin until it's closed or the test times out.
        while (!exchangeClient.isClosed()) {
            Thread.sleep(1);
        }

        PageBufferClientStatus clientStatus1 = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertStatus(clientStatus1, location1, "closed", "not scheduled");

        PageBufferClientStatus clientStatus2 = exchangeClient.getStatus().getPageBufferClientStatuses().get(1);
        assertStatus(clientStatus2, location2, "closed", "not scheduled");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static SerializedPage getNextPage(ExchangeClient exchangeClient)
    {
        ListenableFuture<SerializedPage> futurePage = Futures.transform(exchangeClient.isBlocked(), ignored -> exchangeClient.pollPage(), directExecutor());
        return tryGetFutureValue(futurePage, 100, TimeUnit.SECONDS).orElse(null);
    }

    private static void assertPageEquals(SerializedPage actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(PAGES_SERDE.deserialize(actualPage).getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(
            PageBufferClientStatus clientStatus,
            URI location,
            String status,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }

    private static void assertStatus(
            PageBufferClientStatus clientStatus,
            URI location,
            String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            String httpRequestState)
    {
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), status, "status");
        assertEquals(clientStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(clientStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(clientStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(clientStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }

    private <T> void waitUntilEquals(Supplier<T> actualSupplier, T expected, Duration timeout)
    {
        long nanoUntil = System.nanoTime() + timeout.toMillis() * 1_000_000;
        while (System.nanoTime() - nanoUntil < 0) {
            if (expected.equals(actualSupplier.get())) {
                return;
            }
            try {
                Thread.sleep(10);
            }
            catch (InterruptedException e) {
                // do nothing
            }
        }
        assertEquals(actualSupplier.get(), expected);
    }
}
