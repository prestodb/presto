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
import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.spi.Page;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableMap;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.net.URI;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.testing.Assertions.assertLessThan;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestExchangeClient
{
    private ScheduledExecutorService executor;

    private static final BlockEncodingManager blockEncodingManager = new BlockEncodingManager(new TypeRegistry());

    @BeforeClass
    public void setUp()
    {
        executor = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
    }

    @AfterClass
    public void tearDown()
    {
        if (executor != null) {
            executor.shutdownNow();
            executor = null;
        }
    }

    @Test
    public void testHappyPath()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(blockEncodingManager, new DataSize(32, Unit.MEGABYTE), maxResponseSize, 1, new Duration(1, TimeUnit.MINUTES), new TestingHttpClient(processor, executor), executor, deltaMemoryInBytes -> { });

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));
        assertNull(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.isClosed(), true);
        if (exchangeClient.getStatus().getBufferedPages() != 0) {
            assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        }
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 3, 3, "not scheduled");
    }

    @Test(timeOut = 10000)
    public void testAddLocation()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(10, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(blockEncodingManager, new DataSize(32, Unit.MEGABYTE), maxResponseSize, 1, new Duration(1, TimeUnit.MINUTES), new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))), executor, deltaMemoryInBytes -> { });

        URI location1 = URI.create("http://localhost:8081/foo");
        processor.addPage(location1, createPage(1));
        processor.addPage(location1, createPage(2));
        processor.addPage(location1, createPage(3));
        processor.setComplete(location1);
        exchangeClient.addLocation(location1);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(1));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(2));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));

        assertNull(exchangeClient.getNextPage(new Duration(10, MILLISECONDS)));
        assertEquals(exchangeClient.isClosed(), false);

        URI location2 = URI.create("http://localhost:8082/bar");
        processor.addPage(location2, createPage(4));
        processor.addPage(location2, createPage(5));
        processor.addPage(location2, createPage(6));
        processor.setComplete(location2);
        exchangeClient.addLocation(location2);

        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(4));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(5));
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(6));

        assertNull(exchangeClient.getNextPage(new Duration(10, MILLISECONDS)));
        assertEquals(exchangeClient.isClosed(), false);

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
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");

        // add a pages
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));
        processor.setComplete(location);

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(blockEncodingManager, new DataSize(1, Unit.BYTE), maxResponseSize, 1, new Duration(1, TimeUnit.MINUTES), new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))), executor, deltaMemoryInBytes -> { });

        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();
        assertEquals(exchangeClient.isClosed(), false);

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
        assertPageEquals(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)), createPage(1));
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
        assertPageEquals(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)), createPage(2));
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
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(3));

        //  wait for client to decide there are no more pages
        assertNull(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertTrue(exchangeClient.getStatus().getBufferedBytes() == 0);
        assertEquals(exchangeClient.isClosed(), true);
        assertStatus(exchangeClient.getStatus().getPageBufferClientStatuses().get(0), location, "closed", 3, 5, 5, "not scheduled");
    }

    @Test
    public void testClose()
            throws Exception
    {
        DataSize maxResponseSize = new DataSize(1, Unit.BYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(maxResponseSize);

        URI location = URI.create("http://localhost:8080");
        processor.addPage(location, createPage(1));
        processor.addPage(location, createPage(2));
        processor.addPage(location, createPage(3));

        @SuppressWarnings("resource")
        ExchangeClient exchangeClient = new ExchangeClient(blockEncodingManager, new DataSize(1, Unit.BYTE), maxResponseSize, 1, new Duration(1, TimeUnit.MINUTES), new TestingHttpClient(processor, newCachedThreadPool(daemonThreadsNamed("test-%s"))), executor, deltaMemoryInBytes -> { });
        exchangeClient.addLocation(location);
        exchangeClient.noMoreLocations();

        // fetch a page
        assertEquals(exchangeClient.isClosed(), false);
        assertPageEquals(exchangeClient.getNextPage(new Duration(1, TimeUnit.SECONDS)), createPage(1));

        // close client while pages are still available
        exchangeClient.close();
        while (!exchangeClient.isFinished()) {
            MILLISECONDS.sleep(10);
        }
        assertEquals(exchangeClient.isClosed(), true);
        assertNull(exchangeClient.getNextPage(new Duration(0, TimeUnit.SECONDS)));
        assertEquals(exchangeClient.getStatus().getBufferedPages(), 0);
        assertEquals(exchangeClient.getStatus().getBufferedBytes(), 0);

        // client should have sent only 2 requests: one to get all pages and once to get the done signal
        PageBufferClientStatus clientStatus = exchangeClient.getStatus().getPageBufferClientStatuses().get(0);
        assertEquals(clientStatus.getUri(), location);
        assertEquals(clientStatus.getState(), "closed", "status");
        assertEquals(clientStatus.getHttpRequestState(), "not scheduled", "httpRequestState");
    }

    private static Page createPage(int size)
    {
        return new Page(BlockAssertions.createLongSequenceBlock(0, size));
    }

    private static void assertPageEquals(Page actualPage, Page expectedPage)
    {
        assertNotNull(actualPage);
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private static void assertStatus(PageBufferClientStatus clientStatus,
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
}
