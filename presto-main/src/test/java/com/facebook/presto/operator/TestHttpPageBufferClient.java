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

import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.annotation.Nullable;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.util.Threads.daemonThreadsNamed;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static org.testng.Assert.assertEquals;

public class TestHttpPageBufferClient
{
    private ExecutorService executor;

    @BeforeClass
    public void setUp()
    {
        executor = newCachedThreadPool(daemonThreadsNamed("test-%s"));
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
        Page expectedPage = new Page(100);

        DataSize expectedMaxSize = new DataSize(11, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(expectedMaxSize);

        CyclicBarrier requestComplete = new CyclicBarrier(2);

        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, executor),
                expectedMaxSize,
                location,
                callback,
                executor);

        assertStatus(client, location, "queued", 0, 0, 0, "queued");

        // fetch a page and verify
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 1, 1, "queued");

        // fetch no data and verify
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 2, 2, "queued");

        // fetch two more pages and verify
        processor.addPage(location, expectedPage);
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 2);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertPageEquals(expectedPage, callback.getPages().get(1));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        callback.resetStats();
        assertStatus(client, location, "queued", 3, 3, 3, "queued");

        // finish and verify
        callback.resetStats();
        processor.setComplete(location);
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 0);
        assertEquals(callback.getFinishedBuffers(), 1);
        assertStatus(client, location, "closed", 3, 4, 4, "queued");
    }

    @Test
    public void testLifecycle()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.<String, String>of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, executor),
                new DataSize(10, Unit.MEGABYTE),
                location,
                callback,
                executor);

        assertStatus(client, location, "queued", 0, 0, 0, "queued");

        client.scheduleRequest();
        beforeRequest.await(1, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, "PROCESSING_REQUEST");
        assertEquals(client.isRunning(), true);
        afterRequest.await(1, TimeUnit.SECONDS);

        requestComplete.await(1, TimeUnit.SECONDS);
        assertStatus(client, location, "queued", 0, 1, 1, "queued");

        client.close();
        assertStatus(client, location, "closed", 0, 1, 1, "queued");
    }

    @Test
    public void testInvalidResponses()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(1);
        CyclicBarrier afterRequest = new CyclicBarrier(1);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, executor),
                new DataSize(10, Unit.MEGABYTE),
                location,
                callback,
                executor);

        assertStatus(client, location, "queued", 0, 0, 0, "queued");

        // send not found response and verify response was ignored
        processor.setResponse(new TestingResponse(HttpStatus.NOT_FOUND, ImmutableListMultimap.of(CONTENT_TYPE, PRESTO_PAGES), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, "queued");

        // send invalid content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "INVALID_TYPE"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 2, 2, "queued");

        // send unexpected content type response and verify response was ignored
        callback.resetStats();
        processor.setResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), new byte[0]));
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 3, 3, "queued");

        // close client and verify
        client.close();
        assertStatus(client, location, "closed", 0, 3, 3, "queued");
    }

    @Test
    public void testCloseDuringPendingRequest()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.<String, String>of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, executor),
                new DataSize(10, Unit.MEGABYTE),
                location,
                callback,
                executor);

        assertStatus(client, location, "queued", 0, 0, 0, "queued");

        // send request
        client.scheduleRequest();
        beforeRequest.await(1, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, "PROCESSING_REQUEST");
        assertEquals(client.isRunning(), true);
        // request is pending, now close it
        client.close();

        try {
            requestComplete.await(1, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException ignored) {
        }
        assertStatus(client, location, "closed", 0, 1, 1, "queued");
    }

    @Test
    public void testExceptionFromResponseHandler()
            throws Exception
    {
        Function<Request, Response> processor = new Function<Request, Response>()
        {
            @Override
            public Response apply(Request input)
            {
                throw new RuntimeException("Foo");
            }
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, executor),
                new DataSize(10, Unit.MEGABYTE),
                location,
                callback,
                executor);

        assertStatus(client, location, "queued", 0, 0, 0, "queued");

        // request processor will throw exception, verify the request is marked a completed
        client.scheduleRequest();
        requestComplete.await(1, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, "queued");
    }

    private void assertStatus(HttpPageBufferClient client, URI location, String status, int pagesReceived, int requestsScheduled, int requestsCompleted, String httpRequestState)
    {
        PageBufferClientStatus actualStatus = client.getStatus();
        assertEquals(actualStatus.getUri(), location);
        assertEquals(actualStatus.getState(), status, "status");
        assertEquals(actualStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(actualStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(actualStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(actualStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
    }

    private void assertPageEquals(Page expectedPage, Page actualPage)
    {
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private static class TestingClientCallback
            implements ClientCallback
    {
        private final CyclicBarrier done;
        private final List<Page> pages = Collections.synchronizedList(new ArrayList<Page>());
        private final AtomicInteger completedRequests = new AtomicInteger();
        private final AtomicInteger finishedBuffers = new AtomicInteger();

        public TestingClientCallback(CyclicBarrier done)
        {
            this.done = done;
        }

        public List<Page> getPages()
        {
            return pages;
        }

        private int getCompletedRequests()
        {
            return completedRequests.get();
        }

        private int getFinishedBuffers()
        {
            return finishedBuffers.get();
        }

        @Override
        public void addPage(HttpPageBufferClient client, Page page)
        {
            pages.add(page);
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            completedRequests.getAndIncrement();
            try {
                done.await(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (BrokenBarrierException | TimeoutException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            finishedBuffers.getAndIncrement();
            try {
                done.await(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (BrokenBarrierException | TimeoutException e) {
                throw Throwables.propagate(e);
            }
        }

        public void resetStats()
        {
            pages.clear();
            completedRequests.set(0);
            finishedBuffers.set(0);
        }
    }

    private static class StaticRequestProcessor
            implements Function<Request, Response>
    {
        private final AtomicReference<Response> response = new AtomicReference<>();
        private final CyclicBarrier beforeRequest;
        private final CyclicBarrier afterRequest;

        private StaticRequestProcessor(CyclicBarrier beforeRequest, CyclicBarrier afterRequest)
        {
            this.beforeRequest = beforeRequest;
            this.afterRequest = afterRequest;
        }

        private void setResponse(Response response)
        {
            this.response.set(response);
        }

        @Override
        public Response apply(@Nullable Request request)
        {
            try {
                beforeRequest.await(1, TimeUnit.SECONDS);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (BrokenBarrierException | TimeoutException e) {
                throw Throwables.propagate(e);
            }

            try {
                return response.get();
            }
            finally {
                try {
                    afterRequest.await(1, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw Throwables.propagate(e);
                }
                catch (BrokenBarrierException | TimeoutException e) {
                    throw Throwables.propagate(e);
                }
            }
        }
    }
}
