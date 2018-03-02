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

import com.facebook.presto.execution.buffer.BufferSummary;
import com.facebook.presto.execution.buffer.PagesSerde;
import com.facebook.presto.execution.buffer.SerializedPage;
import com.facebook.presto.operator.HttpPageBufferClient.ClientCallback;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.Page;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.testing.TestingHttpClient;
import io.airlift.http.client.testing.TestingResponse;
import io.airlift.json.JsonCodec;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.testing.TestingTicker;
import io.airlift.units.DataSize;
import io.airlift.units.DataSize.Unit;
import io.airlift.units.Duration;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.MediaType;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES;
import static com.facebook.presto.SequencePageBuilder.createSequencePage;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_BUFFER_COMPLETE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_NEXT_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_TOKEN;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TASK_INSTANCE_ID;
import static com.facebook.presto.execution.buffer.BufferSummary.emptySummary;
import static com.facebook.presto.execution.buffer.PagesSerdeUtil.writePages;
import static com.facebook.presto.execution.buffer.TestingPagesSerdeFactory.testingPagesSerde;
import static com.facebook.presto.server.TaskResource.PATH_BUFFER_DATA;
import static com.facebook.presto.server.TaskResource.PATH_BUFFER_SUMMARY;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TOO_LARGE;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.PAGE_TRANSPORT_TIMEOUT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.util.Failures.WORKER_NODE_ERROR;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.concurrent.Threads.daemonThreadsNamed;
import static io.airlift.json.JsonCodec.jsonCodec;
import static io.airlift.testing.Assertions.assertContains;
import static io.airlift.testing.Assertions.assertGreaterThan;
import static io.airlift.testing.Assertions.assertInstanceOf;
import static java.lang.String.format;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.testng.Assert.assertEquals;

public class TestHttpPageBufferClient
{
    private ScheduledExecutorService scheduler;
    private ExecutorService pageBufferClientCallbackExecutor;

    private static final String TASK_INSTANCE_ID = "task-instance-id";
    private static final PagesSerde PAGES_SERDE = testingPagesSerde();
    private static final JsonCodec<BufferSummary> BUFFER_SUMMARY_CODEC = jsonCodec(BufferSummary.class);

    @BeforeClass
    public void setUp()
    {
        scheduler = newScheduledThreadPool(4, daemonThreadsNamed("test-%s"));
        pageBufferClientCallbackExecutor = Executors.newSingleThreadExecutor();
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
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                expectedMaxSize,
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // fetch a page and verify
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);

        // only page summary at this moment
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, 0, "not scheduled", 0, 0, 0, 0);

        // schedule to fetch the page
        callback.resetStats();
        long bytes = client.getTotalPendingSize();
        long totalBytes = bytes;
        client.scheduleRequest(bytes);
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), bytes);
        assertStatus(client, location, "queued", 1, 2, 2, 0, "not scheduled", totalBytes, totalBytes, totalBytes, 0);

        // fetch no data and verify
        callback.resetStats();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 3, 3, 0, "not scheduled", totalBytes, totalBytes, totalBytes, 0);

        // fetch two more pages and verify
        processor.addPage(location, expectedPage);
        processor.addPage(location, expectedPage);
        callback.resetStats();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);

        // only page summary at this moment
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertStatus(client, location, "queued", 1, 4, 4, 0, "not scheduled", totalBytes, totalBytes, totalBytes, 0);

        // schedule to fetch the pages
        callback.resetStats();
        bytes = client.getTotalPendingSize();
        totalBytes += bytes;
        client.scheduleRequest(bytes);
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 2);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertPageEquals(expectedPage, callback.getPages().get(1));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), bytes);
        callback.resetStats();
        assertStatus(client, location, "queued", 3, 5, 5, 0, "not scheduled", totalBytes, totalBytes, totalBytes, 0);

        // finish and verify
        callback.resetStats();
        processor.setComplete(location);
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);

        // get the buffer complete signal
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);

        // schedule the delete call to the buffer
        callback.resetStats();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getFinishedBuffers(), 1);

        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 0);
        assertEquals(callback.getFailedBuffers(), 0);

        assertStatus(client, location, "closed", 3, 7, 7, 0, "not scheduled", totalBytes, totalBytes, totalBytes, 0);
    }

    @Test
    public void testLimitedQuota()
            throws Exception
    {
        Page expectedPage = new Page(100);

        DataSize expectedMaxSize = new DataSize(11, Unit.MEGABYTE);
        MockExchangeRequestProcessor processor = new MockExchangeRequestProcessor(expectedMaxSize);

        CyclicBarrier requestComplete = new CyclicBarrier(2);

        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                expectedMaxSize,
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // fetch 4 pages and verify
        int pageCount = 4;
        for (int i = 0; i < pageCount; i++) {
            processor.addPage(location, expectedPage);
        }
        callback.resetStats();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);

        // only page summary at this moment
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, 0, "not scheduled", 0, 0, 0, 0);

        // limit quota so we fetch a controlled number of pages a time
        long bytePerPage = client.getTotalPendingSize() / pageCount;

        // fetch a single page and verify
        callback.resetStats();
        client.scheduleRequest(bytePerPage);
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), bytePerPage);
        assertStatus(client, location, "queued", 1, 2, 2, 0, "not scheduled", bytePerPage, bytePerPage, bytePerPage, 0);

        // fetch the next two pages and verify
        callback.resetStats();
        client.scheduleRequest(bytePerPage * 2);
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 2);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertPageEquals(expectedPage, callback.getPages().get(1));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), bytePerPage * 2);
        assertStatus(client, location, "queued", 3, 3, 3, 0, "not scheduled", bytePerPage * 3, bytePerPage * 3, bytePerPage * 3, 0);

        // fetch the last page and verify
        callback.resetStats();
        client.scheduleRequest(bytePerPage);
        requestComplete.await(10, TimeUnit.SECONDS);

        assertEquals(callback.getPages().size(), 1);
        assertPageEquals(expectedPage, callback.getPages().get(0));
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getReleasedBytes(), bytePerPage);
        assertStatus(client, location, "queued", 4, 4, 4, 0, "not scheduled", bytePerPage * 4, bytePerPage * 4, bytePerPage * 4, 0);
    }

    @Test
    public void testLifecycle()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        // NO_CONTENT will fail the get size request
        processor.setSummaryResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        client.scheduleRequest(client.getTotalPendingSize());
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST", 0, 0, 0, 0);
        assertEquals(client.isRunning(), true);
        afterRequest.await(10, TimeUnit.SECONDS);

        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled", 0, 0, 0, 0);

        // Make get size request pass
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(emptySummary(TASK_INSTANCE_ID, 0, false))));

        client.scheduleRequest(client.getTotalPendingSize());
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 2, 1, 1, "PROCESSING_REQUEST", 0, 0, 0, 0);
        assertEquals(client.isRunning(), true);
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "queued", 0, 2, 2, 1, "not scheduled", 0, 0, 0, 0);

        // Send delete request
        processor.setDataResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));
        client.close();
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 2, 2, 1, "PROCESSING_REQUEST", 0, 0, 0, 0);
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 2, 3, 1, "not scheduled", 0, 0, 0, 0);
    }

    @Test
    public void testInvalidSummaryResponses()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(1);
        CyclicBarrier afterRequest = new CyclicBarrier(1);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // send not found response and verify response was ignored
        processor.setSummaryResponse(new TestingResponse(HttpStatus.NOT_FOUND, ImmutableListMultimap.of(CONTENT_TYPE, PRESTO_PAGES), new byte[0]));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected response code to be 200, but was 404 Not Found");
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled", 0, 0, 0, 0);

        // send invalid content type response and verify response was ignored
        callback.resetStats();
        processor.setSummaryResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "INVALID_TYPE"), new byte[0]));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), format("Error fetching http://localhost:8080/%s/0: Could not parse 'INVALID_TYPE'", PATH_BUFFER_SUMMARY));
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled", 0, 0, 0, 0);

        // send unexpected content type response and verify response was ignored
        callback.resetStats();
        processor.setSummaryResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), new byte[0]));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected application/json response from server but got text/plain");
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled", 0, 0, 0, 0);

        // send buffer complete with non-empty buffer and verify response was ignored
        callback.resetStats();
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, true, ImmutableList.of(1L)))));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), format("Error fetching http://localhost:8080/%s/0: buffer completes with 1 pages", PATH_BUFFER_SUMMARY));
        assertStatus(client, location, "queued", 0, 4, 4, 4, "not scheduled", 0, 0, 0, 0);

        // send response empty task id and verify response was ignored
        callback.resetStats();
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(emptySummary(TASK_INSTANCE_ID, 0, false))));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        String newTaskID = TASK_INSTANCE_ID + "_DIFF";
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(emptySummary(newTaskID, 0, false))));
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 2);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), format("Error fetching http://localhost:8080/%s/0: expected task id [%s]; found [%s]", PATH_BUFFER_SUMMARY, TASK_INSTANCE_ID, newTaskID));
        assertStatus(client, location, "queued", 0, 6, 6, 5, "not scheduled", 0, 0, 0, 0);

        // close client and verify
        processor.setDataResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));
        client.close();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 6, 7, 5, "not scheduled", 0, 0, 0, 0);
    }

    @Test
    public void testInvalidDataResponses()
            throws Exception
    {
        DynamicSliceOutput output = new DynamicSliceOutput(256);
        writePages(PAGES_SERDE, output, createSequencePage(ImmutableList.of(VARCHAR), 10, 100));

        CyclicBarrier beforeRequest = new CyclicBarrier(1);
        CyclicBarrier afterRequest = new CyclicBarrier(1);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // send not found response and verify response was ignored
        processor.setDataResponse(new TestingResponse(HttpStatus.NOT_FOUND, ImmutableListMultimap.of(CONTENT_TYPE, PRESTO_PAGES), new byte[0]));
        // force client to fetch pages
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, false, ImmutableList.of(1L)))));
        // first schedule to make sure we have the page summary available
        assertEquals(client.getTotalPendingSize(), 0);
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        // second schedule to fetch pages
        long totalBytes = client.getTotalPendingSize();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 2);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected response code to be 200, but was 404 Not Found");
        assertStatus(client, location, "queued", 0, 2, 2, 1, "not scheduled", totalBytes, totalBytes, 0, 0);

        // send invalid content type response and verify response was ignored
        callback.resetStats();
        processor.setDataResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "INVALID_TYPE"), new byte[0]));
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, false, ImmutableList.of(1L)))));
        totalBytes += client.getTotalPendingSize();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected application/x-presto-pages response from server but got INVALID_TYPE");
        assertStatus(client, location, "queued", 0, 3, 3, 2, "not scheduled", totalBytes, totalBytes, 0, 0);

        // send unexpected content type response and verify response was ignored
        callback.resetStats();
        processor.setDataResponse(new TestingResponse(HttpStatus.OK, ImmutableListMultimap.of(CONTENT_TYPE, "text/plain"), new byte[0]));
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, false, ImmutableList.of(1L)))));
        totalBytes += client.getTotalPendingSize();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), "Expected application/x-presto-pages response from server but got text/plain");
        assertStatus(client, location, "queued", 0, 4, 4, 3, "not scheduled", totalBytes, totalBytes, 0, 0);

        // send unmatched token response and verify response was ignored
        callback.resetStats();
        processor.setDataResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(
                        CONTENT_TYPE, PRESTO_PAGES,
                        PRESTO_TASK_INSTANCE_ID, TASK_INSTANCE_ID,
                        PRESTO_PAGE_TOKEN, "100",
                        PRESTO_PAGE_NEXT_TOKEN, "101",
                        PRESTO_BUFFER_COMPLETE, String.valueOf(false)),
                output.slice().getInput()));
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, false, ImmutableList.of(1L)))));
        totalBytes += client.getTotalPendingSize();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), format("Error fetching http://localhost:8080/%s/0: expected token [100]; found [0]", PATH_BUFFER_DATA));
        assertStatus(client, location, "queued", 0, 5, 5, 4, "not scheduled", totalBytes, totalBytes, 0, 0);

        // send unmatched task id response and verify response was ignored
        callback.resetStats();
        String newTaskID = TASK_INSTANCE_ID + "_DIFF";
        processor.setDataResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(
                        CONTENT_TYPE, PRESTO_PAGES,
                        PRESTO_TASK_INSTANCE_ID, newTaskID,
                        PRESTO_PAGE_TOKEN, "0",
                        PRESTO_PAGE_NEXT_TOKEN, "1",
                        PRESTO_BUFFER_COMPLETE, String.valueOf(false)),
                output.slice().getInput()));
        processor.setSummaryResponse(new TestingResponse(
                HttpStatus.OK,
                ImmutableListMultimap.of(CONTENT_TYPE, MediaType.APPLICATION_JSON),
                BUFFER_SUMMARY_CODEC.toJsonBytes(new BufferSummary(TASK_INSTANCE_ID, 0, false, ImmutableList.of(1L)))));
        totalBytes += client.getTotalPendingSize();
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportErrorException.class);
        assertContains(callback.getFailure().getMessage(), format("Error fetching http://localhost:8080/%s/0: expected task id [%s]; found [%s]", PATH_BUFFER_DATA, TASK_INSTANCE_ID, newTaskID));
        assertStatus(client, location, "queued", 0, 6, 6, 5, "not scheduled", totalBytes, totalBytes, 0, 0);

        // close client and verify
        client.close();
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 6, 7, 5, "not scheduled", totalBytes, totalBytes, 0, 0);
    }

    @Test
    public void testCloseDuringPendingRequest()
            throws Exception
    {
        CyclicBarrier beforeRequest = new CyclicBarrier(2);
        CyclicBarrier afterRequest = new CyclicBarrier(2);
        StaticRequestProcessor processor = new StaticRequestProcessor(beforeRequest, afterRequest);
        processor.setSummaryResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));
        processor.setDataResponse(new TestingResponse(HttpStatus.NO_CONTENT, ImmutableListMultimap.of(), new byte[0]));

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(1, TimeUnit.MINUTES),
                location,
                callback,
                scheduler,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // send request
        client.scheduleRequest(client.getTotalPendingSize());
        beforeRequest.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "running", 0, 1, 0, 0, "PROCESSING_REQUEST", 0, 0, 0, 0);
        assertEquals(client.isRunning(), true);
        // request is pending, now close it
        client.close();

        try {
            requestComplete.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException ignored) {
        }
        try {
            afterRequest.await(10, TimeUnit.SECONDS);
        }
        catch (BrokenBarrierException ignored) {
            afterRequest.reset();
        }
        // client.close() triggers a DELETE request, so wait for it to finish
        beforeRequest.await(10, TimeUnit.SECONDS);
        afterRequest.await(10, TimeUnit.SECONDS);
        requestComplete.await(10, TimeUnit.SECONDS);
        assertStatus(client, location, "closed", 0, 1, 2, 1, "not scheduled", 0, 0, 0, 0);
    }

    @Test
    public void testExceptionFromResponseHandler()
            throws Exception
    {
        TestingTicker ticker = new TestingTicker();
        AtomicReference<Duration> tickerIncrement = new AtomicReference<>(new Duration(0, TimeUnit.SECONDS));

        TestingHttpClient.Processor processor = (input) -> {
            Duration delta = tickerIncrement.get();
            ticker.increment(delta.toMillis(), TimeUnit.MILLISECONDS);
            throw new RuntimeException("Foo");
        };

        CyclicBarrier requestComplete = new CyclicBarrier(2);
        TestingClientCallback callback = new TestingClientCallback(requestComplete);

        URI location = URI.create("http://localhost:8080");
        HttpPageBufferClient client = new HttpPageBufferClient(new TestingHttpClient(processor, scheduler),
                new DataSize(10, Unit.MEGABYTE),
                new Duration(30, TimeUnit.SECONDS),
                location,
                callback,
                scheduler,
                ticker,
                pageBufferClientCallbackExecutor,
                BUFFER_SUMMARY_CODEC);

        assertStatus(client, location, "queued", 0, 0, 0, 0, "not scheduled", 0, 0, 0, 0);

        // request processor will throw exception, verify the request is marked a completed
        // this starts the error stopwatch
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 1);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 1, 1, 1, "not scheduled", 0, 0, 0, 0);

        // advance time forward, but not enough to fail the client
        tickerIncrement.set(new Duration(30, TimeUnit.SECONDS));

        // verify that the client has not failed
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 2);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 0);
        assertStatus(client, location, "queued", 0, 2, 2, 2, "not scheduled", 0, 0, 0, 0);

        // advance time forward beyond the minimum error duration
        tickerIncrement.set(new Duration(31, TimeUnit.SECONDS));

        // verify that the client has failed
        client.scheduleRequest(client.getTotalPendingSize());
        requestComplete.await(10, TimeUnit.SECONDS);
        assertEquals(callback.getPages().size(), 0);
        assertEquals(callback.getCompletedRequests(), 3);
        assertEquals(callback.getFinishedBuffers(), 0);
        assertEquals(callback.getFailedBuffers(), 1);
        assertInstanceOf(callback.getFailure(), PageTransportTimeoutException.class);
        assertContains(
                callback.getFailure().getMessage(),
                format("%s (http://localhost:8080/%s/0 - 3 failures, failure duration 31.00s, total failed request time 31.00s)", WORKER_NODE_ERROR, PATH_BUFFER_SUMMARY));
        assertStatus(client, location, "queued", 0, 3, 3, 3, "not scheduled", 0, 0, 0, MILLISECONDS.toNanos(50));
    }

    @Test
    public void testErrorCodes()
    {
        assertEquals(new PageTooLargeException().getErrorCode(), PAGE_TOO_LARGE.toErrorCode());
        assertEquals(new PageTransportErrorException("").getErrorCode(), PAGE_TRANSPORT_ERROR.toErrorCode());
        assertEquals(new PageTransportTimeoutException(HostAddress.fromParts("127.0.0.1", 8080), "", null).getErrorCode(), PAGE_TRANSPORT_TIMEOUT.toErrorCode());
    }

    private static void assertStatus(
            HttpPageBufferClient client,
            URI location, String status,
            int pagesReceived,
            int requestsScheduled,
            int requestsCompleted,
            int requestsFailed,
            String httpRequestState,
            long reservedBytes,
            long releasedBytes,
            long receivedBytes,
            long backoffNanos)
    {
        PageBufferClientStatus actualStatus = client.getStatus();
        assertEquals(actualStatus.getUri(), location);
        assertEquals(actualStatus.getState(), status, "status");
        assertEquals(actualStatus.getPagesReceived(), pagesReceived, "pagesReceived");
        assertEquals(actualStatus.getRequestsScheduled(), requestsScheduled, "requestsScheduled");
        assertEquals(actualStatus.getRequestsCompleted(), requestsCompleted, "requestsCompleted");
        assertEquals(actualStatus.getRequestsFailed(), requestsFailed, "requestsFailed");
        assertEquals(actualStatus.getHttpRequestState(), httpRequestState, "httpRequestState");
        assertEquals(actualStatus.getReservedBytes(), reservedBytes, "reservedBytes");
        assertEquals(actualStatus.getReleasedBytes(), releasedBytes, "releasedBytes");
        assertEquals(actualStatus.getReceivedBytes(), receivedBytes, "receivedBytes");
        assertEquals(actualStatus.getBackoffNanos(), backoffNanos, "backoffNanos");
        // we should never leak a byte
        assertEquals(actualStatus.getReservedBytes(), actualStatus.getReleasedBytes());
    }

    private static void assertPageEquals(Page expectedPage, Page actualPage)
    {
        assertEquals(actualPage.getPositionCount(), expectedPage.getPositionCount());
        assertEquals(actualPage.getChannelCount(), expectedPage.getChannelCount());
    }

    private static class TestingClientCallback
            implements ClientCallback
    {
        private final CyclicBarrier done;
        private final List<SerializedPage> pages = Collections.synchronizedList(new ArrayList<>());
        private final AtomicInteger completedRequests = new AtomicInteger();
        private final AtomicInteger finishedBuffers = new AtomicInteger();
        private final AtomicInteger failedBuffers = new AtomicInteger();
        private final AtomicReference<Throwable> failure = new AtomicReference<>();
        private final AtomicLong releasedBytes = new AtomicLong();

        public TestingClientCallback(CyclicBarrier done)
        {
            this.done = done;
        }

        public List<Page> getPages()
        {
            return pages.stream()
                    .map(PAGES_SERDE::deserialize)
                    .collect(Collectors.toList());
        }

        private int getCompletedRequests()
        {
            return completedRequests.get();
        }

        private int getFinishedBuffers()
        {
            return finishedBuffers.get();
        }

        public int getFailedBuffers()
        {
            return failedBuffers.get();
        }

        public Throwable getFailure()
        {
            return failure.get();
        }

        public long getReleasedBytes()
        {
            return releasedBytes.get();
        }

        @Override
        public boolean addPages(HttpPageBufferClient client, List<SerializedPage> pages)
        {
            this.pages.addAll(pages);
            return true;
        }

        @Override
        public void requestComplete(HttpPageBufferClient client)
        {
            completedRequests.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFinished(HttpPageBufferClient client)
        {
            finishedBuffers.getAndIncrement();
            awaitDone();
        }

        @Override
        public void clientFailed(HttpPageBufferClient client, Throwable cause)
        {
            failedBuffers.getAndIncrement();
            failure.compareAndSet(null, cause);
            // requestComplete() will be called after this
        }

        @Override
        public void releaseQuota(long bytes)
        {
            releasedBytes.addAndGet(bytes);
        }

        public void resetStats()
        {
            pages.clear();
            completedRequests.set(0);
            finishedBuffers.set(0);
            failedBuffers.set(0);
            failure.set(null);
            releasedBytes.set(0);
        }

        private void awaitDone()
        {
            try {
                done.await(10, TimeUnit.SECONDS);
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

    private static class StaticRequestProcessor
            implements TestingHttpClient.Processor
    {
        private final AtomicReference<Response> summaryResponse = new AtomicReference<>();
        private final AtomicReference<Response> dataResponse = new AtomicReference<>();
        private final CyclicBarrier beforeRequest;
        private final CyclicBarrier afterRequest;

        private StaticRequestProcessor(CyclicBarrier beforeRequest, CyclicBarrier afterRequest)
        {
            this.beforeRequest = beforeRequest;
            this.afterRequest = afterRequest;
        }

        private void setSummaryResponse(Response response)
        {
            this.summaryResponse.set(response);
        }

        private void setDataResponse(Response response)
        {
            this.dataResponse.set(response);
        }

        @SuppressWarnings({"ThrowFromFinallyBlock", "Finally"})
        @Override
        public Response handle(Request request)
                throws Exception
        {
            beforeRequest.await(10, TimeUnit.SECONDS);
            String[] parts = request.getUri().toString().split("/");
            int length = parts.length;
            assertGreaterThan(parts.length, 2);

            try {
                if (parts[length - 2].equals(PATH_BUFFER_SUMMARY)) {
                    return summaryResponse.get();
                }
                else {
                    //assertEquals(parts[length - 2], PATH_BUFFER_DATA);
                    return dataResponse.get();
                }
            }
            finally {
                afterRequest.await(10, TimeUnit.SECONDS);
            }
        }
    }
}
