/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.serde.PagesSerde;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.net.MediaType;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.AsyncHttpClient.AsyncHttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.HttpUriBuilder;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import io.airlift.slice.InputStreamSliceInput;
import io.airlift.units.DataSize;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import java.io.Closeable;
import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_MAX_SIZE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PAGE_SEQUENCE_ID;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

@ThreadSafe
public class HttpPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(HttpPageBufferClient.class);
    private final Executor executor;

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or bufferFinished.  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     * <p/>
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        void addPage(HttpPageBufferClient client, Page page);

        void requestComplete(HttpPageBufferClient client);

        void clientFinished(HttpPageBufferClient client);
    }

    private final AsyncHttpClient httpClient;
    private final DataSize maxResponseSize;
    private final URI location;
    private final ClientCallback clientCallback;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private AsyncHttpResponseFuture<PagesResponse, RuntimeException> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();
    @GuardedBy("this")
    private long sequenceId;

    private final AtomicInteger pagesReceived = new AtomicInteger();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();

    public HttpPageBufferClient(AsyncHttpClient httpClient, DataSize maxResponseSize, URI location, ClientCallback clientCallback, Executor executor)
    {
        this.httpClient = checkNotNull(httpClient, "httpClient is null");
        this.maxResponseSize = checkNotNull(maxResponseSize, "maxResponseSize is null");
        this.location = checkNotNull(location, "location is null");
        this.clientCallback = checkNotNull(clientCallback, "clientCallback is null");
        this.executor = checkNotNull(executor, "executor is null");
    }

    public synchronized PageBufferClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        }
        else if (future != null) {
            state = "running";
        }
        else {
            state = "queued";
        }
        String httpRequestState = "queued";
        if (future != null) {
            httpRequestState = future.getState();
        }
        return new PageBufferClientStatus(location, state, lastUpdate, pagesReceived.get(), requestsScheduled.get(), requestsCompleted.get(), httpRequestState);
    }

    public synchronized boolean isRunning()
    {
        return future != null;
    }

    public synchronized long getSequenceId()
    {
        return sequenceId;
    }

    public void close()
    {
        Future<?> future;
        synchronized (this) {
            closed = true;

            future = this.future;
            this.future = null;

            lastUpdate = DateTime.now();
        }

        if (future != null) {
            future.cancel(true);
        }

        // abort the output buffer on the remote node; response of delete is ignored
        httpClient.executeAsync(prepareDelete().setUri(location).build(), createStatusResponseHandler());
    }

    public synchronized void scheduleRequest()
    {
        if (closed) {
            log.debug("scheduleRequest() called, but client has been closed");
            return;
        }
        if (future != null) {
            log.debug("scheduleRequest() called, but future is not null");
            return;
        }

        URI uri = HttpUriBuilder.uriBuilderFrom(location).appendPath(String.valueOf(sequenceId)).build();
        future = httpClient.executeAsync(prepareGet()
                .setHeader(PRESTO_MAX_SIZE, maxResponseSize.toString())
                .setUri(uri).build(), new PageResponseHandler());

        Futures.addCallback(future, new FutureCallback<PagesResponse>()
        {
            @Override
            public void onSuccess(PagesResponse result)
            {
                if (Thread.holdsLock(HttpPageBufferClient.this)) {
                    log.error("Can not handle callback while holding a lock on this");
                }

                requestsCompleted.incrementAndGet();

                List<Page> pages;
                synchronized (HttpPageBufferClient.this) {
                    pages = result.getPages(sequenceId);
                    sequenceId += pages.size();
                }

                // add pages
                for (Page page : pages) {
                    pagesReceived.incrementAndGet();
                    clientCallback.addPage(HttpPageBufferClient.this, page);
                }

                // complete request or close client
                if (result.isClientClosed()) {
                    synchronized (HttpPageBufferClient.this) {
                        closed = true;
                        future = null;
                        lastUpdate = DateTime.now();
                    }
                    clientCallback.clientFinished(HttpPageBufferClient.this);
                }
                else {
                    synchronized (HttpPageBufferClient.this) {
                        future = null;
                        lastUpdate = DateTime.now();
                    }
                    clientCallback.requestComplete(HttpPageBufferClient.this);
                }
            }

            @Override
            public void onFailure(Throwable t)
            {
                if (Thread.holdsLock(HttpPageBufferClient.this)) {
                    log.error("Can not handle callback while holding a lock on this");
                }

                requestsCompleted.incrementAndGet();
                synchronized (HttpPageBufferClient.this) {
                    future = null;
                    lastUpdate = DateTime.now();
                }
                clientCallback.requestComplete(HttpPageBufferClient.this);
            }
        }, executor);

        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        HttpPageBufferClient that = (HttpPageBufferClient) o;

        if (!location.equals(that.location)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        return location.hashCode();
    }

    @Override
    public String toString()
    {
        String state;
        synchronized (this) {
            if (closed) {
                state = "CLOSED";
            }
            else if (future != null) {
                state = "RUNNING";
            }
            else {
                state = "QUEUED";
            }
        }
        return Objects.toStringHelper(this)
                .add("location", location)
                .addValue(state)
                .toString();
    }

    public static class PageResponseHandler
            implements ResponseHandler<PagesResponse, RuntimeException>
    {
        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            throw Throwables.propagate(exception);
        }

        @Override
        public PagesResponse handle(Request request, Response response)
        {
            String sequenceIdHeader = response.getHeader(PRESTO_PAGE_SEQUENCE_ID);
            if (sequenceIdHeader == null) {
                throw new IllegalStateException("Expected " + PRESTO_PAGE_SEQUENCE_ID + " header");
            }
            long startingSequenceId = Long.parseLong(sequenceIdHeader);

            // job is finished when we get a GONE response
            if (response.getStatusCode() == HttpStatus.GONE.code()) {
                return PagesResponse.createClosedResponse(startingSequenceId);
            }

            // no content means no content was created within the wait period, but query is still ok
            if (response.getStatusCode() == HttpStatus.NO_CONTENT.code()) {
                return PagesResponse.createEmptyPagesResponse(startingSequenceId);
            }

            // otherwise we must have gotten an OK response, everything else is considered fatal
            if (response.getStatusCode() != HttpStatus.OK.code()) {
                log.debug("Expected response code to be 200, but was %s: request=%s, response=%s", response.getStatusCode(), request, response);
                return PagesResponse.createEmptyPagesResponse(startingSequenceId);
            }

            String contentType = response.getHeader(CONTENT_TYPE);
            if (contentType == null || !MediaType.parse(contentType).is(PRESTO_PAGES_TYPE)) {
                // this can happen when an error page is returned, but is unlikely given the above 200
                log.debug("Expected %s response from server but got %s: uri=%s, response=%s", PRESTO_PAGES_TYPE, contentType, request.getUri(), response);
                return PagesResponse.createEmptyPagesResponse(startingSequenceId);
            }

            try {
                InputStreamSliceInput sliceInput = new InputStreamSliceInput(response.getInputStream());
                return PagesResponse.createPagesResponse(startingSequenceId, ImmutableList.copyOf(PagesSerde.readPages(sliceInput)));
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    public static class PagesResponse
    {
        public static PagesResponse createPagesResponse(long startingSequenceId, Iterable<Page> pages)
        {
            return new PagesResponse(startingSequenceId, pages, false);
        }

        public static PagesResponse createEmptyPagesResponse(long startingSequenceId)
        {
            return new PagesResponse(startingSequenceId, ImmutableList.<Page>of(), false);
        }

        public static PagesResponse createClosedResponse(long startingSequenceId)
        {
            return new PagesResponse(startingSequenceId, ImmutableList.<Page>of(), true);
        }

        private final long startingSequenceId;
        private final List<Page> pages;
        private final boolean clientClosed;

        public PagesResponse(long startingSequenceId, Iterable<Page> pages, boolean clientClosed)
        {
            this.startingSequenceId = startingSequenceId;
            this.pages = ImmutableList.copyOf(pages);
            this.clientClosed = clientClosed;
        }

        public List<Page> getPages(long sequenceId)
        {
            // if sequenceId is before the start of this response, return an empty list
            if (sequenceId < startingSequenceId) {
                log.warn("Unexpected response: expected startingSequenceId %s to be less than or equal to %s", startingSequenceId, sequenceId);
                // this will cause the request to be resent with the existing sequence id
                return ImmutableList.of();
            }

            // if sequenceId is after the end of this response, return and empty list
            int startOffset = Ints.saturatedCast(sequenceId - startingSequenceId);
            if (startOffset >= pages.size()) {
                // most likely a duplicate of an old request
                return ImmutableList.of();
            }

            // return the pages from the requested sequenceId to the end of this response
            return pages.subList(startOffset, pages.size());
        }

        public boolean isClientClosed()
        {
            return clientClosed;
        }
    }
}
