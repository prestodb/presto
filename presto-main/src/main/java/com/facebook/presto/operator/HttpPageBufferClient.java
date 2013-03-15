/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.operator;

import com.facebook.presto.operator.ExchangeOperator.ExchangeClientStatus;
import com.facebook.presto.serde.PagesSerde;
import com.facebook.presto.slice.InputStreamSliceInput;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.net.MediaType;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.log.Logger;
import org.joda.time.DateTime;

import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;
import javax.ws.rs.core.Response.Status;
import java.io.Closeable;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;

import static com.facebook.presto.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

@ThreadSafe
public class HttpPageBufferClient
        implements Closeable
{
    private static final Logger log = Logger.get(HttpPageBufferClient.class);

    /**
     * For each request, the addPage method will be called zero or more times,
     * followed by either requestComplete or bufferFinished.  If the client is
     * closed, requestComplete or bufferFinished may never be called.
     *
     * <b>NOTE:</b> Implementations of this interface are not allowed to perform
     * blocking operations.
     */
    public interface ClientCallback
    {
        void addPage(HttpPageBufferClient client, Page page);

        void requestComplete(HttpPageBufferClient client);

        void bufferFinished(HttpPageBufferClient client);
    }

    private final AsyncHttpClient httpClient;
    private final URI location;
    private final ClientCallback clientCallback;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private Future<?> future;
    @GuardedBy("this")
    private DateTime lastUpdate = DateTime.now();

    private final AtomicInteger requestsScheduled = new AtomicInteger();
    private final AtomicInteger requestsCompleted = new AtomicInteger();

    public HttpPageBufferClient(AsyncHttpClient httpClient, URI location, ClientCallback clientCallback)
    {
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(location, "location is null");
        Preconditions.checkNotNull(clientCallback, "clientCallback is null");

        this.httpClient = httpClient;
        this.location = location;
        this.clientCallback = clientCallback;
    }

    public synchronized ExchangeClientStatus getStatus()
    {
        String state;
        if (closed) {
            state = "closed";
        } else if (future != null) {
            state = "running";
        } else {
            state = "queued";
        }
        return new ExchangeClientStatus(location, state, lastUpdate, requestsScheduled.get(), requestsCompleted.get());
    }

    public synchronized boolean isRunning()
    {
        return future != null;
    }

    public synchronized boolean isClosed()
    {
        return closed;
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
        Preconditions.checkState(!closed, getClass().getSimpleName() + " is closed");
        Preconditions.checkState(future == null, getClass().getSimpleName() + " is already running");

        future = httpClient.executeAsync(prepareGet().setUri(location).build(), new PageResponseHandler());
        lastUpdate = DateTime.now();
        requestsScheduled.incrementAndGet();
    }

    private void requestComplete()
    {
        synchronized (this) {
            future = null;
            lastUpdate = DateTime.now();
        }

        clientCallback.requestComplete(this);
    }


    private void bufferFinished()
    {
        synchronized (this) {
            closed = true;
            future = null;
            lastUpdate = DateTime.now();
        }

        clientCallback.bufferFinished(this);
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

    public class PageResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            requestsCompleted.incrementAndGet();

            log.warn(exception, "Error fetching pages from  %s", request.getUri());
            requestComplete();
            throw Throwables.propagate(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            requestsCompleted.incrementAndGet();

            // job is finished when we get a GONE response
            if (response.getStatusCode() == Status.GONE.getStatusCode()) {
                bufferFinished();
                return null;
            }

            try {
                // no content means no content was created within the wait period, but query is still ok
                if (response.getStatusCode() != Status.NO_CONTENT.getStatusCode()) {
                    // otherwise we must have gotten an OK response, everything else is considered fatal
                    if (response.getStatusCode() != Status.OK.getStatusCode()) {
                        log.debug("Expected response code to be 200, but was %s: request=%s, response=%s", response.getStatusCode(), request, response);
                        return null;
                    }

                    String contentType = response.getHeader("Content-Type");
                    if (!MediaType.parse(contentType).is(PRESTO_PAGES_TYPE)) {
                        // this can happen when an error page is returned, but is unlikely given the above 200
                        log.debug("Expected %s response from server but got %s: uri=%s, response=%s", PRESTO_PAGES_TYPE, contentType, location, response);
                        return null;
                    }

                    InputStreamSliceInput sliceInput = new InputStreamSliceInput(response.getInputStream());

                    Iterator<Page> pageIterator = PagesSerde.readPages(sliceInput);
                    while (pageIterator.hasNext()) {
                        if (isClosed()) {
                            // finally block, calls requestComplete even though it is not required by the api
                            return null;
                        }
                        Page page = pageIterator.next();
                        clientCallback.addPage(HttpPageBufferClient.this, page);
                    }
                }
            }
            catch (Exception e) {
                // reschedule on error
                log.warn(e, "Error fetching pages from  %s: status: %s %s", request.getUri(), response.getStatusCode(), response.getStatusMessage());
            }
            finally {
                requestComplete();
            }
            return null;
        }
    }
}
