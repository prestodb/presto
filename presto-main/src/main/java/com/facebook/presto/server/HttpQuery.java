/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.operator.Page;
import com.facebook.presto.serde.PagesSerde;
import com.facebook.presto.slice.InputStreamSliceInput;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.Request;
import io.airlift.http.client.Response;
import io.airlift.http.client.ResponseHandler;
import io.airlift.http.client.UnexpectedResponseException;
import io.airlift.log.Logger;

import javax.annotation.concurrent.GuardedBy;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response.Status;
import java.net.URI;
import java.util.Iterator;
import java.util.concurrent.Future;

import static com.facebook.presto.server.PrestoMediaTypes.PRESTO_PAGES_TYPE;
import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.Request.Builder.prepareGet;

public class HttpQuery
        implements QueryDriver
{
    private static final Logger log = Logger.get(HttpQuery.class);
    private final URI location;
    private final QueryState queryState;
    private final AsyncHttpClient httpClient;

    @GuardedBy("this")
    private boolean done;

    @GuardedBy("this")
    private Future<Void> currentRequest;

    public HttpQuery(URI location, QueryState queryState, AsyncHttpClient httpClient)
    {
        checkNotNull(location, "location is null");
        checkNotNull(queryState, "queryState is null");
        checkNotNull(httpClient, "httpClient is null");

        this.location = location;
        this.queryState = queryState;
        this.httpClient = httpClient;
    }

    @Override
    public synchronized void start()
    {
        Preconditions.checkState(!done, "Query is already finished");
        currentRequest = httpClient.execute(prepareGet().setUri(location).build(), new PageResponseHandler(location));
    }

    @Override
    public synchronized boolean isDone()
    {
        return done;
    }

    @Override
    public synchronized void cancel()
    {
        if (!done) {
            queryState.sourceFinished();
            done = true;
            if (currentRequest != null) {
                currentRequest.cancel(true);
                currentRequest = null;
            }
        }
    }

    private synchronized void done()
    {
        if (!done) {
            queryState.sourceFinished();
            done = true;
            currentRequest = null;
        }
    }

    private synchronized void fail(Throwable throwable)
    {
        done = true;
        queryState.queryFailed(throwable);
        // todo send delete command
    }

    private synchronized void setCurrentRequest(Future<Void> currentRequest)
    {
        this.currentRequest = currentRequest;
    }

    public class PageResponseHandler
            implements ResponseHandler<Void, RuntimeException>
    {
        private final URI queryUri;

        public PageResponseHandler(URI queryUri)
        {
            this.queryUri = queryUri;
        }

        @Override
        public RuntimeException handleException(Request request, Exception exception)
        {
            // reschedule on error
            log.warn(exception, "Error fetching pages from  %s", request.getUri());
            rescheduleRequest();
            throw Throwables.propagate(exception);
        }

        @Override
        public Void handle(Request request, Response response)
        {
            if (isDone()) {
                return null;
            }

            try {
                // job is finished when we get a GONE response
                if (response.getStatusCode() == Status.GONE.getStatusCode()) {
                    done();
                    return null;
                }
                // no content means no content was created within the wait period, but query is still ok
                if (response.getStatusCode() != Status.NO_CONTENT.getStatusCode()) {
                    // otherwise we must have gotten an OK response, everything else is considered fatal
                    if (response.getStatusCode() != Status.OK.getStatusCode()) {
                        fail(new UnexpectedResponseException(
                                String.format("Expected response code to be 200, but was %d: %s", response.getStatusCode(), response.getStatusMessage()),
                                request,
                                response));
                        return null;
                    }

                    String contentType = response.getHeader("Content-Type");
                    if (!MediaType.valueOf(contentType).isCompatible(PRESTO_PAGES_TYPE)) {
                        throw new UnexpectedResponseException(String.format("Expected %s response from server but got %s", PRESTO_PAGES_TYPE, contentType), request, response);
                    }

                    InputStreamSliceInput sliceInput = new InputStreamSliceInput(response.getInputStream());

                    Iterator<Page> pageIterator = PagesSerde.readPages(sliceInput);
                    while (pageIterator.hasNext()) {
                        Page page = pageIterator.next();
                        queryState.addPage(page);
                    }
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                fail(e);
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                // reschedule on error
                log.warn(e, "Error fetching pages from  %s: status: %d %s", request.getUri(), response.getStatusCode(), response.getStatusMessage());
            }
            rescheduleRequest();
            return null;
        }

        private void rescheduleRequest()
        {
            setCurrentRequest(httpClient.execute(prepareGet().setUri(queryUri).build(), this));
        }
    }
}
