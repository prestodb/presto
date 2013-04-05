package com.facebook.presto.client;

import com.google.common.base.Charsets;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpStatus.Family;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static java.lang.String.format;

@ThreadSafe
public class StatementClient
        implements Closeable
{
    private final AsyncHttpClient httpClient;
    private final FullJsonResponseHandler<QueryResults> responseHandler;
    private final boolean debug;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();
    private final AtomicBoolean valid = new AtomicBoolean(true);

    public StatementClient(AsyncHttpClient httpClient, JsonCodec<QueryResults> queryResultsCodec, ClientSession session, String query)
    {
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");
        checkNotNull(session, "session is null");
        checkNotNull(query, "query is null");

        this.httpClient = httpClient;
        this.responseHandler = createFullJsonResponseHandler(queryResultsCodec);
        this.debug = session.isDebug();
        this.query = query;

        Request request = buildQueryRequest(session, query);
        currentResults.set(httpClient.execute(request, responseHandler).getValue());
    }

    private static Request buildQueryRequest(ClientSession session, String query)
    {
        Request.Builder builder = preparePost()
                .setUri(uriBuilderFrom(session.getServer()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(query, Charsets.UTF_8));

        if (session.getUser() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_USER, session.getUser());
        }
        if (session.getCatalog() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }

        return builder.build();
    }

    public String getQuery()
    {
        return query;
    }

    public boolean isDebug()
    {
        return debug;
    }

    public boolean isClosed()
    {
        return closed.get();
    }

    public boolean isGone()
    {
        return gone.get();
    }

    public boolean isFailed()
    {
        return currentResults.get().getError() != null;
    }

    public QueryResults current()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    public QueryResults finalResults()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentResults.get();
    }

    public boolean isValid()
    {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    public boolean advance()
    {
        if (isClosed() || (current().getNext() == null)) {
            valid.set(false);
            return false;
        }

        // TODO: retry on error and handle 503
        Request request = prepareGet().setUri(current().getNext()).build();
        JsonResponse<QueryResults> response;
        try {
            response = httpClient.execute(request, responseHandler);
        }
        catch (RuntimeException e) {
            gone.set(true);
            throw new RuntimeException("Error fetching next", e);
        }

        if (response.hasValue()) {
            currentResults.set(response.getValue());
            return true;
        }

        gone.set(true);
        throw new RuntimeException(format("Error fetching next: %s %s: %s",
                response.getStatusCode(),
                response.getStatusMessage(),
                response.getException()));
    }

    public boolean cancelLeafStage()
    {
        checkState(!isClosed(), "client is closed");

        URI uri = current().getPartialCancelUri();
        if (uri == null) {
            return false;
        }

        Request request = prepareDelete().setUri(uri).build();
        StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
        return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true)) {
            URI uri = currentResults.get().getNext();
            if (uri != null) {
                Request request = prepareDelete().setUri(uri).build();
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }
}
