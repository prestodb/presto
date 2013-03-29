package com.facebook.presto.client;

import com.facebook.presto.PrestoHeaders;
import com.facebook.presto.cli.ClientSession;
import com.google.common.base.Charsets;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.JsonResponseHandler;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.http.client.HttpStatus.Family;
import static io.airlift.http.client.HttpStatus.familyForStatusCode;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;
import static io.airlift.http.client.StatusResponseHandler.StatusResponse;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;

@ThreadSafe
public class StatementClient
        implements Closeable
{
    private final AsyncHttpClient httpClient;
    private final JsonResponseHandler<QueryResults> responseHandler;
    private final boolean debug;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();

    public StatementClient(AsyncHttpClient httpClient, JsonCodec<QueryResults> queryResultsCodec, ClientSession session, String query)
    {
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");
        checkNotNull(session, "session is null");
        checkNotNull(query, "query is null");

        this.httpClient = httpClient;
        this.responseHandler = createJsonResponseHandler(queryResultsCodec);
        this.debug = session.isDebug();
        this.query = query;

        Request request = buildQueryRequest(session, query);
        currentResults.set(httpClient.execute(request, responseHandler));
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

    public QueryResults current()
    {
        return currentResults.get();
    }

    public boolean hasNext()
    {
        return (!isClosed()) && (current().getNext() != null);
    }

    public synchronized QueryResults next()
    {
        checkState(!isClosed(), "client is closed");
        checkState(hasNext(), "no next");

        // TODO: retry on error
        Request request = prepareGet().setUri(current().getNext()).build();
        QueryResults results = httpClient.execute(request, responseHandler);
        currentResults.set(results);
        return results;
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
            URI uri = current().getNext();
            if (uri != null) {
                Request request = prepareDelete().setUri(uri).build();
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }
}
