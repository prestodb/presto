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
package com.facebook.presto.client;

import com.google.common.base.Charsets;
import com.google.common.base.Objects;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

@ThreadSafe
public class StatementClient
        implements Closeable
{
    private static final String USER_AGENT_VALUE = StatementClient.class.getSimpleName() +
            "/" +
            Objects.firstNonNull(StatementClient.class.getPackage().getImplementationVersion(), "unknown");

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
        if (session.getSource() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }
        if (session.getCatalog() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE);

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
        if (isClosed() || (current().getNextUri() == null)) {
            valid.set(false);
            return false;
        }

        Request request = prepareGet()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(current().getNextUri())
                .build();

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        do {
            // back-off on retry
            if (attempts > 0) {
                sleepUninterruptibly(attempts * 100, MILLISECONDS);
            }
            attempts++;

            JsonResponse<QueryResults> response;
            try {
                response = httpClient.execute(request, responseHandler);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                currentResults.set(response.getValue());
                return true;
            }

            if (response.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                gone.set(true);
                throw new RuntimeException(format("Error fetching next at %s returned %s: %s",
                        request.getUri(),
                        response.getStatusCode(),
                        response.getStatusMessage()));
            }
        }
        while ((System.nanoTime() - start) < MINUTES.toNanos(2) && !isClosed());

        gone.set(true);
        throw new RuntimeException("Error fetching next", cause);
    }

    public boolean cancelLeafStage()
    {
        checkState(!isClosed(), "client is closed");

        URI uri = current().getPartialCancelUri();
        if (uri == null) {
            return false;
        }

        Request request = prepareDelete()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(uri)
                .build();
        StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
        return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                Request request = prepareDelete()
                        .setHeader(USER_AGENT, USER_AGENT_VALUE)
                        .setUri(uri)
                        .build();
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }
}
