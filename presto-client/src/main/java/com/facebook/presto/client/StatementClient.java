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

import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpClient.HttpResponseFuture;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;

import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
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
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class StatementClient
        implements Closeable
{
    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClient.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClient.class.getPackage().getImplementationVersion(), "unknown");

    private final HttpClient httpClient;
    private final FullJsonResponseHandler<QueryResults> responseHandler;
    private final boolean debug;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedtransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final String timeZoneId;
    private final long requestTimeoutNanos;
    private final String user;

    public StatementClient(HttpClient httpClient, JsonCodec<QueryResults> queryResultsCodec, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(queryResultsCodec, "queryResultsCodec is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.responseHandler = createFullJsonResponseHandler(queryResultsCodec);
        this.debug = session.isDebug();
        this.timeZoneId = session.getTimeZoneId();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);
        JsonResponse<QueryResults> response = httpClient.execute(request, responseHandler);

        if (response.getStatusCode() != HttpStatus.OK.code() || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        processResponse(response);
    }

    private Request buildQueryRequest(ClientSession session, String query)
    {
        Request.Builder builder = prepareRequest(preparePost(), uriBuilderFrom(session.getServer()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

        if (session.getSource() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SOURCE, session.getSource());
        }
        if (session.getCatalog() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.setHeader(PrestoHeaders.PRESTO_SCHEMA, session.getSchema());
        }
        builder.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
        builder.setHeader(PrestoHeaders.PRESTO_LANGUAGE, session.getLocale().toLanguageTag());

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        builder.setHeader(PrestoHeaders.PRESTO_TRANSACTION_ID, session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        return builder.build();
    }

    public String getQuery()
    {
        return query;
    }

    public String getTimeZoneId()
    {
        return timeZoneId;
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

    public StatementStats getStats()
    {
        return currentResults.get().getStats();
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

    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    public String getStartedtransactionId()
    {
        return startedtransactionId.get();
    }

    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    public boolean isValid()
    {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    private Request.Builder prepareRequest(Request.Builder builder, URI nextUri)
    {
        builder.setHeader(PrestoHeaders.PRESTO_USER, user);
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(nextUri);

        return builder;
    }

    public boolean advance()
    {
        URI nextUri = current().getNextUri();
        if (isClosed() || (nextUri == null)) {
            valid.set(false);
            return false;
        }

        Request request = prepareRequest(prepareGet(), nextUri).build();

        Exception cause = null;
        long start = System.nanoTime();
        long attempts = 0;

        do {
            // back-off on retry
            if (attempts > 0) {
                try {
                    MILLISECONDS.sleep(attempts * 100);
                }
                catch (InterruptedException e) {
                    try {
                        close();
                    }
                    finally {
                        Thread.currentThread().interrupt();
                    }
                    throw new RuntimeException("StatementClient thread was interrupted");
                }
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
                processResponse(response);
                return true;
            }

            if (response.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                throw requestFailedException("fetching next", request, response);
            }
        }
        while (((System.nanoTime() - start) < requestTimeoutNanos) && !isClosed());

        gone.set(true);
        throw new RuntimeException("Error fetching next", cause);
    }

    private void processResponse(JsonResponse<QueryResults> response)
    {
        for (String setSession : response.getHeaders(PRESTO_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), keyValue.size() > 1 ? keyValue.get(1) : "");
        }
        for (String clearSession : response.getHeaders(PRESTO_CLEAR_SESSION)) {
            resetSessionProperties.add(clearSession);
        }

        String startedTransactionId = response.getHeader(PRESTO_STARTED_TRANSACTION_ID);
        if (startedTransactionId != null) {
            this.startedtransactionId.set(startedTransactionId);
        }
        if (response.getHeader(PRESTO_CLEAR_TRANSACTION_ID) != null) {
            clearTransactionId.set(true);
        }

        currentResults.set(response.getValue());
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<QueryResults> response)
    {
        gone.set(true);
        if (!response.hasValue()) {
            return new RuntimeException(
                    format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.getUri(), response, response.getResponseBody()),
                    response.getException());
        }
        return new RuntimeException(format("Error %s at %s returned %s: %s", task, request.getUri(), response.getStatusCode(), response.getStatusMessage()));
    }

    public boolean cancelLeafStage(Duration timeout)
    {
        checkState(!isClosed(), "client is closed");

        URI uri = current().getPartialCancelUri();
        if (uri == null) {
            return false;
        }

        Request request = prepareRequest(prepareDelete(), uri).build();

        HttpResponseFuture<StatusResponse> response = httpClient.executeAsync(request, createStatusResponseHandler());
        try {
            StatusResponse status = response.get(timeout.toMillis(), MILLISECONDS);
            return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
        catch (TimeoutException e) {
            return false;
        }
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                Request request = prepareRequest(prepareDelete(), uri).build();
                httpClient.executeAsync(request, createStatusResponseHandler());
            }
        }
    }
}
