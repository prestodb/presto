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

import com.facebook.presto.client.OkHttpUtil.NullCallback;
import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.json.JsonCodec;
import okhttp3.Headers;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.io.Closeable;
import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_ADDED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLIENT_INFO;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_DEALLOCATED_PREPARE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_LANGUAGE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_PREPARED_STATEMENT;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_CATALOG;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SCHEMA;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SOURCE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_STARTED_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TIME_ZONE;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_TRANSACTION_ID;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.NANOSECONDS;

@ThreadSafe
public class StatementClient
        implements Closeable
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json; charset=utf-8");
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);

    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClient.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClient.class.getPackage().getImplementationVersion(), "unknown");

    private final OkHttpClient httpClient;
    private final boolean debug;
    private final String query;
    private final AtomicReference<QueryResults> currentResults = new AtomicReference<>();
    private final AtomicReference<String> setCatalog = new AtomicReference<>();
    private final AtomicReference<String> setSchema = new AtomicReference<>();
    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final Map<String, String> addedPreparedStatements = new ConcurrentHashMap<>();
    private final Set<String> deallocatedPreparedStatements = Sets.newConcurrentHashSet();
    private final AtomicReference<String> startedTransactionId = new AtomicReference<>();
    private final AtomicBoolean clearTransactionId = new AtomicBoolean();
    private final AtomicBoolean closed = new AtomicBoolean();
    private final AtomicBoolean gone = new AtomicBoolean();
    private final AtomicBoolean valid = new AtomicBoolean(true);
    private final TimeZoneKey timeZone;
    private final long requestTimeoutNanos;
    private final String user;

    public StatementClient(OkHttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = httpClient;
        this.debug = session.isDebug();
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);

        JsonResponse<QueryResults> response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
        if ((response.getStatusCode() != HTTP_OK) || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        processResponse(response.getHeaders(), response.getValue());
    }

    private Request buildQueryRequest(ClientSession session, String query)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v1/statement").build();

        Request.Builder builder = prepareRequest(url)
                .post(RequestBody.create(MEDIA_TYPE_JSON, query));

        if (session.getSource() != null) {
            builder.addHeader(PRESTO_SOURCE, session.getSource());
        }
        if (session.getClientInfo() != null) {
            builder.addHeader(PRESTO_CLIENT_INFO, session.getClientInfo());
        }
        if (session.getCatalog() != null) {
            builder.addHeader(PRESTO_CATALOG, session.getCatalog());
        }
        if (session.getSchema() != null) {
            builder.addHeader(PRESTO_SCHEMA, session.getSchema());
        }
        builder.addHeader(PRESTO_TIME_ZONE, session.getTimeZone().getId());
        if (session.getLocale() != null) {
            builder.addHeader(PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        }

        Map<String, String> property = session.getProperties();
        for (Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        Map<String, String> statements = session.getPreparedStatements();
        for (Entry<String, String> entry : statements.entrySet()) {
            builder.addHeader(PRESTO_PREPARED_STATEMENT, urlEncode(entry.getKey()) + "=" + urlEncode(entry.getValue()));
        }

        builder.addHeader(PRESTO_TRANSACTION_ID, session.getTransactionId() == null ? "NONE" : session.getTransactionId());

        return builder.build();
    }

    public String getQuery()
    {
        return query;
    }

    public TimeZoneKey getTimeZone()
    {
        return timeZone;
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

    public QueryStatusInfo currentStatusInfo()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    public QueryData currentData()
    {
        checkState(isValid(), "current position is not valid (cursor past end)");
        return currentResults.get();
    }

    public QueryStatusInfo finalStatusInfo()
    {
        checkState((!isValid()) || isFailed(), "current position is still valid");
        return currentResults.get();
    }

    public Optional<String> getSetCatalog()
    {
        return Optional.ofNullable(setCatalog.get());
    }

    public Optional<String> getSetSchema()
    {
        return Optional.ofNullable(setSchema.get());
    }

    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    public Map<String, String> getAddedPreparedStatements()
    {
        return ImmutableMap.copyOf(addedPreparedStatements);
    }

    public Set<String> getDeallocatedPreparedStatements()
    {
        return ImmutableSet.copyOf(deallocatedPreparedStatements);
    }

    @Nullable
    public String getStartedTransactionId()
    {
        return startedTransactionId.get();
    }

    public boolean isClearTransactionId()
    {
        return clearTransactionId.get();
    }

    public boolean isValid()
    {
        return valid.get() && (!isGone()) && (!isClosed());
    }

    private Request.Builder prepareRequest(HttpUrl url)
    {
        return new Request.Builder()
                .addHeader(PRESTO_USER, user)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .url(url);
    }

    public boolean advance()
    {
        URI nextUri = currentStatusInfo().getNextUri();
        if (isClosed() || (nextUri == null)) {
            valid.set(false);
            return false;
        }

        Request request = prepareRequest(HttpUrl.get(nextUri)).build();

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
                response = JsonResponse.execute(QUERY_RESULTS_CODEC, httpClient, request);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if ((response.getStatusCode() == HTTP_OK) && response.hasValue()) {
                processResponse(response.getHeaders(), response.getValue());
                return true;
            }

            if (response.getStatusCode() != HTTP_UNAVAILABLE) {
                throw requestFailedException("fetching next", request, response);
            }
        }
        while (((System.nanoTime() - start) < requestTimeoutNanos) && !isClosed());

        gone.set(true);
        throw new RuntimeException("Error fetching next", cause);
    }

    private void processResponse(Headers headers, QueryResults results)
    {
        setCatalog.set(headers.get(PRESTO_SET_CATALOG));
        setSchema.set(headers.get(PRESTO_SET_SCHEMA));

        for (String setSession : headers.values(PRESTO_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), keyValue.get(1));
        }
        resetSessionProperties.addAll(headers.values(PRESTO_CLEAR_SESSION));

        for (String entry : headers.values(PRESTO_ADDED_PREPARE)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(entry);
            if (keyValue.size() != 2) {
                continue;
            }
            addedPreparedStatements.put(urlDecode(keyValue.get(0)), urlDecode(keyValue.get(1)));
        }
        for (String entry : headers.values(PRESTO_DEALLOCATED_PREPARE)) {
            deallocatedPreparedStatements.add(urlDecode(entry));
        }

        String startedTransactionId = headers.get(PRESTO_STARTED_TRANSACTION_ID);
        if (startedTransactionId != null) {
            this.startedTransactionId.set(startedTransactionId);
        }
        if (headers.get(PRESTO_CLEAR_TRANSACTION_ID) != null) {
            clearTransactionId.set(true);
        }

        currentResults.set(results);
    }

    private RuntimeException requestFailedException(String task, Request request, JsonResponse<QueryResults> response)
    {
        gone.set(true);
        if (!response.hasValue()) {
            if (response.getStatusCode() == HTTP_UNAUTHORIZED) {
                return new ClientException("Authentication failed" +
                        Optional.ofNullable(response.getStatusMessage())
                                .map(message -> ": " + message)
                                .orElse(""));
            }
            return new RuntimeException(
                    format("Error %s at %s returned an invalid response: %s [Error: %s]", task, request.url(), response, response.getResponseBody()),
                    response.getException());
        }
        return new RuntimeException(format("Error %s at %s returned HTTP %s", task, request.url(), response.getStatusCode()));
    }

    public void cancelLeafStage()
    {
        checkState(!isClosed(), "client is closed");

        URI uri = currentStatusInfo().getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public void close()
    {
        if (!closed.getAndSet(true)) {
            URI uri = currentResults.get().getNextUri();
            if (uri != null) {
                httpDelete(uri);
            }
        }
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri))
                .delete()
                .build();
        httpClient.newCall(request).enqueue(new NullCallback());
    }

    private static String urlEncode(String value)
    {
        try {
            return URLEncoder.encode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }

    private static String urlDecode(String value)
    {
        try {
            return URLDecoder.decode(value, "UTF-8");
        }
        catch (UnsupportedEncodingException e) {
            throw new AssertionError(e);
        }
    }
}
