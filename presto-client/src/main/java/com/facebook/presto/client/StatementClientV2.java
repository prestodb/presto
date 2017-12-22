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

import com.facebook.presto.spi.type.TimeZoneKey;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.json.JsonCodec;
import io.airlift.units.Duration;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import okhttp3.Response;
import okhttp3.ResponseBody;

import javax.annotation.Nullable;
import javax.annotation.concurrent.GuardedBy;
import javax.annotation.concurrent.ThreadSafe;

import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_DATA_NEXT_URI;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_USER;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.LOCATION;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Futures.immediateFuture;
import static io.airlift.json.JsonCodec.jsonCodec;
import static java.net.HttpURLConnection.HTTP_NO_CONTENT;
import static java.net.HttpURLConnection.HTTP_OK;
import static java.net.HttpURLConnection.HTTP_UNAUTHORIZED;
import static java.net.HttpURLConnection.HTTP_UNAVAILABLE;
import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.NANOSECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@ThreadSafe
public class StatementClientV2
        implements StatementClient
{
    private static final MediaType MEDIA_TYPE_JSON = MediaType.parse("application/json");
    private static final JsonCodec<CreateQueryRequest> CREATE_QUERY_REQUEST_JSON_CODEC = jsonCodec(CreateQueryRequest.class);
    private static final JsonCodec<QueryResults> QUERY_RESULTS_CODEC = jsonCodec(QueryResults.class);
    private static final JsonCodec<DataResults> DATA_RESULTS_JSON_CODEC = jsonCodec(DataResults.class);
    private static final DataResults EMPTY_DATA = new DataResults(null);
    private static final String NO_DATA_MAX_WAIT = new Duration(1, SECONDS).toString();

    private static final String USER_AGENT_VALUE = StatementClientV2.class.getSimpleName() + "/" +
            firstNonNull(StatementClientV2.class.getPackage().getImplementationVersion(), "unknown");
    private final AsyncOkHttpClient httpClient;
    private final TimeZoneKey timeZone;
    private final String query;
    private final long requestTimeoutNanos;
    private final String user;

    @GuardedBy("this")
    private Optional<String> setCatalog = Optional.empty();
    @GuardedBy("this")
    private Optional<String> setSchema = Optional.empty();
    @GuardedBy("this")
    private final Map<String, String> setSessionProperties = new HashMap<>();
    @GuardedBy("this")
    private final Set<String> resetSessionProperties = new HashSet<>();
    @GuardedBy("this")
    private final Map<String, String> addedPreparedStatements = new HashMap<>();
    @GuardedBy("this")
    private final Set<String> deallocatedPreparedStatements = new HashSet<>();
    @GuardedBy("this")
    private String startedTransactionId;
    @GuardedBy("this")
    private boolean clearTransactionId;

    @GuardedBy("this")
    private QueryStatusInfo currentStatus;
    @GuardedBy("this")
    private QueryData currentData = EMPTY_DATA;

    private final AtomicReference<State> state = new AtomicReference<>(State.RUNNING);

    @GuardedBy("this")
    private List<Column> columns;
    @GuardedBy("this")
    private boolean clientsCreated;
    @GuardedBy("this")
    private boolean closed;
    @GuardedBy("this")
    private URI lastAvailableStatusUri;
    @GuardedBy("this")
    private URI nextDataUri;

    @GuardedBy("this")
    private ListenableFuture<QueryResults> statusFuture;

    public StatementClientV2(OkHttpClient httpClient, ClientSession session, String query)
    {
        requireNonNull(httpClient, "httpClient is null");
        requireNonNull(session, "session is null");
        requireNonNull(query, "query is null");

        this.httpClient = new AsyncOkHttpClient(httpClient);
        this.timeZone = session.getTimeZone();
        this.query = query;
        this.requestTimeoutNanos = session.getClientRequestTimeout().roundTo(NANOSECONDS);
        this.user = session.getUser();

        Request request = buildQueryRequest(session, query);

        ListenableFuture<Response> responseFuture = this.httpClient.executeAsync(request);
        ListenableFuture<Response> redirectedFuture = Futures.transformAsync(responseFuture, this::handleResponseRedirect);
        ListenableFuture<QueryResults> queryResultsFuture = Futures.transform(redirectedFuture, StatementClientV2::parseQueryResultsResponse);

        try {
            processStatusResponse(queryResultsFuture.get());
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            checkState(e.getCause() != null, "cause of execution exception is null");
            throw new RuntimeException(e.getCause());
        }
        catch (Exception e) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            throw e;
        }
    }

    // block of final fields

    @Override
    public String getQuery()
    {
        return query;
    }

    @Override
    public TimeZoneKey getTimeZone()
    {
        return timeZone;
    }

    // block of updatable fields

    @Override
    public synchronized Optional<String> getSetCatalog()
    {
        return setCatalog;
    }

    @Override
    public synchronized Optional<String> getSetSchema()
    {
        return setSchema;
    }

    @Override
    public synchronized Map<String, String> getSetSessionProperties()
    {
        return unmodifiableMap(setSessionProperties);
    }

    @Override
    public synchronized Set<String> getResetSessionProperties()
    {
        return unmodifiableSet(resetSessionProperties);
    }

    @Override
    public synchronized Map<String, String> getAddedPreparedStatements()
    {
        return unmodifiableMap(addedPreparedStatements);
    }

    @Override
    public synchronized Set<String> getDeallocatedPreparedStatements()
    {
        return unmodifiableSet(deallocatedPreparedStatements);
    }

    @Override
    @Nullable
    public synchronized String getStartedTransactionId()
    {
        return startedTransactionId;
    }

    @Override
    public synchronized boolean isClearTransactionId()
    {
        return clearTransactionId;
    }

    // block of state-related methods

    public boolean isRunning()
    {
        return state.get() == State.RUNNING;
    }

    public boolean isClientAborted()
    {
        return state.get() == State.CLIENT_ABORTED;
    }

    public boolean isClientError()
    {
        return state.get() == State.CLIENT_ERROR;
    }

    public boolean isFinished()
    {
        return state.get() == State.FINISHED;
    }

    // get current state and data

    @Override
    public synchronized StatementStats getStats()
    {
        return currentStatus.getStats();
    }

    @Override
    public synchronized QueryStatusInfo currentStatusInfo()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return currentStatus;
    }

    @Override
    public synchronized QueryData currentData()
    {
        checkState(isRunning(), "current position is not valid (cursor past end)");
        return currentData;
    }

    @Override
    public synchronized QueryStatusInfo finalStatusInfo()
    {
        checkState(!isRunning(), "current position is still valid");
        return currentStatus;
    }

    // navigation-related methods

    @Override
    public synchronized boolean advance()
    {
        if (!isRunning()) {
            return false;
        }

        URI nextStatusUri = currentStatus.getNextUri();
        if (nextStatusUri == null) {
            state.compareAndSet(State.RUNNING, State.FINISHED);
            return false;
        }

        if (statusFuture == null) {
            HttpUrl statusUrl = HttpUrl.get(nextStatusUri);
            if (nextDataUri == null) {
                // no data yet, so wait less
                statusUrl = statusUrl.newBuilder().addQueryParameter("maxWait", NO_DATA_MAX_WAIT).build();
            }
            Request statusRequest = prepareRequest(statusUrl, user).build();
            ListenableFuture<Response> statusRequestFuture = httpClient.executeAsync(statusRequest);
            ListenableFuture<Response> retriedStatusRequestFuture = Futures.transformAsync(statusRequestFuture, response -> retryServerError(response, 0, System.nanoTime()));
            statusFuture = Futures.transform(retriedStatusRequestFuture, StatementClientV2::parseQueryResultsResponse);
        }

        ListenableFuture<DataResultsWithMetadata> dataFuture = null;
        if (nextDataUri != null) {
            Request dataRequest = prepareRequest(HttpUrl.get(nextDataUri), user).build();
            ListenableFuture<Response> dataRequestFuture = httpClient.executeAsync(dataRequest);
            ListenableFuture<Response> retriedDataRequestFuture = Futures.transformAsync(dataRequestFuture, response -> retryServerError(response, 0, System.nanoTime()));
            dataFuture = Futures.transform(retriedDataRequestFuture, StatementClientV2::parseDataResultsResponse);
        }

        try {
            if (dataFuture == null) {
                // status only
                statusFuture.get(requestTimeoutNanos, NANOSECONDS);
            }
            else {
                // status and data
                DataResultsWithMetadata dataResults = dataFuture.get(requestTimeoutNanos, NANOSECONDS);
                processDataResponse(dataResults);
            }
            if (statusFuture.isDone()) {
                processStatusResponse(statusFuture.get());
                statusFuture = null;
            }
            return true;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            cancelQuietly(dataFuture);
            throw new RuntimeException(e);
        }
        catch (ExecutionException e) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            cancelQuietly(dataFuture);
            checkState(e.getCause() != null, "cause of execution exception is null");
            throw new RuntimeException(e.getCause());
        }
        catch (TimeoutException e) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            cancelQuietly(dataFuture);
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ERROR);
            cancelQuietly(dataFuture);
            throw e;
        }
    }

    // actions

    @Override
    public synchronized void cancelLeafStage()
    {
        checkState(!closed, "client is closed");

        URI uri = currentStatus.getPartialCancelUri();
        if (uri != null) {
            httpDelete(uri);
        }
    }

    @Override
    public synchronized void close()
    {
        if (!closed) {
            state.compareAndSet(State.RUNNING, State.CLIENT_ABORTED);
            cancelQuietly(statusFuture);
            statusFuture = null;
            if (lastAvailableStatusUri != null) {
                httpDelete(lastAvailableStatusUri);
            }
            closed = true;
        }
    }

    // helper methods

    private ListenableFuture<Response> handleResponseRedirect(Response response)
    {
        if (response.isRedirect()) {
            String location = response.header(LOCATION);
            if (location != null) {
                try {
                    Request newRequest = response.request().newBuilder().url(location).build();
                    ListenableFuture<Response> newResponseFuture = httpClient.executeAsync(newRequest);
                    return Futures.transformAsync(newResponseFuture, this::handleResponseRedirect);
                }
                finally {
                    response.close();
                }
            }
        }
        return immediateFuture(response);
    }

    private ListenableFuture<Response> retryServerError(Response response, int attempt, long startNanos)
    {
        switch (response.code()) {
            case HTTP_OK:
            case HTTP_NO_CONTENT:
                return immediateFuture(response);
            case HTTP_UNAVAILABLE:
                if (System.nanoTime() - startNanos >= requestTimeoutNanos) {
                    throw new RuntimeException("Out of retries for " + response.request().url());
                }
                if (!isRunning()) {
                    throw new RuntimeException("Client is already aborted");
                }
                try {
                    Thread.sleep(attempt * 100);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Interrupted", e);
                }
                ListenableFuture<Response> newResponseFuture = httpClient.executeAsync(response.request());
                return Futures.transformAsync(newResponseFuture, newResponse -> retryServerError(newResponse, attempt + 1, startNanos));
            default:
                throw new RuntimeException(String.format("Unexpected return code [%s] for [%s]", response.code(), response.request().url()));
        }
    }

    private synchronized void processStatusResponse(QueryResults results)
    {
        checkState(results.getData() == null, "data must not be present in status message in v2");
        if (results.getColumns() != null && columns == null) {
            columns = ImmutableList.copyOf(results.getColumns());
        }
        if (results.getDataUris() != null && !results.getDataUris().isEmpty()) {
            if (results.getDataUris().size() > 1) {
                throw new RuntimeException("Current client supports only 1 data uri");
            }
            if (!clientsCreated) {
                // expects a list of next data URIs to be cumulative,
                // so don't verify URIs from subsequent responses after picked one for processing
                nextDataUri = results.getDataUris().get(0);
                clientsCreated = true;
            }
        }

        currentStatus = results;
        if (results.getNextUri() != null) {
            lastAvailableStatusUri = results.getNextUri();
        }

        QueryActions actions = results.getActions();
        if (actions != null) {
            setCatalog = Optional.ofNullable(actions.getSetCatalog());
            setSchema = Optional.ofNullable(actions.getSetSchema());
            if (actions.getSetSessionProperties() != null) {
                setSessionProperties.putAll(actions.getSetSessionProperties());
            }
            if (actions.getClearSessionProperties() != null) {
                resetSessionProperties.addAll(actions.getClearSessionProperties());
            }
            if (actions.getAddedPreparedStatements() != null) {
                addedPreparedStatements.putAll(actions.getAddedPreparedStatements());
            }
            if (actions.getDeallocatedPreparedStatements() != null) {
                deallocatedPreparedStatements.addAll(actions.getDeallocatedPreparedStatements());
            }
            if (actions.getStartedTransactionId() != null) {
                startedTransactionId = actions.getStartedTransactionId();
            }
            if (actions.isClearTransactionId() != null && actions.isClearTransactionId()) {
                clearTransactionId = true;
            }
        }
    }

    private synchronized void processDataResponse(DataResultsWithMetadata dataResults)
    {
        checkState(columns != null, "columns must be present");
        currentData = dataResults.getDataResults().withFixedData(columns);
        nextDataUri = dataResults.getNextUri();
    }

    private void httpDelete(URI uri)
    {
        Request request = prepareRequest(HttpUrl.get(uri), user)
                .delete()
                .build();
        // TODO: add retries
        Response response = null;
        try {
            response = httpClient.execute(request);
        }
        catch (IOException e) {
            throw new RuntimeException("Error sending delete request to " + uri, e);
        }
        finally {
            if (response != null) {
                response.close();
            }
        }
    }

    // static helper methods

    private static Request buildQueryRequest(ClientSession session, String query)
    {
        HttpUrl url = HttpUrl.get(session.getServer());
        if (url == null) {
            throw new ClientException("Invalid server URL: " + session.getServer());
        }
        url = url.newBuilder().encodedPath("/v2/statement").build();

        CreateQueryRequest createQueryRequest = new CreateQueryRequest(
                new CreateQuerySession(
                        session.getCatalog(),
                        session.getSchema(),
                        session.getUser(),
                        session.getSource(),
                        USER_AGENT_VALUE,
                        session.getTimeZone() != null ? session.getTimeZone().getId() : null,
                        session.getLocale() != null ? session.getLocale().toLanguageTag() : null,
                        session.getClientTags(),
                        session.getProperties(),
                        session.getPreparedStatements(),
                        session.getTransactionId(),
                        true,
                        session.getClientInfo()),
                query);
        return prepareRequest(url, session.getUser())
                .post(RequestBody.create(MEDIA_TYPE_JSON, CREATE_QUERY_REQUEST_JSON_CODEC.toJsonBytes(createQueryRequest)))
                .build();
    }

    private static Request.Builder prepareRequest(HttpUrl url, String user)
    {
        // TODO: find out if user header is required
        return new Request.Builder()
                .addHeader(PRESTO_USER, user)
                .addHeader(USER_AGENT, USER_AGENT_VALUE)
                .addHeader(ACCEPT, MEDIA_TYPE_JSON.toString())
                .url(url);
    }

    private static boolean isJson(MediaType type)
    {
        return (type != null) && "application".equals(type.type()) && "json".equals(type.subtype());
    }

    private static QueryResults parseQueryResultsResponse(Response response)
    {
        try (ResponseBody responseBody = response.body()) {
            String responseText = responseText(responseBody);
            boolean noResponseText = isNullOrEmpty(responseText);
            if (response.code() != HTTP_OK || noResponseText) {
                if (response.code() == HTTP_UNAUTHORIZED && noResponseText) {
                    throw new ClientException("Authentication failed" + Optional.ofNullable(response.message()).map(message -> ": " + message).orElse(""));
                }
                throw new RuntimeException(String.format("Error getting query response from %s. Received HTTP %s. Response body: %s",
                        response.request().url(), response.code(), responseText));
            }
            if (!isJson(responseBody.contentType())) {
                throw new RuntimeException(String.format("Error getting query response from %s. Content type is not application/json. Response body: %s",
                        response.request().url(), responseText));
            }
            return QUERY_RESULTS_CODEC.fromJson(responseText);
        }
    }

    private static DataResultsWithMetadata parseDataResultsResponse(Response response)
    {
        try (ResponseBody responseBody = response.body()) {
            String nextUriHeader = response.header(PRESTO_DATA_NEXT_URI);
            URI nextUri = nextUriHeader == null ? null : URI.create(nextUriHeader);
            if (response.code() == HTTP_NO_CONTENT) {
                checkArgument(responseBody == null || responseBody.contentLength() == 0, "Response body is not empty while return code is 204: \"no content\"");
                return new DataResultsWithMetadata(EMPTY_DATA, nextUri);
            }
            String responseText = responseText(responseBody);
            if (response.code() != HTTP_OK || isNullOrEmpty(responseText)) {
                throw new RuntimeException(String.format("Error getting data response from %s. Received HTTP %s. Response body: %s",
                        response.request().url(), response.code(), responseText));
            }
            if (!isJson(responseBody.contentType())) {
                throw new RuntimeException(String.format("Error getting data response from %s. Content type is not application/json. Response body: %s",
                        response.request().url(), responseText));
            }
            return new DataResultsWithMetadata(DATA_RESULTS_JSON_CODEC.fromJson(responseText), nextUri);
        }
    }

    @Nullable
    private static String responseText(ResponseBody responseBody)
    {
        if (responseBody == null) {
            return null;
        }
        try {
            return responseBody.string();
        }
        catch (IOException e) {
            return null;
        }
    }

    private static void cancelQuietly(ListenableFuture<?> future)
    {
        if (future != null) {
            future.cancel(true);
        }
    }

    private static class DataResultsWithMetadata
    {
        private final DataResults dataResults;
        private final URI nextUri;

        public DataResultsWithMetadata(DataResults dataResults, @Nullable URI nextUri)
        {
            this.dataResults = requireNonNull(dataResults, "dataResults is null");
            this.nextUri = nextUri;
        }

        public DataResults getDataResults()
        {
            return dataResults;
        }

        @Nullable
        public URI getNextUri()
        {
            return nextUri;
        }
    }

    private enum State
    {
        /**
         * submitted to server, not in terminal state (including planning, queued, running, etc)
         */
        RUNNING,
        CLIENT_ERROR,
        CLIENT_ABORTED,
        /**
         * finished on remote Presto server (including failed and successfully completed)
         */
        FINISHED,
    }
}
