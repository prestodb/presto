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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import io.airlift.http.client.FullJsonResponseHandler;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.HttpStatus;
import io.airlift.http.client.Request;
import io.airlift.json.JsonCodec;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.client.PrestoHeaders.PRESTO_CLEAR_SESSION;
import static com.facebook.presto.client.PrestoHeaders.PRESTO_SET_SESSION;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.net.HttpHeaders.USER_AGENT;
import static com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
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
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

public class AirliftQueryHttpClient implements QueryHttpClient
{
    private static final Splitter SESSION_HEADER_SPLITTER = Splitter.on('=').limit(2).trimResults();
    private static final String USER_AGENT_VALUE = StatementClient.class.getSimpleName() +
            "/" +
            firstNonNull(StatementClient.class.getPackage().getImplementationVersion(), "unknown");

    private final HttpClient httpClient;
    private final FullJsonResponseHandler<QueryResults> responseHandler;

    private final Map<String, String> setSessionProperties = new ConcurrentHashMap<>();
    private final Set<String> resetSessionProperties = Sets.newConcurrentHashSet();
    private final AtomicBoolean closed = new AtomicBoolean();

    public AirliftQueryHttpClient(HttpClient httpClient, JsonCodec<QueryResults> queryResultsCodec)
    {
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(queryResultsCodec, "queryResultsCodec is null");

        this.httpClient = httpClient;
        this.responseHandler = createFullJsonResponseHandler(queryResultsCodec);
    }
    @Override
    public QueryResults startQuery(ClientSession session, String query)
    {
        Request request = buildQueryRequest(session, query);
        FullJsonResponseHandler.JsonResponse<QueryResults> response = httpClient.execute(request, responseHandler);

        if (response.getStatusCode() != HttpStatus.OK.code() || !response.hasValue()) {
            throw requestFailedException("starting query", request, response);
        }

        return processResponse(response);
    }

    @Override
    public void deleteAsync(URI uri)
    {
        Request request = prepareDelete()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(uri)
                .build();
        httpClient.executeAsync(request, createStatusResponseHandler());
    }

    @Override
    public boolean delete(URI uri)
    {
        Request request = prepareDelete()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(uri)
                .build();
        StatusResponse status = httpClient.execute(request, createStatusResponseHandler());
        return familyForStatusCode(status.getStatusCode()) == Family.SUCCESSFUL;
    }

    @Override
    public QueryResults execute(URI uri)
    {
        Request request = prepareGet()
                .setHeader(USER_AGENT, USER_AGENT_VALUE)
                .setUri(uri)
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

            FullJsonResponseHandler.JsonResponse<QueryResults> response;
            try {
                response = httpClient.execute(request, responseHandler);
            }
            catch (RuntimeException e) {
                cause = e;
                continue;
            }

            if (response.getStatusCode() == HttpStatus.OK.code() && response.hasValue()) {
                return processResponse(response);
            }

            if (response.getStatusCode() != HttpStatus.SERVICE_UNAVAILABLE.code()) {
                throw requestFailedException("fetching next", request, response);
            }
        }
        while ((System.nanoTime() - start) < MINUTES.toNanos(2) && !isClosed());

        throw new RuntimeException("Error fetching next", cause);
    }

    @Override
    public Map<String, String> getSetSessionProperties()
    {
        return ImmutableMap.copyOf(setSessionProperties);
    }

    @Override
    public Set<String> getResetSessionProperties()
    {
        return ImmutableSet.copyOf(resetSessionProperties);
    }

    @Override
    public boolean isClosed()
    {
        return closed.get();
    }

    @Override
    public void close()
    {
        closed.set(true);
    }

    private QueryResults processResponse(FullJsonResponseHandler.JsonResponse<QueryResults> response)
    {
        for (String setSession : response.getHeaders().get(PRESTO_SET_SESSION)) {
            List<String> keyValue = SESSION_HEADER_SPLITTER.splitToList(setSession);
            if (keyValue.size() != 2) {
                continue;
            }
            setSessionProperties.put(keyValue.get(0), keyValue.size() > 1 ? keyValue.get(1) : "");
        }
        for (String clearSession : response.getHeaders().get(PRESTO_CLEAR_SESSION)) {
            resetSessionProperties.add(clearSession);
        }
        return response.getValue();
    }

    private static Request buildQueryRequest(ClientSession session, String query)
    {
        Request.Builder builder = preparePost()
                .setUri(uriBuilderFrom(session.getServer()).replacePath("/v1/statement").build())
                .setBodyGenerator(createStaticBodyGenerator(query, UTF_8));

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
        builder.setHeader(PrestoHeaders.PRESTO_TIME_ZONE, session.getTimeZoneId());
        builder.setHeader(PrestoHeaders.PRESTO_LANGUAGE, session.getLocale().toLanguageTag());
        builder.setHeader(USER_AGENT, USER_AGENT_VALUE);

        Map<String, String> property = session.getProperties();
        for (Map.Entry<String, String> entry : property.entrySet()) {
            builder.addHeader(PrestoHeaders.PRESTO_SESSION, entry.getKey() + "=" + entry.getValue());
        }

        return builder.build();
    }

    private RuntimeException requestFailedException(String task, Request request, FullJsonResponseHandler.JsonResponse<QueryResults> response)
    {
        if (!response.hasValue()) {
            return new RuntimeException(format("Error " + task + " at %s returned an invalid response: %s", request.getUri(), response), response.getException());
        }
        return new RuntimeException(format("Error " + task + " at %s returned %s: %s",
                request.getUri(),
                response.getStatusCode(),
                response.getStatusMessage()));
    }
}
