/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.HttpClient;
import io.airlift.http.client.Request;

import javax.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.HttpUriBuilder.uriBuilderFrom;
import static io.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.prepareDelete;
import static io.airlift.http.client.Request.Builder.prepareGet;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.http.client.StatusResponseHandler.createStatusResponseHandler;
import static io.airlift.json.JsonCodec.jsonCodec;

public class HttpQueryProvider
        implements QueryDriverProvider
{
    private final HttpClient httpClient;
    private final ExecutorService executor;
    private final URI location;
    private final List<TupleInfo> tupleInfos;

    public HttpQueryProvider(HttpClient httpClient, ExecutorService executor, URI location, List<TupleInfo> tupleInfos)
    {
        this.httpClient = httpClient;
        this.executor = executor;
        this.location = location;
        this.tupleInfos = tupleInfos;
    }

    public HttpQueryProvider(BodyGenerator bodyGenerator, Optional<String> mediaType, HttpClient httpClient, ExecutorService executor, URI uri)
    {
        checkNotNull(bodyGenerator, "bodyGenerator is null");
        checkNotNull(mediaType, "mediaType is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(executor, "executor is null");

        this.httpClient = httpClient;
        this.executor = executor;

        Request.Builder requestBuilder = preparePost()
                .setUri(uri)
                .setBodyGenerator(bodyGenerator);

        if (mediaType.isPresent()) {
            requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, mediaType.get());
        }

        Request request = requestBuilder.build();

        JsonResponse<QueryInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(QueryInfo.class)));
        Preconditions.checkState(response.getStatusCode() == 201,
                "Expected response code to be 201, but was %d: %s",
                response.getStatusCode(),
                response.getStatusMessage());
        String location = response.getHeader("Location");
        Preconditions.checkState(location != null);

        this.location = URI.create(location);
        this.tupleInfos = response.getValue().getTupleInfos();
    }

    public URI getLocation()
    {
        return location;
    }

    public QueryInfo getQueryInfo()
    {
        URI statusUri = uriBuilderFrom(location).appendPath("info").build();
        QueryInfo queryInfo = httpClient.execute(prepareGet().setUri(statusUri).build(), createJsonResponseHandler(jsonCodec(QueryInfo.class)));
        return queryInfo;
    }

    @Override
    public int getChannelCount()
    {
        return tupleInfos.size();
    }

    @Override
    public List<TupleInfo> getTupleInfos()
    {
        return tupleInfos;
    }

    @Override
    public QueryDriver create(QueryState queryState)
    {
        HttpQuery httpQuery = new HttpQuery(location, queryState, new AsyncHttpClient(httpClient, executor));
        return httpQuery;
    }

    public void destroy()
    {
        try {
            Request.Builder requestBuilder = prepareDelete().setUri(location);
            Request request = requestBuilder.build();
            httpClient.execute(request, createStatusResponseHandler());
        }
        catch (RuntimeException ignored) {
        }
    }
}
