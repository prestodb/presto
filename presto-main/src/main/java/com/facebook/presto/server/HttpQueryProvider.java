/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.BodyGenerator;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;

import javax.ws.rs.core.HttpHeaders;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;

public class HttpQueryProvider
        implements QueryDriverProvider
{
    private final AsyncHttpClient httpClient;
    private final URI location;
    private final List<TupleInfo> tupleInfos;

    public HttpQueryProvider(AsyncHttpClient httpClient, URI location, List<TupleInfo> tupleInfos)
    {
        this.httpClient = httpClient;
        this.location = location;
        this.tupleInfos = tupleInfos;
    }

    public HttpQueryProvider(BodyGenerator bodyGenerator, Optional<String> mediaType, AsyncHttpClient httpClient, URI uri)
    {
        checkNotNull(bodyGenerator, "bodyGenerator is null");
        checkNotNull(mediaType, "mediaType is null");
        checkNotNull(httpClient, "httpClient is null");

        this.httpClient = httpClient;

        Request.Builder requestBuilder = preparePost()
                .setUri(uri)
                .setBodyGenerator(bodyGenerator);

        if (mediaType.isPresent()) {
            requestBuilder.setHeader(HttpHeaders.CONTENT_TYPE, mediaType.get());
        }

        Request request = requestBuilder.build();

        try {
            JsonResponse<QueryInfo> response = httpClient.execute(request, createFullJsonResponseHandler(jsonCodec(QueryInfo.class))).get();
            Preconditions.checkState(response.getStatusCode() == 201);
            String location = response.getHeader("Location");
            Preconditions.checkState(location != null);

            this.location = URI.create(location);
            this.tupleInfos = response.getValue().getTupleInfos();
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }

    public URI getLocation()
    {
        return location;
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
        HttpQuery httpQuery = new HttpQuery(location, queryState, httpClient);
        return httpQuery;
    }
}
