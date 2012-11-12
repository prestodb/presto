/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.FullJsonResponseHandler.JsonResponse;
import io.airlift.http.client.Request;
import io.airlift.http.client.StaticBodyGenerator;

import java.net.URI;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.airlift.http.client.FullJsonResponseHandler.createFullJsonResponseHandler;
import static io.airlift.http.client.Request.Builder.preparePost;
import static io.airlift.json.JsonCodec.jsonCodec;

/**
 * This class is a hack to transfer the tuple info to the client.
 *
 * TODO rewrite this class
 */
public class CliHttpQueryProvider
        implements QueryDriverProvider
{
    private final String query;
    private final AsyncHttpClient httpClient;
    private final List<TupleInfo> tupleInfos;
    private final URI location;

    public CliHttpQueryProvider(String query, AsyncHttpClient httpClient, URI uri)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");

        this.query = query;
        this.httpClient = httpClient;

        Request request = preparePost()
                .setUri(uri)
                .setBodyGenerator(StaticBodyGenerator.createStaticBodyGenerator(query, Charsets.UTF_8))
                .build();

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
        return HttpQuery.fetchResults(query, queryState, httpClient, location);
    }
}
