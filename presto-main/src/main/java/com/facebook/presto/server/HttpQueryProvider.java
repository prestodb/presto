/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;

import java.net.URI;
import java.util.List;

public class HttpQueryProvider implements QueryDriverProvider
{
    private final String query;
    private final AsyncHttpClient httpClient;
    private final URI uri;
    private final List<TupleInfo> tupleInfos;

    public HttpQueryProvider(String query, AsyncHttpClient httpClient, URI uri, List<TupleInfo> tupleInfos)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(uri, "uri is null");
        Preconditions.checkNotNull(tupleInfos, "tupleInfos is null");

        this.query = query;
        this.httpClient = httpClient;
        this.uri = uri;
        this.tupleInfos = tupleInfos;
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
        HttpQuery httpQuery = new HttpQuery(query, queryState, httpClient, uri);
        return httpQuery;
    }
}
