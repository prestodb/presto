/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.google.common.base.Preconditions;
import io.airlift.http.client.AsyncHttpClient;

import java.net.URI;

public class HttpQueryProvider implements QueryDriverProvider
{
    private final String query;
    private final AsyncHttpClient httpClient;
    private final URI uri;

    public HttpQueryProvider(String query, AsyncHttpClient httpClient, URI uri)
    {
        Preconditions.checkNotNull(query, "query is null");
        Preconditions.checkNotNull(httpClient, "httpClient is null");
        Preconditions.checkNotNull(uri, "uri is null");
        this.query = query;
        this.httpClient = httpClient;
        this.uri = uri;
    }

    @Override
    public int getChannelCount()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public QueryDriver create(QueryState queryState)
    {
        HttpQuery httpQuery = new HttpQuery(query, queryState, httpClient, uri);
        return httpQuery;
    }
}
