/*
 * Copyright 2004-present Facebook. All Rights Reserved.
 */
package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Charsets;
import com.google.common.base.Optional;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.BodyGenerator;

import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;
import static io.airlift.http.client.StaticBodyGenerator.createStaticBodyGenerator;

public class HttpQueryProvider implements QueryDriverProvider
{
    private final BodyGenerator bodyGenerator;
    private final Optional<String> mediaType;
    private final AsyncHttpClient httpClient;
    private final URI uri;
    private final List<TupleInfo> tupleInfos;

    public HttpQueryProvider(String query, AsyncHttpClient httpClient, URI uri, List<TupleInfo> tupleInfos)
    {
        this(createStaticBodyGenerator(checkNotNull(query, "query is null"), Charsets.UTF_8),
                Optional.<String>absent(),
                httpClient,
                uri,
                tupleInfos);
    }

    public HttpQueryProvider(BodyGenerator bodyGenerator, Optional<String> mediaType, AsyncHttpClient httpClient, URI uri, List<TupleInfo> tupleInfos)
    {
        checkNotNull(bodyGenerator, "bodyGenerator is null");
        checkNotNull(mediaType, "mediaType is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(uri, "uri is null");
        checkNotNull(tupleInfos, "tupleInfos is null");

        this.bodyGenerator = bodyGenerator;
        this.mediaType = mediaType;
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
        HttpQuery httpQuery = new HttpQuery(bodyGenerator, mediaType, queryState, httpClient, uri);
        return httpQuery;
    }
}
