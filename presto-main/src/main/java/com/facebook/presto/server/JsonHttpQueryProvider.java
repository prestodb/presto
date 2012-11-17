package com.facebook.presto.server;

import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Optional;
import io.airlift.http.client.AsyncHttpClient;
import io.airlift.http.client.JsonBodyGenerator;
import io.airlift.json.JsonCodec;

import javax.ws.rs.core.MediaType;
import java.net.URI;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class JsonHttpQueryProvider<T>
    implements QueryDriverProvider
{
    private final T body;
    private final JsonCodec<T> jsonCodec;
    private final AsyncHttpClient httpClient;
    private final URI uri;
    private final List<TupleInfo> tupleInfos;

    public JsonHttpQueryProvider(T body, JsonCodec<T> jsonCodec, AsyncHttpClient httpClient, URI uri, List<TupleInfo> tupleInfos)
    {
        checkNotNull(body, "body is null");
        checkNotNull(jsonCodec, "jsonCodec is null");
        checkNotNull(httpClient, "httpClient is null");
        checkNotNull(uri, "uri is null");
        checkNotNull(tupleInfos, "tupleInfos is null");

        this.body = body;
        this.jsonCodec = jsonCodec;
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
        return new HttpQuery(JsonBodyGenerator.jsonBodyGenerator(jsonCodec, body), Optional.of(MediaType.APPLICATION_JSON), queryState, httpClient, uri);
    }
}
