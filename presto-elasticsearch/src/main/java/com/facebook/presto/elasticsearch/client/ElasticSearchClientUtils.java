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
package com.facebook.presto.elasticsearch.client;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.HttpHost;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestHighLevelClient;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;
import static org.elasticsearch.client.RequestOptions.DEFAULT;

public class ElasticSearchClientUtils
{
    private ElasticSearchClientUtils() {}

    public static void setHosts(RestHighLevelClient client, HttpHost... hosts)
    {
        client.getLowLevelClient().setNodes(stream(hosts)
                .map(Node::new)
                .collect(toImmutableList()));
    }

    public static Response performRequest(String method, String endpoint, RestHighLevelClient client, Header... headers)
            throws IOException
    {
        return client.getLowLevelClient().performRequest(toRequest(method, endpoint, headers));
    }

    public static Response performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, RestHighLevelClient client, Header... headers)
            throws IOException
    {
        return client.getLowLevelClient().performRequest(toRequest(method, endpoint, params, entity, headers));
    }

    public static Request toRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Header... headers)
    {
        requireNonNull(params, "parameters cannot be null");
        Request request = toRequest(method, endpoint, headers);
        for (Map.Entry<String, String> entry : params.entrySet()) {
            request.addParameter(entry.getKey(), entry.getValue());
        }
        request.setEntity(entity);
        return request;
    }

    public static Request toRequest(String method, String endpoint, Header... headers)
    {
        requireNonNull(headers, "headers cannot be null");
        Request request = new Request(method, endpoint);
        RequestOptions.Builder options = request.getOptions().toBuilder();
        for (Header header : headers) {
            options.addHeader(header.getName(), header.getValue());
        }
        request.setOptions(options);
        return request;
    }

    public static SearchResponse search(SearchRequest searchRequest, RestHighLevelClient client)
            throws IOException
    {
        return client.search(searchRequest, DEFAULT);
    }

    public static SearchResponse searchScroll(SearchScrollRequest searchScrollRequest, RestHighLevelClient client)
            throws IOException
    {
        return client.scroll(searchScrollRequest, DEFAULT);
    }

    public static ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, RestHighLevelClient client)
            throws IOException
    {
        return client.clearScroll(clearScrollRequest, DEFAULT);
    }
}
