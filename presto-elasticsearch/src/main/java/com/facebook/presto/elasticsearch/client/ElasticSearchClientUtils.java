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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.ClearScrollRequest;
import co.elastic.clients.elasticsearch.core.ClearScrollResponse;
import co.elastic.clients.elasticsearch.core.CountRequest;
import co.elastic.clients.elasticsearch.core.CountResponse;
import co.elastic.clients.elasticsearch.core.ScrollRequest;
import co.elastic.clients.elasticsearch.core.ScrollResponse;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest5_client.Rest5ClientTransport;
import co.elastic.clients.transport.rest5_client.low_level.Node;
import co.elastic.clients.transport.rest5_client.low_level.Request;
import co.elastic.clients.transport.rest5_client.low_level.RequestOptions;
import co.elastic.clients.transport.rest5_client.low_level.Response;
import co.elastic.clients.transport.rest5_client.low_level.Rest5Client;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpHost;

import java.io.IOException;
import java.util.Map;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Arrays.stream;
import static java.util.Objects.requireNonNull;

public class ElasticSearchClientUtils
{
    private ElasticSearchClientUtils() {}

    public static void setHosts(Rest5Client client, HttpHost... hosts)
    {
        client.setNodes(stream(hosts)
                .map(Node::new)
                .collect(toImmutableList()));
    }

    public static Response performRequest(String method, String endpoint, Rest5Client client, Header... headers)
            throws IOException
    {
        return client.performRequest(toRequest(method, endpoint, headers));
    }

    public static Response performRequest(String method, String endpoint, Map<String, String> params, HttpEntity entity, Rest5Client client, Header... headers)
            throws IOException
    {
        return client.performRequest(toRequest(method, endpoint, params, entity, headers));
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

    public static ElasticsearchClient createClient(Rest5Client client)
    {
        Rest5ClientTransport transport = new Rest5ClientTransport(client, new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }

    public static SearchResponse<JsonData> search(SearchRequest searchRequest, Rest5Client client)
            throws IOException
    {
        return createClient(client).search(searchRequest, JsonData.class);
    }

    public static ScrollResponse<JsonData> searchScroll(ScrollRequest scrollRequest, Rest5Client client)
            throws IOException
    {
        return createClient(client).scroll(scrollRequest, JsonData.class);
    }

    public static ClearScrollResponse clearScroll(ClearScrollRequest clearScrollRequest, Rest5Client client)
            throws IOException
    {
        return createClient(client).clearScroll(clearScrollRequest);
    }

    public static CountResponse count(CountRequest countRequest, Rest5Client client)
            throws IOException
    {
        return createClient(client).count(countRequest);
    }
}
