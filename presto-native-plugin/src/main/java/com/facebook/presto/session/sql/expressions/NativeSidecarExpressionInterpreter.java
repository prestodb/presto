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
package com.facebook.presto.session.sql.expressions;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeSidecarExpressionInterpreter
{
    private final NodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<List<RowExpression>> rowExpressionSerde;

    @Inject
    public NativeSidecarExpressionInterpreter(NodeManager nodeManager, @ForSidecarInfo HttpClient httpClient, JsonCodec<List<RowExpression>> rowExpressionSerde)
    {
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    public Map<RowExpression, Object> optimizeBatch(ConnectorSession session, Map<RowExpression, RowExpression> expressions, ExpressionOptimizer.Level level)
    {
        ImmutableList.Builder<RowExpression> unaliasedBuilder = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> aliasedBuilder = ImmutableList.builder();
        for (Map.Entry<RowExpression, RowExpression> entry : expressions.entrySet()) {
            unaliasedBuilder.add(entry.getKey());
            aliasedBuilder.add(entry.getValue());
        }
        List<RowExpression> unaliased = unaliasedBuilder.build();
        List<RowExpression> aliased = aliasedBuilder.build();

        Request request = preparePost()
                .setUri(getLocation())
                .setBodyGenerator(jsonBodyGenerator(rowExpressionSerde, aliased))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString())
                .build();

        List<RowExpression> optimized = httpClient.execute(request, createJsonResponseHandler(rowExpressionSerde));
        checkArgument(optimized.size() == aliased.size(), "Expected %s optimized expressions, but got %s", aliased.size(), optimized.size());

        ImmutableMap.Builder<RowExpression, Object> result = ImmutableMap.builder();
        for (int i = 0; i < optimized.size(); i++) {
            result.put(unaliased.get(i), optimized.get(i));
        }
        return result.build();
    }

    private URI getLocation()
    {
        Node sidecarNode = nodeManager.getSidecarNode();
        return HttpUriBuilder.uriBuilder()
                .scheme("http") // The sidecar is presumed to be colocated with the coordinator
                .host(sidecarNode.getHost())
                .port(sidecarNode.getHostAndPort().getPort())
                .appendPath("/v1/expressions")
                .build();
    }
}
