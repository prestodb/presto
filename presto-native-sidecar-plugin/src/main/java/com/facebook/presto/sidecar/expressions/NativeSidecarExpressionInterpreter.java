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
package com.facebook.presto.sidecar.expressions;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.http.client.HttpUriBuilder;
import com.facebook.airlift.http.client.Request;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.sidecar.ForSidecarInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
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
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeSidecarExpressionInterpreter
{
    private static final String PRESTO_TIME_ZONE_HEADER = "X-Presto-Time-Zone";
    private static final String PRESTO_USER_HEADER = "X-Presto-User";
    private static final String PRESTO_EXPRESSION_OPTIMIZER_LEVEL_HEADER = "X-Presto-Expression-Optimizer-Level";
    private static final String EXPRESSIONS_ENDPOINT = "/v1/expressions";
    private final NodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<List<RowExpression>> rowExpressionSerde;

    @Inject
    public NativeSidecarExpressionInterpreter(@ForSidecarInfo HttpClient httpClient, NodeManager nodeManager, JsonCodec<List<RowExpression>> rowExpressionSerde)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.rowExpressionSerde = requireNonNull(rowExpressionSerde, "rowExpressionSerde is null");
    }

    public Map<RowExpression, RowExpression> optimizeBatch(ConnectorSession session, Map<RowExpression, RowExpression> expressions, ExpressionOptimizer.Level level)
    {
        ImmutableList.Builder<RowExpression> originalExpressionsBuilder = ImmutableList.builder();
        ImmutableList.Builder<RowExpression> resolvedExpressionsBuilder = ImmutableList.builder();
        for (Map.Entry<RowExpression, RowExpression> entry : expressions.entrySet()) {
            originalExpressionsBuilder.add(entry.getKey());
            resolvedExpressionsBuilder.add(entry.getValue());
        }
        List<RowExpression> originalExpressions = originalExpressionsBuilder.build();
        List<RowExpression> resolvedExpressions = resolvedExpressionsBuilder.build();

        List<RowExpression> optimizedExpressions;
        try {
            optimizedExpressions = httpClient.execute(
                    getSidecarRequest(session, level, resolvedExpressions),
                    createJsonResponseHandler(rowExpressionSerde));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get optimized expressions from sidecar.", e);
        }

        checkArgument(
                optimizedExpressions.size() == resolvedExpressions.size(),
                "Expected %s optimized expressions, but got %s",
                resolvedExpressions.size(),
                optimizedExpressions.size());

        ImmutableMap.Builder<RowExpression, RowExpression> result = ImmutableMap.builder();
        for (int i = 0; i < optimizedExpressions.size(); i++) {
            result.put(originalExpressions.get(i), optimizedExpressions.get(i));
        }
        return result.build();
    }

    private Request getSidecarRequest(ConnectorSession session, Level level, List<RowExpression> resolvedExpressions)
    {
        return preparePost()
                .setUri(getSidecarLocation())
                .setBodyGenerator(jsonBodyGenerator(rowExpressionSerde, resolvedExpressions))
                .setHeader(CONTENT_TYPE, JSON_UTF_8.toString())
                .setHeader(ACCEPT, JSON_UTF_8.toString())
                .setHeader(PRESTO_TIME_ZONE_HEADER, session.getSqlFunctionProperties().getTimeZoneKey().getId())
                .setHeader(PRESTO_USER_HEADER, session.getUser())
                .setHeader(PRESTO_EXPRESSION_OPTIMIZER_LEVEL_HEADER, level.name())
                .build();
    }

    private URI getSidecarLocation()
    {
        Node sidecarNode = nodeManager.getSidecarNode();
        return HttpUriBuilder.uriBuilder()
                .scheme("http")
                .host(sidecarNode.getHost())
                .port(sidecarNode.getHostAndPort().getPort())
                .appendPath(EXPRESSIONS_ENDPOINT)
                .build();
    }
}
