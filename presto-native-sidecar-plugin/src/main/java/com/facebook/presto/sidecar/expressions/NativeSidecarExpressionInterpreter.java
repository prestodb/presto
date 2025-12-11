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
import com.facebook.presto.sidecar.NativeSidecarFailureInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Node;
import com.facebook.presto.spi.NodeManager;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Inject;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.airlift.http.client.JsonBodyGenerator.jsonBodyGenerator;
import static com.facebook.airlift.http.client.JsonResponseHandler.createJsonResponseHandler;
import static com.facebook.airlift.http.client.Request.Builder.preparePost;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_ARGUMENTS;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.net.HttpHeaders.ACCEPT;
import static com.google.common.net.HttpHeaders.CONTENT_TYPE;
import static com.google.common.net.MediaType.JSON_UTF_8;
import static java.util.Objects.requireNonNull;

public class NativeSidecarExpressionInterpreter
{
    public static final String PRESTO_TIME_ZONE_HEADER = "X-Presto-Time-Zone";
    public static final String PRESTO_USER_HEADER = "X-Presto-User";
    public static final String PRESTO_EXPRESSION_OPTIMIZER_LEVEL_HEADER = "X-Presto-Expression-Optimizer-Level";
    private static final String EXPRESSIONS_ENDPOINT = "/v1/expressions";

    private final NodeManager nodeManager;
    private final HttpClient httpClient;
    private final JsonCodec<List<RowExpression>> rowExpressionCodec;
    private final JsonCodec<List<RowExpressionOptimizationResult>> rowExpressionOptimizationResultJsonCodec;

    @Inject
    public NativeSidecarExpressionInterpreter(
            @ForSidecarInfo HttpClient httpClient,
            NodeManager nodeManager,
            JsonCodec<List<RowExpressionOptimizationResult>> rowExpressionOptimizationResultJsonCodec,
            JsonCodec<List<RowExpression>> rowExpressionCodec)
    {
        this.httpClient = requireNonNull(httpClient, "httpClient is null");
        this.nodeManager = requireNonNull(nodeManager, "nodeManager is null");
        this.rowExpressionOptimizationResultJsonCodec = requireNonNull(rowExpressionOptimizationResultJsonCodec, "rowExpressionOptimizationResultJsonCodec is null");
        this.rowExpressionCodec = requireNonNull(rowExpressionCodec, "rowExpressionCodec is null");
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

        List<RowExpressionOptimizationResult> rowExpressionOptimizationResults = optimize(session, level, resolvedExpressions);

        Optional<Exception> exception = rePackageExceptions(rowExpressionOptimizationResults);
        if (exception.isPresent()) {
            throw new PrestoException(GENERIC_USER_ERROR, "Errors encountered while optimizing expressions.", exception.get());
        }

        checkArgument(
                rowExpressionOptimizationResults.size() == resolvedExpressions.size(),
                "Expected %s optimized expressions, but got %s",
                resolvedExpressions.size(),
                rowExpressionOptimizationResults.size());

        ImmutableMap.Builder<RowExpression, RowExpression> result = ImmutableMap.builder();
        for (int i = 0; i < rowExpressionOptimizationResults.size(); i++) {
            result.put(originalExpressions.get(i), rowExpressionOptimizationResults.get(i).getOptimizedExpression());
        }
        return result.build();
    }

    public List<RowExpressionOptimizationResult> optimize(ConnectorSession session, ExpressionOptimizer.Level level, List<RowExpression> resolvedExpressions)
    {
        List<RowExpressionOptimizationResult> optimizedExpressions;
        try {
            optimizedExpressions = httpClient.execute(
                    getSidecarRequest(session, level, resolvedExpressions),
                    createJsonResponseHandler(rowExpressionOptimizationResultJsonCodec));
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_ARGUMENTS, "Failed to get optimized expressions from sidecar.", e);
        }
        return optimizedExpressions;
    }

    private Request getSidecarRequest(ConnectorSession session, Level level, List<RowExpression> resolvedExpressions)
    {
        return preparePost()
                .setUri(getSidecarLocation())
                .setBodyGenerator(jsonBodyGenerator(rowExpressionCodec, resolvedExpressions))
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
        return HttpUriBuilder
                .uriBuilderFrom(sidecarNode.getHttpUri())
                .appendPath(EXPRESSIONS_ENDPOINT)
                .build();
    }

    private static Optional<Exception> rePackageExceptions(List<RowExpressionOptimizationResult> rowExpressionOptimizationResults)
    {
        // Extract all exceptions from rowExpressionOptimizationResults
        List<Exception> exceptions = rowExpressionOptimizationResults.stream()
                .map(RowExpressionOptimizationResult::getExpressionFailureInfo)
                .map(NativeSidecarFailureInfo::toException)
                .filter(e -> e.getMessage() != null && !e.getMessage().isEmpty())
                .collect(toImmutableList());

        if (exceptions.isEmpty()) {
            return Optional.empty();
        }

        Exception primary = exceptions.get(0);

        for (int i = 1; i < exceptions.size(); i++) {
            primary.addSuppressed(exceptions.get(i));
        }

        return Optional.of(primary);
    }
}
