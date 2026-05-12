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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.MaterializedViewUtils;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Identifier;
import com.google.common.collect.ImmutableList;

import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.sql.MaterializedViewUtils.ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.MaterializedViewUtils.NON_ASSOCIATIVE_REWRITE_FUNCTIONS;
import static com.facebook.presto.sql.analyzer.MaterializedViewInformationExtractor.MaterializedViewInfo;
import static com.facebook.presto.sql.analyzer.SemanticErrorCode.NOT_SUPPORTED;
import static java.util.Objects.requireNonNull;

/**
 * Expression-level rewriter for materialized view query optimization.
 * Centralizes column resolution and function-call rewriting so callers
 * (the single-table optimizer today) share one implementation.
 *
 * Argument recursion is delegated back to the caller via a callback so the
 * caller's outer visitor can keep handling other expression node types.
 */
public class MaterializedViewExpressionRewriter
{
    private final MaterializedViewInfo mvInfo;
    private final Set<String> builtInScalarFunctionNames;

    public MaterializedViewExpressionRewriter(MaterializedViewInfo mvInfo, Set<String> builtInScalarFunctionNames)
    {
        this.mvInfo = requireNonNull(mvInfo, "mvInfo is null");
        this.builtInScalarFunctionNames = requireNonNull(builtInScalarFunctionNames, "builtInScalarFunctionNames is null");
    }

    public Expression rewriteIdentifier(Identifier node)
    {
        Map<Expression, Identifier> columnMap = mvInfo.getBaseToViewColumnMap();
        if (!columnMap.containsKey(node)) {
            throw new IllegalStateException("Materialized view definition does not contain mapping for the column: " + node.getValue());
        }
        return new Identifier(columnMap.get(node).getValue(), node.isDelimited());
    }

    public Expression rewriteFunctionCall(FunctionCall node, Function<Expression, Expression> argRewriter)
    {
        Map<Expression, Identifier> baseToViewColumnMap = mvInfo.getBaseToViewColumnMap();

        if (NON_ASSOCIATIVE_REWRITE_FUNCTIONS.containsKey(node.getName())) {
            return MaterializedViewUtils.rewriteNonAssociativeFunction(node, baseToViewColumnMap);
        }

        if (!ASSOCIATIVE_REWRITE_FUNCTIONS.contains(node.getName())) {
            if (!isScalarFunction(node)) {
                throw new SemanticException(NOT_SUPPORTED, node, "Unsupported function for materialized view rewrite: " + node.getName());
            }
            return rebuildWithRewrittenArgs(node, argRewriter);
        }

        if (baseToViewColumnMap.containsKey(node)) {
            Identifier derivedColumn = baseToViewColumnMap.get(node);
            if (node.isDistinct()) {
                throw new SemanticException(NOT_SUPPORTED, node, "COUNT(DISTINCT) is not supported for materialized view query rewrite optimization");
            }
            return MaterializedViewUtils.rewriteAssociativeFunction(node, derivedColumn);
        }

        if (mvInfo.getGroupBy().isPresent()) {
            throw new SemanticException(NOT_SUPPORTED, node, "Materialized view does not pre-compute aggregate: " + node.getName());
        }

        return rebuildWithRewrittenArgs(node, argRewriter);
    }

    public boolean isScalarFunction(FunctionCall functionCall)
    {
        return !functionCall.getWindow().isPresent()
                && builtInScalarFunctionNames.contains(functionCall.getName().getSuffix().toLowerCase(Locale.ENGLISH));
    }

    private static FunctionCall rebuildWithRewrittenArgs(FunctionCall node, Function<Expression, Expression> argRewriter)
    {
        ImmutableList.Builder<Expression> rewrittenArgs = ImmutableList.builder();
        for (Expression argument : node.getArguments()) {
            rewrittenArgs.add(argRewriter.apply(argument));
        }
        return new FunctionCall(
                node.getName(),
                node.getWindow(),
                node.getFilter(),
                node.getOrderBy(),
                node.isDistinct(),
                node.isIgnoreNulls(),
                rewrittenArgs.build());
    }
}
