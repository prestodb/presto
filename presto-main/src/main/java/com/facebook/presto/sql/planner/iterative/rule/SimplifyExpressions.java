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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralEncoder;
import com.facebook.presto.sql.planner.NoOpVariableResolver;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static com.facebook.presto.sql.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class SimplifyExpressions
        extends ExpressionRewriteRuleSet
{
    @VisibleForTesting
    static Expression rewrite(Expression expression, Session session, PlanVariableAllocator variableAllocator, Metadata metadata, LiteralEncoder literalEncoder, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        if (expression instanceof SymbolReference) {
            return expression;
        }
        expression = pushDownNegations(expression);
        expression = extractCommonPredicates(expression);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, variableAllocator.getTypes(), expression, emptyList(), WarningCollector.NOOP);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, session, expressionTypes);
        return literalEncoder.toExpression(interpreter.optimize(NoOpVariableResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
    }

    public SimplifyExpressions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewrite(metadata, sqlParser));
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return ImmutableSet.of(
                projectExpressionRewrite(),
                filterExpressionRewrite(),
                joinExpressionRewrite(),
                valuesExpressionRewrite()); // ApplyNode and AggregationNode are not supported, because ExpressionInterpreter doesn't support them
    }

    private static ExpressionRewriter createRewrite(Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        LiteralEncoder literalEncoder = new LiteralEncoder(metadata.getBlockEncodingSerde());

        return (expression, context) -> rewrite(expression, context.getSession(), context.getVariableAllocator(), metadata, literalEncoder, sqlParser);
    }
}
