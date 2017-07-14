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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.planner.NoOpSymbolResolver;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.iterative.RuleSet;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.FilterExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.JoinExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.ProjectExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.TableScanExpressionRewrite;
import com.facebook.presto.sql.planner.iterative.rule.ExpressionRewriteRuleSet.ValuesExpressionRewrite;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.planner.iterative.rule.ExtractCommonPredicatesExpressionRewriter.extractCommonPredicates;
import static com.facebook.presto.sql.planner.iterative.rule.PushDownNegationsExpressionRewriter.pushDownNegations;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class SimplifyExpressions
        implements RuleSet
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    private final Set<Rule<?>> rules = ImmutableSet.of(
                    new ProjectExpressionRewrite(this::rewrite),
                    new FilterExpressionRewrite(this::rewrite),
                    new TableScanExpressionRewrite(this::rewrite),
                    new JoinExpressionRewrite(this::rewrite),
                    new ValuesExpressionRewrite(this::rewrite));

    static Expression rewrite(Expression expression, Rule.Context context, Metadata metadata, SqlParser sqlParser)
    {
        requireNonNull(metadata, "metadata is null");
        requireNonNull(sqlParser, "sqlParser is null");
        if (expression instanceof SymbolReference) {
            return expression;
        }
        expression = pushDownNegations(expression);
        expression = extractCommonPredicates(expression);
        Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(context.getSession(), metadata, sqlParser, context.getSymbolAllocator().getTypes(), expression, emptyList() /* parameters already replaced */);
        ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(expression, metadata, context.getSession(), expressionTypes);
        return LiteralInterpreter.toExpression(interpreter.optimize(NoOpSymbolResolver.INSTANCE), expressionTypes.get(NodeRef.of(expression)));
    }

    public SimplifyExpressions(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = metadata;
        this.sqlParser = sqlParser;
    }

    @Override
    public Set<Rule<?>> rules()
    {
        return rules;
    }

    private Expression rewrite(Expression expression, Rule.Context context)
    {
        return rewrite(expression, context, metadata, sqlParser);
    }
}
