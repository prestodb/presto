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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.type.FunctionType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.execution.warnings.WarningCollector.NOOP;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyList;

public class TranslateExpressions
        extends RowExpressionRewriteRuleSet
{
    public TranslateExpressions(Metadata metadata, SqlParser sqlParser)
    {
        super(createRewriter(metadata, sqlParser));
    }

    private static RowExpressionRewriter createRewriter(Metadata metadata, SqlParser sqlParser)
    {
        return new RowExpressionRewriter()
        {
            @Override
            public RowExpression rewrite(RowExpression expression, Rule.Context context)
            {
                return removeOriginalExpression(expression, context);
            }

            @Override
            public AggregationNode.Aggregation rewrite(AggregationNode.Aggregation aggregation, Rule.Context context)
            {
                // Do not evaluate aggregation
                return translateAggregation(aggregation, context.getSession(), context.getSymbolAllocator().getTypes());
            }

            private AggregationNode.Aggregation translateAggregation(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
            {
                Map<NodeRef<Expression>, Type> types = analyzeAggregationExpressionTypes(aggregation, session, typeProvider);

                return new AggregationNode.Aggregation(
                        new CallExpression(
                                aggregation.getCall().getDisplayName(),
                                aggregation.getCall().getFunctionHandle(),
                                aggregation.getCall().getType(),
                                aggregation.getArguments().stream().map(argument -> removeOriginalExpression(argument, session, types)).collect(toImmutableList())),
                        aggregation.getFilter().map(filter -> removeOriginalExpression(filter, session, types)),
                        aggregation.getOrderBy(),
                        aggregation.isDistinct(),
                        aggregation.getMask());
            }

            private Map<NodeRef<Expression>, Type> analyzeAggregationExpressionTypes(AggregationNode.Aggregation aggregation, Session session, TypeProvider typeProvider)
            {
                List<LambdaExpression> lambdaExpressions = aggregation.getArguments().stream()
                        .filter(OriginalExpressionUtils::isExpression)
                        .map(OriginalExpressionUtils::castToExpression)
                        .filter(LambdaExpression.class::isInstance)
                        .map(LambdaExpression.class::cast)
                        .collect(toImmutableList());
                ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
                if (!lambdaExpressions.isEmpty()) {
                    List<FunctionType> functionTypes = metadata.getFunctionManager().getFunctionMetadata(aggregation.getFunctionHandle()).getArgumentTypes().stream()
                            .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                            .map(typeSignature -> (FunctionType) (metadata.getTypeManager().getType(typeSignature)))
                            .collect(toImmutableList());
                    InternalAggregationFunction internalAggregationFunction = metadata.getFunctionManager().getAggregateFunctionImplementation(aggregation.getFunctionHandle());
                    List<Class> lambdaInterfaces = internalAggregationFunction.getLambdaInterfaces();
                    verify(lambdaExpressions.size() == functionTypes.size());
                    verify(lambdaExpressions.size() == lambdaInterfaces.size());

                    for (int i = 0; i < lambdaExpressions.size(); i++) {
                        LambdaExpression lambdaExpression = lambdaExpressions.get(i);
                        FunctionType functionType = functionTypes.get(i);

                        // To compile lambda, LambdaDefinitionExpression needs to be generated from LambdaExpression,
                        // which requires the types of all sub-expressions.
                        //
                        // In project and filter expression compilation, ExpressionAnalyzer.getExpressionTypesFromInput
                        // is used to generate the types of all sub-expressions. (see visitScanFilterAndProject and visitFilter)
                        //
                        // This does not work here since the function call representation in final aggregation node
                        // is currently a hack: it takes intermediate type as input, and may not be a valid
                        // function call in Presto.
                        //
                        // TODO: Once the final aggregation function call representation is fixed,
                        // the same mechanism in project and filter expression should be used here.
                        verify(lambdaExpression.getArguments().size() == functionType.getArgumentTypes().size());
                        Map<NodeRef<Expression>, Type> lambdaArgumentExpressionTypes = new HashMap<>();
                        Map<Symbol, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                        for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                            LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                            Type type = functionType.getArgumentTypes().get(j);
                            lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                            lambdaArgumentSymbolTypes.put(new Symbol(argument.getName().getValue()), type);
                        }
                        // the lambda expression itself
                        builder.put(NodeRef.of(lambdaExpression), functionType)
                                // expressions from lambda arguments
                                .putAll(lambdaArgumentExpressionTypes)
                                // expressions from lambda body
                                .putAll(getExpressionTypes(
                                        session,
                                        metadata,
                                        sqlParser,
                                        TypeProvider.copyOf(lambdaArgumentSymbolTypes),
                                        lambdaExpression.getBody(),
                                        emptyList(),
                                        NOOP));
                    }
                }
                for (RowExpression argument : aggregation.getArguments()) {
                    if (!isExpression(argument) || castToExpression(argument) instanceof LambdaExpression) {
                        continue;
                    }
                    builder.putAll(analyze(castToExpression(argument), session, typeProvider));
                }
                if (aggregation.getFilter().isPresent() && isExpression(aggregation.getFilter().get())) {
                    builder.putAll(analyze(castToExpression(aggregation.getFilter().get()), session, typeProvider));
                }
                return builder.build();
            }

            private Map<NodeRef<Expression>, Type> analyze(Expression expression, Session session, TypeProvider typeProvider)
            {
                return getExpressionTypes(
                        session,
                        metadata,
                        sqlParser,
                        typeProvider,
                        expression,
                        emptyList(),
                        NOOP);
            }

            private RowExpression toRowExpression(Expression expression, Session session, Map<NodeRef<Expression>, Type> types)
            {
                return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionManager(), metadata.getTypeManager(), session, false);
            }

            private RowExpression removeOriginalExpression(RowExpression expression, Rule.Context context)
            {
                if (isExpression(expression)) {
                    return toRowExpression(
                            castToExpression(expression),
                            context.getSession(),
                            analyze(castToExpression(expression), context.getSession(), context.getSymbolAllocator().getTypes()));
                }
                return expression;
            }

            private RowExpression removeOriginalExpression(RowExpression rowExpression, Session session, Map<NodeRef<Expression>, Type> types)
            {
                if (isExpression(rowExpression)) {
                    Expression expression = castToExpression(rowExpression);
                    return toRowExpression(expression, session, types);
                }
                return rowExpression;
            }
        };
    }
}
