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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.expressions.RowExpressionRewriter;
import com.facebook.presto.expressions.RowExpressionTreeRewriter;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.SqlFunctionId;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalysis;
import com.facebook.presto.sql.parser.ParsingOptions;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.iterative.rule.LambdaCaptureDesugaringRewriter;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeSqlFunctionExpression;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DECIMAL;
import static com.facebook.presto.sql.parser.ParsingOptions.DecimalLiteralTreatment.AS_DOUBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.function.Function.identity;

public final class SqlFunctionUtils
{
    private SqlFunctionUtils() {}

    public static Expression getSqlFunctionExpression(
            FunctionMetadata functionMetadata,
            SqlInvokedScalarFunctionImplementation implementation,
            Metadata metadata,
            PlanVariableAllocator variableAllocator,
            SqlFunctionProperties sqlFunctionProperties,
            List<Expression> arguments)
    {
        Map<String, VariableReferenceExpression> argumentVariables = allocateFunctionArgumentVariables(functionMetadata, metadata, variableAllocator);
        Expression expression = getSqlFunctionImplementationExpression(functionMetadata, implementation, metadata, variableAllocator, sqlFunctionProperties, argumentVariables);
        return SqlFunctionArgumentBinder.bindFunctionArguments(
                expression,
                functionMetadata.getArgumentNames().get(),
                arguments,
                argumentVariables);
    }

    public static RowExpression getSqlFunctionRowExpression(
            FunctionMetadata functionMetadata,
            SqlInvokedScalarFunctionImplementation implementation,
            Metadata metadata,
            SqlFunctionProperties sqlFunctionProperties,
            Map<SqlFunctionId, SqlInvokedFunction> sessionFunctions,
            List<RowExpression> arguments)
    {
        PlanVariableAllocator variableAllocator = new PlanVariableAllocator();
        Map<String, VariableReferenceExpression> argumentVariables = allocateFunctionArgumentVariables(functionMetadata, metadata, variableAllocator);
        Expression expression = getSqlFunctionImplementationExpression(functionMetadata, implementation, metadata, variableAllocator, sqlFunctionProperties, argumentVariables);

        // Translate to row expression
        return SqlFunctionArgumentBinder.bindFunctionArguments(
                SqlToRowExpressionTranslator.translate(
                        expression,
                        analyzeSqlFunctionExpression(
                                metadata,
                                sqlFunctionProperties,
                                expression,
                                argumentVariables.values().stream()
                                        .collect(toImmutableMap(VariableReferenceExpression::getName, VariableReferenceExpression::getType))).getExpressionTypes(),
                        ImmutableMap.of(),
                        metadata.getFunctionAndTypeManager(),
                        Optional.empty(),
                        Optional.empty(),
                        sqlFunctionProperties,
                        sessionFunctions),
                functionMetadata.getArgumentNames().get(),
                arguments,
                argumentVariables);
    }

    private static Expression getSqlFunctionImplementationExpression(
            FunctionMetadata functionMetadata,
            SqlInvokedScalarFunctionImplementation implementation,
            Metadata metadata,
            PlanVariableAllocator variableAllocator,
            SqlFunctionProperties sqlFunctionProperties,
            Map<String, VariableReferenceExpression> argumentVariables)
    {
        checkArgument(functionMetadata.getImplementationType().equals(SQL), format("Expect SQL function, get %s", functionMetadata.getImplementationType()));
        checkArgument(functionMetadata.getArgumentNames().isPresent(), "ArgumentNames is missing");
        Expression expression = normalizeParameters(functionMetadata.getArgumentNames().get(), parseSqlFunctionExpression(implementation, sqlFunctionProperties));
        ExpressionAnalysis functionAnalysis = analyzeSqlFunctionExpression(
                metadata,
                sqlFunctionProperties,
                expression,
                argumentVariables.entrySet().stream()
                        .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getType())));
        expression = coerceIfNecessary(expression, functionAnalysis);
        return rewriteLambdaExpression(expression, argumentVariables, functionAnalysis, variableAllocator);
    }

    private static Expression normalizeParameters(List<String> argumentNames, Expression sqlFunction)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<String, String>>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Map<String, String> context, ExpressionTreeRewriter<Map<String, String>> treeRewriter)
            {
                String name = node.getValue().toLowerCase(ENGLISH);
                if (context.containsKey(name)) {
                    return new Identifier(context.get(name));
                }
                return node;
            }
        }, sqlFunction, argumentNames.stream().collect(toImmutableMap(String::toLowerCase, identity())));
    }

    private static Expression parseSqlFunctionExpression(SqlInvokedScalarFunctionImplementation functionImplementation, SqlFunctionProperties sqlFunctionProperties)
    {
        ParsingOptions parsingOptions = ParsingOptions.builder()
                .setDecimalLiteralTreatment(sqlFunctionProperties.isParseDecimalLiteralAsDouble() ? AS_DOUBLE : AS_DECIMAL)
                .build();
        // TODO: Use injector-created SqlParser, which could potentially be different from the adhoc SqlParser.
        return new SqlParser().createReturn(functionImplementation.getImplementation(), parsingOptions).getExpression();
    }

    private static Map<String, VariableReferenceExpression> allocateFunctionArgumentVariables(FunctionMetadata functionMetadata, Metadata metadata, PlanVariableAllocator variableAllocator)
    {
        List<String> argumentNames = functionMetadata.getArgumentNames().get();
        List<Type> argumentTypes = functionMetadata.getArgumentTypes().stream().map(metadata::getType).collect(toImmutableList());
        checkState(argumentNames.size() == argumentTypes.size(), format("Expect argumentNames (size %d) and argumentTypes (size %d) to be of the same size", argumentNames.size(), argumentTypes.size()));
        ImmutableMap.Builder<String, VariableReferenceExpression> builder = ImmutableMap.builder();
        for (int i = 0; i < argumentNames.size(); i++) {
            builder.put(argumentNames.get(i), variableAllocator.newVariable(argumentNames.get(i), argumentTypes.get(i)));
        }
        return builder.build();
    }

    private static Expression rewriteLambdaExpression(Expression sqlFunction, Map<String, VariableReferenceExpression> arguments, ExpressionAnalysis functionAnalysis, PlanVariableAllocator variableAllocator)
    {
        Map<NodeRef<Identifier>, LambdaArgumentDeclaration> lambdaArgumentReferences = functionAnalysis.getLambdaArgumentReferences();
        Map<NodeRef<Expression>, Type> expressionTypes = functionAnalysis.getExpressionTypes();
        // Rewrite reference to LambdaArgumentDeclaration
        Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> variables = expressionTypes.entrySet().stream()
                .filter(entry -> entry.getKey().getNode() instanceof LambdaArgumentDeclaration)
                .distinct()
                .collect(toImmutableMap(entry -> NodeRef.of((LambdaArgumentDeclaration) entry.getKey().getNode()), entry -> variableAllocator.newVariable(((LambdaArgumentDeclaration) entry.getKey().getNode()).getName(), entry.getValue(), "lambda")));

        Expression rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<NodeRef<Identifier>, LambdaArgumentDeclaration>>()
        {
            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Map<NodeRef<Identifier>, LambdaArgumentDeclaration> context, ExpressionTreeRewriter<Map<NodeRef<Identifier>, LambdaArgumentDeclaration>> treeRewriter)
            {
                return new LambdaExpression(
                        node.getArguments().stream()
                                .map(argument -> new LambdaArgumentDeclaration(new Identifier(variables.get(NodeRef.of(argument)).getName())))
                                .collect(toImmutableList()),
                        treeRewriter.rewrite(node.getBody(), context));
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Map<NodeRef<Identifier>, LambdaArgumentDeclaration> context, ExpressionTreeRewriter<Map<NodeRef<Identifier>, LambdaArgumentDeclaration>> treeRewriter)
            {
                NodeRef<Identifier> ref = NodeRef.of(node);
                if (context.containsKey(ref)) {
                    return new SymbolReference(variables.get(NodeRef.of(context.get(ref))).getName());
                }
                return node;
            }
        }, sqlFunction, lambdaArgumentReferences);

        // Rewrite function input referenced in lambda
        rewritten = ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Map<String, VariableReferenceExpression>>()
        {
            @Override
            public Expression rewriteIdentifier(Identifier node, Map<String, VariableReferenceExpression> context, ExpressionTreeRewriter<Map<String, VariableReferenceExpression>> treeRewriter)
            {
                if (context.containsKey(node.getValue())) {
                    return new SymbolReference(context.get(node.getValue()).getName());
                }
                return node;
            }
        }, rewritten, arguments);

        // Desugar lambda capture
        return LambdaCaptureDesugaringRewriter.rewrite(rewritten, variableAllocator);
    }

    private static Expression coerceIfNecessary(Expression sqlFunction, ExpressionAnalysis analysis)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<ExpressionAnalysis>()
        {
            @Override
            public Expression rewriteExpression(Expression expression, ExpressionAnalysis context, ExpressionTreeRewriter<ExpressionAnalysis> treeRewriter)
            {
                Expression rewritten = treeRewriter.defaultRewrite(expression, null);

                Type coercion = analysis.getCoercion(expression);
                if (coercion != null) {
                    return new Cast(
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(expression));
                }
                return rewritten;
            }
        }, sqlFunction, analysis);
    }

    private static final class SqlFunctionArgumentBinder
    {
        private SqlFunctionArgumentBinder() {}

        public static Expression bindFunctionArguments(Expression function, List<String> argumentNames, List<Expression> argumentValues, Map<String, VariableReferenceExpression> argumentVariables)
        {
            checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
            ImmutableMap.Builder<String, Expression> argumentBindings = ImmutableMap.builder();
            for (int i = 0; i < argumentNames.size(); i++) {
                String argumentName = argumentNames.get(i);
                argumentBindings.put(argumentVariables.get(argumentName).getName(), argumentValues.get(i));
            }
            return ExpressionTreeRewriter.rewriteWith(new ExpressionFunctionVisitor(), function, argumentBindings.build());
        }

        public static RowExpression bindFunctionArguments(RowExpression function, List<String> argumentNames, List<RowExpression> argumentValues, Map<String, VariableReferenceExpression> argumentVariables)
        {
            checkArgument(argumentNames.size() == argumentValues.size(), format("Expect same size for argumentNames (%d) and argumentValues (%d)", argumentNames.size(), argumentValues.size()));
            ImmutableMap.Builder<String, RowExpression> argumentBindings = ImmutableMap.builder();
            for (int i = 0; i < argumentNames.size(); i++) {
                String argumentName = argumentNames.get(i);
                argumentBindings.put(argumentVariables.get(argumentName).getName(), argumentValues.get(i));
            }
            return RowExpressionTreeRewriter.rewriteWith(new RowExpressionRewriter<Map<String, RowExpression>>()
            {
                @Override
                public RowExpression rewriteVariableReference(VariableReferenceExpression variable, Map<String, RowExpression> context, RowExpressionTreeRewriter<Map<String, RowExpression>> treeRewriter)
                {
                    if (context.containsKey(variable.getName())) {
                        return context.get(variable.getName());
                    }
                    return variable;
                }
            }, function, argumentBindings.build());
        }

        private static class ExpressionFunctionVisitor
                extends ExpressionRewriter<Map<String, Expression>>
        {
            @Override
            public Expression rewriteSymbolReference(SymbolReference node, Map<String, Expression> context, ExpressionTreeRewriter<Map<String, Expression>> treeRewriter)
            {
                if (context.containsKey(node.getName())) {
                    return context.get(node.getName());
                }
                return node;
            }
        }
    }
}
