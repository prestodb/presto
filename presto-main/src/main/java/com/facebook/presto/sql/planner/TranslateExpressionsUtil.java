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
package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.FunctionType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.security.DenyAllAccessControl;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.RelationId;
import com.facebook.presto.sql.analyzer.RelationType;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.relational.SqlToRowExpressionTranslator;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.WarningCollector.NOOP;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.analyzeExpression;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Collections.emptyMap;

public class TranslateExpressionsUtil
{
    private TranslateExpressionsUtil() {}

    public static RowExpression toRowExpression(Expression expression, Metadata metadata, Session session, SqlParser sqlParser, VariableAllocator variableAllocator, Analysis analysis, SqlToRowExpressionTranslator.Context context)
    {
        Scope scope = Scope.builder().withRelationType(RelationId.anonymous(), new RelationType()).build();
        analyzeExpression(session,
                metadata,
                new DenyAllAccessControl(),
                sqlParser,
                scope,
                TypeProvider.viewOf(variableAllocator.getVariables()),
                analysis,
                expression,
                WarningCollector.NOOP,
                analysis.getTypes()).getExpressionTypes();
        return toRowExpression(
                expression,
                metadata,
                session,
                analysis.getTypes(), // We need to pass all types when translating subqueries. TODO(pranjalssh): Add a proper test for complex queries which need this
                context);
    }

    public static RowExpression toRowExpression(Expression expression, Metadata metadata, Session session, Map<NodeRef<Expression>, Type> types, SqlToRowExpressionTranslator.Context context)
    {
        return SqlToRowExpressionTranslator.translate(expression, types, ImmutableMap.of(), metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(), session, context);
    }

    public static Map<NodeRef<Expression>, Type> analyzeCallExpressionTypes(
            FunctionHandle functionHandle,
            List<Expression> arguments,
            Metadata metadata,
            SqlParser sqlParser,
            Session session,
            TypeProvider typeProvider)
    {
        List<LambdaExpression> lambdaExpressions = arguments.stream()
                .filter(LambdaExpression.class::isInstance)
                .map(LambdaExpression.class::cast)
                .collect(toImmutableList());
        ImmutableMap.Builder<NodeRef<Expression>, Type> builder = ImmutableMap.<NodeRef<Expression>, Type>builder();
        if (!lambdaExpressions.isEmpty()) {
            List<FunctionType> functionTypes = metadata.getFunctionAndTypeManager().getFunctionMetadata(functionHandle).getArgumentTypes().stream()
                    .filter(typeSignature -> typeSignature.getBase().equals(FunctionType.NAME))
                    .map(typeSignature -> (FunctionType) (metadata.getFunctionAndTypeManager().getType(typeSignature)))
                    .collect(toImmutableList());
            JavaAggregationFunctionImplementation javaAggregateFunctionImplementation = metadata.getFunctionAndTypeManager().getJavaAggregateFunctionImplementation(functionHandle);
            if (javaAggregateFunctionImplementation instanceof BuiltInAggregationFunctionImplementation) {
                List<Class> lambdaInterfaces = ((BuiltInAggregationFunctionImplementation) javaAggregateFunctionImplementation).getLambdaInterfaces();
                verify(lambdaExpressions.size() == functionTypes.size());
                verify(lambdaExpressions.size() == lambdaInterfaces.size());
            }

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
                Map<String, Type> lambdaArgumentSymbolTypes = new HashMap<>();
                for (int j = 0; j < lambdaExpression.getArguments().size(); j++) {
                    LambdaArgumentDeclaration argument = lambdaExpression.getArguments().get(j);
                    Type type = functionType.getArgumentTypes().get(j);
                    lambdaArgumentExpressionTypes.put(NodeRef.of(argument), type);
                    lambdaArgumentSymbolTypes.put(argument.getName().getValue(), type);
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
                                emptyMap(),
                                NOOP));
            }
        }
        for (Expression argument : arguments) {
            if (argument instanceof LambdaExpression) {
                continue;
            }
            builder.putAll(analyze(argument, metadata, sqlParser, session, typeProvider));
        }
        return builder.build();
    }

    private static Map<NodeRef<Expression>, Type> analyze(Expression expression, Metadata metadata, SqlParser sqlParser, Session session, TypeProvider typeProvider)
    {
        return getExpressionTypes(
                session,
                metadata,
                sqlParser,
                typeProvider,
                expression,
                emptyMap(),
                NOOP);
    }
}
