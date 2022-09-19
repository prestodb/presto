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
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.json.ir.IrJsonPath;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.ResolvedField;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.EnumLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.JsonExists;
import com.facebook.presto.sql.tree.JsonPathParameter;
import com.facebook.presto.sql.tree.JsonQuery;
import com.facebook.presto.sql.tree.JsonValue;
import com.facebook.presto.sql.tree.LambdaArgumentDeclaration;
import com.facebook.presto.sql.tree.LambdaExpression;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.type.JsonPath2016Type;
import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.TypeUtils.isEnumType;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.MetadataUtil.createQualifiedName;
import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.JSON_NO_PARAMETERS_ROW_TYPE;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.createSymbolReference;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getNodeLocation;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.resolveEnumLiteral;
import static com.facebook.presto.sql.tree.JsonQuery.QuotesBehavior.KEEP;
import static com.facebook.presto.sql.tree.JsonQuery.QuotesBehavior.OMIT;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final Analysis analysis;
    private final Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap;
    private final FunctionAndTypeManager functionAndTypeManager;
    private final Session session;

    // current mappings of underlying field -> symbol for translating direct field references
    private final VariableReferenceExpression[] fieldVariables;

    // current mappings of sub-expressions -> symbol
    private final Map<Expression, VariableReferenceExpression> expressionToVariables = new HashMap<>();
    private final Map<Expression, Expression> expressionToExpressions = new HashMap<>();

    public TranslationMap(FunctionAndTypeManager functionAndTypeManager, Session session, RelationPlan rewriteBase, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> lambdaDeclarationToVariableMap)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.session = requireNonNull(session, "session is null");
        this.rewriteBase = requireNonNull(rewriteBase, "rewriteBase is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaDeclarationToVariableMap = requireNonNull(lambdaDeclarationToVariableMap, "lambdaDeclarationToVariableMap is null");

        fieldVariables = new VariableReferenceExpression[rewriteBase.getFieldMappings().size()];
    }

    public FunctionAndTypeManager getFunctionAndTypeManager()
    {
        return functionAndTypeManager;
    }

    public Session getSession()
    {
        return session;
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public Map<NodeRef<LambdaArgumentDeclaration>, VariableReferenceExpression> getLambdaDeclarationToVariableMap()
    {
        return lambdaDeclarationToVariableMap;
    }

    public void setFieldMappings(List<VariableReferenceExpression> variables)
    {
        checkArgument(variables.size() == fieldVariables.length, "size of variables list (%s) doesn't match number of expected fields (%s)", variables.size(), fieldVariables.length);

        for (int i = 0; i < variables.size(); i++) {
            this.fieldVariables[i] = variables.get(i);
        }
    }

    public void copyMappingsFrom(TranslationMap other)
    {
        checkArgument(other.fieldVariables.length == fieldVariables.length,
                "number of fields in other (%s) doesn't match number of expected fields (%s)",
                other.fieldVariables.length,
                fieldVariables.length);

        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
        System.arraycopy(other.fieldVariables, 0, fieldVariables, 0, other.fieldVariables.length);
    }

    public void putExpressionMappingsFrom(TranslationMap other)
    {
        expressionToVariables.putAll(other.expressionToVariables);
        expressionToExpressions.putAll(other.expressionToExpressions);
    }

    public Expression rewrite(Expression expression)
    {
        // first, translate names from sql-land references to plan symbols
        Expression mapped = translateNamesToSymbols(expression);

        // then rewrite subexpressions in terms of the current mappings
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (expressionToVariables.containsKey(node)) {
                    return new SymbolReference(expression.getLocation(), expressionToVariables.get(node).getName());
                }

                Expression translated = expressionToExpressions.getOrDefault(node, node);
                return treeRewriter.defaultRewrite(translated, context);
            }
        }, mapped);
    }

    public void put(Expression expression, VariableReferenceExpression variable)
    {
        if (expression instanceof FieldReference) {
            int fieldIndex = ((FieldReference) expression).getFieldIndex();
            fieldVariables[fieldIndex] = variable;
            expressionToVariables.put(new SymbolReference(expression.getLocation(), rewriteBase.getVariable(fieldIndex).getName()), variable);
            return;
        }

        Expression translated = translateNamesToSymbols(expression);
        expressionToVariables.put(translated, variable);

        // also update the field mappings if this expression is a field reference
        rewriteBase.getScope().tryResolveField(expression)
                .filter(ResolvedField::isLocal)
                .ifPresent(field -> fieldVariables[field.getHierarchyFieldIndex()] = variable);
    }

    public boolean containsSymbol(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            return fieldVariables[field] != null;
        }

        Expression translated = translateNamesToSymbols(expression);
        return expressionToVariables.containsKey(translated);
    }

    public VariableReferenceExpression get(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            checkArgument(fieldVariables[field] != null, "No mapping for field: %s", field);
            return fieldVariables[field];
        }

        Expression translated = translateNamesToSymbols(expression);
        if (!expressionToVariables.containsKey(translated)) {
            checkArgument(expressionToExpressions.containsKey(translated), "No mapping for expression: %s", expression);
            return get(expressionToExpressions.get(translated));
        }

        return expressionToVariables.get(translated);
    }

    public void put(Expression expression, Expression rewritten)
    {
        expressionToExpressions.put(translateNamesToSymbols(expression), rewritten);
    }

    private Expression translateNamesToSymbols(Expression expression)
    {
        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteExpression(Expression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Expression rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                return coerceIfNecessary(node, rewrittenExpression);
            }

            @Override
            public Expression rewriteFieldReference(FieldReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                VariableReferenceExpression variable = rewriteBase.getVariable(node.getFieldIndex());
                checkState(variable != null, "No variable mapping for node '%s' (%s)", node, node.getFieldIndex());
                return new SymbolReference(getNodeLocation(variable.getSourceLocation()), variable.getName());
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(node);
                if (referencedLambdaArgumentDeclaration != null) {
                    VariableReferenceExpression variable = lambdaDeclarationToVariableMap.get(NodeRef.of(referencedLambdaArgumentDeclaration));
                    return coerceIfNecessary(node, createSymbolReference(variable));
                }
                else {
                    return rewriteExpressionWithResolvedName(node);
                }
            }

            private Expression rewriteExpressionWithResolvedName(Expression node)
            {
                return getVariable(rewriteBase, node)
                        .map(variable -> coerceIfNecessary(node, createSymbolReference(variable)))
                        .orElse(coerceIfNecessary(node, node));
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isColumnReference(node)) {
                    Optional<ResolvedField> resolvedField = rewriteBase.getScope().tryResolveField(node);
                    if (resolvedField.isPresent()) {
                        if (resolvedField.get().isLocal()) {
                            return getVariable(rewriteBase, node)
                                    .map(variable -> coerceIfNecessary(node, createSymbolReference(variable)))
                                    .orElseThrow(() -> new IllegalStateException("No symbol mapping for node " + node));
                        }
                    }
                    // do not rewrite outer references, it will be handled in outer scope planner
                    return node;
                }

                Type nodeType = analysis.getType(node);
                Type baseType = analysis.getType(node.getBase());
                if (isEnumType(baseType) && isEnumType(nodeType)) {
                    return new EnumLiteral(node.getLocation(), nodeType.getTypeSignature().toString(), resolveEnumLiteral(node, nodeType));
                }
                return rewriteExpression(node, context, treeRewriter);
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");

                ImmutableList.Builder<LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
                for (LambdaArgumentDeclaration argument : node.getArguments()) {
                    VariableReferenceExpression variable = lambdaDeclarationToVariableMap.get(NodeRef.of(argument));
                    newArguments.add(new LambdaArgumentDeclaration(new Identifier(argument.getLocation(), variable.getName())));
                }
                Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), null);
                return new LambdaExpression(node.getLocation(), newArguments.build(), rewrittenBody);
            }

            @Override
            public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getParameters().size() > node.getPosition(), "Too few parameter values");
                return coerceIfNecessary(node, analysis.getParameters().get(NodeRef.of(node)));
            }

            @Override
            public Expression rewriteJsonExists(JsonExists node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                FunctionHandle resolvedFunction = analysis.getFunctionHandle(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);
                FunctionMetadata resolvedFunctionMetadata = functionAndTypeManager.getFunctionMetadata(resolvedFunction);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonExists rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonExists.ErrorBehavior.ERROR ? "true" : "false");
                FunctionHandle inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                FunctionMetadata inputFunctionMetadata = functionAndTypeManager.getFunctionMetadata(inputToJson);
                Expression input = new FunctionCall(createQualifiedName(inputFunctionMetadata.getName()), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunctionMetadata.getArgumentTypes().get(2).toString(),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, functionAndTypeManager).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(functionAndTypeManager.getBlockEncodingSerde()).toExpression(path, functionAndTypeManager.getType(parseTypeSignature(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));
                Expression result = new FunctionCall(createQualifiedName(resolvedFunctionMetadata.getName()), arguments.build());

                return coerceIfNecessary(node, result);
            }

            @Override
            public Expression rewriteJsonValue(JsonValue node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                FunctionHandle resolvedFunction = analysis.getFunctionHandle(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);
                FunctionMetadata resolvedFunctionMetadata = functionAndTypeManager.getFunctionMetadata(resolvedFunction);

                // rewrite the input expression, default expressions, and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonValue rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonValue.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                FunctionHandle inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(inputToJson);
                Expression input = new FunctionCall(createQualifiedName(functionMetadata.getName()), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunctionMetadata.getArgumentTypes().get(2).toString(),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, functionAndTypeManager).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(functionAndTypeManager.getBlockEncodingSerde()).toExpression(path, functionAndTypeManager.getType(parseTypeSignature(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(rewritten.getEmptyDefault().orElse(new Cast(new NullLiteral(), resolvedFunctionMetadata.getReturnType().toString())))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())))
                        .add(rewritten.getErrorDefault().orElse(new Cast(new NullLiteral(), resolvedFunctionMetadata.getReturnType().toString())));

                Expression result = new FunctionCall(createQualifiedName(resolvedFunctionMetadata.getName()), arguments.build());

                return coerceIfNecessary(node, result);
            }

            @Override
            public Expression rewriteJsonQuery(JsonQuery node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                Optional<Expression> mapped = tryGetMapping(node);
                if (mapped.isPresent()) {
                    return coerceIfNecessary(node, mapped.get());
                }

                FunctionHandle resolvedFunction = analysis.getFunctionHandle(node);
                checkArgument(resolvedFunction != null, "Function has not been analyzed: %s", node);
                FunctionMetadata resolvedFunctionMetadata = functionAndTypeManager.getFunctionMetadata(resolvedFunction);

                // rewrite the input expression and JSON path parameters
                // the rewrite also applies any coercions necessary for the input functions, which are applied in the next step
                JsonQuery rewritten = treeRewriter.defaultRewrite(node, context);

                // apply the input function to the input expression
                BooleanLiteral failOnError = new BooleanLiteral(node.getErrorBehavior() == JsonQuery.EmptyOrErrorBehavior.ERROR ? "true" : "false");
                FunctionHandle inputToJson = analysis.getJsonInputFunction(node.getJsonPathInvocation().getInputExpression());
                FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(inputToJson);
                Expression input = new FunctionCall(createQualifiedName(functionMetadata.getName()), ImmutableList.of(rewritten.getJsonPathInvocation().getInputExpression(), failOnError));

                // apply the input functions to the JSON path parameters having FORMAT,
                // and collect all JSON path parameters in a Row
                ParametersRow orderedParameters = getParametersRow(
                        node.getJsonPathInvocation().getPathParameters(),
                        rewritten.getJsonPathInvocation().getPathParameters(),
                        resolvedFunctionMetadata.getArgumentTypes().get(2).toString(),
                        failOnError);

                IrJsonPath path = new JsonPathTranslator(session, functionAndTypeManager).rewriteToIr(analysis.getJsonPathAnalysis(node), orderedParameters.getParametersOrder());
                Expression pathExpression = new LiteralEncoder(functionAndTypeManager.getBlockEncodingSerde()).toExpression(path, functionAndTypeManager.getType(parseTypeSignature(JsonPath2016Type.NAME)));

                ImmutableList.Builder<Expression> arguments = ImmutableList.<Expression>builder()
                        .add(input)
                        .add(pathExpression)
                        .add(orderedParameters.getParametersRow())
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getWrapperBehavior().ordinal())))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getEmptyBehavior().ordinal())))
                        .add(new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal())));

                Expression function = new FunctionCall(createQualifiedName(resolvedFunctionMetadata.getName()), arguments.build());

                // apply function to format output
                GenericLiteral errorBehavior = new GenericLiteral("tinyint", String.valueOf(rewritten.getErrorBehavior().ordinal()));
                BooleanLiteral omitQuotes = new BooleanLiteral(node.getQuotesBehavior().orElse(KEEP) == OMIT ? "true" : "false");
                FunctionHandle outputFunction = analysis.getJsonOutputFunction(node);
                FunctionMetadata outputFunctionMetadata = functionAndTypeManager.getFunctionMetadata(outputFunction);
                Expression result = new FunctionCall(createQualifiedName(outputFunctionMetadata.getName()), ImmutableList.of(function, errorBehavior, omitQuotes));

                // cast to requested returned type
                Type returnedType = node.getReturnedType()
                        .map(Expression::toString)
                        .map(TypeSignature::parseTypeSignature)
                        .map(functionAndTypeManager::getType)
                        .orElse(VARCHAR);

                Type resultType = functionAndTypeManager.getType(outputFunctionMetadata.getReturnType());
                if (!resultType.equals(returnedType)) {
                    result = new Cast(result, returnedType.toString());
                }

                return coerceIfNecessary(node, result);
            }

            private ParametersRow getParametersRow(
                    List<JsonPathParameter> pathParameters,
                    List<JsonPathParameter> rewrittenPathParameters,
                    String parameterRowType,
                    BooleanLiteral failOnError)
            {
                Expression parametersRow;
                List<String> parametersOrder;
                if (!pathParameters.isEmpty()) {
                    ImmutableList.Builder<Expression> parameters = ImmutableList.builder();
                    for (int i = 0; i < pathParameters.size(); i++) {
                        FunctionHandle parameterToJson = analysis.getJsonInputFunction(pathParameters.get(i).getParameter());
                        Expression rewrittenParameter = rewrittenPathParameters.get(i).getParameter();
                        if (parameterToJson != null) {
                            FunctionMetadata functionMetadata = functionAndTypeManager.getFunctionMetadata(parameterToJson);
                            parameters.add(new FunctionCall(createQualifiedName(functionMetadata.getName()), ImmutableList.of(rewrittenParameter, failOnError)));
                        }
                        else {
                            parameters.add(rewrittenParameter);
                        }
                    }
                    parametersRow = new Cast(new Row(parameters.build()), parameterRowType);
                    parametersOrder = pathParameters.stream()
                            .map(parameter -> parameter.getName().getCanonicalValue())
                            .collect(toImmutableList());
                }
                else {
                    checkState(parameterRowType.equals(JSON_NO_PARAMETERS_ROW_TYPE.toString()), "invalid type of parameters row when no parameters are passed");
                    parametersRow = new Cast(new NullLiteral(), JSON_NO_PARAMETERS_ROW_TYPE.toString());
                    parametersOrder = ImmutableList.of();
                }

                return new ParametersRow(parametersRow, parametersOrder);
            }

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
                Type coercion = analysis.getCoercion(original);
                if (coercion != null) {
                    rewritten = new Cast(
                            original.getLocation(),
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(original));
                }
                return rewritten;
            }
        }, expression, null);
    }

    private Optional<Expression> tryGetMapping(Expression expression)
    {
        if (expressionToVariables.containsKey(expression)) {
            SymbolReference symbolReference = new SymbolReference(expression.getLocation(), expressionToVariables.get(expression).getName());
            return Optional.of(symbolReference);
        }
        return Optional.empty();
    }

    private Optional<VariableReferenceExpression> getVariable(RelationPlan plan, Expression expression)
    {
        if (!analysis.isColumnReference(expression)) {
            // Expression can be a reference to lambda argument (or DereferenceExpression based on lambda argument reference).
            // In such case, the expression might still be resolvable with plan.getScope() but we should not resolve it.
            return Optional.empty();
        }
        return plan.getScope()
                .tryResolveField(expression)
                .filter(ResolvedField::isLocal)
                .map(field -> requireNonNull(plan.getFieldMappings().get(field.getHierarchyFieldIndex())));
    }

    private static class ParametersRow
    {
        private final Expression parametersRow;
        private final List<String> parametersOrder;

        public ParametersRow(Expression parametersRow, List<String> parametersOrder)
        {
            this.parametersRow = requireNonNull(parametersRow, "parametersRow is null");
            this.parametersOrder = requireNonNull(parametersOrder, "parametersOrder is null");
        }

        public Expression getParametersRow()
        {
            return parametersRow;
        }

        public List<String> getParametersOrder()
        {
            return parametersOrder;
        }
    }
}
