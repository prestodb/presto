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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.ResolvedField;
import io.prestosql.sql.tree.Cast;
import io.prestosql.sql.tree.DereferenceExpression;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.ExpressionRewriter;
import io.prestosql.sql.tree.ExpressionTreeRewriter;
import io.prestosql.sql.tree.FieldReference;
import io.prestosql.sql.tree.Identifier;
import io.prestosql.sql.tree.LambdaArgumentDeclaration;
import io.prestosql.sql.tree.LambdaExpression;
import io.prestosql.sql.tree.NodeRef;
import io.prestosql.sql.tree.Parameter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final Analysis analysis;
    private final Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap;

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<Expression, Symbol> expressionToSymbols = new HashMap<>();
    private final Map<Expression, Expression> expressionToExpressions = new HashMap<>();

    public TranslationMap(RelationPlan rewriteBase, Analysis analysis, Map<NodeRef<LambdaArgumentDeclaration>, Symbol> lambdaDeclarationToSymbolMap)
    {
        this.rewriteBase = requireNonNull(rewriteBase, "rewriteBase is null");
        this.analysis = requireNonNull(analysis, "analysis is null");
        this.lambdaDeclarationToSymbolMap = requireNonNull(lambdaDeclarationToSymbolMap, "lambdaDeclarationToSymbolMap is null");

        fieldSymbols = new Symbol[rewriteBase.getFieldMappings().size()];
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public Analysis getAnalysis()
    {
        return analysis;
    }

    public Map<NodeRef<LambdaArgumentDeclaration>, Symbol> getLambdaDeclarationToSymbolMap()
    {
        return lambdaDeclarationToSymbolMap;
    }

    public void setFieldMappings(List<Symbol> symbols)
    {
        checkArgument(symbols.size() == fieldSymbols.length, "size of symbols list (%s) doesn't match number of expected fields (%s)", symbols.size(), fieldSymbols.length);

        for (int i = 0; i < symbols.size(); i++) {
            this.fieldSymbols[i] = symbols.get(i);
        }
    }

    public void copyMappingsFrom(TranslationMap other)
    {
        checkArgument(other.fieldSymbols.length == fieldSymbols.length,
                "number of fields in other (%s) doesn't match number of expected fields (%s)",
                other.fieldSymbols.length,
                fieldSymbols.length);

        expressionToSymbols.putAll(other.expressionToSymbols);
        expressionToExpressions.putAll(other.expressionToExpressions);
        System.arraycopy(other.fieldSymbols, 0, fieldSymbols, 0, other.fieldSymbols.length);
    }

    public void putExpressionMappingsFrom(TranslationMap other)
    {
        expressionToSymbols.putAll(other.expressionToSymbols);
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
                if (expressionToSymbols.containsKey(node)) {
                    return expressionToSymbols.get(node).toSymbolReference();
                }

                Expression translated = expressionToExpressions.getOrDefault(node, node);
                return treeRewriter.defaultRewrite(translated, context);
            }
        }, mapped);
    }

    public void put(Expression expression, Symbol symbol)
    {
        if (expression instanceof FieldReference) {
            int fieldIndex = ((FieldReference) expression).getFieldIndex();
            fieldSymbols[fieldIndex] = symbol;
            expressionToSymbols.put(rewriteBase.getSymbol(fieldIndex).toSymbolReference(), symbol);
            return;
        }

        Expression translated = translateNamesToSymbols(expression);
        expressionToSymbols.put(translated, symbol);

        // also update the field mappings if this expression is a field reference
        rewriteBase.getScope().tryResolveField(expression)
                .filter(ResolvedField::isLocal)
                .ifPresent(field -> fieldSymbols[field.getHierarchyFieldIndex()] = symbol);
    }

    public boolean containsSymbol(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            return fieldSymbols[field] != null;
        }

        Expression translated = translateNamesToSymbols(expression);
        return expressionToSymbols.containsKey(translated);
    }

    public Symbol get(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            checkArgument(fieldSymbols[field] != null, "No mapping for field: %s", field);
            return fieldSymbols[field];
        }

        Expression translated = translateNamesToSymbols(expression);
        if (!expressionToSymbols.containsKey(translated)) {
            checkArgument(expressionToExpressions.containsKey(translated), "No mapping for expression: %s", expression);
            return get(expressionToExpressions.get(translated));
        }

        return expressionToSymbols.get(translated);
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
                Symbol symbol = rewriteBase.getSymbol(node.getFieldIndex());
                checkState(symbol != null, "No symbol mapping for node '%s' (%s)", node, node.getFieldIndex());
                return symbol.toSymbolReference();
            }

            @Override
            public Expression rewriteIdentifier(Identifier node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                LambdaArgumentDeclaration referencedLambdaArgumentDeclaration = analysis.getLambdaArgumentReference(node);
                if (referencedLambdaArgumentDeclaration != null) {
                    Symbol symbol = lambdaDeclarationToSymbolMap.get(NodeRef.of(referencedLambdaArgumentDeclaration));
                    return coerceIfNecessary(node, symbol.toSymbolReference());
                }
                else {
                    return rewriteExpressionWithResolvedName(node);
                }
            }

            private Expression rewriteExpressionWithResolvedName(Expression node)
            {
                return getSymbol(rewriteBase, node)
                        .map(symbol -> coerceIfNecessary(node, symbol.toSymbolReference()))
                        .orElse(coerceIfNecessary(node, node));
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.isColumnReference(node)) {
                    Optional<ResolvedField> resolvedField = rewriteBase.getScope().tryResolveField(node);
                    if (resolvedField.isPresent()) {
                        if (resolvedField.get().isLocal()) {
                            return getSymbol(rewriteBase, node)
                                    .map(symbol -> coerceIfNecessary(node, symbol.toSymbolReference()))
                                    .orElseThrow(() -> new IllegalStateException("No symbol mapping for node " + node));
                        }
                    }
                    // do not rewrite outer references, it will be handled in outer scope planner
                    return node;
                }
                return rewriteExpression(node, context, treeRewriter);
            }

            @Override
            public Expression rewriteLambdaExpression(LambdaExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getCoercion(node) == null, "cannot coerce a lambda expression");

                ImmutableList.Builder<LambdaArgumentDeclaration> newArguments = ImmutableList.builder();
                for (LambdaArgumentDeclaration argument : node.getArguments()) {
                    Symbol symbol = lambdaDeclarationToSymbolMap.get(NodeRef.of(argument));
                    newArguments.add(new LambdaArgumentDeclaration(new Identifier(symbol.getName())));
                }
                Expression rewrittenBody = treeRewriter.rewrite(node.getBody(), null);
                return new LambdaExpression(newArguments.build(), rewrittenBody);
            }

            @Override
            public Expression rewriteParameter(Parameter node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                checkState(analysis.getParameters().size() > node.getPosition(), "Too few parameter values");
                return coerceIfNecessary(node, analysis.getParameters().get(node.getPosition()));
            }

            private Expression coerceIfNecessary(Expression original, Expression rewritten)
            {
                Type coercion = analysis.getCoercion(original);
                if (coercion != null) {
                    rewritten = new Cast(
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            analysis.isTypeOnlyCoercion(original));
                }
                return rewritten;
            }
        }, expression, null);
    }

    private Optional<Symbol> getSymbol(RelationPlan plan, Expression expression)
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
}
