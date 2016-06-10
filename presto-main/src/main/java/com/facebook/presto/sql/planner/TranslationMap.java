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

import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FieldReference;
import com.facebook.presto.sql.tree.QualifiedNameReference;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final Analysis analysis;

    // current mappings of underlying field -> symbol for translating direct field references
    private final Symbol[] fieldSymbols;

    // current mappings of sub-expressions -> symbol
    private final Map<Expression, Symbol> expressionToSymbols = new HashMap<>();
    private final Map<Expression, Expression> expressionToExpressions = new HashMap<>();

    public TranslationMap(RelationPlan rewriteBase, Analysis analysis)
    {
        this.rewriteBase = rewriteBase;
        this.analysis = analysis;

        fieldSymbols = new Symbol[rewriteBase.getOutputSymbols().size()];
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public Analysis getAnalysis()
    {
        return analysis;
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
                else if (expressionToExpressions.containsKey(node)) {
                    Expression mapping = expressionToExpressions.get(node);
                    mapping = translateNamesToSymbols(mapping);
                    return treeRewriter.defaultRewrite(mapping, context);
                }
                else {
                    return treeRewriter.defaultRewrite(node, context);
                }
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
        analysis.getFieldIndex(expression).ifPresent(index -> fieldSymbols[index] = symbol);
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
        checkArgument(expressionToSymbols.containsKey(translated), "No mapping for expression: %s", expression);
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
            public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return rewriteExpressionWithResolvedName(node);
            }

            private Expression rewriteExpressionWithResolvedName(Expression node)
            {
                Optional<Integer> fieldIndex = analysis.getFieldIndex(node);
                checkState(fieldIndex.isPresent(), "No field mapping for node '%s'", node);

                Symbol symbol = rewriteBase.getSymbol(fieldIndex.get());
                checkState(symbol != null, "No symbol mapping for node '%s' (%s)", node, fieldIndex.get());
                Expression rewrittenExpression = symbol.toSymbolReference();

                return coerceIfNecessary(node, rewrittenExpression);
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (analysis.getFieldIndex(node).isPresent()) {
                    return rewriteExpressionWithResolvedName(node);
                }
                return rewriteExpression(node, context, treeRewriter);
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
        }, expression);
    }
}
