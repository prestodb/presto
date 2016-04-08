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
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.type.TypeRegistry.isTypeOnlyCoercion;
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
    private final Map<Expression, Symbol> expressionMappings = new HashMap<>();

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

    public void setFieldMappings(List<Symbol> symbols)
    {
        Preconditions.checkArgument(symbols.size() == fieldSymbols.length, "size of symbols list (%s) doesn't match number of expected fields (%s)", symbols.size(), fieldSymbols.length);

        for (int i = 0; i < symbols.size(); i++) {
            this.fieldSymbols[i] = symbols.get(i);
        }
    }

    public void copyMappingsFrom(TranslationMap other)
    {
        Preconditions.checkArgument(other.fieldSymbols.length == fieldSymbols.length,
                "number of fields in other (%s) doesn't match number of expected fields (%s)",
                other.fieldSymbols.length,
                fieldSymbols.length);

        expressionMappings.putAll(other.expressionMappings);
        System.arraycopy(other.fieldSymbols, 0, fieldSymbols, 0, other.fieldSymbols.length);
    }

    public void putExpressionMappingsFrom(TranslationMap other)
    {
        expressionMappings.putAll(other.expressionMappings);
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
                // convert expression to qualified name reference (containing a symbol) if rewrite registered
                Expression rewrittenExpression;
                Symbol symbol = expressionMappings.get(node);
                if (symbol != null) {
                    rewrittenExpression = new QualifiedNameReference(symbol.toQualifiedName());
                }
                else {
                    rewrittenExpression = treeRewriter.defaultRewrite(node, context);
                }

                return rewrittenExpression;
            }
        }, mapped);
    }

    public void put(Expression expression, Symbol symbol)
    {
        if (expression instanceof FieldReference) {
            int fieldIndex = ((FieldReference) expression).getFieldIndex();
            fieldSymbols[fieldIndex] = symbol;
            expressionMappings.put(new QualifiedNameReference(rewriteBase.getSymbol(fieldIndex).toQualifiedName()), symbol);
            return;
        }

        Expression translated = translateNamesToSymbols(expression);
        expressionMappings.put(translated, symbol);

        // also update the field mappings if this expression is a field reference
        analysis.getFieldIndex(expression).ifPresent(index -> fieldSymbols[index] = symbol);
    }

    public Symbol get(Expression expression)
    {
        if (expression instanceof FieldReference) {
            int field = ((FieldReference) expression).getFieldIndex();
            Preconditions.checkArgument(fieldSymbols[field] != null, "No mapping for field: %s", field);
            return fieldSymbols[field];
        }

        Expression translated = translateNamesToSymbols(expression);

        Preconditions.checkArgument(expressionMappings.containsKey(translated), "No mapping for expression: %s", expression);
        return expressionMappings.get(translated);
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
                return new QualifiedNameReference(symbol.toQualifiedName());
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
                Expression rewrittenExpression = new QualifiedNameReference(symbol.toQualifiedName());

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
                    Type type = analysis.getType(original);
                    rewritten = new Cast(
                            rewritten,
                            coercion.getTypeSignature().toString(),
                            false,
                            isTypeOnlyCoercion(type, coercion));
                }
                return rewritten;
            }
        }, expression);
    }
}
