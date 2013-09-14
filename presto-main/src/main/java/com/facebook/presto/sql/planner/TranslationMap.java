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

import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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
                Symbol symbol = expressionMappings.get(node);
                if (symbol != null) {
                    return new QualifiedNameReference(symbol.toQualifiedName());
                }

                return treeRewriter.defaultRewrite(node, context);
            }
        }, mapped);
    }

    public Expression rewrite(FieldOrExpression fieldOrExpression)
    {
        if (fieldOrExpression.isFieldReference()) {
            int fieldIndex = fieldOrExpression.getFieldIndex();
            Symbol symbol = fieldSymbols[fieldIndex];
            Preconditions.checkState(symbol != null, "No mapping for field '%s'", fieldIndex);

            return new QualifiedNameReference(symbol.toQualifiedName());
        }
        else {
            return rewrite(fieldOrExpression.getExpression());
        }
    }

    public void put(Expression expression, Symbol symbol)
    {
        Expression translated = translateNamesToSymbols(expression);
        expressionMappings.put(translated, symbol);

        // also update the field mappings if this expression is a simple field reference
        if (expression instanceof QualifiedNameReference) {
            int fieldIndex = analysis.getResolvedNames(expression).get(((QualifiedNameReference) expression).getName());
            fieldSymbols[fieldIndex] = symbol;
        }
    }

    public void put(FieldOrExpression fieldOrExpression, Symbol symbol)
    {
        if (fieldOrExpression.isFieldReference()) {
            int fieldIndex = fieldOrExpression.getFieldIndex();
            fieldSymbols[fieldIndex] = symbol;
        }
        else {
            put(fieldOrExpression.getExpression(), symbol);
        }
    }

    public Symbol get(Expression expression)
    {
        Expression translated = translateNamesToSymbols(expression);

        Preconditions.checkArgument(expressionMappings.containsKey(translated), "No mapping for expression: %s", expression);
        return expressionMappings.get(translated);
    }

    public Symbol get(FieldOrExpression fieldOrExpression)
    {
        if (fieldOrExpression.isFieldReference()) {
            int field = fieldOrExpression.getFieldIndex();
            Preconditions.checkArgument(fieldSymbols[field] != null, "No mapping for field: %s", field);
            return fieldSymbols[field];
        }
        else {
            return get(fieldOrExpression.getExpression());
        }
    }

    private Expression translateNamesToSymbols(Expression expression)
    {
        final Map<QualifiedName, Integer> resolvedNames = analysis.getResolvedNames(expression);
        Preconditions.checkArgument(resolvedNames != null, "No resolved names for expression %s", expression);

        return ExpressionTreeRewriter.rewriteWith(new ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteQualifiedNameReference(QualifiedNameReference node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                QualifiedName name = node.getName();

                Integer fieldIndex = resolvedNames.get(name);
                Preconditions.checkState(fieldIndex != null, "No field mapping for name '%s'", name);

                Symbol symbol = rewriteBase.getSymbol(fieldIndex);
                Preconditions.checkState(symbol != null, "No symbol mapping for name '%s' (%s)", name, fieldIndex);

                return new QualifiedNameReference(symbol.toQualifiedName());
            }
        }, expression);
    }
}
