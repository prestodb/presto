package com.facebook.presto.sql.planner;

import com.facebook.presto.sql.ExpressionFormatter;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.FieldOrExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRewriter;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.TreeRewriter;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.Map;

/**
 * Keeps track of fields and expressions and their mapping to symbols in the current plan
 */
class TranslationMap
{
    // all expressions are rewritten in terms of fields declared by this relation plan
    private final RelationPlan rewriteBase;
    private final Analysis analysis;

    private final Map<Field, Symbol> fieldMappings = new HashMap<>();
    private final Map<Expression, Symbol> expressionMappings = new HashMap<>();

    public TranslationMap(RelationPlan rewriteBase, Analysis analysis)
    {
        this.rewriteBase = rewriteBase;
        this.analysis = analysis;
    }

    public RelationPlan getRelationPlan()
    {
        return rewriteBase;
    }

    public void addMappings(Map<Field, Symbol> mappings)
    {
        this.fieldMappings.putAll(mappings);
    }

    public void addMappingsFrom(TranslationMap other)
    {
        expressionMappings.putAll(other.expressionMappings);
        fieldMappings.putAll(other.fieldMappings);
    }

    public Expression rewrite(Expression expression)
    {
        // first, translate names from sql-land references to plan symbols
        Expression mapped = translateNamesToSymbols(expression);

        // then rewrite subexpressions in terms of the current mappings
        return TreeRewriter.rewriteWith(new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteExpression(Expression node, Void context, TreeRewriter<Void> treeRewriter)
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
        if (fieldOrExpression.getField().isPresent()) {
            Symbol symbol = fieldMappings.get(fieldOrExpression.getField().get());
            Preconditions.checkState(symbol != null, "No mapping for field '%s'", fieldOrExpression.getField().get());

            return new QualifiedNameReference(symbol.toQualifiedName());
        }
        else {
            return rewrite(fieldOrExpression.getExpression().get());
        }
    }


    public void put(Expression expression, Symbol symbol)
    {
        expressionMappings.put(translateNamesToSymbols(expression), symbol);
    }

    public void put(FieldOrExpression fieldOrExpression, Symbol symbol)
    {
        if (fieldOrExpression.getExpression().isPresent()) {
            put(fieldOrExpression.getExpression().get(), symbol);
        }
        else {
            fieldMappings.put(fieldOrExpression.getField().get(), symbol);
        }
    }


    public Symbol get(Expression expression)
    {
        Expression translated = translateNamesToSymbols(expression);

        Preconditions.checkArgument(expressionMappings.containsKey(translated), "No mapping for expression: %s", ExpressionFormatter.toString(expression));
        return expressionMappings.get(translated);
    }

    public Symbol get(FieldOrExpression fieldOrExpression)
    {
        if (fieldOrExpression.getExpression().isPresent()) {
            return get(fieldOrExpression.getExpression().get());
        }
        else {
            Field field = fieldOrExpression.getField().get();
            Preconditions.checkArgument(fieldMappings.containsKey(field), "No mapping for field: %s", field);
            return fieldMappings.get(field);
        }
    }


    private Expression translateNamesToSymbols(Expression expression)
    {
        final Map<QualifiedName, Field> resolvedNames = analysis.getResolvedNames(expression);
        Preconditions.checkArgument(resolvedNames != null, "No resolved names for expression %s", ExpressionFormatter.toString(expression));

        return TreeRewriter.rewriteWith(new NodeRewriter<Void>()
        {
            @Override
            public Node rewriteQualifiedNameReference(QualifiedNameReference node, Void context, TreeRewriter<Void> treeRewriter)
            {
                QualifiedName name = node.getName();

                Field field = resolvedNames.get(name);
                Preconditions.checkState(field != null, "No field mapping for name '%s'", name);

                Symbol symbol = rewriteBase.getSymbol(field);
                Preconditions.checkState(symbol != null, "No symbol mapping for name '%s' (%s)", name, field);

                return new QualifiedNameReference(symbol.toQualifiedName());
            }
        }, expression);
    }
}
