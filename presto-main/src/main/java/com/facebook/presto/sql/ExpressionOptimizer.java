package com.facebook.presto.sql;

import com.facebook.presto.sql.analyzer.Symbol;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

public class ExpressionOptimizer
{
    private final Map<Symbol, String> bindings;

    // TODO: support non-string values
    public ExpressionOptimizer(Map<Symbol, String> bindings)
    {
        Preconditions.checkNotNull(bindings, "bindings is null");

        this.bindings = ImmutableMap.copyOf(bindings);
    }

    public Expression optimize(Expression expression)
    {
        return expression.accept(new Visitor(), null);
    }

    private class Visitor
        extends AstVisitor<Expression, Void>
    {
        @Override
        protected Expression visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            Symbol symbol = Symbol.fromQualifiedName(node.getName());
            String value = bindings.get(symbol);
            if (value != null) {
                return new StringLiteral(value);
            }

            return node;
        }

        @Override
        protected Expression visitComparisonExpression(ComparisonExpression node, Void context)
        {
            Expression left = process(node.getLeft(), null);
            Expression right = process(node.getRight(), null);

            if (left instanceof NullLiteral || right instanceof NullLiteral) {
                return BooleanLiteral.FALSE_LITERAL;
            }

            if (!(left instanceof Literal) || !(right instanceof Literal)) {
                return node;
            }

            Boolean result = null;
            switch (node.getType()) {
                case EQUAL:
                    result = left.equals(right);
                    break;
                case NOT_EQUAL:
                    result = !left.equals(right);
                    break;
            }

            if (result == null) {
                return node;
            }

            return result ? BooleanLiteral.TRUE_LITERAL : BooleanLiteral.FALSE_LITERAL;
        }

        @Override
        protected Expression visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            Expression left = process(node.getLeft(), context);
            Expression right = process(node.getRight(), context);

            switch (node.getType()) {
                case AND: {
                    // if either left or right is false, result is always false regardless of nulls
                    if (left.equals(BooleanLiteral.FALSE_LITERAL) || right.equals(BooleanLiteral.TRUE_LITERAL)) {
                        return left;
                    }

                    if (right.equals(BooleanLiteral.FALSE_LITERAL) || left.equals(BooleanLiteral.TRUE_LITERAL)) {
                        return right;
                    }
                }
                case OR: {
                    // if either left or right is true, result is always true regardless of nulls
                    if (left.equals(BooleanLiteral.TRUE_LITERAL) || right.equals(BooleanLiteral.FALSE_LITERAL)) {
                        return left;
                    }

                    if (right.equals(BooleanLiteral.TRUE_LITERAL) || left.equals(BooleanLiteral.FALSE_LITERAL)) {
                        return right;
                    }
                }
            }

            return node;
        }

        @Override
        protected Expression visitLiteral(Literal node, Void context)
        {
            return node;
        }

        @Override
        protected Expression visitExpression(Expression node, Void context)
        {
            return node;
        }
    }
}
