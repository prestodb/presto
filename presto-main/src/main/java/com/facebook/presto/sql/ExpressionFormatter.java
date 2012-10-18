package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedExpression;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LikePredicate;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class ExpressionFormatter
{
    public static String toString(Expression expression)
    {
        return new Formatter().process(expression, null);
    }

    public static Function<Expression, String> expressionFormatterFunction()
    {
        return new Function<Expression, String>()
        {
            @Override
            public String apply(Expression input)
            {
                return ExpressionFormatter.toString(input);
            }
        };
    }

    private static class Formatter
            extends AstVisitor<String, Void>
    {
        @Override
        protected String visitNode(Node node, Void context)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        protected String visitExpression(Expression node, Void context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        @Override
        protected String visitStringLiteral(StringLiteral node, Void context)
        {
            return "'" + node.getValue() + "'";
        }

        @Override
        protected String visitLongLiteral(LongLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected String visitDoubleLiteral(DoubleLiteral node, Void context)
        {
            return node.getValue();
        }

        @Override
        protected String visitQualifiedNameReference(QualifiedNameReference node, Void context)
        {
            return node.getName().toString();
        }

        @Override
        protected String visitAliasedExpression(AliasedExpression node, Void context)
        {
            return ExpressionFormatter.toString(node.getExpression()) + ' ' + node.getAlias();
        }

        @Override
        protected String visitFunctionCall(FunctionCall node, Void context)
        {
            return node.getName() + "(" + Joiner.on(", ").join(Iterables.transform(node.getArguments(), expressionFormatterFunction())) + ")";
        }

        @Override
        protected String visitLogicalBinaryExpression(LogicalBinaryExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().toString(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitComparisonExpression(ComparisonExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitArithmeticExpression(ArithmeticExpression node, Void context)
        {
            return formatBinaryExpression(node.getType().getValue(), node.getLeft(), node.getRight());
        }

        @Override
        protected String visitLikePredicate(LikePredicate node, Void context)
        {
            StringBuilder builder = new StringBuilder();

            builder.append('(')
                    .append(ExpressionFormatter.toString(node.getValue()))
                    .append(" LIKE ")
                    .append(ExpressionFormatter.toString(node.getPattern()));

            if (node.getEscape() != null) {
                builder.append(" ESCAPE ")
                        .append(ExpressionFormatter.toString(node.getEscape()));
            }

            builder.append(')');

            return builder.toString();
        }

        @Override
        protected String visitAllColumns(AllColumns node, Void context)
        {
            if (node.getPrefix().isPresent()) {
                return node.getPrefix().get() + ".*";
            }

            return "*";
        }

        private String formatBinaryExpression(String operator, Expression left, Expression right)
        {
            return '(' + ExpressionFormatter.toString(left) + ' ' + operator + ' ' + ExpressionFormatter.toString(right) + ')';
        }
    }
}
