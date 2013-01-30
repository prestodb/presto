package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.Subquery;
import com.facebook.presto.sql.tree.Table;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.Iterator;

import static com.facebook.presto.sql.ExpressionFormatter.expressionFormatterFunction;
import static com.google.common.base.Preconditions.checkArgument;

public class SqlFormatter
{
    private static final String INDENT = "   ";

    public static String toString(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    private static class Formatter
            extends DefaultTraversalVisitor<Void, Integer>
    {
        private final StringBuilder builder;

        public Formatter(StringBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        protected Void visitNode(Node node, Integer indent)
        {
            throw new UnsupportedOperationException("not yet implemented: " + node);
        }

        @Override
        protected Void visitExpression(Expression node, Integer indent)
        {
            checkArgument(indent == 0, "visitExpression should only be called at root");
            builder.append(ExpressionFormatter.toString(node));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            process(node.getSelect(), indent);

            append(indent, "FROM ");
            if (node.getFrom().size() > 1) {
                builder.append('\n');
                Iterator<Relation> relations = node.getFrom().iterator();
                while (relations.hasNext()) {
                    process(relations.next(), indent);
                    if (relations.hasNext()) {
                        builder.append(", ");
                    }
                }
            }
            else {
                process(Iterables.getOnlyElement(node.getFrom()), indent);
            }

            builder.append('\n');

            if (node.getWhere() != null) {
                append(indent, "WHERE " + ExpressionFormatter.toString(node.getWhere()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(Iterables.transform(node.getGroupBy(), expressionFormatterFunction())))
                        .append('\n');
            }

            if (node.getHaving() != null) {
                append(indent, "HAVING " + ExpressionFormatter.toString(node.getHaving()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + Joiner.on(", ").join(Iterables.transform(node.getOrderBy(), orderByFormatterFunction())));
            }

            if (node.getLimit() != null) {
                append(indent, "LIMIT " + node.getLimit());
            }

            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT ")
                    .append(Joiner.on(", ").join(Iterables.transform(node.getSelectItems(), expressionFormatterFunction())))
                    .append('\n');

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(node.getName().toString());
            return null;
        }

        @Override
        protected Void visitAliasedRelation(AliasedRelation node, Integer indent)
        {
            if (node.getColumnNames() != null && !node.getColumnNames().isEmpty()) {
                throw new UnsupportedOperationException("not yet implemented: relation alias with column mappings"); // TODO
            }

            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(node.getAlias());

            return null;
        }

        @Override
        protected Void visitSubquery(Subquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ")");

            return null;
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(Strings.repeat(INDENT, indent))
                    .append(value);
        }
    }

    private static Function<SortItem, String> orderByFormatterFunction()
    {
        return new Function<SortItem, String>()
        {
            @Override
            public String apply(SortItem input)
            {
                StringBuilder builder = new StringBuilder();

                builder.append(ExpressionFormatter.toString(input.getSortKey()))
                        .append(' ');

                switch (input.getOrdering()) {
                    case ASCENDING:
                        builder.append("ASC");
                        break;
                    case DESCENDING:
                        builder.append("DESC");
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown ordering: " + input.getOrdering());
                }

                switch (input.getNullOrdering()) {
                    case FIRST:
                        builder.append(" NULLS FIRST");
                        break;
                    case LAST:
                        builder.append(" NULLS LAST");
                        break;
                    case UNDEFINED:
                        // no op
                        break;
                    default:
                        throw new UnsupportedOperationException("unknown null ordering: " + input.getNullOrdering());
                }

                return builder.toString();
            }
        };
    }
}
