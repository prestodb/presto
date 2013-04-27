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
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.sql.ExpressionFormatter.expressionFormatterFunction;
import static com.google.common.base.Preconditions.checkArgument;

public final class SqlFormatter
{
    private static final String INDENT = "   ";

    private SqlFormatter() {}

    public static String formatSql(Node root)
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
            builder.append(ExpressionFormatter.formatExpression(node));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH ");
                if (with.isRecursive()) {
                    builder.append("RECURSIVE ");
                }
                builder.append('\n');
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    builder.append(query.getName());
                    appendAliasColumns(builder, query.getColumnNames());
                    builder.append(" AS ");
                    process(new Subquery(query.getQuery()), indent);
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

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

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + ExpressionFormatter.formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(Iterables.transform(node.getGroupBy(), expressionFormatterFunction())))
                        .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + ExpressionFormatter.formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + Joiner.on(", ").join(Iterables.transform(node.getOrderBy(), orderByFormatterFunction())));
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT ")
                    .append(node.isDistinct() ? "DISTINCT " : "")
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
            process(node.getRelation(), indent);

            builder.append(' ')
                    .append(node.getAlias());

            appendAliasColumns(builder, node.getColumnNames());

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

    static Function<SortItem, String> orderByFormatterFunction()
    {
        return new Function<SortItem, String>()
        {
            @Override
            public String apply(SortItem input)
            {
                StringBuilder builder = new StringBuilder();

                builder.append(ExpressionFormatter.formatExpression(input.getSortKey()))
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

    private static void appendAliasColumns(StringBuilder builder, List<String> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }
}
