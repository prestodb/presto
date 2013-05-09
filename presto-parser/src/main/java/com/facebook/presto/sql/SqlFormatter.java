package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.sql.tree.TableSubquery;
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
import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
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
            extends AstVisitor<Void, Integer>
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
            builder.append(formatExpression(node));
            return null;
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    builder.append(" RECURSIVE");
                }
                builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, query.getName());
                    appendAliasColumns(builder, query.getColumnNames());
                    builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    builder.append('\n');
                    if (queries.hasNext()) {
                        builder.append(", ");
                    }
                }
            }

            process(node.getSelect(), indent);

            if (node.getFrom() != null) {
                append(indent, "FROM");
                if (node.getFrom().size() > 1) {
                    builder.append('\n');
                    append(indent, "  ");
                    Iterator<Relation> relations = node.getFrom().iterator();
                    while (relations.hasNext()) {
                        process(relations.next(), indent);
                        if (relations.hasNext()) {
                            builder.append('\n');
                            append(indent, ", ");
                        }
                    }
                }
                else {
                    builder.append(' ');
                    process(Iterables.getOnlyElement(node.getFrom()), indent);
                }
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(Iterables.transform(node.getGroupBy(), expressionFormatterFunction())))
                        .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + Joiner.on(", ").join(Iterables.transform(node.getOrderBy(), orderByFormatterFunction())))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitSelect(Select node, Integer indent)
        {
            append(indent, "SELECT");
            if (node.isDistinct()) {
                builder.append(" DISTINCT");
            }

            if (node.getSelectItems().size() > 1) {
                builder.append("\n  ");
                append(indent, Joiner.on('\n' + indentString(indent) + ", ")
                        .join(Iterables.transform(node.getSelectItems(), expressionFormatterFunction())));
            }
            else {
                builder.append(' ')
                        .append(formatExpression(Iterables.getOnlyElement(node.getSelectItems())));
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitTable(Table node, Integer indent)
        {
            builder.append(node.getName().toString());
            return null;
        }

        @Override
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria();
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            builder.append('(');
            process(node.getLeft(), indent);

            builder.append('\n');
            append(indent, type).append(" JOIN ");

            process(node.getRight(), indent);

            if (criteria instanceof JoinUsing) {
                JoinUsing using = (JoinUsing) criteria;
                builder.append(" USING (")
                        .append(Joiner.on(", ").join(using.getColumns()))
                        .append(")");
            }
            else if (criteria instanceof JoinOn) {
                JoinOn on = (JoinOn) criteria;
                builder.append(" ON (")
                        .append(formatExpression(on.getExpression()))
                        .append(")");
            }
            else if (!(criteria instanceof NaturalJoin)) {
                throw new UnsupportedOperationException("unknown join criteria: " + criteria);
            }

            builder.append(")");

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
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ")");

            return null;
        }

        private StringBuilder append(int indent, String value)
        {
            return builder.append(indentString(indent))
                    .append(value);
        }

        private static String indentString(int indent)
        {
            return Strings.repeat(INDENT, indent);
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

                builder.append(formatExpression(input.getSortKey()));

                switch (input.getOrdering()) {
                    case ASCENDING:
                        builder.append(" ASC");
                        break;
                    case DESCENDING:
                        builder.append(" DESC");
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
