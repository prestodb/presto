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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.AliasedRelation;
import com.facebook.presto.sql.tree.AllColumns;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.CreateTable;
import com.facebook.presto.sql.tree.DropTable;
import com.facebook.presto.sql.tree.Except;
import com.facebook.presto.sql.tree.Explain;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.sql.tree.ExplainOption;
import com.facebook.presto.sql.tree.ExplainType;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Intersect;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.SampledRelation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.facebook.presto.sql.tree.ShowCatalogs;
import com.facebook.presto.sql.tree.ShowColumns;
import com.facebook.presto.sql.tree.ShowFunctions;
import com.facebook.presto.sql.tree.ShowPartitions;
import com.facebook.presto.sql.tree.ShowSchemas;
import com.facebook.presto.sql.tree.ShowTables;
import com.facebook.presto.sql.tree.SingleColumn;
import com.facebook.presto.sql.tree.Table;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.Union;
import com.facebook.presto.sql.tree.Values;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.sql.ExpressionFormatter.expressionFormatterFunction;
import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionFormatter.formatSortItems;
import static com.facebook.presto.sql.ExpressionFormatter.formatStringLiteral;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Iterables.transform;

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

            processRelation(node.getQueryBody(), indent);

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
                        .append('\n');
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT " + node.getLimit().get())
                        .append('\n');
            }

            if (node.getApproximate().isPresent()) {
                String confidence = node.getApproximate().get().getConfidence();
                append(indent, "APPROXIMATE AT " + confidence + " CONFIDENCE")
                        .append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
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
                    process(getOnlyElement(node.getFrom()), indent);
                }
            }

            builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpression(node.getWhere().get()))
                        .append('\n');
            }

            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(transform(node.getGroupBy(), expressionFormatterFunction())))
                        .append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpression(node.getHaving().get()))
                        .append('\n');
            }

            if (!node.getOrderBy().isEmpty()) {
                append(indent, "ORDER BY " + formatSortItems(node.getOrderBy()))
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
                boolean first = true;
                for (SelectItem item : node.getSelectItems()) {
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");

                    process(item, indent);
                    first = false;
                }
            }
            else {
                builder.append(' ');
                process(getOnlyElement(node.getSelectItems()), indent);
            }

            builder.append('\n');

            return null;
        }

        @Override
        protected Void visitSingleColumn(SingleColumn node, Integer indent)
        {
            builder.append(formatExpression(node.getExpression()));
            if (node.getAlias().isPresent()) {
                builder.append(' ')
                        .append('"')
                        .append(node.getAlias().get())
                        .append('"'); // TODO: handle quoting properly
            }

            return null;
        }

        @Override
        protected Void visitAllColumns(AllColumns node, Integer context)
        {
            builder.append(node.toString());

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
            JoinCriteria criteria = node.getCriteria().orNull();
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
            else if (node.getType() != Join.Type.CROSS && !(criteria instanceof NaturalJoin)) {
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
        protected Void visitSampledRelation(SampledRelation node, Integer indent)
        {
            process(node.getRelation(), indent);

            builder.append(" TABLESAMPLE ")
                    .append(node.getType())
                    .append(" (")
                    .append(node.getSamplePercentage())
                    .append(')');

            if (node.getColumnsToStratifyOn().isPresent()) {
                builder.append(" STRATIFY ON ")
                        .append(" (")
                        .append(Joiner.on(",").join(node.getColumnsToStratifyOn().get()));
                builder.append(')');
            }

            return null;
        }

        @Override
        protected Void visitValues(Values node, Integer indent)
        {
            builder.append(" VALUES ");

            boolean first = true;
            for (Row row : node.getRows()) {
                builder.append("\n")
                        .append(indentString(indent))
                        .append(first ? "  " : ", ");

                process(row, indent + 1);
                first = false;
            }

            return null;
        }

        @Override
        protected Void visitRow(Row node, Integer indent)
        {
            builder.append('(')
                    .append(Joiner.on(", ").join(transform(node.getItems(), expressionFormatterFunction())))
                    .append(')');

            return null;
        }

        @Override
        protected Void visitTableSubquery(TableSubquery node, Integer indent)
        {
            builder.append('(')
                    .append('\n');

            process(node.getQuery(), indent + 1);

            append(indent, ") ");

            return null;
        }

        @Override
        protected Void visitUnion(Union node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("UNION ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExcept(Except node, Integer indent)
        {
            processRelation(node.getLeft(), indent);

            builder.append("EXCEPT ");
            if (!node.isDistinct()) {
                builder.append("ALL ");
            }

            processRelation(node.getRight(), indent);

            return null;
        }

        @Override
        protected Void visitIntersect(Intersect node, Integer indent)
        {
            Iterator<Relation> relations = node.getRelations().iterator();

            while (relations.hasNext()) {
                processRelation(relations.next(), indent);

                if (relations.hasNext()) {
                    builder.append("INTERSECT ");
                    if (!node.isDistinct()) {
                        builder.append("ALL ");
                    }
                }
            }

            return null;
        }

        @Override
        protected Void visitExplain(Explain node, Integer indent)
        {
            builder.append("EXPLAIN ");

            List<String> options = new ArrayList<>();

            for (ExplainOption option : node.getOptions()) {
                if (option instanceof ExplainType) {
                    options.add("TYPE " + ((ExplainType) option).getType());
                }
                else if (option instanceof ExplainFormat) {
                    options.add("FORMAT " + ((ExplainFormat) option).getType());
                }
                else {
                    throw new UnsupportedOperationException("unhandled explain option: " + option);
                }
            }

            if (!options.isEmpty()) {
                builder.append("(");
                Joiner.on(", ").appendTo(builder, options);
                builder.append(")");
            }

            builder.append("\n");

            process(node.getStatement(), indent);

            return null;
        }

        @Override
        protected Void visitShowCatalogs(ShowCatalogs node, Integer context)
        {
            builder.append("SHOW CATALOGS");

            return null;
        }

        @Override
        protected Void visitShowSchemas(ShowSchemas node, Integer context)
        {
            builder.append("SHOW SCHEMAS");

            if (node.getCatalog().isPresent()) {
                builder.append(" FROM ")
                        .append(node.getCatalog().get());
            }

            return null;
        }

        @Override
        protected Void visitShowTables(ShowTables node, Integer context)
        {
            builder.append("SHOW TABLES");

            if (node.getSchema() != null) {
                builder.append(" FROM ")
                        .append(node.getSchema());
            }

            if (node.getLikePattern() != null) {
                builder.append(" LIKE ")
                        .append(formatStringLiteral(node.getLikePattern()));
            }

            return null;
        }

        @Override
        protected Void visitShowColumns(ShowColumns node, Integer context)
        {
            builder.append("SHOW COLUMNS FROM ")
                    .append(node.getTable());

            return null;
        }

        @Override
        protected Void visitShowPartitions(ShowPartitions node, Integer context)
        {
            builder.append("SHOW PARTITIONS FROM ")
                    .append(node.getTable());

            if (node.getWhere().isPresent()) {
                builder.append(" WHERE ")
                        .append(formatExpression(node.getWhere().get()));
            }

            if (!node.getOrderBy().isEmpty()) {
                builder.append(" ORDER BY ")
                        .append(formatSortItems(node.getOrderBy()));
            }

            if (node.getLimit().isPresent()) {
                builder.append(" LIMIT ")
                        .append(node.getLimit().get());
            }

            return null;
        }

        @Override
        protected Void visitShowFunctions(ShowFunctions node, Integer context)
        {
            builder.append("SHOW FUNCTIONS");

            return null;
        }

        @Override
        protected Void visitCreateTable(CreateTable node, Integer indent)
        {
            builder.append("CREATE TABLE ")
                    .append(node.getName())
                    .append(" AS ");

            process(node.getQuery(), indent);

            return null;
        }

        @Override
        protected Void visitDropTable(DropTable node, Integer context)
        {
            builder.append("DROP TABLE ")
                    .append(node.getTableName());

            return null;
        }

        private void processRelation(Relation relation, Integer indent)
        {
            // TODO: handle this properly
            if (relation instanceof Table) {
                builder.append("TABLE ")
                        .append(((Table) relation).getName())
                        .append('\n');
            }
            else {
                process(relation, indent);
            }
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

    private static void appendAliasColumns(StringBuilder builder, List<String> columns)
    {
        if ((columns != null) && (!columns.isEmpty())) {
            builder.append(" (");
            Joiner.on(", ").appendTo(builder, columns);
            builder.append(')');
        }
    }
}
