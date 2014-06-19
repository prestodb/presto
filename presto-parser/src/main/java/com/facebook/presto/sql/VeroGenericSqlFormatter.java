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

import static com.facebook.presto.sql.ExpressionFormatter.formatExpression;
import static com.facebook.presto.sql.ExpressionFormatter.formatSortItems;
import static com.google.common.collect.Iterables.getOnlyElement;

import java.util.Iterator;

import com.facebook.presto.sql.tree.CreateTempTable;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Join;
import com.facebook.presto.sql.tree.JoinCriteria;
import com.facebook.presto.sql.tree.JoinOn;
import com.facebook.presto.sql.tree.JoinUsing;
import com.facebook.presto.sql.tree.NaturalJoin;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.Relation;
import com.facebook.presto.sql.tree.Select;
import com.facebook.presto.sql.tree.SelectItem;
import com.google.common.base.Joiner;

public class VeroGenericSqlFormatter
{
    private static final String INDENT = "    ";

    private VeroGenericSqlFormatter() {}

    public static String formatSql(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder).process(root, 0);
        return builder.toString();
    }

    public static class Formatter
        extends SqlFormatter.Formatter
    {
        public Formatter(StringBuilder builder)
        {
            super(builder);
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

            /* ToDo: yulin
            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY " + Joiner.on(", ").join(Iterables.transform(node.getGroupBy(), expressionFormatterFunction())))
                        .append('\n');
            }
            */
            // yulin: new format
            if (!node.getGroupBy().isEmpty()) {
                append(indent, "GROUP BY");
                //append(indent, "  ");
                Iterator<Expression> expressions = node.getGroupBy().iterator();
                while (expressions.hasNext()) {
                    builder.append('\n');
                    append(indent + 1, formatExpression(expressions.next()));
                    if (expressions.hasNext()) {
                        builder.append(',');
                    }
                }
                builder.append('\n');
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
                    // ToDo: yulin
                    /*
                    builder.append("\n")
                            .append(indentString(indent))
                            .append(first ? "  " : ", ");
                    */
                    // yulin
                    if (first) {
                        builder.append("\n").append(indentString(indent + 1));
                    }
                    else {
                        builder.append(",").append("\n").append(indentString(indent + 1));
                    }

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
        protected Void visitJoin(Join node, Integer indent)
        {
            JoinCriteria criteria = node.getCriteria().orNull();
            String type = node.getType().toString();
            if (criteria instanceof NaturalJoin) {
                type = "NATURAL " + type;
            }

            // TODO: yulin remove extra parentheses
            //builder.append('(');
            process(node.getLeft(), indent);

            builder.append('\n');
            // ToDo: yulin
            //append(indent, type).append(" JOIN ");
            append(indent + 1, type).append(" JOIN ");

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

            // TODO: yulin remove extra parentheses
            //builder.append(")");

            return null;
        }

        @Override
        protected Void visitCreateTempTable(CreateTempTable node, Integer indent)
        {
            builder.append("CREATE TEMP TABLE ")
                    .append(node.getName())
                    .append(" AS ");

            process(node.getQuery(), indent);

            return null;
        }
    }
}
