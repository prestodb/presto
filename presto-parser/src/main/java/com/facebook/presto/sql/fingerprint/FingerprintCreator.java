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
package com.facebook.presto.sql.fingerprint;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.QueryFingerprint;
import com.facebook.presto.sql.SqlFormatter;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.QuerySpecification;
import com.facebook.presto.sql.tree.TableSubquery;
import com.facebook.presto.sql.tree.With;
import com.facebook.presto.sql.tree.WithQuery;
import com.google.common.hash.Hashing;

import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionFormatter.formatGroupBy;

/**
 * Contains a static method for creating a fingerprint from a given AST Node.
 * We use Farm Hash since it's one of the fastest consistent hash algorithms.
 * Moreover we use a 64 bit hash since it's much shorter than MD5 or SHA.
 */
public class FingerprintCreator
{
    private static final Logger log = Logger.get(FingerprintCreator.class);

    private FingerprintCreator() {}

    public static QueryFingerprint createFingerPrint(Node root)
    {
        try {
            String fingerprint = format(root);
            return new QueryFingerprint(
                    fingerprint,
                    Hashing.farmHashFingerprint64()
                            .hashString(fingerprint, StandardCharsets.UTF_8)
                            .toString());
        }
        catch (Exception e) {
            log.error("Error in creating fingerprint for query", e);
            return null;
        }
    }

    public static String format(Node root)
    {
        StringBuilder builder = new StringBuilder();
        new Formatter(builder, Optional.empty()).process(root, 0);
        return builder.toString();
    }

    /**
     * Taken almost as is from SqlFormatter.Formatter, with the exception of overriding
     * the Expression Generator and a couple of methods where we need to do manual
     * literal substitution in the LIMIT clause.
     */
    private static class Formatter
            extends SqlFormatter.Formatter
    {
        private final FingerprintVisitorContext visitorContext;

        public Formatter(StringBuilder builder, Optional<List<Expression>> parameters)
        {
            super(builder, parameters);
            this.expressionGenerator = this::formatExpressionInternal;
            this.visitorContext = new FingerprintVisitorContext();
        }

        @Override
        protected Void visitQuery(Query node, Integer indent)
        {
            if (node.getWith().isPresent()) {
                With with = node.getWith().get();
                append(indent, "WITH");
                if (with.isRecursive()) {
                    super.builder.append(" RECURSIVE");
                }
                super.builder.append("\n  ");
                Iterator<WithQuery> queries = with.getQueries().iterator();
                while (queries.hasNext()) {
                    WithQuery query = queries.next();
                    append(indent, expressionGenerator.apply(query.getName(), parameters));
                    query.getColumnNames().ifPresent(columnNames -> appendAliasColumns(super.builder, columnNames));
                    super.builder.append(" AS ");
                    process(new TableSubquery(query.getQuery()), indent);
                    super.builder.append('\n');
                    if (queries.hasNext()) {
                        super.builder.append(", ");
                    }
                }
            }

            processRelation(node.getQueryBody(), indent);

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT ?").append('\n');
            }

            return null;
        }

        @Override
        protected Void visitQuerySpecification(QuerySpecification node, Integer indent)
        {
            process(node.getSelect(), indent);

            if (node.getFrom().isPresent()) {
                append(indent, "FROM");
                super.builder.append('\n');
                append(indent, "  ");
                process(node.getFrom().get(), indent);
            }

            super.builder.append('\n');

            if (node.getWhere().isPresent()) {
                append(indent, "WHERE " + formatExpressionInternal(node.getWhere().get(), parameters)).append('\n');
            }

            if (node.getGroupBy().isPresent()) {
                append(indent, "GROUP BY " + (node.getGroupBy().get().isDistinct() ? " DISTINCT " : "") + formatGroupBy(node.getGroupBy().get().getGroupingElements())).append('\n');
            }

            if (node.getHaving().isPresent()) {
                append(indent, "HAVING " + formatExpressionInternal(node.getHaving().get(), parameters))
                        .append('\n');
            }

            if (node.getOrderBy().isPresent()) {
                process(node.getOrderBy().get(), indent);
            }

            if (node.getOffset().isPresent()) {
                process(node.getOffset().get(), indent);
            }

            if (node.getLimit().isPresent()) {
                append(indent, "LIMIT ?").append('\n');
            }
            return null;
        }

        public String formatExpressionInternal(Expression expression, Optional<List<Expression>> parameters)
        {
            return new FingerprintExpressionFormatter(this.visitorContext, parameters)
                    .process(expression, null);
        }
    }
}
