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
package com.facebook.presto.sql.rewrite;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.QualifiedObjectName;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.type.FixedWidthType;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DescribeOutput;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.identifier;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.values;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

final class DescribeOutputRewrite
        implements StatementRewrite.Rewrite
{
    @Override
    public Statement rewrite(
            Session session,
            Metadata metadata,
            SqlParser parser,
            Optional<QueryExplainer> queryExplainer,
            Statement node,
            List<Expression> parameters,
            AccessControl accessControl)
    {
        return (Statement) new Visitor(session, parser, metadata, queryExplainer, parameters, accessControl).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final SqlParser parser;
        private final Metadata metadata;
        private final Optional<QueryExplainer> queryExplainer;
        private final List<Expression> parameters;
        private final AccessControl accessControl;

        public Visitor(
                Session session,
                SqlParser parser,
                Metadata metadata,
                Optional<QueryExplainer> queryExplainer,
                List<Expression> parameters,
                AccessControl accessControl)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = parser;
            this.metadata = metadata;
            this.queryExplainer = queryExplainer;
            this.parameters = parameters;
            this.accessControl = accessControl;
        }

        @Override
        protected Node visitDescribeOutput(DescribeOutput node, Void context)
        {
            String sqlString = session.getPreparedStatement(node.getName());
            Statement statement = parser.createStatement(sqlString);

            Analyzer analyzer = new Analyzer(session, metadata, parser, accessControl, queryExplainer, parameters);
            Analysis analysis = analyzer.analyze(statement, true);

            Optional<String> limit = Optional.empty();
            Row[] rows = analysis.getRootScope().getRelationType().getVisibleFields().stream().map(field -> createDescribeOutputRow(field, analysis)).toArray(Row[]::new);
            if (rows.length == 0) {
                NullLiteral nullLiteral = new NullLiteral();
                rows = new Row[] {row(nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral)};
                limit = Optional.of("0");
            }
            return simpleQuery(
                    selectList(
                            identifier("Column Name"),
                            identifier("Catalog"),
                            identifier("Schema"),
                            identifier("Table"),
                            identifier("Type"),
                            identifier("Type Size"),
                            identifier("Aliased")),
                    aliased(
                            values(rows),
                            "Statement Output",
                            ImmutableList.of("Column Name", "Catalog", "Schema", "Table", "Type", "Type Size", "Aliased")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    emptyList(),
                    limit);
        }

        private static Row createDescribeOutputRow(Field field, Analysis analysis)
        {
            LongLiteral typeSize = new LongLiteral("0");
            if (field.getType() instanceof FixedWidthType) {
                typeSize = new LongLiteral(String.valueOf(((FixedWidthType) field.getType()).getFixedSize()));
            }

            String columnName;
            if (field.getName().isPresent()) {
                columnName = field.getName().get();
            }
            else {
                int columnIndex = ImmutableList.copyOf(analysis.getOutputDescriptor().getVisibleFields()).indexOf(field);
                columnName = "_col" + columnIndex;
            }

            Optional<QualifiedObjectName> originTable = field.getOriginTable();

            return row(
                    new StringLiteral(columnName),
                    new StringLiteral(originTable.map(QualifiedObjectName::getCatalogName).orElse("")),
                    new StringLiteral(originTable.map(QualifiedObjectName::getSchemaName).orElse("")),
                    new StringLiteral(originTable.map(QualifiedObjectName::getObjectName).orElse("")),
                    new StringLiteral(field.getType().getDisplayName()),
                    typeSize,
                    new BooleanLiteral(String.valueOf(field.isAliased())));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
