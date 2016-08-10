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
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.values;
import static com.facebook.presto.sql.tree.BooleanLiteral.FALSE_LITERAL;
import static com.facebook.presto.sql.tree.BooleanLiteral.TRUE_LITERAL;
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
            AccessControl accessControl,
            boolean experimentalSyntaxEnabled)
    {
        return (Statement) new Visitor(session, parser, metadata, queryExplainer, parameters, accessControl, experimentalSyntaxEnabled).process(node, null);
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
        private final boolean experimentalSyntaxEnabled;

        public Visitor(
                Session session,
                SqlParser parser,
                Metadata metadata,
                Optional<QueryExplainer> queryExplainer,
                List<Expression> parameters,
                AccessControl accessControl,
                boolean experimentalSyntaxEnabled)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = parser;
            this.metadata = metadata;
            this.queryExplainer = queryExplainer;
            this.parameters = parameters;
            this.accessControl = accessControl;
            this.experimentalSyntaxEnabled = experimentalSyntaxEnabled;
        }

        @Override
        protected Node visitDescribeOutput(DescribeOutput node, Void context)
        {
            String sqlString = session.getPreparedStatement(node.getName());
            Statement statement = parser.createStatement(sqlString);

            Analyzer analyzer = new Analyzer(session, metadata, parser, accessControl, queryExplainer, experimentalSyntaxEnabled, parameters);
            Analysis analysis = analyzer.analyze(statement, true);

            Row[] rows = analysis.getRootScope().getRelationType().getVisibleFields().stream().map(field -> createDescribeOutputRow(field, analysis)).toArray(Row[]::new);
            Query query = simpleQuery(
                    selectList(nameReference("Column Name"),
                            nameReference("Table"),
                            nameReference("Schema"),
                            nameReference("Connector"),
                            nameReference("Type"),
                            nameReference("Type Size"),
                            nameReference("Aliased"),
                            nameReference("Row Count Query")),
                    aliased(
                            values(rows),
                            "Statement Output",
                            ImmutableList.of("Column Name", "Table", "Schema", "Connector", "Type", "Type Size", "Aliased", "Row Count Query")));
            return query;
        }

        private Row createDescribeOutputRow(Field field, Analysis analysis)
        {
            NullLiteral nullLiteral = new NullLiteral();
            if (analysis.isRowCountQuery()) {
                return row(nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, nullLiteral, TRUE_LITERAL);
            }

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

            Optional<QualifiedObjectName> qualifiedOriginTable = field.getQualifiedOriginTable();

            StringLiteral empty = new StringLiteral("");

            return row(
                    new StringLiteral(columnName),
                    (!qualifiedOriginTable.isPresent()) ? empty : new StringLiteral(qualifiedOriginTable.get().getObjectName()),
                    (!qualifiedOriginTable.isPresent()) ? empty : new StringLiteral(qualifiedOriginTable.get().getSchemaName()),
                    (!qualifiedOriginTable.isPresent()) ? empty : new StringLiteral(qualifiedOriginTable.get().getCatalogName()),
                    new StringLiteral(field.getType().getDisplayName()),
                    typeSize,
                    new BooleanLiteral(String.valueOf(field.isAliased())),
                    FALSE_LITERAL);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
