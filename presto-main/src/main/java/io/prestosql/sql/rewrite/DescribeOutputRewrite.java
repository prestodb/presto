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
package io.prestosql.sql.rewrite;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.QualifiedObjectName;
import io.prestosql.security.AccessControl;
import io.prestosql.spi.type.FixedWidthType;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.Field;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.BooleanLiteral;
import io.prestosql.sql.tree.DescribeOutput;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.values;
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
            WarningCollector warningCollector)
    {
        return (Statement) new Visitor(session, parser, metadata, queryExplainer, parameters, accessControl, warningCollector).process(node, null);
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
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                Metadata metadata,
                Optional<QueryExplainer> queryExplainer,
                List<Expression> parameters,
                AccessControl accessControl,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.parser = parser;
            this.metadata = metadata;
            this.queryExplainer = queryExplainer;
            this.parameters = parameters;
            this.accessControl = accessControl;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitDescribeOutput(DescribeOutput node, Void context)
        {
            String sqlString = session.getPreparedStatement(node.getName().getValue());
            Statement statement = parser.createStatement(sqlString, createParsingOptions(session));

            Analyzer analyzer = new Analyzer(session, metadata, parser, accessControl, queryExplainer, parameters, warningCollector);
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
                    Optional.empty(),
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
