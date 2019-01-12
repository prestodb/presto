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
import io.prestosql.security.AccessControl;
import io.prestosql.spi.type.Type;
import io.prestosql.sql.analyzer.Analysis;
import io.prestosql.sql.analyzer.Analyzer;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.DescribeInput;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.Parameter;
import io.prestosql.sql.tree.Row;
import io.prestosql.sql.tree.Statement;
import io.prestosql.sql.tree.StringLiteral;

import java.util.List;
import java.util.Optional;

import static io.prestosql.execution.ParameterExtractor.getParameters;
import static io.prestosql.sql.ParsingUtil.createParsingOptions;
import static io.prestosql.sql.QueryUtil.aliased;
import static io.prestosql.sql.QueryUtil.ascending;
import static io.prestosql.sql.QueryUtil.identifier;
import static io.prestosql.sql.QueryUtil.ordering;
import static io.prestosql.sql.QueryUtil.row;
import static io.prestosql.sql.QueryUtil.selectList;
import static io.prestosql.sql.QueryUtil.simpleQuery;
import static io.prestosql.sql.QueryUtil.values;
import static io.prestosql.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

final class DescribeInputRewrite
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
            this.accessControl = accessControl;
            this.parameters = parameters;
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitDescribeInput(DescribeInput node, Void context)
                throws SemanticException
        {
            String sqlString = session.getPreparedStatement(node.getName().getValue());
            Statement statement = parser.createStatement(sqlString, createParsingOptions(session));

            // create  analysis for the query we are describing.
            Analyzer analyzer = new Analyzer(session, metadata, parser, accessControl, queryExplainer, parameters, warningCollector);
            Analysis analysis = analyzer.analyze(statement, true);

            // get all parameters in query
            List<Parameter> parameters = getParameters(statement);

            // return the positions and types of all parameters
            Row[] rows = parameters.stream().map(parameter -> createDescribeInputRow(parameter, analysis)).toArray(Row[]::new);
            Optional<String> limit = Optional.empty();
            if (rows.length == 0) {
                rows = new Row[] {row(new NullLiteral(), new NullLiteral())};
                limit = Optional.of("0");
            }

            return simpleQuery(
                    selectList(identifier("Position"), identifier("Type")),
                    aliased(
                            values(rows),
                            "Parameter Input",
                            ImmutableList.of("Position", "Type")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.of(ordering(ascending("Position"))),
                    limit);
        }

        private static Row createDescribeInputRow(Parameter parameter, Analysis queryAnalysis)
        {
            Type type = queryAnalysis.getCoercion(parameter);
            if (type == null) {
                type = UNKNOWN;
            }

            return row(
                    new LongLiteral(Integer.toString(parameter.getPosition())),
                    new StringLiteral(type.getTypeSignature().getBase()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
