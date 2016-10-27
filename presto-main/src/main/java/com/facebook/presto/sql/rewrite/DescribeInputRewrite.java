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
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Analysis;
import com.facebook.presto.sql.analyzer.Analyzer;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.DescribeInput;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.StringLiteral;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.execution.ParameterExtractor.getParameters;
import static com.facebook.presto.sql.QueryUtil.aliased;
import static com.facebook.presto.sql.QueryUtil.ascending;
import static com.facebook.presto.sql.QueryUtil.nameReference;
import static com.facebook.presto.sql.QueryUtil.ordering;
import static com.facebook.presto.sql.QueryUtil.row;
import static com.facebook.presto.sql.QueryUtil.selectList;
import static com.facebook.presto.sql.QueryUtil.simpleQuery;
import static com.facebook.presto.sql.QueryUtil.values;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
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
            this.accessControl = accessControl;
            this.parameters = parameters;
        }

        @Override
        protected Node visitDescribeInput(DescribeInput node, Void context)
                throws SemanticException
        {
            String sqlString = session.getPreparedStatement(node.getName());
            Statement statement = parser.createStatement(sqlString);

            // create  analysis for the query we are describing.
            Analyzer analyzer = new Analyzer(session, metadata, parser, accessControl, queryExplainer, parameters);
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
                    selectList(nameReference("Position"), nameReference("Type")),
                    aliased(
                            values(rows),
                            "Parameter Input",
                            ImmutableList.of("Position", "Type")),
                    Optional.empty(),
                    Optional.empty(),
                    Optional.empty(),
                    ordering(ascending("Position")),
                    limit
            );
        }

        private Row createDescribeInputRow(Parameter parameter, Analysis queryAnalysis)
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
