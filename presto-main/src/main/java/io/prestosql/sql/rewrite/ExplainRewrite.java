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

import io.prestosql.Session;
import io.prestosql.execution.QueryPreparer;
import io.prestosql.execution.QueryPreparer.PreparedQuery;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.security.AccessControl;
import io.prestosql.sql.analyzer.QueryExplainer;
import io.prestosql.sql.analyzer.SemanticException;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.tree.AstVisitor;
import io.prestosql.sql.tree.Explain;
import io.prestosql.sql.tree.ExplainFormat;
import io.prestosql.sql.tree.ExplainOption;
import io.prestosql.sql.tree.ExplainType;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.Node;
import io.prestosql.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static io.prestosql.sql.QueryUtil.singleValueQuery;
import static io.prestosql.sql.tree.ExplainFormat.Type.JSON;
import static io.prestosql.sql.tree.ExplainFormat.Type.TEXT;
import static io.prestosql.sql.tree.ExplainType.Type.IO;
import static io.prestosql.sql.tree.ExplainType.Type.LOGICAL;
import static io.prestosql.sql.tree.ExplainType.Type.VALIDATE;
import static java.util.Objects.requireNonNull;

final class ExplainRewrite
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
        return (Statement) new Visitor(session, parser, queryExplainer, warningCollector).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Session session;
        private final QueryPreparer queryPreparer;
        private final Optional<QueryExplainer> queryExplainer;
        private final WarningCollector warningCollector;

        public Visitor(
                Session session,
                SqlParser parser,
                Optional<QueryExplainer> queryExplainer,
                WarningCollector warningCollector)
        {
            this.session = requireNonNull(session, "session is null");
            this.queryPreparer = new QueryPreparer(requireNonNull(parser, "queryPreparer is null"));
            this.queryExplainer = requireNonNull(queryExplainer, "queryExplainer is null");
            this.warningCollector = requireNonNull(warningCollector, "warningCollector is null");
        }

        @Override
        protected Node visitExplain(Explain node, Void context)
                throws SemanticException
        {
            if (node.isAnalyze()) {
                Statement statement = (Statement) process(node.getStatement(), context);
                return new Explain(statement, node.isAnalyze(), node.isVerbose(), node.getOptions());
            }

            ExplainType.Type planType = LOGICAL;
            ExplainFormat.Type planFormat = TEXT;
            List<ExplainOption> options = node.getOptions();

            for (ExplainOption option : options) {
                if (option instanceof ExplainType) {
                    planType = ((ExplainType) option).getType();
                    // Use JSON as the default format for EXPLAIN (TYPE IO).
                    if (planType == IO) {
                        planFormat = JSON;
                    }
                    break;
                }
            }

            for (ExplainOption option : options) {
                if (option instanceof ExplainFormat) {
                    planFormat = ((ExplainFormat) option).getType();
                    break;
                }
            }

            return getQueryPlan(node, planType, planFormat);
        }

        private Node getQueryPlan(Explain node, ExplainType.Type planType, ExplainFormat.Type planFormat)
                throws IllegalArgumentException
        {
            PreparedQuery preparedQuery = queryPreparer.prepareQuery(session, node.getStatement());

            if (planType == VALIDATE) {
                queryExplainer.get().analyze(session, preparedQuery.getStatement(), preparedQuery.getParameters(), warningCollector);
                return singleValueQuery("Valid", true);
            }

            String plan;
            switch (planFormat) {
                case GRAPHVIZ:
                    plan = queryExplainer.get().getGraphvizPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                case JSON:
                    plan = queryExplainer.get().getJsonPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                case TEXT:
                    plan = queryExplainer.get().getPlan(session, preparedQuery.getStatement(), planType, preparedQuery.getParameters(), warningCollector);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid Explain Format: " + planFormat.toString());
            }
            return singleValueQuery("Query Plan", plan);
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
