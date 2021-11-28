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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.security.AccessControl;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Query;
import com.facebook.presto.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.rewrite.MaterializedViewOptimizationRewriteUtils.optimizeQueryUsingMaterializedView;
import static java.util.Objects.requireNonNull;

public class MaterializedViewOptimizationRewrite
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
        return (Statement) new MaterializedViewOptimizationRewrite.Visitor(metadata, session, parser, accessControl).process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        private final Metadata metadata;
        private final Session session;
        private final SqlParser sqlParser;
        private final AccessControl accessControl;

        public Visitor(
                Metadata metadata,
                Session session,
                SqlParser parser,
                AccessControl accessControl)
        {
            this.metadata = requireNonNull(metadata, "metadata is null");
            this.session = requireNonNull(session, "session is null");
            this.sqlParser = requireNonNull(parser, "queryPreparer is null");
            this.accessControl = requireNonNull(accessControl, "access control is null");
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }

        protected Node visitQuery(Query query, Void context)
        {
            if (SystemSessionProperties.isQueryOptimizationWithMaterializedViewEnabled(session)) {
                return optimizeQueryUsingMaterializedView(metadata, session, sqlParser, accessControl, query);
            }
            return query;
        }
    }
}
