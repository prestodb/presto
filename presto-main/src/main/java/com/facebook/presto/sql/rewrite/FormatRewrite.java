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
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Format;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.Statement;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.QueryUtil.singleValueQuery;
import static com.facebook.presto.sql.SqlFormatter.formatSql;

final class FormatRewrite
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
        return (Statement) new Visitor().process(node, null);
    }

    private static final class Visitor
            extends AstVisitor<Node, Void>
    {
        @Override
        protected Node visitFormat(Format node, Void context)
                throws SemanticException
        {
            return singleValueQuery("SQL", formatSql(node.getStatement(), Optional.empty()));
        }

        @Override
        protected Node visitNode(Node node, Void context)
        {
            return node;
        }
    }
}
