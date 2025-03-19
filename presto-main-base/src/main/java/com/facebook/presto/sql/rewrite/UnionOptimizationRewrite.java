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
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.security.AccessControl;
import com.facebook.presto.sql.analyzer.QueryExplainer;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Statement;
import com.facebook.presto.sql.tree.Union;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isOptimizeUnionToUnionAll;

public class UnionOptimizationRewrite
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
            Map<NodeRef<Parameter>, Expression> parameterLookup,
            AccessControl accessControl,
            WarningCollector warningCollector)
    {
        if (isOptimizeUnionToUnionAll(session)) {
            return (Statement) new UnionRewriter()
                    .process(node, null);
        }

        return node;
    }

    private static final class UnionRewriter
            extends DefaultASTRewriter<Void>
    {
        @Override
        protected Node visitUnion(Union node, Void context)
        {
            // Skip rewrite for union and union distinct cases
            if (node.isDistinct().orElse(false)) {
                return node;
            }

            return new Union(node.getRelations(), Optional.of(false));
        }
    }
}
