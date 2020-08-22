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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionExtractor;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.OriginalExpressionUtils;
import com.facebook.presto.sql.tree.DefaultTraversalVisitor;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SubqueryExpression;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class NoSubqueryExpressionLeftChecker
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        List<Expression> expressions = ExpressionExtractor.extractExpressions(plan)
                .stream()
                .filter(OriginalExpressionUtils::isExpression)
                .map(OriginalExpressionUtils::castToExpression)
                .collect(toImmutableList());
        for (Expression expression : expressions) {
            new DefaultTraversalVisitor<Void, Void>()
            {
                @Override
                protected Void visitSubqueryExpression(SubqueryExpression node, Void context)
                {
                    throw new IllegalStateException(format("Unexpected subquery expression in logical plan: %s", node));
                }
            }.process(expression, null);
        }
    }
}
