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
import com.facebook.presto.expressions.DefaultRowExpressionTraversalVisitor;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.IntermediateFormExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionExtractor;
import com.facebook.presto.sql.planner.TypeProvider;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;

public final class VerifyNoIntermediateFormExpression
        implements PlanChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        List<RowExpression> expressions = ExpressionExtractor.extractExpressions(plan)
                .stream()
                .collect(toImmutableList());
        for (RowExpression expression : expressions) {
            expression.accept(
                    new DefaultRowExpressionTraversalVisitor<Void>()
                    {
                        @Override
                        public Void visitIntermediateFormExpression(IntermediateFormExpression node, Void context)
                        {
                            throw new IllegalStateException(format("Unexpected IntermediateFormExpression in logical plan: %s", node));
                        }
                    },
                    null);
        }
    }
}
