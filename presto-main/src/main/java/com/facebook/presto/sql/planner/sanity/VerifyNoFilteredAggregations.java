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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.Map;

public final class VerifyNoFilteredAggregations
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, Map<Symbol, Type> types)
    {
        plan.accept(new Visitor(), null);
    }

    private static class Visitor
        extends SimplePlanVisitor<Void>
    {
        @Override
        public Void visitAggregation(AggregationNode node, Void context)
        {
            super.visitAggregation(node, context);

            for (FunctionCall call : node.getAggregations().values()) {
                if (call.getFilter().isPresent()) {
                    throw new IllegalStateException("Generated plan contains unimplemented filtered aggregations");
                }
            }

            return null;
        }
    }
}
