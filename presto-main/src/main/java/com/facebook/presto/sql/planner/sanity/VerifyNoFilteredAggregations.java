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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;

public final class VerifyNoFilteredAggregations
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        searchFrom(plan)
                .where(AggregationNode.class::isInstance)
                .<AggregationNode>findAll()
                .stream()
                .flatMap(node -> node.getAggregations().values().stream())
                .filter(aggregation -> aggregation.getCall().getFilter().isPresent())
                .forEach(ignored -> {
                    throw new IllegalStateException("Generated plan contains unimplemented filtered aggregations");
                });
    }
}
