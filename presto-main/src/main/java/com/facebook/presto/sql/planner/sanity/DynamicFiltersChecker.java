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
import com.facebook.presto.sql.DynamicFilters;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.DynamicFilters.extractDynamicFilters;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;

public class DynamicFiltersChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types)
    {
        plan.accept(new SimplePlanVisitor<Set<String>>()
        {
            @Override
            public Void visitJoin(JoinNode node, Set<String> expected)
            {
                for (Symbol symbol : node.getDynamicFilters().values()) {
                    verify(node.getRight().getOutputSymbols().contains(symbol), "join input does't contain symbol for dynamic filter: %s", symbol);
                }
                ImmutableSet<String> currentJoinDynamicFilters = node.getDynamicFilters().entrySet().stream()
                        .map(Map.Entry::getKey)
                        .collect(toImmutableSet());

                expected.addAll(currentJoinDynamicFilters);
                node.getLeft().accept(this, expected);
                verify(intersection(currentJoinDynamicFilters, expected).isEmpty());
                node.getRight().accept(this, expected);
                return null;
            }

            @Override
            public Void visitExchange(ExchangeNode node, Set<String> expected)
            {
                Set<String> filtersLeft = null;
                for (PlanNode source : node.getSources()) {
                    Set<String> copy = new HashSet<>(expected);
                    source.accept(this, copy);
                    if (filtersLeft == null) {
                        filtersLeft = copy;
                    }
                    else {
                        checkState(filtersLeft.equals(copy), "filters do not match: %s != %s", filtersLeft, copy);
                    }
                }
                if (filtersLeft != null) {
                    expected.clear();
                    expected.addAll(filtersLeft);
                }
                return null;
            }

            @Override
            public Void visitFilter(FilterNode node, Set<String> expected)
            {
                List<DynamicFilters.Descriptor> dynamicFilters = extractDynamicFilters(node.getPredicate()).getDynamicConjuncts();
                dynamicFilters.stream()
                        .map(DynamicFilters.Descriptor::getId)
                        .forEach(expected::remove);
                return node.getSource().accept(this, expected);
            }
        }, new HashSet<>());
    }
}
