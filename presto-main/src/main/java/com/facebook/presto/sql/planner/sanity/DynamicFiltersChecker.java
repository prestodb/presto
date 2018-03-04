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
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static com.facebook.presto.sql.DynamicFilterUtils.extractDynamicFilters;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static com.google.common.collect.Sets.intersection;
import static java.util.Objects.requireNonNull;

public class DynamicFiltersChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, Map<Symbol, Type> types)
    {
        plan.accept(new SimplePlanVisitor<Set<Filter>>()
        {
            @Override
            public Void visitJoin(JoinNode node, Set<Filter> expected)
            {
                ImmutableSet<Filter> currentJoinDynamicFilters = node.getDynamicFilterAssignments()
                        .entrySet()
                        .stream()
                        .map(assignment -> new Filter(node.getId().toString(), assignment.getKey().getName()))
                        .collect(toImmutableSet());

                expected.addAll(currentJoinDynamicFilters);
                node.getLeft().accept(this, expected);
                checkState(intersection(currentJoinDynamicFilters, expected).isEmpty());
                node.getRight().accept(this, expected);
                return null;
            }

            @Override
            public Void visitExchange(ExchangeNode node, Set<Filter> expected)
            {
                Set<Filter> filtersLeft = null;
                for (PlanNode source : node.getSources()) {
                    Set<Filter> copy = new HashSet<>(expected);
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
            public Void visitFilter(FilterNode node, Set<Filter> expected)
            {
                extractDynamicFilters(node.getPredicate())
                        .getDynamicFilters()
                        .stream()
                        .map(dynamicFilter -> new Filter(dynamicFilter.getSourceId(), dynamicFilter.getDfSymbol()))
                        .forEach(expected::remove);
                return node.getSource().accept(this, expected);
            }
        }, new HashSet<>());
    }

    private static class Filter
    {
        private final String source;
        private final String name;

        private Filter(String source, String name)
        {
            this.source = requireNonNull(source, "source is null");
            this.name = requireNonNull(name, "name is null");
        }

        public String getSource()
        {
            return source;
        }

        public String getName()
        {
            return name;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Filter filter = (Filter) o;
            return Objects.equals(source, filter.source) &&
                    Objects.equals(name, filter.name);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(source, name);
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("source", source)
                    .add("name", name)
                    .toString();
        }
    }
}
