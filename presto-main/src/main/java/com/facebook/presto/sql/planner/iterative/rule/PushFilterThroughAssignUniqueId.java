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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AssignUniqueId;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.ExpressionUtils.combineConjuncts;
import static com.facebook.presto.sql.ExpressionUtils.extractConjuncts;
import static com.facebook.presto.sql.planner.DependencyExtractor.extractUnique;
import static java.util.stream.Collectors.partitioningBy;

public class PushFilterThroughAssignUniqueId
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof FilterNode)) {
            return Optional.empty();
        }
        FilterNode parent = (FilterNode) node;

        PlanNode childNode = lookup.resolve(parent.getSource());
        if (!(childNode instanceof AssignUniqueId)) {
            return Optional.empty();
        }
        AssignUniqueId child = (AssignUniqueId) childNode;

        Map<Boolean, List<Expression>> conjuncts = extractConjuncts(parent.getPredicate()).stream()
                .collect(partitioningBy(conjunct -> extractUnique(conjunct).contains(child.getIdColumn())));

        if (isNullOrEmpty(conjuncts.get(false))) {
            return Optional.empty();
        }

        PlanNode result = child.replaceChildren(ImmutableList.of(
                new FilterNode(idAllocator.getNextId(), child.getSource(), combineConjuncts(conjuncts.get(false)))));

        if (!isNullOrEmpty(conjuncts.get((true)))) {
            result = new FilterNode(idAllocator.getNextId(), result, combineConjuncts(conjuncts.get(true)));
        }

        return Optional.of(result);
    }

    private static boolean isNullOrEmpty(List list)
    {
        return list == null || list.isEmpty();
    }
}
