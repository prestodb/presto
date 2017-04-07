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

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.joins.JoinGraph;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins.buildJoinTree;
import static com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins.getJoinOrder;
import static com.facebook.presto.sql.planner.optimizations.EliminateCrossJoins.isOriginalOrder;

public class EliminateCrossJoins
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof JoinNode)) {
            return Optional.empty();
        }

        if (!SystemSessionProperties.isJoinReorderingEnabled(session)) {
            return Optional.empty();
        }

        JoinGraph joinGraph = JoinGraph.buildShallowFrom(node, lookup);
        if (joinGraph.size() < 3) {
            return Optional.empty();
        }

        List<Integer> joinOrder = getJoinOrder(joinGraph);
        if (isOriginalOrder(joinOrder)) {
            return Optional.empty();
        }

        PlanNode replacement = buildJoinTree(node.getOutputSymbols(), joinGraph, joinOrder, idAllocator);
        return Optional.of(replacement);
    }
}
