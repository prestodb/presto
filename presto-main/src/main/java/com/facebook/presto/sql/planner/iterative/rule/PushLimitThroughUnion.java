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
import com.facebook.presto.sql.planner.plan.ChildReplacer;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.UnionNode;

import java.util.Optional;
import java.util.stream.Collectors;

public class PushLimitThroughUnion
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof LimitNode)) {
            return Optional.empty();
        }

        LimitNode parent = (LimitNode) node;

        if (parent.getStep() == LimitNode.Step.FINAL) {
            return Optional.empty();
        }

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof UnionNode)) {
            return Optional.empty();
        }

        UnionNode child = (UnionNode) source;

        PlanNode result = ChildReplacer.replaceChildren(
                child,
                child.getSources().stream()
                        .map(s -> new LimitNode(idAllocator.getNextId(), s, parent.getCount(), LimitNode.Step.PARTIAL))
                        .collect(Collectors.toList()));

        if (parent.getStep() != LimitNode.Step.PARTIAL) {
            result = new LimitNode(parent.getId(), result, parent.getCount(), LimitNode.Step.FINAL);
        }

        return Optional.of(result);
    }
}
