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
package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.optimizations.PlanOptimizer;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.common.collect.ImmutableSet;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkState;

public class IterativeOptimizer
        implements PlanOptimizer
{
    private final Set<Rule> rules;

    public IterativeOptimizer(Set<Rule> rules)
    {
        this.rules = ImmutableSet.copyOf(rules);
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        if (!SystemSessionProperties.isNewOptimizerEnabled(session)) {
            return plan;
        }

        Memo memo = new Memo(idAllocator, plan);

        Lookup lookup = node -> {
            if (node instanceof GroupReference) {
                return memo.getNode(((GroupReference) node).getGroupId());
            }

            return node;
        };

        exploreGroup(memo, lookup, idAllocator, symbolAllocator, memo.getRootGroup());

        return memo.extract();
    }

    private boolean exploreGroup(Memo memo, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, int group)
    {
        // tracks whether this group or any children groups change as
        // this method executes
        boolean progress = exploreNode(memo, lookup, idAllocator, symbolAllocator, group);

        while (exploreChildren(memo, lookup, idAllocator, symbolAllocator, group)) {
            progress = true;

            // if children changed, try current group again
            // in case we can match additional rules
            if (!exploreNode(memo, lookup, idAllocator, symbolAllocator, group)) {
                // no additional matches, so bail out
                break;
            }
        }

        return progress;
    }

    private boolean exploreNode(
            Memo memo,
            Lookup lookup,
            PlanNodeIdAllocator idAllocator,
            SymbolAllocator symbolAllocator,
            int group)
    {
        PlanNode node = memo.getNode(group);

        boolean done = false;
        boolean progress = false;

        while (!done) {
            done = true;
            for (Rule rule : rules) {
                Optional<PlanNode> transformed = rule.apply(node, lookup, idAllocator, symbolAllocator);

                if (transformed.isPresent()) {
                    memo.replace(group, transformed.get(), rule.getClass().getName());
                    node = transformed.get();

                    done = false;
                    progress = true;
                }
            }
        }

        return progress;
    }

    private boolean exploreChildren(Memo memo, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, int group)
    {
        boolean progress = false;

        PlanNode expression = memo.getNode(group);
        for (PlanNode child : expression.getSources()) {
            checkState(child instanceof GroupReference, "Expected child to be a group reference. Found: " + child.getClass().getName());

            if (exploreGroup(memo, lookup, idAllocator, symbolAllocator, ((GroupReference) child).getGroupId())) {
                progress = true;
            }
        }

        return progress;
    }
}
