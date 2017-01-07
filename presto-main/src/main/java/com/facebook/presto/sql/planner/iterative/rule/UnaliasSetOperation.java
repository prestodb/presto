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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExceptNode;
import com.facebook.presto.sql.planner.plan.IntersectNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SetOperationNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.google.common.collect.ImmutableListMultimap;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class UnaliasSetOperation
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof UnionNode) && !(node instanceof IntersectNode) && !(node instanceof ExceptNode)) {
            return Optional.empty();
        }

        SetOperationNode parent = (SetOperationNode) node;

        List<PlanNode> newChildren = new ArrayList<>();
        ImmutableListMultimap.Builder<Symbol, Symbol> mappings = ImmutableListMultimap.builder();

        boolean changed = false;
        for (int childIndex = 0; childIndex < parent.getSources().size(); childIndex++) {
            PlanNode child = lookup.resolve(parent.getSources().get(childIndex));

            Renamer renamer = Renamer.empty();
            if (child instanceof ProjectNode) {
                ProjectNode project = (ProjectNode) child;
                renamer = Renamer.from(project);

                if (renamer.hasRenames()) {
                    changed = true;
                }

                child = renamer.renameOutputs(project);
            }

            for (Symbol output : parent.getOutputSymbols()) {
                Symbol input = parent.getSymbolMapping().get(output).get(childIndex);
                mappings.put(output, renamer.rename(input));
            }

            newChildren.add(child);
        }

        if (!changed) {
            return Optional.empty();
        }

        PlanNode result;
        if (node instanceof UnionNode) {
            result = new UnionNode(parent.getId(), newChildren, mappings.build(), parent.getOutputSymbols());
        }
        else if (node instanceof IntersectNode) {
            result = new IntersectNode(parent.getId(), newChildren, mappings.build(), parent.getOutputSymbols());
        }
        else if (node instanceof ExceptNode) {
            result = new ExceptNode(parent.getId(), newChildren, mappings.build(), parent.getOutputSymbols());
        }
        else {
            throw new UnsupportedOperationException("Unsupported set operation node type: " + node.getClass().getName());
        }

        return Optional.of(result);
    }
}
