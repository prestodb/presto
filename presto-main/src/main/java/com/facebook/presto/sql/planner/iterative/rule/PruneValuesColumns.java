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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class PruneValuesColumns
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode parent = (ProjectNode) node;

        PlanNode child = lookup.resolve(parent.getSource());
        if (!(child instanceof ValuesNode)) {
            return Optional.empty();
        }

        ValuesNode values = (ValuesNode) child;

        Set<Symbol> childOutputs = new HashSet<>(child.getOutputSymbols());

        // we need to compute the intersection in case some dependencies are symbols from
        // the outer scope (i.e., correlated queries)
        Set<Symbol> dependencies = Sets.intersection(DependencyExtractor.extractUnique(parent.getAssignments().getExpressions()), childOutputs);
        if (dependencies.equals(childOutputs)) {
            return Optional.empty();
        }

        List<Symbol> newOutputs = ImmutableList.copyOf(dependencies);

        // for each output of project, the corresponding column in the values node
        int[] mapping = new int[newOutputs.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = values.getOutputSymbols().indexOf(newOutputs.get(i));
        }

        ImmutableList.Builder<List<Expression>> rowsBuilder = ImmutableList.builder();
        for (List<Expression> row : values.getRows()) {
            rowsBuilder.add(Arrays.stream(mapping)
                    .mapToObj(row::get)
                    .collect(Collectors.toList()));
        }

        return Optional.of(
                new ProjectNode(
                        parent.getId(),
                        new ValuesNode(values.getId(), newOutputs, rowsBuilder.build()),
                        parent.getAssignments()));
    }
}
