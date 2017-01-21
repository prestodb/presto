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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.iterative.rule.Util.pruneInputs;

public class PruneTableScanColumns
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode parent = (ProjectNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode child = (TableScanNode) source;

        Optional<List<Symbol>> dependencies = pruneInputs(child.getOutputSymbols(), parent.getAssignments().getExpressions());
        if (!dependencies.isPresent()) {
            return Optional.empty();
        }

        List<Symbol> newOutputs = dependencies.get();
        return Optional.of(
                new ProjectNode(
                        parent.getId(),
                        new TableScanNode(
                                child.getId(),
                                child.getTable(),
                                newOutputs,
                                newOutputs.stream()
                                        .collect(Collectors.toMap(Function.identity(), e -> child.getAssignments().get(e))),
                                child.getLayout(),
                                child.getCurrentConstraint(),
                                child.getOriginalConstraint()),
                        parent.getAssignments()));
    }
}
