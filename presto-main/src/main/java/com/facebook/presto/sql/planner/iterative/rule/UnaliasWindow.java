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
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.FunctionCall;

import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class UnaliasWindow
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof WindowNode)) {
            return Optional.empty();
        }

        WindowNode parent = (WindowNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode child = (ProjectNode) source;

        Renamer renamer = Renamer.from(child);

        if (!renamer.hasRenames()) {
            return Optional.empty();
        }

        WindowNode rewrittenNode = new WindowNode(
                parent.getId(),
                renamer.renameOutputs(child),
                new WindowNode.Specification(
                        renamer.renameAll(parent.getSpecification().getPartitionBy()),
                        renamer.renameAll(parent.getSpecification().getOrderBy()),
                        parent.getSpecification().getOrderings()
                                .entrySet().stream()
                                .collect(Collectors.toMap(
                                        e -> renamer.rename(e.getKey()),
                                        Map.Entry::getValue))),
                parent.getWindowFunctions()
                        .entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> renameFunction(renamer, e.getValue()))),
                parent.getHashSymbol().map(renamer::rename),
                renamer.renameAll(parent.getPrePartitionedInputs()),
                parent.getPreSortedOrderPrefix());

        // compute assignments to preserve original output names
        Assignments.Builder finalAssignments = Assignments.builder();
        for (Symbol symbol : parent.getOutputSymbols()) {
            finalAssignments.put(symbol, renamer.rename(symbol).toSymbolReference());
        }

        return Optional.of(new ProjectNode(idAllocator.getNextId(), rewrittenNode, finalAssignments.build()));
    }

    private WindowNode.Function renameFunction(Renamer renamer, WindowNode.Function function)
    {
        WindowNode.Frame frame = function.getFrame();

        return new WindowNode.Function(
                (FunctionCall) renamer.rename(function.getFunctionCall()),
                function.getSignature(),
                new WindowNode.Frame(
                        frame.getType(),
                        frame.getStartType(),
                        frame.getStartValue().map(renamer::rename),
                        frame.getEndType(),
                        frame.getEndValue().map(renamer::rename)));
    }
}
