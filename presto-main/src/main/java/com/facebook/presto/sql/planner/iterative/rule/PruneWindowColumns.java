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

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.optimizations.WindowNodeUtil;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.window;

public class PruneWindowColumns
        extends ProjectOffPushDownRule<WindowNode>
{
    public PruneWindowColumns()
    {
        super(window());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, WindowNode windowNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        Map<VariableReferenceExpression, WindowNode.Function> referencedFunctions = Maps.filterKeys(windowNode.getWindowFunctions(), referencedOutputs::contains);

        if (referencedFunctions.isEmpty()) {
            return Optional.of(windowNode.getSource());
        }

        ImmutableSet.Builder<VariableReferenceExpression> referencedInputs = ImmutableSet.<VariableReferenceExpression>builder()
                .addAll(windowNode.getSource().getOutputVariables().stream()
                        .filter(referencedOutputs::contains)
                        .iterator())
                .addAll(windowNode.getPartitionBy());

        windowNode.getOrderingScheme().ifPresent(
                orderingScheme -> orderingScheme
                        .getOrderByVariables()
                        .forEach(referencedInputs::add));
        windowNode.getHashVariable().ifPresent(referencedInputs::add);

        for (WindowNode.Function windowFunction : referencedFunctions.values()) {
            referencedInputs.addAll(WindowNodeUtil.extractWindowFunctionUniqueVariables(windowFunction, variableAllocator.getTypes()));
            windowFunction.getFrame().getStartValue().ifPresent(referencedInputs::add);
            windowFunction.getFrame().getEndValue().ifPresent(referencedInputs::add);
        }

        PlanNode prunedWindowNode = new WindowNode(
                windowNode.getId(),
                restrictOutputs(idAllocator, windowNode.getSource(), referencedInputs.build(), false)
                        .orElse(windowNode.getSource()),
                windowNode.getSpecification(),
                referencedFunctions,
                windowNode.getHashVariable(),
                windowNode.getPrePartitionedInputs(),
                windowNode.getPreSortedOrderPrefix());

        if (prunedWindowNode.getOutputVariables().size() == windowNode.getOutputVariables().size()) {
            // Neither function pruning nor input pruning was successful.
            return Optional.empty();
        }

        return Optional.of(prunedWindowNode);
    }
}
