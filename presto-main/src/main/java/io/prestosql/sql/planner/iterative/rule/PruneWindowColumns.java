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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.SymbolsExtractor;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.WindowNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.sql.planner.iterative.rule.Util.restrictOutputs;
import static io.prestosql.sql.planner.plan.Patterns.window;

public class PruneWindowColumns
        extends ProjectOffPushDownRule<WindowNode>
{
    public PruneWindowColumns()
    {
        super(window());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, WindowNode windowNode, Set<Symbol> referencedOutputs)
    {
        Map<Symbol, WindowNode.Function> referencedFunctions = Maps.filterKeys(
                windowNode.getWindowFunctions(),
                referencedOutputs::contains);

        if (referencedFunctions.isEmpty()) {
            return Optional.of(windowNode.getSource());
        }

        ImmutableSet.Builder<Symbol> referencedInputs = ImmutableSet.<Symbol>builder()
                .addAll(windowNode.getSource().getOutputSymbols().stream()
                        .filter(referencedOutputs::contains)
                        .iterator())
                .addAll(windowNode.getPartitionBy());

        windowNode.getOrderingScheme().ifPresent(
                orderingScheme -> orderingScheme
                        .getOrderBy()
                        .forEach(referencedInputs::add));
        windowNode.getHashSymbol().ifPresent(referencedInputs::add);

        for (WindowNode.Function windowFunction : referencedFunctions.values()) {
            referencedInputs.addAll(SymbolsExtractor.extractUnique(windowFunction.getFunctionCall()));
            windowFunction.getFrame().getStartValue().ifPresent(referencedInputs::add);
            windowFunction.getFrame().getEndValue().ifPresent(referencedInputs::add);
        }

        PlanNode prunedWindowNode = new WindowNode(
                windowNode.getId(),
                restrictOutputs(idAllocator, windowNode.getSource(), referencedInputs.build())
                        .orElse(windowNode.getSource()),
                windowNode.getSpecification(),
                referencedFunctions,
                windowNode.getHashSymbol(),
                windowNode.getPrePartitionedInputs(),
                windowNode.getPreSortedOrderPrefix());

        if (prunedWindowNode.getOutputSymbols().size() == windowNode.getOutputSymbols().size()) {
            // Neither function pruning nor input pruning was successful.
            return Optional.empty();
        }

        return Optional.of(prunedWindowNode);
    }
}
