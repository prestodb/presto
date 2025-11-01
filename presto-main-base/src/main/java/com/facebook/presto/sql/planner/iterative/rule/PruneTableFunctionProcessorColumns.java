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

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.tableFunctionProcessor;
import static com.google.common.collect.ImmutableList.toImmutableList;

/**
 * TableFunctionProcessorNode has two kinds of outputs:
 * - proper outputs, which are the columns produced by the table function,
 * - pass-through outputs, which are the columns copied from table arguments.
 * This rule filters out unreferenced pass-through symbols.
 * Unreferenced proper symbols are not pruned, because there is currently no way
 * to communicate to the table function the request for not producing certain columns.
 * // TODO prune table function's proper outputs
 */
public class PruneTableFunctionProcessorColumns
        extends ProjectOffPushDownRule<TableFunctionProcessorNode>
{
    public PruneTableFunctionProcessorColumns()
    {
        super(tableFunctionProcessor());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, VariableAllocator variableAllocator, TableFunctionProcessorNode node, Set<VariableReferenceExpression> referencedOutputs)
    {
        List<TableFunctionNode.PassThroughSpecification> prunedPassThroughSpecifications = node.getPassThroughSpecifications().stream()
                .map(sourceSpecification -> {
                    List<TableFunctionNode.PassThroughColumn> prunedPassThroughColumns = sourceSpecification.getColumns().stream()
                            .filter(column -> referencedOutputs.contains(column.getOutputVariables()))
                            .collect(toImmutableList());
                    return new TableFunctionNode.PassThroughSpecification(sourceSpecification.isDeclaredAsPassThrough(), prunedPassThroughColumns);
                })
                .collect(toImmutableList());

        int originalPassThroughCount = node.getPassThroughSpecifications().stream()
                .map(TableFunctionNode.PassThroughSpecification::getColumns)
                .mapToInt(List::size)
                .sum();

        int prunedPassThroughCount = prunedPassThroughSpecifications.stream()
                .map(TableFunctionNode.PassThroughSpecification::getColumns)
                .mapToInt(List::size)
                .sum();

        if (originalPassThroughCount == prunedPassThroughCount) {
            return Optional.empty();
        }

        return Optional.of(new TableFunctionProcessorNode(
                node.getId(),
                node.getName(),
                node.getProperOutputs(),
                node.getSource(),
                node.isPruneWhenEmpty(),
                prunedPassThroughSpecifications,
                node.getRequiredVariables(),
                node.getMarkerVariables(),
                node.getSpecification(),
                node.getPrePartitioned(),
                node.getPreSorted(),
                node.getHashSymbol(),
                node.getHandle()));
    }
}
