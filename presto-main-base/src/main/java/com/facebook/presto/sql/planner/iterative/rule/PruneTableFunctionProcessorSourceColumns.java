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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.TableFunctionNode;
import com.facebook.presto.sql.planner.plan.TableFunctionProcessorNode;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.Util.restrictOutputs;
import static com.facebook.presto.sql.planner.plan.Patterns.tableFunctionProcessor;
import static com.google.common.collect.Maps.filterKeys;

/**
 * This rule prunes unreferenced outputs of TableFunctionProcessorNode.
 * First, it extracts all symbols required for:
 * - pass-through
 * - table function computation
 * - partitioning and ordering (including the hashSymbol)
 * Next, a mapping of input symbols to marker symbols is updated
 * so that it only contains mappings for the required symbols.
 * Last, all the remaining marker symbols are added to the collection
 * of required symbols.
 * Any source output symbols not included in the required symbols
 * can be pruned.
 */
public class PruneTableFunctionProcessorSourceColumns
        implements Rule<TableFunctionProcessorNode>
{
    private static final Pattern<TableFunctionProcessorNode> PATTERN = tableFunctionProcessor();

    @Override
    public Pattern<TableFunctionProcessorNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(TableFunctionProcessorNode node, Captures captures, Context context)
    {
        if (!node.getSource().isPresent()) {
            return Result.empty();
        }

        ImmutableSet.Builder<VariableReferenceExpression> requiredInputs = ImmutableSet.builder();

        node.getPassThroughSpecifications().stream()
                .map(TableFunctionNode.PassThroughSpecification::getColumns)
                .flatMap(Collection::stream)
                .map(TableFunctionNode.PassThroughColumn::getOutputVariables)
                .forEach(requiredInputs::add);

        node.getRequiredVariables()
                .forEach(requiredInputs::addAll);

        node.getSpecification().ifPresent(specification -> {
            requiredInputs.addAll(specification.getPartitionBy());
            specification.getOrderingScheme().ifPresent(orderingScheme -> requiredInputs.addAll(orderingScheme.getOrderByVariables()));
        });

        node.getHashSymbol().ifPresent(requiredInputs::add);

        Optional<Map<VariableReferenceExpression, VariableReferenceExpression>> updatedMarkerSymbols = node.getMarkerVariables()
                .map(mapping -> filterKeys(mapping, requiredInputs.build()::contains));

        updatedMarkerSymbols.ifPresent(mapping -> requiredInputs.addAll(mapping.values()));

        return restrictOutputs(context.getIdAllocator(), node.getSource().orElseThrow(NoSuchElementException::new), requiredInputs.build())
                .map(child -> Result.ofPlanNode(new TableFunctionProcessorNode(
                        node.getId(),
                        node.getName(),
                        node.getProperOutputs(),
                        Optional.of(child),
                        node.isPruneWhenEmpty(),
                        node.getPassThroughSpecifications(),
                        node.getRequiredVariables(),
                        updatedMarkerSymbols,
                        node.getSpecification(),
                        node.getPrePartitioned(),
                        node.getPreSorted(),
                        node.getHashSymbol(),
                        node.getHandle())))
                .orElse(Result.empty());
    }
}
