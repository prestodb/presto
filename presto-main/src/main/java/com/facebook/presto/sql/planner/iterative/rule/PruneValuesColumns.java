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
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.google.common.collect.ImmutableList;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.planner.plan.Patterns.values;
import static com.facebook.presto.util.MoreLists.filteredCopy;

public class PruneValuesColumns
        extends ProjectOffPushDownRule<ValuesNode>
{
    public PruneValuesColumns()
    {
        super(values());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, ValuesNode valuesNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        List<VariableReferenceExpression> newOutputs = filteredCopy(valuesNode.getOutputVariables(), referencedOutputs::contains);

        List<VariableReferenceExpression> newOutputVariables = filteredCopy(valuesNode.getOutputVariables(), referencedOutputs::contains);

        // for each output of project, the corresponding column in the values node
        int[] mapping = new int[newOutputs.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = valuesNode.getOutputVariables().indexOf(newOutputs.get(i));
        }

        ImmutableList.Builder<List<RowExpression>> rowsBuilder = ImmutableList.builder();
        for (List<RowExpression> row : valuesNode.getRows()) {
            rowsBuilder.add(Arrays.stream(mapping)
                    .mapToObj(row::get)
                    .collect(Collectors.toList()));
        }

        return Optional.of(new ValuesNode(valuesNode.getId(), newOutputVariables, rowsBuilder.build()));
    }
}
