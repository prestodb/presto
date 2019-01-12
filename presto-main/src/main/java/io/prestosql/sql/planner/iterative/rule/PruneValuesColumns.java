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

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static io.prestosql.sql.planner.plan.Patterns.values;
import static io.prestosql.util.MoreLists.filteredCopy;

public class PruneValuesColumns
        extends ProjectOffPushDownRule<ValuesNode>
{
    public PruneValuesColumns()
    {
        super(values());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, ValuesNode valuesNode, Set<Symbol> referencedOutputs)
    {
        List<Symbol> newOutputs = filteredCopy(valuesNode.getOutputSymbols(), referencedOutputs::contains);

        // for each output of project, the corresponding column in the values node
        int[] mapping = new int[newOutputs.size()];
        for (int i = 0; i < mapping.length; i++) {
            mapping[i] = valuesNode.getOutputSymbols().indexOf(newOutputs.get(i));
        }

        ImmutableList.Builder<List<Expression>> rowsBuilder = ImmutableList.builder();
        for (List<Expression> row : valuesNode.getRows()) {
            rowsBuilder.add(Arrays.stream(mapping)
                    .mapToObj(row::get)
                    .collect(Collectors.toList()));
        }

        return Optional.of(new ValuesNode(valuesNode.getId(), newOutputs, rowsBuilder.build()));
    }
}
