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

import com.google.common.collect.Maps;
import io.prestosql.sql.planner.PlanNodeIdAllocator;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static io.prestosql.sql.planner.plan.Patterns.aggregation;

public class PruneAggregationColumns
        extends ProjectOffPushDownRule<AggregationNode>
{
    public PruneAggregationColumns()
    {
        super(aggregation());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(
            PlanNodeIdAllocator idAllocator,
            AggregationNode aggregationNode,
            Set<Symbol> referencedOutputs)
    {
        Map<Symbol, AggregationNode.Aggregation> prunedAggregations = Maps.filterKeys(
                aggregationNode.getAggregations(),
                referencedOutputs::contains);

        if (prunedAggregations.size() == aggregationNode.getAggregations().size()) {
            return Optional.empty();
        }

        // PruneAggregationSourceColumns will subsequently project off any newly unused inputs.
        return Optional.of(
                new AggregationNode(
                        aggregationNode.getId(),
                        aggregationNode.getSource(),
                        prunedAggregations,
                        aggregationNode.getGroupingSets(),
                        aggregationNode.getPreGroupedSymbols(),
                        aggregationNode.getStep(),
                        aggregationNode.getHashSymbol(),
                        aggregationNode.getGroupIdSymbol()));
    }
}
