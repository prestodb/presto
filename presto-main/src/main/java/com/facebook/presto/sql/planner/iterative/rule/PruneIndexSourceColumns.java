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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanVariableAllocator;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.indexSource;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneIndexSourceColumns
        extends ProjectOffPushDownRule<IndexSourceNode>
{
    public PruneIndexSourceColumns()
    {
        super(indexSource());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, PlanVariableAllocator variableAllocator, IndexSourceNode indexSourceNode, Set<VariableReferenceExpression> referencedOutputs)
    {
        Set<VariableReferenceExpression> prunedLookupSymbols = indexSourceNode.getLookupVariables().stream()
                .filter(referencedOutputs::contains)
                .collect(toImmutableSet());

        Map<VariableReferenceExpression, ColumnHandle> prunedAssignments = Maps.filterEntries(
                indexSourceNode.getAssignments(),
                entry -> referencedOutputs.contains(entry.getKey()) ||
                        tupleDomainReferencesColumnHandle(indexSourceNode.getCurrentConstraint(), entry.getValue()));

        List<VariableReferenceExpression> prunedOutputList =
                indexSourceNode.getOutputVariables().stream()
                        .filter(referencedOutputs::contains)
                        .collect(toImmutableList());

        return Optional.of(
                new IndexSourceNode(
                        indexSourceNode.getId(),
                        indexSourceNode.getIndexHandle(),
                        indexSourceNode.getTableHandle(),
                        prunedLookupSymbols,
                        prunedOutputList,
                        prunedAssignments,
                        indexSourceNode.getCurrentConstraint()));
    }

    private static boolean tupleDomainReferencesColumnHandle(
            TupleDomain<ColumnHandle> tupleDomain,
            ColumnHandle columnHandle)
    {
        return tupleDomain.getDomains()
                .map(domains -> domains.containsKey(columnHandle))
                .orElse(false);
    }
}
