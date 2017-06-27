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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.IndexSourceNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.iterative.rule.Util.pruneInputs;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class PruneIndexSourceColumns
        implements Rule
{
    private static final Pattern PATTERN = Pattern.typeOf(ProjectNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        ProjectNode parent = (ProjectNode) node;

        PlanNode source = lookup.resolve(parent.getSource());
        if (!(source instanceof IndexSourceNode)) {
            return Optional.empty();
        }

        IndexSourceNode indexSourceNode = (IndexSourceNode) source;

        Optional<Set<Symbol>> prunedIndexSourceOutputs = pruneInputs(indexSourceNode.getOutputSymbols(), parent.getAssignments().getExpressions());
        if (!prunedIndexSourceOutputs.isPresent()) {
            return Optional.empty();
        }

        Set<Symbol> newLookupSymbols = indexSourceNode.getLookupSymbols().stream()
                .filter(prunedIndexSourceOutputs.get()::contains)
                .collect(toImmutableSet());

        Map<Symbol, ColumnHandle> newAssignments = Maps.filterEntries(
                indexSourceNode.getAssignments(),
                entry -> prunedIndexSourceOutputs.get().contains(entry.getKey()) ||
                        tupleDomainReferencesColumnHandle(indexSourceNode.getEffectiveTupleDomain(), entry.getValue()));

        List<Symbol> prunedIndexSourceOutputList =
                indexSourceNode.getOutputSymbols().stream()
                        .filter(prunedIndexSourceOutputs.get()::contains)
                        .collect(toImmutableList());

        return Optional.of(
                parent.replaceChildren(ImmutableList.of(
                        new IndexSourceNode(
                                indexSourceNode.getId(),
                                indexSourceNode.getIndexHandle(),
                                indexSourceNode.getTableHandle(),
                                indexSourceNode.getLayout(),
                                newLookupSymbols,
                                prunedIndexSourceOutputList,
                                newAssignments,
                                indexSourceNode.getEffectiveTupleDomain()))));
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
