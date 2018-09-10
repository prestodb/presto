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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableMap.toImmutableMap;

public class PruneTableScanColumnFields
        extends ProjectOffPushDownFieldRule<TableScanNode>
{
    public PruneTableScanColumnFields()
    {
        super(tableScan());
    }

    @Override
    protected Optional<PlanNode> pushDownProjectOff(PlanNodeIdAllocator idAllocator, TableScanNode tableScanNode, Set<Symbol> referencedOutputs)
    {
        Map<String, Symbol> referencedOutputsMap = referencedOutputs.stream()
                .collect(toImmutableMap(Symbol::getName, symbol -> symbol));
        return Optional.of(
                new TableScanNode(
                        tableScanNode.getId(),
                        tableScanNode.getTable(),
                        filteredCopy(tableScanNode.getOutputSymbols(), referencedOutputsMap),
                        filterKeys(tableScanNode.getAssignments(), referencedOutputsMap),
                        tableScanNode.getLayout(),
                        tableScanNode.getCurrentConstraint(),
                        tableScanNode.getEnforcedConstraint()));
    }

    private List<Symbol> filteredCopy(List<Symbol> outputSymbols, Map<String, Symbol> referencedOutputsMap)
    {
        return outputSymbols.stream()
                .map(Symbol::getName)
                .filter(referencedOutputsMap::containsKey)
                .map(referencedOutputsMap::get)
                .collect(toImmutableList());
    }

    private Map<Symbol, ColumnHandle> filterKeys(Map<Symbol, ColumnHandle> assignments, Map<String, Symbol> referencedOutputsMap)
    {
        return assignments.keySet().stream()
                .filter(symbol -> referencedOutputsMap.containsKey(symbol.getName()))
                .collect(toImmutableMap(symbol -> referencedOutputsMap.get(symbol.getName()), assignments::get));
    }
}
