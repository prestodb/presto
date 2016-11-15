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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;

import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class SymbolAliases
{
    private final Map<String, SymbolReference> map;

    public SymbolAliases()
    {
        this.map = new HashMap<>();
    }

    public SymbolAliases(SymbolAliases symbolAliases)
    {
        requireNonNull(symbolAliases, "symbolAliases are null");
        this.map = new HashMap<>(symbolAliases.map);
    }

    public void put(String alias, SymbolReference symbolReference)
    {
        requireNonNull(symbolReference, "symbolReference is null");
        alias = toKey(alias);
        checkState(!map.containsKey(alias), "Alias '%s' already bound to expression '%s'. Tried to rebind to '%s'", alias, map.get(alias), symbolReference);
        checkState(!map.values().contains(symbolReference), "Expression '%s' is already bound in %s. Tried to rebind as '%s'.", symbolReference, map, alias);
        map.put(alias, symbolReference);
    }

    public void putSourceAliases(SymbolAliases sourceAliases)
    {
        for (Map.Entry<String, SymbolReference> alias : sourceAliases.map.entrySet()) {
            put(alias.getKey(), alias.getValue());
        }
    }

    public SymbolReference get(String alias)
    {
        alias = toKey(alias);
        SymbolReference result = map.get(alias);
        /*
         * It's still kind of an open question if the right combination of anyTree() and
         * a sufficiently complex and/or ambiguous plan might make throwing here a
         * theoretically incorrect thing to do.
         *
         * If you run into a case that you think justifies changing this, please consider
         * that it's already pretty hard to determine if a failure is because the test
         * is written incorrectly or because the actual plan really doesn't match a
         * correctly written test. Having this throw makes it a lot easier to track down
         * missing aliases in incorrect plans.
         */
        checkState(result != null, format("missing expression for alias %s", alias));
        return result;
    }

    private String toKey(String alias)
    {
        // Required because the SqlParser lower cases SymbolReferences in the expressions we parse with it.
        return alias.toLowerCase();
    }

    private Map<String, SymbolReference> getUpdatedAssignments(Map<Symbol, Expression> assignments)
    {
        ImmutableMap.Builder<String, SymbolReference> mapUpdate = ImmutableMap.builder();
        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            for (Map.Entry<String, SymbolReference> existingAlias : map.entrySet()) {
                if (assignment.getValue().equals(existingAlias.getValue())) {
                    // Simple symbol rename
                    mapUpdate.put(existingAlias.getKey(), assignment.getKey().toSymbolReference());
                }
                else if (assignment.getKey().toSymbolReference().equals(existingAlias.getValue())) {
                    /*
                     * Special case for nodes that can alias symbols in the node's assignment map.
                     * In this case, we've already added the alias in the map, but we won't include it
                     * as a simple rename as covered above. Add the existing alias to the result if
                     * the LHS of the assignment matches the symbol reference of the existing alias.
                     *
                     * This comes up when we alias expressions in project nodes for use further up the tree.
                     * At the beginning for the function, map contains { NEW_ALIAS: SymbolReference("expr_2" }
                     * and the assignments map contains { expr_2 := <some expression> }.
                     */
                    mapUpdate.put(existingAlias.getKey(), existingAlias.getValue());
                }
            }
        }
        return mapUpdate.build();
    }

    /*
     * Update assignments in SymbolAliases.map based on assignments given that
     * assignments is a map of newSymbol := oldSymbolReference. RETAIN aliases for
     * SymbolReferences that aren't in assignments.values()
     *
     * Example:
     * SymbolAliases.map = { "ALIAS": SymbolReference("foo") }
     * updateAssignments({"bar": SymbolReference("foo")})
     * results in
     * SymbolAliases.map = { "ALIAS": SymbolReference("bar") }
     */
    public void updateAssignments(Map<Symbol, Expression> assignments)
    {
        Map<String, SymbolReference> additions = getUpdatedAssignments(assignments);
        map.putAll(additions);
    }

    /*
     * Update assignments in SymbolAliases.map based on assignments given that
     * assignments is a map of newSymbol := oldSymbolReference. DISCARD aliases for
     * SymbolReferences that aren't in assignments.values()
     *
     * When you pass through a project node, all of the aliases need to be updated, and
     * aliases for symbols that aren't projected need to be removed.
     *
     * Example:
     * PlanMatchPattern.tableScan("nation", ImmutableMap.of("NK", "nationkey", "RK", "regionkey")
     * applied to
     * TableScanNode { col1 := ColumnHandle(nation, nationkey), col2 := ColumnHandle(nation, regionkey) }
     * gives SymbolAliases.map
     * { "NK": SymbolReference("col1"), "RK": SymbolReference("col2") }
     *
     * ... Visit some other nodes, one of which presumably consumes col1, and none of which add any new aliases ...
     *
     * If we then visit a project node
     * Project { value3 := col2 }
     * SymbolAliases.map should be
     * { "RK": SymbolReference("value3") }
     */
    public void replaceAssignments(Map<Symbol, Expression> assignments)
    {
        Map<String, SymbolReference> newAssignments = getUpdatedAssignments(assignments);
        map.clear();
        map.putAll(newAssignments);
    }
}
