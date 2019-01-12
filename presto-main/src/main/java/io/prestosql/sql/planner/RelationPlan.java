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
package io.prestosql.sql.planner;

import com.google.common.collect.ImmutableList;
import io.prestosql.sql.analyzer.RelationType;
import io.prestosql.sql.analyzer.Scope;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

/**
 * The purpose of this class is to hold the current plan built so far
 * for a relation (query, table, values, etc.), and the mapping to
 * indicate how the fields (by position) in the relation map to
 * the outputs of the plan.
 * <p>
 * Fields are resolved by {@link TranslationMap} within local scopes hierarchy.
 * Indexes of resolved parent scope fields start from "total number of child scope fields".
 * For instance if a child scope has n fields, then first parent scope field
 * will have index n.
 */
class RelationPlan
{
    private final PlanNode root;
    private final List<Symbol> fieldMappings; // for each field in the relation, the corresponding symbol from "root"
    private final Scope scope;

    public RelationPlan(PlanNode root, Scope scope, List<Symbol> fieldMappings)
    {
        requireNonNull(root, "root is null");
        requireNonNull(fieldMappings, "outputSymbols is null");
        requireNonNull(scope, "scope is null");

        int allFieldCount = getAllFieldCount(scope);
        checkArgument(allFieldCount == fieldMappings.size(),
                "Number of outputs (%s) doesn't match number of fields in scopes tree (%s)",
                fieldMappings.size(),
                allFieldCount);

        this.root = root;
        this.scope = scope;
        this.fieldMappings = ImmutableList.copyOf(fieldMappings);
    }

    public Symbol getSymbol(int fieldIndex)
    {
        checkArgument(fieldIndex >= 0 && fieldIndex < fieldMappings.size(), "No field->symbol mapping for field %s", fieldIndex);
        return fieldMappings.get(fieldIndex);
    }

    public PlanNode getRoot()
    {
        return root;
    }

    public List<Symbol> getFieldMappings()
    {
        return fieldMappings;
    }

    public RelationType getDescriptor()
    {
        return scope.getRelationType();
    }

    public Scope getScope()
    {
        return scope;
    }

    private static int getAllFieldCount(Scope root)
    {
        int allFieldCount = 0;
        Optional<Scope> current = Optional.of(root);
        while (current.isPresent()) {
            allFieldCount += current.get().getRelationType().getAllFieldCount();
            current = current.get().getLocalParent();
        }
        return allFieldCount;
    }
}
