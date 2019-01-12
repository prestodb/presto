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
package io.prestosql.sql.planner.assertions;

import com.google.common.collect.ImmutableSet;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class StrictSymbolsMatcher
        extends BaseStrictSymbolsMatcher
{
    private final List<String> expectedAliases;

    public StrictSymbolsMatcher(Function<PlanNode, Set<Symbol>> getActual, List<String> expectedAliases)
    {
        super(getActual);
        this.expectedAliases = requireNonNull(expectedAliases, "expectedAliases is null");
    }

    @Override
    protected Set<Symbol> getExpectedSymbols(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return expectedAliases.stream()
                .map(symbolAliases::get)
                .map(Symbol::from)
                .collect(toImmutableSet());
    }

    public static Function<PlanNode, Set<Symbol>> actualOutputs()
    {
        return node -> ImmutableSet.copyOf(node.getOutputSymbols());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exact outputs", expectedAliases)
                .toString();
    }
}
