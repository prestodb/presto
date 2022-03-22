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

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class StrictSymbolsMatcher
        extends BaseStrictSymbolsMatcher
{
    private final List<String> expectedAliases;

    public StrictSymbolsMatcher(Function<PlanNode, Set<VariableReferenceExpression>> getActual, List<String> expectedAliases)
    {
        super(getActual);
        this.expectedAliases = requireNonNull(expectedAliases, "expectedAliases is null");
    }

    @Override
    protected Set<VariableReferenceExpression> getExpectedVariables(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return expectedAliases.stream()
                .map(symbolAliases::get)
                .map(symbolReference -> new VariableReferenceExpression(symbolReference.getName(), UNKNOWN))
                .collect(toImmutableSet());
    }

    public static Function<PlanNode, Set<VariableReferenceExpression>> actualOutputs()
    {
        return node -> ImmutableSet.copyOf(node.getOutputVariables());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exact outputs", expectedAliases)
                .toString();
    }
}
