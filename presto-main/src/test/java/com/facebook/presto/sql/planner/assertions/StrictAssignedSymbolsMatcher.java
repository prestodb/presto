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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableSet;

import java.util.Collection;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class StrictAssignedSymbolsMatcher
        extends BaseStrictSymbolsMatcher
{
    private final Collection<? extends RvalueMatcher> getExpected;

    public StrictAssignedSymbolsMatcher(Function<PlanNode, Set<VariableReferenceExpression>> getActual, Collection<? extends RvalueMatcher> getExpected)
    {
        super(getActual);
        this.getExpected = requireNonNull(getExpected, "getExpected is null");
    }

    @Override
    protected Set<VariableReferenceExpression> getExpectedVariables(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        ImmutableSet.Builder<VariableReferenceExpression> expected = ImmutableSet.builder();
        for (RvalueMatcher matcher : getExpected) {
            Optional<VariableReferenceExpression> assigned = matcher.getAssignedVariable(node, session, metadata, symbolAliases);
            if (!assigned.isPresent()) {
                return null;
            }

            expected.add(assigned.get());
        }

        return expected.build();
    }

    public static Function<PlanNode, Set<VariableReferenceExpression>> actualAssignments()
    {
        return node -> ((ProjectNode) node).getAssignments().getVariables();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("exact assignments", getExpected)
                .toString();
    }
}
