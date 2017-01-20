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

import com.facebook.presto.sql.planner.DependencyExtractor;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

class Util
{
    private Util()
    {
    }

    /**
     * Prune the list of available inputs to those required by the given expressions.
     *
     * If all inputs are used, return Optional.empty() to indicate that no pruning is necessary.
     */
    public static Optional<List<Symbol>> pruneInputs(Collection<Symbol> availableInputs, Collection<Expression> expressions)
    {
        Set<Symbol> available = new HashSet<>(availableInputs);
        Set<Symbol> required = DependencyExtractor.extractUnique(expressions);

        // we need to compute the intersection in case some dependencies are symbols from
        // the outer scope (i.e., correlated queries)
        Set<Symbol> used = Sets.intersection(required, available);
        if (used.size() == available.size()) {
            // no need to prune... every available input is being used
            return Optional.empty();
        }

        return Optional.of(ImmutableList.copyOf(used));
    }

    /**
     * Transforms a plan like P->C->X to C->P->X
     */
    public static PlanNode transpose(PlanNode parent, PlanNode child)
    {
        return child.replaceChildren(ImmutableList.of(
                parent.replaceChildren(
                        child.getSources())));
    }
}
