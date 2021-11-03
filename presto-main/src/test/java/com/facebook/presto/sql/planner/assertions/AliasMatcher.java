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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class AliasMatcher
        implements Matcher
{
    private final Optional<String> alias;
    private final RvalueMatcher matcher;

    AliasMatcher(Optional<String> alias, RvalueMatcher matcher)
    {
        this.alias = requireNonNull(alias, "alias is null");
        this.matcher = requireNonNull(matcher, "matcher is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    /*
     * Aliases must be collected on the way back up the tree for several reasons:
     * 1) The rvalue may depend on previously bound aliases (in the case of an
     *    Expression or FunctionCall)
     * 2) Scope: aliases bound in a node are only in scope in nodes higher up
     *    the tree, just as symbols in a node's output are only in scope in nodes
     *    higher up the tree.
     */
    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<VariableReferenceExpression> variable = matcher.getAssignedVariable(node, session, metadata, symbolAliases);
        if (variable.isPresent() && alias.isPresent()) {
            return match(alias.get(), new SymbolReference(variable.get().getName()));
        }
        return new MatchResult(variable.isPresent());
    }

    @Override
    public String toString()
    {
        if (alias.isPresent()) {
            return format("bind %s -> %s", alias.get(), matcher);
        }
        return format("bind %s", matcher);
    }
}
