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
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Optional;

import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class Alias
    implements Matcher
{
    private final Optional<String> alias;
    private final RvalueMatcher matcher;

    Alias(Optional<String> alias, RvalueMatcher matcher)
    {
        this.alias = requireNonNull(alias, "alias is null");
        this.matcher = requireNonNull(matcher, "matcher is null");
    }

    @Override
    public boolean downMatches(PlanNode node)
    {
        return true;
    }

    /*
     * Add aliases on the way back up the tree. Adding them on the way down would put them in the expressionAliases
     * for matchers that run against the node's sources, which would be incorrect.
     */
    @Override
    public boolean upMatches(PlanNode node, Session session, Metadata metadata, ExpressionAliases expressionAliases)
    {
        Optional<Symbol> symbol = matcher.getAssignedSymbol(node, session, metadata, expressionAliases);
        if (symbol.isPresent() && alias.isPresent()) {
            expressionAliases.put(alias.get(), symbol.get().toSymbolReference());
        }
        return symbol.isPresent();
    }

    @Override
    public String toString()
    {
        return format("bind %s -> %s", alias, matcher);
    }
}
