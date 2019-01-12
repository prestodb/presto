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

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;

import static com.google.common.base.MoreObjects.toStringHelper;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class OutputMatcher
        implements Matcher
{
    private final List<String> aliases;

    OutputMatcher(List<String> aliases)
    {
        this.aliases = ImmutableList.copyOf(requireNonNull(aliases, "aliases is null"));
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return true;
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        int i = 0;
        for (String alias : aliases) {
            Expression expression = symbolAliases.get(alias);
            boolean found = false;
            while (i < node.getOutputSymbols().size()) {
                Symbol outputSymbol = node.getOutputSymbols().get(i++);
                if (expression.equals(outputSymbol.toSymbolReference())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                return NO_MATCH;
            }
        }
        return match();
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputs", aliases)
                .toString();
    }
}
