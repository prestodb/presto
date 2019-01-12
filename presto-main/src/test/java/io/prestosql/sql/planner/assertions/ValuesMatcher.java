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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import io.prestosql.Session;
import io.prestosql.cost.StatsProvider;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ValuesNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.planner.assertions.MatchResult.NO_MATCH;
import static io.prestosql.sql.planner.assertions.MatchResult.match;
import static java.util.Objects.requireNonNull;

public class ValuesMatcher
        implements Matcher
{
    private final Map<String, Integer> outputSymbolAliases;
    private final Optional<Integer> expectedOutputSymbolCount;
    private final Optional<List<List<Expression>>> expectedRows;

    public ValuesMatcher(
            Map<String, Integer> outputSymbolAliases,
            Optional<Integer> expectedOutputSymbolCount,
            Optional<List<List<Expression>>> expectedRows)
    {
        this.outputSymbolAliases = ImmutableMap.copyOf(outputSymbolAliases);
        this.expectedOutputSymbolCount = requireNonNull(expectedOutputSymbolCount, "expectedOutputSymbolCount is null");
        this.expectedRows = requireNonNull(expectedRows, "expectedRows is null");
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return (node instanceof ValuesNode) &&
                expectedOutputSymbolCount.map(Integer.valueOf(node.getOutputSymbols().size())::equals).orElse(true);
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        ValuesNode valuesNode = (ValuesNode) node;

        if (!expectedRows.map(rows -> rows.equals(valuesNode.getRows())).orElse(true)) {
            return NO_MATCH;
        }

        return match(SymbolAliases.builder()
                .putAll(Maps.transformValues(outputSymbolAliases, index -> valuesNode.getOutputSymbols().get(index).toSymbolReference()))
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("outputSymbolAliases", outputSymbolAliases)
                .add("expectedOutputSymbolCount", expectedOutputSymbolCount)
                .add("expectedRows", expectedRows)
                .toString();
    }
}
