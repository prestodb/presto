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
package io.prestosql.cost;

import io.prestosql.Session;
import io.prestosql.matching.Pattern;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.iterative.Lookup;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static io.prestosql.sql.planner.plan.Patterns.project;
import static java.util.Objects.requireNonNull;

public class ProjectStatsRule
        extends SimpleStatsRule<ProjectNode>
{
    private static final Pattern<ProjectNode> PATTERN = project();

    private final ScalarStatsCalculator scalarStatsCalculator;

    public ProjectStatsRule(ScalarStatsCalculator scalarStatsCalculator, StatsNormalizer normalizer)
    {
        super(normalizer);
        this.scalarStatsCalculator = requireNonNull(scalarStatsCalculator, "scalarStatsCalculator is null");
    }

    @Override
    public Pattern<ProjectNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    protected Optional<PlanNodeStatsEstimate> doCalculate(ProjectNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(sourceStats.getOutputRowCount());

        for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
            calculatedStats.addSymbolStatistics(entry.getKey(), scalarStatsCalculator.calculate(entry.getValue(), sourceStats, session, types));
        }
        return Optional.of(calculatedStats.build());
    }
}
