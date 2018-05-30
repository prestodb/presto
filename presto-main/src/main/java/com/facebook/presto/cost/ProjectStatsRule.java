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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ProjectStatsRule
        extends SimpleStatsRule
{
    private static final Pattern<ProjectNode> PATTERN = Pattern.typeOf(ProjectNode.class);

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
    protected Optional<PlanNodeStatsEstimate> doCalculate(PlanNode node, StatsProvider statsProvider, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        ProjectNode projectNode = (ProjectNode) node;

        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(projectNode.getSource());
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(sourceStats.getOutputRowCount());

        for (Map.Entry<Symbol, Expression> entry : projectNode.getAssignments().entrySet()) {
            calculatedStats.addSymbolStatistics(entry.getKey(), scalarStatsCalculator.calculate(entry.getValue(), sourceStats, session));
        }
        return Optional.of(calculatedStats.build());
    }
}
