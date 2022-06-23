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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.project;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
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
                .setOutputRowCount(sourceStats.getOutputRowCount())
                .setConfident(sourceStats.isConfident() && noChangeToSourceColumns(node));

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : node.getAssignments().entrySet()) {
            RowExpression expression = entry.getValue();
            if (isExpression(expression)) {
                calculatedStats.addVariableStatistics(entry.getKey(), scalarStatsCalculator.calculate(castToExpression(expression), sourceStats, session, types));
            }
            else {
                calculatedStats.addVariableStatistics(entry.getKey(), scalarStatsCalculator.calculate(expression, sourceStats, session));
            }
        }
        return Optional.of(calculatedStats.build());
    }

    private boolean noChangeToSourceColumns(ProjectNode node)
    {
        return node.getOutputVariables().containsAll(node.getSource().getOutputVariables());
    }
}
