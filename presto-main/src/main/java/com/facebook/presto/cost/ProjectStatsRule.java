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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Map;
import java.util.Optional;

public class ProjectStatsRule
        implements ComposableStatsCalculator.Rule
{
    private final ScalarStatsCalculator scalarStatsCalculator;

    public ProjectStatsRule(ScalarStatsCalculator scalarStatsCalculator)
    {
        this.scalarStatsCalculator = scalarStatsCalculator;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }
        ProjectNode projectNode = (ProjectNode) node;

        PlanNodeStatsEstimate sourceStats = lookup.getStats(projectNode.getSource(), session, types);
        // TODO handle output size in bytes
        PlanNodeStatsEstimate.Builder calculatedStats = PlanNodeStatsEstimate.builder()
                .setOutputRowCount(sourceStats.getOutputRowCount());

        for (Map.Entry<Symbol, Expression> entry : projectNode.getAssignments().entrySet()) {
            calculatedStats.addSymbolStatistics(entry.getKey(), scalarStatsCalculator.calculate(entry.getValue(), sourceStats, session, types));
        }
        return Optional.of(calculatedStats.build());
    }
}
