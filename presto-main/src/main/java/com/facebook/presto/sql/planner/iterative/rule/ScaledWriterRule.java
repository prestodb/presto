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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.useHistoryBasedScaledWriters;
import static com.facebook.presto.sql.planner.plan.Patterns.tableWriterNode;
import static com.google.common.base.Preconditions.checkState;

public class ScaledWriterRule
        implements Rule<TableWriterNode>
{
    private String statsSource;

    @Override
    public Pattern<TableWriterNode> getPattern()
    {
        return tableWriterNode().matching(x -> !x.getTaskCountIfScaledWriter().isPresent());
    }

    @Override
    public boolean isEnabled(Session session)
    {
        return useHistoryBasedScaledWriters(session);
    }

    @Override
    public boolean isCostBased(Session session)
    {
        return true;
    }

    @Override
    public String getStatsSource()
    {
        return statsSource;
    }

    @Override
    public Result apply(TableWriterNode node, Captures captures, Context context)
    {
        double taskNumber = context.getStatsProvider().getStats(node).getTableWriterNodeStatsEstimate().getTaskCountIfScaledWriter();
        statsSource = context.getStatsProvider().getStats(node).getSourceInfo().getSourceInfoName();
        if (Double.isNaN(taskNumber)) {
            return Result.empty();
        }
        // We start from half of the original number
        int initialTaskNumber = (int) Math.ceil(taskNumber / 2);
        checkState(initialTaskNumber > 0, "taskCountIfScaledWriter should be at least 1");
        return Result.ofPlanNode(new TableWriterNode(
                node.getSourceLocation(),
                node.getId(),
                node.getStatsEquivalentPlanNode(),
                node.getSource(),
                node.getTarget(),
                node.getRowCountVariable(),
                node.getFragmentVariable(),
                node.getTableCommitContextVariable(),
                node.getColumns(),
                node.getColumnNames(),
                node.getNotNullColumnVariables(),
                node.getTablePartitioningScheme(),
                node.getStatisticsAggregation(),
                Optional.of(initialTaskNumber),
                node.getIsTemporaryTableWriter()));
    }
}
