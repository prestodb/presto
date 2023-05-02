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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.Optional;

import static com.facebook.presto.sql.planner.iterative.rule.JoinSwappingUtils.createRuntimeSwappedJoinNode;
import static com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher.searchFrom;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.RIGHT;
import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class RuntimeReorderJoinSides
        implements Rule<JoinNode>
{
    private static final Logger log = Logger.get(RuntimeReorderJoinSides.class);

    private static final Pattern<JoinNode> PATTERN = join();

    private final Metadata metadata;
    private final SqlParser parser;

    public RuntimeReorderJoinSides(Metadata metadata, SqlParser parser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.parser = requireNonNull(parser, "parser is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode joinNode, Captures captures, Context context)
    {
        // Early exit if the leaves of the joinNode subtree include non tableScan nodes.
        if (searchFrom(joinNode, context.getLookup())
                .where(node -> node.getSources().isEmpty() && !(node instanceof TableScanNode))
                .matches()) {
            return Result.empty();
        }

        double leftOutputSizeInBytes = Double.NaN;
        double rightOutputSizeInBytes = Double.NaN;
        StatsProvider statsProvider = context.getStatsProvider();
        if (searchFrom(joinNode, context.getLookup())
                .where(node -> !(node instanceof TableScanNode) && !(node instanceof ExchangeNode))
                .findAll().size() == 1) {
            // Simple plan is characterized as Join directly on tableScanNodes only with exchangeNode in between.
            // For simple plans, directly fetch the overall table sizes as the size of the join sides to have
            // accurate input bytes statistics and meanwhile avoid non-negligible cost of collecting and processing
            // per-column statistics.
            leftOutputSizeInBytes = statsProvider.getStats(joinNode.getLeft()).getOutputSizeInBytes();
            rightOutputSizeInBytes = statsProvider.getStats(joinNode.getRight()).getOutputSizeInBytes();
        }
        if (Double.isNaN(leftOutputSizeInBytes) || Double.isNaN(rightOutputSizeInBytes)) {
            // Per-column estimate left and right output size for complex plans or when size statistics is unavailable.
            leftOutputSizeInBytes = statsProvider.getStats(joinNode.getLeft()).getOutputSizeInBytes(joinNode.getLeft());
            rightOutputSizeInBytes = statsProvider.getStats(joinNode.getRight()).getOutputSizeInBytes(joinNode.getRight());
        }

        if (Double.isNaN(leftOutputSizeInBytes) || Double.isNaN(rightOutputSizeInBytes)) {
            return Result.empty();
        }
        if (rightOutputSizeInBytes <= leftOutputSizeInBytes) {
            return Result.empty();
        }

        // Check if the swapped join is valid.
        if (!isSwappedJoinValid(joinNode)) {
            return Result.empty();
        }

        Optional<JoinNode> rewrittenNode = createRuntimeSwappedJoinNode(joinNode, metadata, parser, context.getLookup(), context.getSession(), context.getVariableAllocator(), context.getIdAllocator());
        if (rewrittenNode.isPresent()) {
            log.debug(format("Probe size: %.2f is smaller than Build size: %.2f => invoke runtime join swapping on JoinNode ID: %s.", leftOutputSizeInBytes, rightOutputSizeInBytes, joinNode.getId()));
            return Result.ofPlanNode(rewrittenNode.get());
        }
        return Result.empty();
    }

    private boolean isSwappedJoinValid(JoinNode join)
    {
        return !(join.getDistributionType().get() == REPLICATED && join.getType() == LEFT) &&
                !(join.getDistributionType().get() == PARTITIONED && join.getCriteria().isEmpty() && join.getType() == RIGHT);
    }
}
