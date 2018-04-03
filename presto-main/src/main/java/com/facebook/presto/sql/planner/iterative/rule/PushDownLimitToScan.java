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

import com.facebook.presto.matching.Capture;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableLayoutProvider;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushDownLimitToScan
        implements Rule<LimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = Capture.newCapture();

    private static final Pattern<LimitNode> PATTERN = limit()
            .with(source().matching(tableScan().capturedAs(TABLE_SCAN)));

    private Metadata metadata;

    public PushDownLimitToScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(LimitNode node, Captures captures, Context context)
    {
        TableScanNode tableScanNode = captures.get(TABLE_SCAN);

        TableLayoutProvider layoutProvider = metadata.getTableLayoutProvider(context.getSession(), tableScanNode.getTable(), tableScanNode.getLayout());

        if (!layoutProvider.getLimitPushdown().isPresent()) {
            return Result.empty();
        }

        layoutProvider.getLimitPushdown().get().pushDownLimit(node.getCount());

        List<TableLayoutResult> layouts = layoutProvider.provide(context.getSession());
        PlanNode result;
        if (layouts.isEmpty()) {
            result = new ValuesNode(context.getIdAllocator().getNextId(), node.getOutputSymbols(), ImmutableList.of());
        }
        else {
            TableLayoutResult layout = layouts.get(0);
            result = new TableScanNode(
                    context.getIdAllocator().getNextId(),
                    tableScanNode.getTable(),
                    node.getOutputSymbols(),
                    tableScanNode.getAssignments(),
                    Optional.of(layout.getLayout().getHandle()),
                    tableScanNode.getCurrentConstraint(),
                    tableScanNode.getOriginalConstraint());
        }
        return Result.ofPlanNode(result);
    }
}
