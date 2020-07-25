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
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.iterative.Rule;

import static com.facebook.presto.matching.Capture.newCapture;
import static com.facebook.presto.sql.planner.plan.Patterns.limit;
import static com.facebook.presto.sql.planner.plan.Patterns.source;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;

public class PushLimitIntoTableScan
        implements Rule<LimitNode>
{
    private static final Capture<TableScanNode> TABLE_SCAN = newCapture();
    private static final Pattern<LimitNode> PATTERN = limit().with(source().matching(
            tableScan().capturedAs(TABLE_SCAN)));

    private final Metadata metadata;

    public PushLimitIntoTableScan(Metadata metadata)
    {
        this.metadata = metadata;
    }

    @Override
    public Pattern<LimitNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Rule.Result apply(LimitNode limit, Captures captures, Rule.Context context)
    {
        TableScanNode tableScan = captures.get(TABLE_SCAN);

        return metadata.applyLimit(context.getSession(), tableScan.getTable(), limit.getCount())
                .map(result -> {
                    PlanNode node = new TableScanNode(
                            tableScan.getId(),
                            result.getHandle(),
                            tableScan.getOutputVariables(),
                            tableScan.getAssignments(),
                            tableScan.getCurrentConstraint(),
                            tableScan.getEnforcedConstraint());

                    if (!result.isLimitGuaranteed()) {
                        node = new LimitNode(limit.getId(), node, limit.getCount(), limit.getStep());
                    }

                    return Result.ofPlanNode(node);
                })
                .orElseGet(Result::empty);
    }
}
