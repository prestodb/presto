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
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutProvider;
import com.facebook.presto.metadata.TableLayoutResult;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.sql.planner.plan.Patterns.join;
import static com.facebook.presto.sql.planner.plan.Patterns.left;
import static com.facebook.presto.sql.planner.plan.Patterns.right;
import static com.facebook.presto.sql.planner.plan.Patterns.tableScan;
import static java.util.Objects.requireNonNull;

public class PushDownJoinToScan
        implements Rule<JoinNode>
{
    private static final Capture<TableScanNode> LEFT_TABLE_SCAN = Capture.newCapture();
    private static final Capture<TableScanNode> RIGHT_TABLE_SCAN = Capture.newCapture();

    private static final Pattern<JoinNode> PATTERN = join()
            .with(left().matching(tableScan().capturedAs(LEFT_TABLE_SCAN)))
            .with(right().matching(tableScan().capturedAs(RIGHT_TABLE_SCAN)));

    private Metadata metadata;

    public PushDownJoinToScan(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    @Override
    public Pattern<JoinNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(JoinNode node, Captures captures, Context context)
    {
        TableScanNode left = captures.get(LEFT_TABLE_SCAN);
        TableScanNode right = captures.get(RIGHT_TABLE_SCAN);

        TableLayoutProvider leftLayoutProvider = metadata.getTableLayoutProvider(context.getSession(), left.getTable(), left.getLayout());

        if (!leftLayoutProvider.getJoinPushdown().isPresent()) {
            return Result.empty();
        }

        leftLayoutProvider.getJoinPushdown().get().pushDownJoin(
                metadata.getTableLayoutProvider(context.getSession(), right.getTable(), right.getLayout()),
                node.getType(),
                node.getCriteria(),
                node.getFilter());

        List<TableLayoutResult> layouts = leftLayoutProvider.provide(context.getSession());
        PlanNode result;
        if (layouts.isEmpty()) {
            result = new ValuesNode(context.getIdAllocator().getNextId(), node.getOutputSymbols(), ImmutableList.of());
        }
        else {
            TableLayoutResult layout = layouts.get(0);
            result = new TableScanNode(
                    context.getIdAllocator().getNextId(),
                    getTableHanble(layout),
                    node.getOutputSymbols(),
                    toAssignments(layout.getLayout().getColumns(), node.getOutputSymbols()),
                    Optional.of(layout.getLayout().getHandle()),
                    toCurrentConstraint(layout),
                    toOriginalExpression(layout));
        }
        return Result.ofPlanNode(result);
    }

    private Expression toOriginalExpression(TableLayoutResult layout)
    {
        // has original expression any sense here? Maybe it should be a conjunct of join conditions and table scan original expressions?
        return BooleanLiteral.TRUE_LITERAL;
    }

    private TupleDomain<ColumnHandle> toCurrentConstraint(TableLayoutResult layout)
    {
        return layout.getLayout().getPredicate();
    }

    private TableHandle getTableHanble(TableLayoutResult layout)
    {
        // should TableHandle be stored in TableLayout?
        return null;
    }

    private Map<Symbol, ColumnHandle> toAssignments(Optional<List<ColumnHandle>> columns, List<Symbol> outputSymbols)
    {
        // how to map these?
        return null;
    }
}
