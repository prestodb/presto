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
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.optimizations.TableLayoutRewriter;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.BooleanLiteral;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

/**
 * Pick a table layout based on the filter above the TableScanNode,
 * or else pick an arbitrary layout if there are no constraints.
 */
public class AddTableLayout
        implements Rule
{
    private final Metadata metadata;

    public AddTableLayout(Metadata metadata)
    {
        requireNonNull(metadata, "metadata is null");

        this.metadata = metadata;
    }

    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator, Session session)
    {
        if (!(node instanceof FilterNode) && !(node instanceof TableScanNode)) {
            return Optional.empty();
        }
        TableLayoutRewriter tableLayoutRewriter = new TableLayoutRewriter(metadata, session, symbolAllocator, idAllocator);

        if (node instanceof TableScanNode && !((TableScanNode) node).getLayout().isPresent()) {
            return Optional.of(tableLayoutRewriter.planTableScan((TableScanNode) node, BooleanLiteral.TRUE_LITERAL));
        }

        if (node instanceof FilterNode) {
            FilterNode filterNode = (FilterNode) node;
            PlanNode source = lookup.resolve(filterNode.getSource());
            if (source instanceof TableScanNode && !((TableScanNode) source).getLayout().isPresent()) {
                return Optional.of(tableLayoutRewriter.planTableScan((TableScanNode) source, filterNode.getPredicate()));
            }
        }
        return Optional.empty();
    }
}
