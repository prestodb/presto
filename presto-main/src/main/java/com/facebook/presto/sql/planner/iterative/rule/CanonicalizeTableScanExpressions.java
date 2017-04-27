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

import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.tree.Expression;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.optimizations.CanonicalizeExpressions.canonicalizeExpression;

public class CanonicalizeTableScanExpressions
        implements Rule
{
    @Override
    public Optional<PlanNode> apply(PlanNode node, Lookup lookup, PlanNodeIdAllocator idAllocator, SymbolAllocator symbolAllocator)
    {
        if (!(node instanceof TableScanNode)) {
            return Optional.empty();
        }

        TableScanNode tableScanNode = (TableScanNode) node;

        Expression originalConstraint = null;
        if (tableScanNode.getOriginalConstraint() != null) {
            originalConstraint = canonicalizeExpression(tableScanNode.getOriginalConstraint());
        }

        if (Objects.equals(tableScanNode.getOriginalConstraint(), originalConstraint)) {
            return Optional.empty();
        }

        TableScanNode replacement = new TableScanNode(
                tableScanNode.getId(),
                tableScanNode.getTable(),
                tableScanNode.getOutputSymbols(),
                tableScanNode.getAssignments(),
                tableScanNode.getLayout(),
                tableScanNode.getCurrentConstraint(),
                originalConstraint);

        return Optional.of(replacement);
    }
}
