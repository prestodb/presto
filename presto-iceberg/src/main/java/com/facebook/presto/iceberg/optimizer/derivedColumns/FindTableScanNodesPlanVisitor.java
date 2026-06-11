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

package com.facebook.presto.iceberg.optimizer.derivedColumns;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanVisitor;
import com.facebook.presto.spi.plan.TableScanNode;

import java.util.HashSet;
import java.util.Set;

/**
 * Find all the table scan nodes under the current Plan node.
 */
public class FindTableScanNodesPlanVisitor
        extends PlanVisitor<Set<TableScanNode>, Void>
{
    Set<TableScanNode> tableScanNodes = new HashSet<>();

    @Override
    public Set<TableScanNode> visitPlan(PlanNode node, Void context)
    {
        processChildren(node, context);
        return tableScanNodes;
    }

    @Override
    public Set<TableScanNode> visitTableScan(TableScanNode node, Void context)
    {
        tableScanNodes.add(node);
        return tableScanNodes;
    }

    private void processChildren(PlanNode node, Void context)
    {
        for (PlanNode child : node.getSources()) {
            child.accept(this, context);
        }
    }
}
