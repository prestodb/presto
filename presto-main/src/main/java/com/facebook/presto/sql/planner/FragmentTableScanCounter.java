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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.InternalPlanVisitor;

import java.util.Collection;

/**
 * Visitor to count number of tables scanned in the current fragment
 * (fragments separated by ExchangeNodes).
 * <p>
 * TODO: remove this class after we make colocated_join always true
 */
public final class FragmentTableScanCounter
{
    private FragmentTableScanCounter() {}

    public static int getNumberOfTableScans(Collection<PlanNode> nodes)
    {
        return getNumberOfTableScans(nodes.toArray(new PlanNode[] {}));
    }

    public static boolean hasMultipleTableScans(PlanNode... nodes)
    {
        return getNumberOfTableScans(nodes) > 1;
    }

    public static int getNumberOfTableScans(PlanNode... nodes)
    {
        int count = 0;
        for (PlanNode node : nodes) {
            count += node.accept(new Visitor(), null);
        }
        return count;
    }

    private static class Visitor
            extends InternalPlanVisitor<Integer, Void>
    {
        @Override
        public Integer visitTableScan(TableScanNode node, Void context)
        {
            return 1;
        }

        @Override
        public Integer visitExchange(ExchangeNode node, Void context)
        {
            if (node.getScope().isRemote()) {
                return 0;
            }
            return visitPlan(node, context);
        }

        @Override
        public Integer visitPlan(PlanNode node, Void context)
        {
            int count = 0;
            for (PlanNode source : node.getSources()) {
                count += source.accept(this, context);
            }
            return count;
        }
    }
}
