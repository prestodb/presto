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
package io.prestosql.sql.planner;

import io.prestosql.sql.planner.plan.ExchangeNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.PlanVisitor;
import io.prestosql.sql.planner.plan.TableScanNode;

import java.util.List;

/**
 * Visitor to count number of tables scanned in the current fragment
 * (fragments separated by ExchangeNodes).
 * <p>
 * TODO: remove this class after we make colocated_join always true
 */
public final class FragmentTableScanCounter
{
    private FragmentTableScanCounter() {}

    public static int countSources(List<PlanNode> nodes)
    {
        int count = 0;
        for (PlanNode node : nodes) {
            count += node.accept(new Visitor(), null);
        }
        return count;
    }

    public static boolean hasMultipleSources(PlanNode... nodes)
    {
        int count = 0;
        for (PlanNode node : nodes) {
            count += node.accept(new Visitor(), null);
        }
        return count > 1;
    }

    private static class Visitor
            extends PlanVisitor<Integer, Void>
    {
        @Override
        public Integer visitTableScan(TableScanNode node, Void context)
        {
            return 1;
        }

        @Override
        public Integer visitExchange(ExchangeNode node, Void context)
        {
            if (node.getScope() == ExchangeNode.Scope.REMOTE) {
                return 0;
            }
            return visitPlan(node, context);
        }

        @Override
        protected Integer visitPlan(PlanNode node, Void context)
        {
            int count = 0;
            for (PlanNode source : node.getSources()) {
                count += source.accept(this, context);
            }
            return count;
        }
    }
}
