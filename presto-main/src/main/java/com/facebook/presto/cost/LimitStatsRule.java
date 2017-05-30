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
package com.facebook.presto.cost;

import com.facebook.presto.Session;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.PlanNode;

import java.util.Map;
import java.util.Optional;

public class LimitStatsRule
        implements ComposableStatsCalculator.Rule
{
    @Override
    public Optional<PlanNodeStatsEstimate> calculate(PlanNode node, Lookup lookup, Session session, Map<Symbol, Type> types)
    {
        if (!(node instanceof LimitNode)) {
            return Optional.empty();
        }
        LimitNode limitNode = (LimitNode) node;

        PlanNodeStatsEstimate sourceStats = lookup.getStats(limitNode.getSource(), session, types);
        PlanNodeStatsEstimate.Builder limitCost = PlanNodeStatsEstimate.builder();
        // TODO special handling for NaN?
        if (sourceStats.getOutputRowCount() < limitNode.getCount()) {
            limitCost.setOutputRowCount(sourceStats.getOutputRowCount());
        }
        else {
            limitCost.setOutputRowCount(limitNode.getCount());
        }
        return Optional.of(limitCost.build());
    }
}
