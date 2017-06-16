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

package com.facebook.presto.sql.planner.iterative;

import com.facebook.presto.Session;
import com.facebook.presto.cost.CostCalculator;
import com.facebook.presto.cost.PlanNodeCost;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.plan.PlanNode;

import javax.inject.Inject;

import java.util.Map;

import static java.util.Objects.requireNonNull;

// TODO: remove.  Eventually all uses of StatelessLookup should be replaced with the Lookup specific to the plan
public class StatelessLookup
        implements Lookup
{
    private final CostCalculator costCalculator;

    @Inject
    public StatelessLookup(CostCalculator costCalculator)
    {
        this.costCalculator = requireNonNull(costCalculator, "costCalculator is null");
    }

    @Override
    public PlanNode resolve(PlanNode node)
    {
        return node;
    }

    @Override
    public PlanNodeCost getCost(PlanNode planNode, Session session, Map<Symbol, Type> types)
    {
        return costCalculator.calculateCost(
                planNode,
                this,
                session,
                types);
    }
}
