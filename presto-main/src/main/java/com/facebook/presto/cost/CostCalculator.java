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
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.inject.BindingAnnotation;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;
import java.util.Map;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * Interface of cost calculator.
 *
 * Computes estimated cost of executing given PlanNode.
 * Implementation may use lookup to compute needed traits for self/source nodes.
 */
@ThreadSafe
public interface CostCalculator
{
    PlanNodeCostEstimate calculateCost(
            PlanNode planNode,
            Lookup lookup,
            Session session,
            Map<Symbol, Type> types);

    default PlanNodeCostEstimate calculateCumulativeCost(
            PlanNode planNode,
            Lookup lookup,
            Session session,
            Map<Symbol, Type> types)
    {
        PlanNodeCostEstimate childrenCost = planNode.getSources().stream()
                .map(child -> lookup.getCumulativeCost(child, session, types))
                .reduce(PlanNodeCostEstimate.builder().build(), PlanNodeCostEstimate::add);

        return calculateCost(planNode, lookup, session, types).add(childrenCost);
    }

    @BindingAnnotation @Target({PARAMETER}) @Retention(RUNTIME)
    @interface EstimatedExchanges {
    }
}
