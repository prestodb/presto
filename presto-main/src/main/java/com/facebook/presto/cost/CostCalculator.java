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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.IterativeOptimizer;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.google.inject.BindingAnnotation;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@ThreadSafe
public interface CostCalculator
{
    /**
     * Calculates non-cumulative cost of a node.
     *
     * @param node The node to compute cost for.
     * @param stats The stats provider for node's stats and child nodes' stats, to be used if stats are needed to compute cost for the {@code node}
     * @param lookup Lookup to be used when resolving source nodes, allowing cost calculation to work within {@link IterativeOptimizer}
     */
    PlanNodeCostEstimate calculateCost(
            PlanNode node,
            StatsProvider stats,
            Lookup lookup,
            Session session,
            TypeProvider types);

    @BindingAnnotation
    @Target({PARAMETER})
    @Retention(RUNTIME)
    @interface EstimatedExchanges {}
}
