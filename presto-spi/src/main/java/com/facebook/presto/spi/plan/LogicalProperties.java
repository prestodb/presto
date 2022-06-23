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
package com.facebook.presto.spi.plan;

import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Set;

/**
 * Logical properties represent constraints that hold for a final or intermediate result produced by a PlanNode.
 * They are used by the optimizer to enable query transformations that lead to better performing plans.
 * For example, unique constraints can be used to eliminate redundant distinct operations.
 * This is the interface used by optimizer rules to perform optimizations based upon logical properties.
 * <p>
 * In this initial phase logical properties computed are based solely upon key constraints. In the future
 * support for referential constraints, functional dependencies, and others will be added and
 * hence this interface will become more robust over time.
 */
public interface LogicalProperties
{
    /**
     * Determines if the set of variables form a unique constraint for a final or
     * intermediate result produced by a PlanNode.
     *
     * @param keyVars
     * @return True if the set of variables form a unique constraint or false otherwise.
     */
    boolean isDistinct(Set<VariableReferenceExpression> keyVars);

    /**
     * Determines if there is provably at most one tuple in a final or
     * intermediate result set produced by a PlanNode.
     *
     * @return True if there is provably at most one tuple or false otherwise.
     */
    boolean isAtMostSingleRow();

    /**
     * Determines if there is provably at most n tuples in a final or
     * intermediate result set produced by a PlanNode.
     *
     * @return True if there is provably at most one tuple or false otherwise.
     */
    boolean isAtMost(long n);
}
