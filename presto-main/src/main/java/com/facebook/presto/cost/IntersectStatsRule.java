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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.plan.IntersectNode;

import static com.facebook.presto.cost.PlanNodeStatsEstimateMath.intersect;

public class IntersectStatsRule
        extends AbstractSetOperationStatsRule
{
    private static final Pattern PATTERN = Pattern.typeOf(IntersectNode.class);

    @Override
    public Pattern getPattern()
    {
        return PATTERN;
    }

    @Override
    protected PlanNodeStatsEstimate operate(PlanNodeStatsEstimate first, PlanNodeStatsEstimate second)
    {
        return intersect(first, second);
    }
}
