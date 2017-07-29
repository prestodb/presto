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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ApplyNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;

public class PlanNodePatterns
{
    private static final Pattern PROJECT = Pattern.typeOf(ProjectNode.class);
    private static final Pattern AGGREGATION = Pattern.typeOf(AggregationNode.class);
    private static final Pattern FILTER = Pattern.typeOf(FilterNode.class);
    private static final Pattern TABLE_SCAN = Pattern.typeOf(TableScanNode.class);
    private static final Pattern JOIN = Pattern.typeOf(JoinNode.class);
    private static final Pattern VALUES = Pattern.typeOf(ValuesNode.class);
    private static final Pattern APPLY = Pattern.typeOf(ApplyNode.class);

    private PlanNodePatterns() {}

    public static Pattern project()
    {
        return PROJECT;
    }

    public static Pattern aggregation()
    {
        return AGGREGATION;
    }

    public static Pattern filter()
    {
        return FILTER;
    }

    public static Pattern tableScan()
    {
        return TABLE_SCAN;
    }

    public static Pattern join()
    {
        return JOIN;
    }

    public static Pattern values()
    {
        return VALUES;
    }

    public static Pattern apply()
    {
        return APPLY;
    }
}
