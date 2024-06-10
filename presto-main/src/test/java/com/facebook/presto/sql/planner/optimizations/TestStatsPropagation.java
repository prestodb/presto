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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Ignore;

import static com.facebook.presto.SystemSessionProperties.ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestStatsPropagation
        extends BasePlanTest
{
    public TestStatsPropagation()
    {
        super(ImmutableMap.of(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "true"));
    }

    @Ignore
    public void testStatsPropagationRandomFunction()
    {
        assertPlan("SELECT 1 FROM lineitem l, orders o WHERE l.orderkey=o.orderkey and l.discount = (SELECT random() FROM nation n where n.nationkey=1)",
                anyTree(project(join(INNER, ImmutableList.of(equiJoinClause("orderkey", "discount")),
                        anyTree(tableScan("orders")), anyTree(anyTree(join(INNER, ImmutableList.of(equiJoinClause("discount", "random")),
                                anyTree(), anyTree())))))));
    }
}
