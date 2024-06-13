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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION;

public class TestStatsPropagation
        extends BasePlanTest
{
    public TestStatsPropagation()
    {
        super(ImmutableMap.of(ENABLE_SCALAR_FUNCTION_STATS_PROPAGATION, "true"));
    }

    @Test
    public void testStatsPropagationScalarRandomFunction()
    {
        assertPlanHasVariableStats("SELECT 1 FROM lineitem l, orders o WHERE l.orderkey=o.orderkey and l.discount = (SELECT random() FROM nation n where n.nationkey=1)",
                getQueryRunner().getDefaultSession());
        assertPlanHasVariableStats("select * FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and substr(lower(l.comment), 2) = 'us'",
                getQueryRunner().getDefaultSession());
    }

    @Test
    public void testStatsPropagationWithLike()
    {
        assertPlanHasVariableStats("select * FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and l.comment LIKE '%u'",
                getQueryRunner().getDefaultSession());
        assertPlanHasVariableStats("select * FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and upper(l.comment) LIKE '%US%'",
                getQueryRunner().getDefaultSession());
    }

    @Test
    public void testStatsPropagationWithConcatFunction()
    {
        assertPlanHasVariableStats("SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and concat(l.shipmode, l.shipinstruct) = 'MAILNONE'" ,
                getQueryRunner().getDefaultSession());
        assertPlanHasVariableStats("SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and concat(l.comment, 'us') = 'testus'" ,
                getQueryRunner().getDefaultSession());
    }
}
