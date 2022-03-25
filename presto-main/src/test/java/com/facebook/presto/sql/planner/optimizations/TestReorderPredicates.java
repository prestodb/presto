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

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestReorderPredicates
        extends BasePlanTest
{
    @Test
    public void testBasicPredicateReordering()
    {
        assertPlan("SELECT COUNT(*) FROM lineitem WHERE REGEXP_LIKE(comment, '.*') AND tax = 0.01",
                anyTree(
                        filter("tax = DOUBLE'0.01' AND REGEXP_LIKE(comment, CAST('.*' AS JoniRegExp))",
                                tableScan("lineitem",
                                        ImmutableMap.of("tax", "tax", "comment", "comment")))));
    }

    @Test
    public void testMultipleReordering()
    {
        assertPlan("SELECT COUNT(*) FROM lineitem WHERE (REGEXP_LIKE(comment, '.*') OR tax = 0.02) AND tax > 0.01",
                anyTree(
                        filter("tax > DOUBLE'0.01' AND (tax = DOUBLE'0.02' OR REGEXP_LIKE(comment, CAST('.*' AS JoniRegExp)))",
                                tableScan("lineitem",
                                        ImmutableMap.of("tax", "tax", "comment", "comment")))));
    }

    @Test
    public void testNestedReordering()
    {
        assertPlan("SELECT * FROM (" +
                        "SELECT COUNT(*) FROM lineitem WHERE REGEXP_LIKE(comment, '.*') AND tax = 0.01" +
                        ") t",
                anyTree(
                        filter("tax = DOUBLE'0.01' AND REGEXP_LIKE(comment, CAST('.*' AS JoniRegExp))",
                                tableScan("lineitem",
                                        ImmutableMap.of("tax", "tax", "comment", "comment")))));

        assertPlan("SELECT COUNT(*) FROM (" +
                        "SELECT * FROM lineitem WHERE tax > 0" +
                        ") t WHERE REGEXP_LIKE(comment, '.*') AND tax = 0.01",
                anyTree(
                        filter("tax = DOUBLE'0.01' AND REGEXP_LIKE(comment, CAST('.*' AS JoniRegExp))",
                                tableScan("lineitem",
                                        ImmutableMap.of("tax", "tax", "comment", "comment")))));
    }
}
