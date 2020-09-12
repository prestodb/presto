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
package com.facebook.presto.sql.planner;

import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.EnforceSingleRowNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.output;

public class TestOrderBy
        extends BasePlanTest
{
    @Test
    public void testRedundantOrderByInSubquery()
    {
        assertPlan("SELECT * FROM (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x)",
                output(
                        node(ValuesNode.class)));
    }

    @Test
    public void testRequiredOrderByInSubquery()
    {
        assertPlan("SELECT * FROM (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x LIMIT 1)",
                output(
                        node(TopNNode.class,
                                anyTree(
                                        node(ValuesNode.class)))));
    }

    @Test
    public void testRedundantOrderByInScalarSubquery()
    {
        assertPlan("SELECT (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x) FROM (VALUES 10)",
                output(
                        node(EnforceSingleRowNode.class,
                                node(ValuesNode.class))));
    }

    @Test
    public void testRequiredOrderByInScalarSubquery()
    {
        assertPlan("SELECT (SELECT * FROM (VALUES 1, 2, 3) t(x) ORDER BY x LIMIT 1) FROM (VALUES 10)",
                output(
                        anyTree(
                                node(TopNNode.class,
                                        node(ValuesNode.class)))));
    }

    @Test
    public void testRequiredOrderByInUnion()
    {
        assertPlan("VALUES 1 " +
                        "UNION ALL " +
                        "VALUES 2 " +
                        "ORDER BY 1 ",
                output(
                        anyTree(
                                node(SortNode.class,
                                        node(ExchangeNode.class,
                                                node(ValuesNode.class),
                                                node(ValuesNode.class))))));
    }

    @Test
    public void testRedundantOrderByInUnion()
    {
        assertPlan("SELECT * FROM (" +
                        "   VALUES 1 " +
                        "   UNION ALL " +
                        "   VALUES 2 " +
                        "   ORDER BY 1 " +
                        ")",
                output(
                        node(ExchangeNode.class,
                                node(ValuesNode.class),
                                node(ValuesNode.class))));
    }
}
