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

import com.facebook.presto.Session;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.TopNRowNumberNode;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

import static com.facebook.presto.SystemSessionProperties.NATIVE_EXECUTION_ENABLED;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_TOP_N_RANK;
import static com.facebook.presto.SystemSessionProperties.OPTIMIZE_TOP_N_ROW_NUMBER;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyNot;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.limit;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestWindowFilterPushDown
        extends BasePlanTest
{
    private void testLimitSql(String sql, boolean rowNumber)
    {
        assertPlanWithSession(
                sql,
                rowNumber ? optimizeTopNRowNumber(true) : optimizeTopNRank(true),
                true,
                anyTree(
                        limit(10, anyTree(
                                node(TopNRowNumberNode.class,
                                        anyTree(
                                                tableScan("lineitem")))))));

        assertPlanWithSession(
                sql,
                rowNumber ? optimizeTopNRowNumber(false) : optimizeTopNRank(false),
                true,
                anyTree(
                        limit(10, anyTree(
                                node(WindowNode.class,
                                        anyTree(
                                                tableScan("lineitem")))))));

        if (!rowNumber) {
            assertPlanWithSession(
                    sql,
                    optimizeTopNRankWithoutNative(true),
                    true,
                    anyTree(
                            limit(10, anyTree(
                                    node(WindowNode.class,
                                            anyTree(
                                                    tableScan("lineitem")))))));
        }
    }
    @Test
    public void testLimitAboveWindow()
    {
        @Language("SQL") String sql = "SELECT " +
                "row_number() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10";
        testLimitSql(sql, true);

        sql = "SELECT " +
                "rank() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10";
        testLimitSql(sql, false);

        sql = "SELECT " +
                "dense_rank() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem LIMIT 10";
        testLimitSql(sql, false);
    }

    private void testFilterSql(String sql, boolean rowNumber)
    {
        assertPlanWithSession(
                sql,
                rowNumber ? optimizeTopNRowNumber(true) : optimizeTopNRank(true),
                true,
                anyTree(
                        anyNot(FilterNode.class,
                                node(TopNRowNumberNode.class,
                                        anyTree(
                                                tableScan("lineitem"))))));

        assertPlanWithSession(
                sql,
                rowNumber ? optimizeTopNRowNumber(false) : optimizeTopNRank(false),
                true,
                anyTree(
                        node(FilterNode.class,
                                anyTree(
                                        node(WindowNode.class,
                                                anyTree(
                                                        tableScan("lineitem")))))));

        if (!rowNumber) {
            assertPlanWithSession(
                    sql,
                    optimizeTopNRankWithoutNative(true),
                    true,
                    anyTree(
                            node(FilterNode.class,
                                    anyTree(
                                            node(WindowNode.class,
                                                    anyTree(
                                                            tableScan("lineitem")))))));
        }
    }
    @Test
    public void testFilterAboveWindow()
    {
        @Language("SQL") String sql = "SELECT * FROM " +
                "(SELECT row_number() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_row_number FROM lineitem) " +
                "WHERE partition_row_number < 10";

        testFilterSql(sql, true);

        sql = "SELECT * FROM " +
                "(SELECT rank() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_rank FROM lineitem) " +
                "WHERE partition_rank < 10";
        testFilterSql(sql, false);

        sql = "SELECT * FROM " +
                "(SELECT dense_rank() OVER (PARTITION BY suppkey ORDER BY orderkey) partition_dense_rank FROM lineitem) " +
                "WHERE partition_dense_rank < 10";
        testFilterSql(sql, false);
    }

    private Session optimizeTopNRowNumber(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(OPTIMIZE_TOP_N_ROW_NUMBER, Boolean.toString(enabled))
                .build();
    }

    private Session optimizeTopNRank(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, Boolean.toString(enabled))
                .setSystemProperty(OPTIMIZE_TOP_N_RANK, Boolean.toString(enabled))
                .build();
    }

    private Session optimizeTopNRankWithoutNative(boolean enabled)
    {
        return Session.builder(this.getQueryRunner().getDefaultSession())
                .setSystemProperty(NATIVE_EXECUTION_ENABLED, Boolean.toString(false))
                .setSystemProperty(OPTIMIZE_TOP_N_RANK, Boolean.toString(enabled))
                .build();
    }
}
