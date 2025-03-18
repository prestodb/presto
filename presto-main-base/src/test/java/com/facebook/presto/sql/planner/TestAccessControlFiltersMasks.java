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

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.security.ViewExpression;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestingAccessControlManager;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.node;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;

public class TestAccessControlFiltersMasks
        extends BasePlanTest
{
    private static final String CATALOG = "local";
    private static final String USER = "user";
    private static final String RUN_AS_USER = "run-as-user";

    private LocalQueryRunner runner;
    private TestingAccessControlManager accessControl;

    @BeforeClass
    public void init()
    {
        runner = getQueryRunner();
        accessControl = getQueryRunner().getAccessControl();
    }

    @Test
    public void testBasicRowFilter()
    {
        executeExclusively(() -> {
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "orderkey < 10"));
            assertPlan("SELECT * FROM orders",
                    anyTree(
                            filter("ORDERKEY < 10",
                                    tableScan("orders", ImmutableMap.of("ORDERKEY", "orderkey")))));
            accessControl.reset();
        });
    }

    @Test
    public void testMultipleIdentityFilters()
    {
        executeExclusively(() -> {
            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    RUN_AS_USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey = 1"));

            accessControl.rowFilter(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    USER,
                    new ViewExpression(RUN_AS_USER, Optional.of(CATALOG), Optional.of("tiny"), "orderkey IN (SELECT orderkey FROM orders)"));
            assertPlan("SELECT count(*) FROM orders",
                    anyTree(
                            node(SemiJoinNode.class,
                                anyTree(filter("O_ORDERKEY = 1", tableScan("orders", ImmutableMap.of("O_ORDERKEY", "orderkey")))),
                                anyTree(filter("S_ORDERKEY = 1", tableScan("orders", ImmutableMap.of("S_ORDERKEY", "orderkey")))))));
            accessControl.reset();
        });
    }

    @Test
    public void testBasicColumnMask()
    {
        executeExclusively(() -> {
            accessControl.columnMask(
                    new QualifiedObjectName(CATALOG, "tiny", "orders"),
                    "custkey",
                    USER,
                    new ViewExpression(USER, Optional.empty(), Optional.empty(), "NULL"));
            assertPlan("SELECT custkey FROM orders WHERE orderkey = 1",
                    anyTree(
                            project(ImmutableMap.of("custkey_0", expression("NULL")),
                                    anyTree(node(TableScanNode.class)))));
            accessControl.reset();
        });
    }

    protected void executeExclusively(Runnable executionBlock)
    {
        runner.getExclusiveLock().lock();
        try {
            executionBlock.run();
        }
        finally {
            runner.getExclusiveLock().unlock();
        }
    }
}
