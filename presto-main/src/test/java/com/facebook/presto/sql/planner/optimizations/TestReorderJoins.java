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
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.planPrinter.PlanPrinter;
import com.facebook.presto.testing.LocalQueryRunner;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static java.util.Objects.requireNonNull;
import static org.testng.Assert.assertEquals;

public class TestReorderJoins
        extends BasePlanTest
{
    public TestReorderJoins()
    {
        super(ImmutableMap.of(
                SystemSessionProperties.JOIN_REORDERING_STRATEGY, "COST_BASED",
                SystemSessionProperties.JOIN_DISTRIBUTION_TYPE, "AUTOMATIC"));
    }

    @Override
    protected LocalQueryRunner createQueryRunner(Session session)
    {
        return LocalQueryRunner.queryRunnerWithFakeNodeCountForStats(session, 8);
    }

    @Test
    public void testPartialTpchQ2JoinOrder()
    {
        assertJoinOrder(
                "SELECT * " +
                        "FROM part p, supplier s, partsupp ps, nation n, region r " +
                        "WHERE p.size = 15 AND p.type like '%BRASS' AND s.suppkey = ps.suppkey AND p.partkey = ps.partkey " +
                        "AND s.nationkey = n.nationkey AND n.regionkey = r.regionkey AND r.name = 'EUROPE'",
                new Join(
                        INNER,
                        PARTITIONED,
                        new Join(
                                INNER,
                                PARTITIONED,
                                new TableScan("tpch:partsupp:sf10.0"),
                                new TableScan("tpch:part:sf10.0")),
                        new Join(
                                INNER,
                                PARTITIONED,
                                new TableScan("tpch:supplier:sf10.0"),
                                new Join(
                                        INNER,
                                        REPLICATED,
                                        new TableScan("tpch:nation:sf10.0"),
                                        new TableScan("tpch:region:sf10.0")))));
    }

    @Test
    public void testPartialTpchQ14JoinOrder()
    {
        // it looks like the join ordering here is optimal
        assertJoinOrder(
                "SELECT * " +
                        "FROM lineitem l, part p " +
                        "WHERE l.partkey = p.partkey AND l.shipdate >= DATE '1995-09-01' AND l.shipdate < DATE '1995-09-01' + INTERVAL '1' MONTH",
                new Join(
                        INNER,
                        REPLICATED, //TODO it should be PARTITIONED
                        new TableScan("tpch:part:sf10.0"),
                        new TableScan("tpch:lineitem:sf10.0")));
    }

    private void assertJoinOrder(String sql, Node expected)
    {
        assertEquals(joinOrderString(sql), expected.print());
    }

    private String joinOrderString(String sql)
    {
        Plan plan = plan(sql);
        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder stringBuilder = new StringBuilder();

        public String result()
        {
            return stringBuilder.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("join (")
                    .append(node.getType())
                    .append(", ")
                    .append(node.getDistributionType().map(JoinNode.DistributionType::toString).orElse("unknown"))
                    .append("):\n");

            super.visitPlan(node.getLeft(), indent + 1);
            super.visitPlan(node.getRight(), indent + 1);

            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(node.getTable().getConnectorHandle().toString())
                    .append("\n");
            return null;
        }
    }

    private static String indentString(int indent)
    {
        return Strings.repeat("    ", indent);
    }

    private interface Node
    {
        void print(StringBuilder stringBuilder, int indent);

        default String print()
        {
            StringBuilder stringBuilder = new StringBuilder();
            print(stringBuilder, 0);
            return stringBuilder.toString();
        }
    }

    private static class Join
            implements Node
    {
        private final JoinNode.Type type;
        private final JoinNode.DistributionType distributionType;
        private final Node left;
        private final Node right;

        private Join(JoinNode.Type type, JoinNode.DistributionType distributionType, Node left, Node right)
        {
            this.left = requireNonNull(left, "left is null");
            this.right = requireNonNull(right, "right is null");
            this.type = requireNonNull(type, "type is null");
            this.distributionType = requireNonNull(distributionType, "distributionType is null");
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append("join (")
                    .append(type)
                    .append(", ")
                    .append(distributionType)
                    .append("):\n");

            left.print(stringBuilder, indent + 1);
            right.print(stringBuilder, indent + 1);
        }
    }

    private static class TableScan
            implements Node
    {
        private final String tableName;

        private TableScan(String tableName)
        {
            this.tableName = tableName;
        }

        @Override
        public void print(StringBuilder stringBuilder, int indent)
        {
            stringBuilder.append(indentString(indent))
                    .append(tableName)
                    .append("\n");
        }
    }
}
