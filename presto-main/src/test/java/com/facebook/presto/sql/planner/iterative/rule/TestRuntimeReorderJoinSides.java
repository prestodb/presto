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

package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.equiJoinClause;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.exchange;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.join;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.PARTITIONED;
import static com.facebook.presto.sql.planner.plan.JoinNode.DistributionType.REPLICATED;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

@Test(singleThreaded = true)
public class TestRuntimeReorderJoinSides
{
    private static final int NODES_COUNT = 4;
    private RuleTester tester;
    private TableHandle nationTableHandle;
    private TableHandle supplierTableHandle;
    private ColumnHandle nationColumnHandle;
    private ColumnHandle suppColumnHandle;
    private ConnectorId connectorId;

    @BeforeClass
    public void setUp()
    {
        tester = new RuleTester(ImmutableList.of(), ImmutableMap.of(), Optional.of(NODES_COUNT));
        connectorId = tester.getCurrentConnectorId();

        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TpchTableHandle supplierTpchTableHandle = new TpchTableHandle("supplier", 1.0);

        nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));
        supplierTableHandle = new TableHandle(
                connectorId,
                supplierTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(supplierTpchTableHandle, TupleDomain.all())));

        nationColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        suppColumnHandle = new TpchColumnHandle("suppkey", BIGINT);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        tester.close();
        tester = null;
        nationTableHandle = null;
        supplierTableHandle = null;
        nationColumnHandle = null;
        suppColumnHandle = null;
        connectorId = null;
    }

    @Test
    public void testDoesNotFireWhenNoJoin()
    {
        assertReorderJoinSides()
                .overrideStats("valuesA", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(100)
                        .build())
                .overrideStats("valuesB", PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .build())
                .on(p -> p.semiJoin(
                        p.values(new PlanNodeId("valuesA"), 100, p.variable("A1", BIGINT)),
                        p.values(new PlanNodeId("valuesB"), 10000, p.variable("B1", BIGINT)),
                        p.variable("A1", BIGINT),
                        p.variable("B1", BIGINT),
                        p.variable("A1", BIGINT),
                        Optional.empty(),
                        Optional.empty(),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenNonTableScanUnderJoin()
    {
        assertReorderJoinSides()
                .on(p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkey", BIGINT)), ImmutableMap.of(p.variable("nationkey", BIGINT), nationColumnHandle)),
                        p.values(new PlanNodeId("valuesB"), 10000, p.variable("B1", BIGINT)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkey", BIGINT), p.variable("B1", BIGINT))),
                        ImmutableList.of(p.variable("nationkey", BIGINT), p.variable("B1", BIGINT)),
                        Optional.empty()))
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWithoutBasicStatistics()
    {
        List<String> nationNodeId = new ArrayList<>();
        List<String> suppNodeId = new ArrayList<>();
        assertReorderJoinSides()
                .on(p -> {
                    TableScanNode nationNode = p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle));
                    TableScanNode suppNode = p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle));
                    nationNodeId.add(nationNode.getId().toString());
                    suppNodeId.add(suppNode.getId().toString());
                    return p.join(
                            INNER,
                            nationNode,
                            p.exchange(e -> e
                                    .addSource(suppNode)
                                    .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                    .fixedHashDistributionParitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                            ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                            ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                            Optional.empty());
                })
                .overrideStats(nationNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .overrideStats(suppNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(Double.NaN)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenProbeSideLarger()
    {
        List<String> nationNodeId = new ArrayList<>();
        List<String> suppNodeId = new ArrayList<>();
        assertReorderJoinSides()
                .on(p -> {
                    TableScanNode nationNode = p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle));
                    TableScanNode suppNode = p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle));
                    nationNodeId.add(nationNode.getId().toString());
                    suppNodeId.add(suppNode.getId().toString());
                    return p.join(
                            INNER,
                            nationNode,
                            p.exchange(e -> e
                                    .addSource(suppNode)
                                    .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                    .fixedHashDistributionParitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                            ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                            ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                            Optional.empty());
                })
                .overrideStats(nationNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(1000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("nationkeyN", BIGINT), new VariableStatsEstimate(0, 100, 0, 8, 100)))
                        .build())
                .overrideStats(suppNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(3000)
                        .addVariableStatistics(ImmutableMap.of(new VariableReferenceExpression("nationkeyS", BIGINT), new VariableStatsEstimate(0, 100, 0.99, 8, 10),
                                new VariableReferenceExpression("suppkey", BIGINT), new VariableStatsEstimate(0, 100, 0.99, 1, 10)))
                        .build())
                .doesNotFire();
    }

    @Test
    public void testDoesNotFireWhenSwappedJoinInvalid()
    {
        List<String> nationNodeId = new ArrayList<>();
        List<String> suppNodeId = new ArrayList<>();
        assertReorderJoinSides()
                .on(p -> {
                    TableScanNode nationNode = p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle));
                    TableScanNode suppNode = p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle));
                    nationNodeId.add(nationNode.getId().toString());
                    suppNodeId.add(suppNode.getId().toString());
                    return p.join(
                            LEFT,
                            nationNode,
                            p.exchange(e -> e
                                    .addSource(suppNode)
                                    .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                    .fixedHashDistributionParitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                            ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                            ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(REPLICATED),
                            ImmutableMap.of());
                })
                .overrideStats(nationNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(25)
                        .build())
                .overrideStats(suppNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .build())
                .doesNotFire();
    }

    @Test
    public void testFlipsAndAdjustExchangeWhenProbeSideSmaller()
    {
        List<String> nationNodeId = new ArrayList<>();
        List<String> suppNodeId = new ArrayList<>();
        assertReorderJoinSides()
                .on(p -> {
                    TableScanNode nationNode = p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle));
                    TableScanNode suppNode = p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle));
                    nationNodeId.add(nationNode.getId().toString());
                    suppNodeId.add(suppNode.getId().toString());
                    return p.join(
                            INNER,
                            nationNode,
                            p.exchange(e -> e
                                    .addSource(suppNode)
                                    .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                    .fixedHashDistributionParitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                            ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                            ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.empty(),
                            Optional.of(PARTITIONED),
                            ImmutableMap.of());
                })
                .overrideStats(nationNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(25)
                        .build())
                .overrideStats(suppNodeId.get(0), PlanNodeStatsEstimate.builder()
                        .setOutputRowCount(10000)
                        .build())
                .matches(join(
                        INNER,
                        ImmutableList.of(equiJoinClause("nationkeyS", "nationkeyN")),
                        Optional.empty(),
                        Optional.of(PARTITIONED),
                        tableScan("supplier", ImmutableMap.of("nationkeyS", "nationkey")),
                        exchange(tableScan("nation", ImmutableMap.of("nationkeyN", "nationkey")))));
    }

    private RuleAssert assertReorderJoinSides()
    {
        return tester.assertThat(new RuntimeReorderJoinSides(tester.getMetadata(), tester.getSqlParser()));
    }
}
