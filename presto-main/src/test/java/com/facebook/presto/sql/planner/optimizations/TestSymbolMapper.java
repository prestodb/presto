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
import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.ValuesNode;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.tpch.TpchMetadata.TINY_SCALE_FACTOR;
import static com.google.common.collect.Iterables.getOnlyElement;
import static io.airlift.testing.Closeables.closeAllRuntimeException;
import static java.util.Collections.singleton;
import static java.util.Collections.singletonList;
import static org.testng.Assert.assertEquals;

public class TestSymbolMapper
{
    private static final Symbol a = new Symbol("a");
    private static final Symbol b = new Symbol("b");
    private static final Symbol c = new Symbol("c");
    private static final Symbol A = new Symbol("A");
    private static final Symbol B = new Symbol("B");

    private static final SymbolMapper SYMBOL_MAPPER = SymbolMapper.builder()
            .put(a, A)
            .put(b, B)
            .put(B, a)
            .build();

    private Metadata metadata;
    private LocalQueryRunner queryRunner;

    @BeforeClass
    public final void setUp()
    {
        Session session = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny")
                .setSystemProperty("task_concurrency", "1") // these tests don't handle exchanges from local parallel
                .build();

        queryRunner = new LocalQueryRunner(session);
        this.metadata = queryRunner.getMetadata();
    }

    @AfterClass(alwaysRun = true)
    public final void tearDown()
    {
        closeAllRuntimeException(queryRunner);
        queryRunner = null;
        metadata = null;
    }

    @Test
    public void testAggregationNodeSymbolMapping()
    {
        PlanBuilder builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        ValuesNode values = builder.values(a, b);
        AggregationNode aggregationNode = builder.aggregation(aggregationBuilder -> aggregationBuilder
                .addGroupingSet(a, b)
                .addAggregation(c, builder.expression("count(b)"), ImmutableList.of(BIGINT), a)
                .hashSymbol(a)
                .groupIdSymbol(a)
                .source(values));

        AggregationNode mappedAggregationNode = SYMBOL_MAPPER.map(aggregationNode, values);
        assertEquals(mappedAggregationNode.getGroupingSets(), singletonList(singletonList(A)));
        assertEquals(mappedAggregationNode.getAggregations().keySet(), singleton(c));
        AggregationNode.Aggregation mappedAggregation = getOnlyElement(mappedAggregationNode.getAggregations().values());
        assertEquals(mappedAggregation.getCall().getArguments(), singletonList(A.toSymbolReference()));
        assertEquals(mappedAggregation.getMask().get(), A);
        assertEquals(mappedAggregationNode.getHashSymbol().get(), A);
        assertEquals(mappedAggregationNode.getGroupIdSymbol().get(), A);
    }

    @Test
    public void testTopNNodeSymbolMapping()
    {
        PlanBuilder builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        ValuesNode values = builder.values(a, b);
        TopNNode topNNode = builder.topN(10, ImmutableList.of(a, b), values);

        TopNNode mappedTopNNode = SYMBOL_MAPPER.map(topNNode, values);
        assertEquals(mappedTopNNode.getOrderBy(), singletonList(A));
        assertEquals(mappedTopNNode.getOrderings().keySet(), singleton(A));
    }

    @Test
    public void testProjectNodeSymbolMapping()
    {
        PlanBuilder builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        ValuesNode values = builder.values(a, b);
        ProjectNode project = builder.project(Assignments.identity(a, b), values);

        ProjectNode mappedProjectNode = SYMBOL_MAPPER.map(project, values);
        assertEquals(mappedProjectNode.getOutputSymbols(), singletonList(A));
    }

    @Test
    public void testTableScanNodeSymbolMapping()
    {
        PlanBuilder builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        Symbol custkey = builder.symbol("custkey");
        Symbol orderkey = builder.symbol("orderkey");

        TableScanNode tableScanNode = builder.tableScan(
                new TableHandle(
                        new ConnectorId("local"),
                        new TpchTableHandle("local", "orders", TINY_SCALE_FACTOR)),
                ImmutableList.of(custkey, orderkey),
                ImmutableMap.of(
                        custkey, new TpchColumnHandle(custkey.getName(), BIGINT),
                        orderkey, new TpchColumnHandle(orderkey.getName(), BIGINT)),
                custkey.toSymbolReference());

        SymbolMapper symbolMapper = SymbolMapper.builder()
                .put(custkey, orderkey)
                .put(orderkey, A)
                .build();

        TableScanNode mappedTopNNode = symbolMapper.map(tableScanNode);
        assertEquals(mappedTopNNode.getOutputSymbols(), singletonList(A));
        assertEquals(mappedTopNNode.getOriginalConstraint(), A.toSymbolReference());
    }

    @Test
    public void testValuesNodeSymbolMapping()
    {
        PlanBuilder builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        ValuesNode values = builder.values(a, b);

        ValuesNode mappedValues = SYMBOL_MAPPER.map(values);
        assertEquals(mappedValues.getOutputSymbols(), singletonList(A));
    }
}
