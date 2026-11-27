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
package com.facebook.presto.lance;

import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestLanceVectorSearchTopNRewriter
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("lance");
    private static final ArrayType ARRAY_DOUBLE = new ArrayType(DOUBLE);
    private static final List<Double> QUERY = ImmutableList.of(0.1, 0.2, 0.3);

    @Test
    public void testInsertsTopNAboveTvfScan()
    {
        LanceVectorSearch search = new LanceVectorSearch(
                "embedding", QUERY, 7, LanceVectorSearch.METRIC_L2,
                Optional.empty(), Optional.empty(), Optional.empty(), true);
        PlanNode scan = buildScan(search, true);

        PlanNode result = new LanceVectorSearchTopNRewriter().optimize(
                scan, null, null, new PlanNodeIdAllocator());

        assertTrue(result instanceof TopNNode, "expected TopNNode at root");
        TopNNode topN = (TopNNode) result;
        assertEquals(topN.getStep(), TopNNode.Step.SINGLE);
        assertEquals(topN.getCount(), 7);

        // ordering: single _distance ASC
        List<Ordering> orderings = topN.getOrderingScheme().getOrderBy();
        assertEquals(orderings.size(), 1);
        assertEquals(orderings.get(0).getSortOrder(), SortOrder.ASC_NULLS_LAST);
        assertEquals(orderings.get(0).getVariable().getName(), "_distance");

        // child scan's handle must have requiresGlobalTopK cleared
        TableScanNode childScan = (TableScanNode) topN.getSource();
        LanceTableHandle lanceHandle = (LanceTableHandle) childScan.getTable().getConnectorHandle();
        assertTrue(lanceHandle.hasVectorSearch());
        assertFalse(lanceHandle.getVectorSearch().get().isRequiresGlobalTopK(),
                "requiresGlobalTopK should be cleared on connector handle");

        // the layout's embedded LanceTableHandle must also have the flag cleared
        LanceTableLayoutHandle layout = (LanceTableLayoutHandle) childScan.getTable().getLayout()
                .orElseThrow(() -> new AssertionError("expected layout"));
        assertFalse(layout.getTable().getVectorSearch().get().isRequiresGlobalTopK(),
                "requiresGlobalTopK should be cleared on layout handle");

        // everything else about the search must be preserved
        LanceVectorSearch cleared = lanceHandle.getVectorSearch().get();
        assertEquals(cleared.getColumn(), "embedding");
        assertEquals(cleared.getK(), 7);
        assertEquals(cleared.getMetric(), LanceVectorSearch.METRIC_L2);
        assertEquals(cleared.getQueryVector(), QUERY);
    }

    @Test
    public void testIgnoresPushdownScan()
    {
        LanceVectorSearch search = new LanceVectorSearch(
                "embedding", QUERY, 5, LanceVectorSearch.METRIC_COSINE,
                Optional.empty(), Optional.empty(), Optional.empty(), false);
        PlanNode scan = buildScan(search, true);

        PlanNode result = new LanceVectorSearchTopNRewriter().optimize(
                scan, null, null, new PlanNodeIdAllocator());

        assertSame(result, scan, "should return same instance when requiresGlobalTopK is false");
    }

    @Test
    public void testIgnoresScanWithoutVectorSearch()
    {
        PlanNode scan = buildPlainScan();

        PlanNode result = new LanceVectorSearchTopNRewriter().optimize(
                scan, null, null, new PlanNodeIdAllocator());

        assertSame(result, scan, "should return same instance when no vector search");
    }

    @Test
    public void testFallsBackWhenDistanceColumnAbsent()
    {
        LanceVectorSearch search = new LanceVectorSearch(
                "embedding", QUERY, 7, LanceVectorSearch.METRIC_L2,
                Optional.empty(), Optional.empty(), Optional.empty(), true);
        PlanNode scan = buildScan(search, false);

        PlanNode result = new LanceVectorSearchTopNRewriter().optimize(
                scan, null, null, new PlanNodeIdAllocator());

        assertSame(result, scan,
                "should return same instance when distance column absent (single-split fallback)");
    }

    private static PlanNode buildScan(LanceVectorSearch vectorSearch, boolean includeDistanceColumn)
    {
        VariableReferenceExpression embedding = new VariableReferenceExpression(
                Optional.empty(), "embedding", ARRAY_DOUBLE);
        LanceColumnHandle embeddingColumn = new LanceColumnHandle("embedding", ARRAY_DOUBLE, true);

        ImmutableList.Builder<VariableReferenceExpression> outputVars = ImmutableList.builder();
        ImmutableMap.Builder<VariableReferenceExpression, ColumnHandle> assignments = ImmutableMap.builder();
        outputVars.add(embedding);
        assignments.put(embedding, embeddingColumn);

        if (includeDistanceColumn) {
            VariableReferenceExpression distance = new VariableReferenceExpression(
                    Optional.empty(), "_distance", RealType.REAL);
            outputVars.add(distance);
            assignments.put(distance, LanceColumnHandle.distanceColumn());
        }

        LanceTableHandle lanceTable = new LanceTableHandle(
                "default", "t", "file:///tmp/lance/t.lance", ImmutableList.of("t"));
        LanceTableHandle withSearch = lanceTable.withVectorSearch(vectorSearch);
        TableHandle tableHandle = new TableHandle(
                CONNECTOR_ID,
                withSearch,
                LanceTransactionHandle.INSTANCE,
                Optional.of(new LanceTableLayoutHandle(withSearch, TupleDomain.all())));
        return new TableScanNode(
                Optional.empty(),
                new PlanNodeId("scan"),
                tableHandle,
                outputVars.build(),
                assignments.build(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }

    private static PlanNode buildPlainScan()
    {
        VariableReferenceExpression embedding = new VariableReferenceExpression(
                Optional.empty(), "embedding", ARRAY_DOUBLE);
        LanceColumnHandle embeddingColumn = new LanceColumnHandle("embedding", ARRAY_DOUBLE, true);

        LanceTableHandle lanceTable = new LanceTableHandle(
                "default", "t", "file:///tmp/lance/t.lance", ImmutableList.of("t"));
        TableHandle tableHandle = new TableHandle(
                CONNECTOR_ID,
                lanceTable,
                LanceTransactionHandle.INSTANCE,
                Optional.of(new LanceTableLayoutHandle(lanceTable, TupleDomain.all())));
        return new TableScanNode(
                Optional.empty(),
                new PlanNodeId("scan"),
                tableHandle,
                ImmutableList.of(embedding),
                ImmutableMap.of(embedding, embeddingColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }
}
