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

import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.SortOrder;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionKind;
import com.facebook.presto.spi.plan.Ordering;
import com.facebook.presto.spi.plan.OrderingScheme;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
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

public class TestLanceVectorSearchPushdown
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("lance");
    private static final ArrayType ARRAY_DOUBLE = new ArrayType(DOUBLE);
    private static final List<Double> QUERY = ImmutableList.of(0.1, 0.2, 0.3);

    @Test
    public void testPushesCosineSimilarityDescIntoTableScan()
    {
        PlanNode plan = topNOverProjectScan("cosine_similarity", SortOrder.DESC_NULLS_LAST, 20);

        PlanNode result = new LanceVectorSearchPushdown().optimize(plan, null, null, null);

        LanceVectorSearch search = pushedSearch(result);
        assertEquals(search.getColumn(), "embedding");
        assertEquals(search.getK(), 20);
        assertEquals(search.getMetric(), LanceVectorSearch.METRIC_COSINE);
        assertEquals(search.getQueryVector(), QUERY);
    }

    @Test
    public void testPushesL2DistanceAscIntoTableScan()
    {
        PlanNode plan = topNOverProjectScan("l2_distance", SortOrder.ASC_NULLS_LAST, 5);

        PlanNode result = new LanceVectorSearchPushdown().optimize(plan, null, null, null);

        LanceVectorSearch search = pushedSearch(result);
        assertEquals(search.getMetric(), LanceVectorSearch.METRIC_L2);
        assertEquals(search.getK(), 5);
    }

    @Test
    public void testDotProductDescPushed()
    {
        PlanNode plan = topNOverProjectScan("dot_product", SortOrder.DESC_NULLS_LAST, 7);

        PlanNode result = new LanceVectorSearchPushdown().optimize(plan, null, null, null);

        assertEquals(pushedSearch(result).getMetric(), LanceVectorSearch.METRIC_DOT);
    }

    @Test
    public void testWrongDirectionNotPushed()
    {
        // cosine_similarity ASC is not the "nearest" direction, so no pushdown
        PlanNode plan = topNOverProjectScan("cosine_similarity", SortOrder.ASC_NULLS_LAST, 20);

        PlanNode result = new LanceVectorSearchPushdown().optimize(plan, null, null, null);

        assertSame(result, plan);
        assertFalse(((LanceTableHandle) scanOf(result).getTable().getConnectorHandle()).hasVectorSearch());
    }

    @Test
    public void testUnrelatedFunctionNotPushed()
    {
        PlanNode plan = topNOverProjectScan("strpos", SortOrder.DESC_NULLS_LAST, 20);

        PlanNode result = new LanceVectorSearchPushdown().optimize(plan, null, null, null);

        assertSame(result, plan);
    }

    private static LanceVectorSearch pushedSearch(PlanNode result)
    {
        TableScanNode scan = scanOf(result);
        LanceTableHandle handle = (LanceTableHandle) scan.getTable().getConnectorHandle();
        assertTrue(handle.hasVectorSearch(), "expected vector search to be pushed");
        // the layout's wrapped table must also carry it (split manager reads it from the layout)
        LanceTableLayoutHandle layout = (LanceTableLayoutHandle) scan.getTable().getLayout().orElseThrow(AssertionError::new);
        assertTrue(layout.getTable().hasVectorSearch());
        return handle.getVectorSearch().orElseThrow(AssertionError::new);
    }

    private static TableScanNode scanOf(PlanNode result)
    {
        ProjectNode project = (ProjectNode) ((TopNNode) result).getSource();
        return (TableScanNode) project.getSource();
    }

    private static PlanNode topNOverProjectScan(String function, SortOrder sortOrder, long count)
    {
        VariableReferenceExpression embedding = new VariableReferenceExpression(Optional.empty(), "embedding", ARRAY_DOUBLE);
        VariableReferenceExpression score = new VariableReferenceExpression(Optional.empty(), "score", DOUBLE);

        LanceColumnHandle embeddingColumn = new LanceColumnHandle("embedding", ARRAY_DOUBLE, true);
        LanceTableHandle lanceTable = new LanceTableHandle("default", "t", "file:///tmp/lance/t.lance", ImmutableList.of("t"));
        TableHandle tableHandle = new TableHandle(
                CONNECTOR_ID,
                lanceTable,
                LanceTransactionHandle.INSTANCE,
                Optional.of(new LanceTableLayoutHandle(lanceTable, TupleDomain.all())));
        TableScanNode scan = new TableScanNode(
                Optional.empty(),
                new PlanNodeId("scan"),
                tableHandle,
                ImmutableList.of(embedding),
                ImmutableMap.<VariableReferenceExpression, ColumnHandle>of(embedding, embeddingColumn),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());

        RowExpression scoreExpression = new CallExpression(
                function,
                stubFunctionHandle(function),
                DOUBLE,
                ImmutableList.of(embedding, constantVector(QUERY)));
        ProjectNode project = new ProjectNode(
                new PlanNodeId("project"),
                scan,
                com.facebook.presto.spi.plan.Assignments.builder()
                        .put(score, scoreExpression)
                        .build());

        OrderingScheme ordering = new OrderingScheme(ImmutableList.of(new Ordering(score, sortOrder)));
        return new TopNNode(Optional.empty(), new PlanNodeId("topn"), project, count, ordering, TopNNode.Step.SINGLE);
    }

    private static ConstantExpression constantVector(List<Double> values)
    {
        BlockBuilder builder = DOUBLE.createBlockBuilder(null, values.size());
        for (double value : values) {
            DOUBLE.writeDouble(builder, value);
        }
        Block block = builder.build();
        return new ConstantExpression(block, ARRAY_DOUBLE);
    }

    private static FunctionHandle stubFunctionHandle(String name)
    {
        return new FunctionHandle()
        {
            @Override
            public CatalogSchemaName getCatalogSchemaName()
            {
                return new CatalogSchemaName("presto", "default");
            }

            @Override
            public String getName()
            {
                return name;
            }

            @Override
            public FunctionKind getKind()
            {
                return FunctionKind.SCALAR;
            }

            @Override
            public List<com.facebook.presto.common.type.TypeSignature> getArgumentTypes()
            {
                return ImmutableList.of();
            }
        };
    }
}
