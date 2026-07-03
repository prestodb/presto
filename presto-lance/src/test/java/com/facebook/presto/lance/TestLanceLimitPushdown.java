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

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.OptionalLong;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

public class TestLanceLimitPushdown
{
    private static final ConnectorId CONNECTOR_ID = new ConnectorId("lance");

    @Test
    public void testPushesLimitIntoTableScan()
    {
        LanceTableHandle lanceTable = new LanceTableHandle("default", "t");
        TableScanNode scan = tableScan(lanceTable);
        LimitNode limit = new LimitNode(Optional.empty(), new PlanNodeId("limit"), scan, 10L, LimitNode.Step.FINAL);

        PlanNode result = new LanceLimitPushdown().optimize(limit, null, null, null);

        assertTrue(result instanceof LimitNode);
        TableScanNode newScan = (TableScanNode) ((LimitNode) result).getSource();
        LanceTableHandle pushed = (LanceTableHandle) newScan.getTable().getConnectorHandle();
        assertEquals(pushed.getLimit(), OptionalLong.of(10));
        // the layout's wrapped table must also carry the limit (split manager reads it from the layout)
        LanceTableLayoutHandle layout = (LanceTableLayoutHandle) newScan.getTable().getLayout().orElseThrow(AssertionError::new);
        assertEquals(layout.getTable().getLimit(), OptionalLong.of(10));
    }

    @Test
    public void testIdempotentWhenAlreadyPushed()
    {
        LanceTableHandle lanceTable = new LanceTableHandle("default", "t").withLimit(10);
        TableScanNode scan = tableScan(lanceTable);
        LimitNode limit = new LimitNode(Optional.empty(), new PlanNodeId("limit"), scan, 10L, LimitNode.Step.FINAL);

        PlanNode result = new LanceLimitPushdown().optimize(limit, null, null, null);

        // nothing tighter to push, returned unchanged
        assertSame(result, limit);
    }

    private static TableScanNode tableScan(LanceTableHandle lanceTable)
    {
        TableHandle tableHandle = new TableHandle(
                CONNECTOR_ID,
                lanceTable,
                LanceTransactionHandle.INSTANCE,
                Optional.of(new LanceTableLayoutHandle(lanceTable, TupleDomain.all())));
        return new TableScanNode(
                Optional.empty(),
                new PlanNodeId("scan"),
                tableHandle,
                ImmutableList.of(),
                ImmutableMap.<com.facebook.presto.spi.relation.VariableReferenceExpression, ColumnHandle>of(),
                TupleDomain.all(),
                TupleDomain.all(),
                Optional.empty());
    }
}
