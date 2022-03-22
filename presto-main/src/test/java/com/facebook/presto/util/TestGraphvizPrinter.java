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
package com.facebook.presto.util;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.cost.StatsAndCosts;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.sql.planner.Partitioning;
import com.facebook.presto.sql.planner.PartitioningScheme;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.PlanFragmentId;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.operator.StageExecutionDescriptor.ungroupedExecution;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.GraphvizPrinter.printDistributed;
import static com.facebook.presto.util.GraphvizPrinter.printLogical;
import static java.lang.String.format;
import static java.lang.String.join;
import static org.testng.Assert.assertEquals;

public class TestGraphvizPrinter
{
    private static final ConnectorId TEST_CONNECTOR_ID = new ConnectorId("connector_id");
    private static final PlanNodeId TEST_TABLE_SCAN_NODE_ID = new PlanNodeId("plan_id");
    private static final ConnectorTableHandle TEST_CONNECTOR_TABLE_HANDLE = new TestingTableHandle();
    private static final PlanNode TEST_TABLE_SCAN_NODE = new TableScanNode(
            TEST_TABLE_SCAN_NODE_ID,
            new TableHandle(TEST_CONNECTOR_ID, TEST_CONNECTOR_TABLE_HANDLE, TestingTransactionHandle.create(), Optional.empty()),
            ImmutableList.of(),
            ImmutableMap.of(),
            TupleDomain.all(),
            TupleDomain.all());
    private static final String TEST_TABLE_SCAN_NODE_INNER_OUTPUT = format(
            "label=\"{TableScan[TableHandle \\{connectorId='%s', connectorHandle='%s', layout='Optional.empty'\\}]}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue",
            TEST_CONNECTOR_ID,
            TEST_CONNECTOR_TABLE_HANDLE);

    @Test
    public void testPrintLogical()
    {
        String actual = printLogical(
                ImmutableList.of(createTestPlanFragment(0, TEST_TABLE_SCAN_NODE)),
                testSessionBuilder().build(),
                createTestFunctionAndTypeManager());
        String expected = join(
                System.lineSeparator(),
                "digraph logical_plan {",
                "subgraph cluster_0 {",
                "label = \"SOURCE\"",
                format("plannode_1[%s];", TEST_TABLE_SCAN_NODE_INNER_OUTPUT),
                "}",
                "}",
                "");
        assertEquals(actual, expected);
    }

    @Test
    public void testPrintDistributed()
    {
        SubPlan tableScanNodeSubPlan = new SubPlan(
                createTestPlanFragment(0, TEST_TABLE_SCAN_NODE),
                ImmutableList.of());
        SubPlan nestedSubPlan = new SubPlan(
                createTestPlanFragment(1, TEST_TABLE_SCAN_NODE),
                ImmutableList.of(tableScanNodeSubPlan));
        String actualNestedSubPlan = printDistributed(
                nestedSubPlan,
                testSessionBuilder().build(),
                createTestFunctionAndTypeManager());
        String expectedNestedSubPlan = join(
                System.lineSeparator(),
                "digraph distributed_plan {",
                "subgraph cluster_1 {",
                "label = \"SOURCE\"",
                format("plannode_1[%s];", TEST_TABLE_SCAN_NODE_INNER_OUTPUT),
                "}",
                "subgraph cluster_0 {",
                "label = \"SOURCE\"",
                format("plannode_1[%s];", TEST_TABLE_SCAN_NODE_INNER_OUTPUT),
                "}",
                "}",
                "");
        assertEquals(actualNestedSubPlan, expectedNestedSubPlan);
    }

    private static PlanFragment createTestPlanFragment(int id, PlanNode node)
    {
        return new PlanFragment(
                new PlanFragmentId(id),
                node,
                ImmutableSet.of(),
                SOURCE_DISTRIBUTION,
                ImmutableList.of(TEST_TABLE_SCAN_NODE_ID),
                new PartitioningScheme(Partitioning.create(SINGLE_DISTRIBUTION, ImmutableList.of()), ImmutableList.of()),
                ungroupedExecution(),
                false,
                StatsAndCosts.empty(),
                Optional.empty());
    }
}
