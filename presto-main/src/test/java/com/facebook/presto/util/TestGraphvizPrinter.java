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
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.PartitioningScheme;
import com.facebook.presto.spi.plan.PlanFragmentId;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.PlanFragment;
import com.facebook.presto.sql.planner.SubPlan;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SINGLE_DISTRIBUTION;
import static com.facebook.presto.sql.planner.SystemPartitioningHandle.SOURCE_DISTRIBUTION;
import static com.facebook.presto.testing.TestingEnvironment.FUNCTION_AND_TYPE_MANAGER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;
import static com.facebook.presto.util.GraphvizPrinter.printDistributed;
import static com.facebook.presto.util.GraphvizPrinter.printDistributedFromFragments;
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
            Optional.empty(),
            TEST_TABLE_SCAN_NODE_ID,
            new TableHandle(TEST_CONNECTOR_ID, TEST_CONNECTOR_TABLE_HANDLE, TestingTransactionHandle.create(), Optional.empty()),
            ImmutableList.of(),
            ImmutableMap.of(),
            TupleDomain.all(),
            TupleDomain.all());
    private static final String TEST_TABLE_SCAN_NODE_INNER_OUTPUT = format(
            "label=\"{TableScan | [TableHandle \\{connectorId='%s', connectorHandle='%s', layout='Optional.empty'\\}]|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                    "}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue",
            TEST_CONNECTOR_ID,
            TEST_CONNECTOR_TABLE_HANDLE);

    @Test
    public void testPrintLogical()
    {
        String actual = printLogical(
                ImmutableList.of(createTestPlanFragment(0, TEST_TABLE_SCAN_NODE)),
                FUNCTION_AND_TYPE_MANAGER,
                testSessionBuilder().build());
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
                FUNCTION_AND_TYPE_MANAGER,
                testSessionBuilder().build());
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

    @Test
    public void testPrintDistributedFromFragments()
    {
        List<PlanFragment> allFragments = new ArrayList<>();
        allFragments.add(createTestPlanFragment(0, TEST_TABLE_SCAN_NODE));
        allFragments.add(createTestPlanFragment(1, TEST_TABLE_SCAN_NODE));
        String actual = printDistributedFromFragments(
                allFragments,
                FUNCTION_AND_TYPE_MANAGER,
                testSessionBuilder().build());
        String expected = "digraph distributed_plan {\n" +
                "subgraph cluster_0 {\n" +
                "label = \"SOURCE\"\n" +
                "plannode_1[label=\"{TableScan | [TableHandle \\{connectorId='connector_id', connectorHandle='com.facebook.presto.testing.TestingMetadata$TestingTableHandle@1af56f7', layout='Optional.empty'\\}]|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                "}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue];\n" +
                "}\n" +
                "subgraph cluster_1 {\n" +
                "label = \"SOURCE\"\n" +
                "plannode_1[label=\"{TableScan | [TableHandle \\{connectorId='connector_id', connectorHandle='com.facebook.presto.testing.TestingMetadata$TestingTableHandle@1af56f7', layout='Optional.empty'\\}]|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                "}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue];\n" +
                "}\n" +
                "}\n";
        assertEquals(actual, expected);
    }

    @Test
    public void testPrintLogicalForJoinNode()
    {
        ValuesNode valuesNode = new ValuesNode(Optional.empty(), new PlanNodeId("right"), ImmutableList.of(), ImmutableList.of(), Optional.empty());

        PlanNode node = new JoinNode(
                Optional.empty(),
                new PlanNodeId("join"),
                JoinType.INNER,
                TEST_TABLE_SCAN_NODE, //Left : Probe side
                valuesNode, //Right : Build side
                Collections.emptyList(), //No Criteria
                ImmutableList.<VariableReferenceExpression>builder()
                        .addAll(TEST_TABLE_SCAN_NODE.getOutputVariables())
                        .addAll(valuesNode.getOutputVariables())
                        .build(),
                Optional.empty(), //NO filter
                Optional.empty(),
                Optional.empty(),
                Optional.of(JoinDistributionType.REPLICATED),
                ImmutableMap.of());

        String actual = printLogical(
                ImmutableList.of(createTestPlanFragment(0, node)),
                FUNCTION_AND_TYPE_MANAGER,
                testSessionBuilder().build());

        String expected = "digraph logical_plan {\n" +
                "subgraph cluster_0 {\n" +
                "label = \"SOURCE\"\n" +
                "plannode_1[label=\"{CrossJoin[REPLICATED]|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                "}\", style=\"rounded, filled\", shape=record, fillcolor=orange];\n" +
                "plannode_2[label=\"{TableScan | [TableHandle \\{connectorId='connector_id', connectorHandle='com.facebook.presto.testing.TestingMetadata$TestingTableHandle@1af56f7', layout='Optional.empty'\\}]|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                "}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue];\n" +
                "plannode_3[label=\"{Values|Estimates: \\{rows: ? (0B), cpu: ?, memory: ?, network: ?\\}\n" +
                "}\", style=\"rounded, filled\", shape=record, fillcolor=deepskyblue];\n" +
                "}\n" +
                "plannode_1 -> plannode_3 [label = \"Build\"];\n" + //valuesNode should be the Build side
                "plannode_1 -> plannode_2 [label = \"Probe\"];\n" + //TEST_TABLE_SCAN_NODE should be the Probe side
                "}\n";

        assertEquals(actual, expected);
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
                Optional.of(StatsAndCosts.empty()),
                Optional.empty());
    }
}
