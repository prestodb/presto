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
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.testing.TestingMetadata.TestingColumnHandle;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.SINGLE_NODE_EXECUTION_ENABLED;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.ConnectorId.createInformationSchemaConnectorId;
import static com.facebook.presto.spi.ConnectorId.createSystemTablesConnectorId;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.GATHER;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

/**
 * Unit tests for {@link AddExchangesForSingleNodeExecution}.
 * <p>
 * Tests verify that:
 * <ul>
 *   <li>Regular table scans are returned unchanged (no exchange added)</li>
 *   <li>System table scans get wrapped with a gathering exchange</li>
 *   <li>TableFinishNode and ExplainAnalyzeNode get gather exchanges</li>
 *   <li>The optimizer is a no-op when single-node execution is disabled</li>
 * </ul>
 */
public class TestAddExchangesForSingleNodeExecutionPlans
        extends BasePlanTest
{
    @Test
    public void testRegularTableScanNotChanged()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", BIGINT);
        TableScanNode tableScan = builder.tableScan(
                "regularCatalog",
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        PlanOptimizerResult result = optimize(tableScan, session, builder, idAllocator);

        // Regular table scan should be returned unchanged
        assertFalse(result.isOptimizerTriggered());
        assertSame(result.getPlanNode(), tableScan);
    }

    @Test
    public void testSystemTableScanGetsGatheringExchange()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", VARCHAR);
        ConnectorId systemConnectorId = createSystemTablesConnectorId(new ConnectorId("local"));
        TableHandle systemTableHandle = new TableHandle(
                systemConnectorId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        TableScanNode systemTableScan = builder.tableScan(
                systemTableHandle,
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        PlanOptimizerResult result = optimize(systemTableScan, session, builder, idAllocator);

        // System table scan should be wrapped with a gathering exchange
        assertTrue(result.getPlanNode() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) result.getPlanNode();
        assertEquals(exchange.getType(), GATHER);
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
        assertEquals(exchange.getSources().size(), 1);
        assertSame(exchange.getSources().get(0), systemTableScan);
    }

    @Test
    public void testInformationSchemaTableScanGetsGatheringExchange()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", VARCHAR);
        ConnectorId infoSchemaConnectorId = createInformationSchemaConnectorId(new ConnectorId("local"));
        TableHandle infoSchemaTableHandle = new TableHandle(
                infoSchemaConnectorId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        TableScanNode infoSchemaScan = builder.tableScan(
                infoSchemaTableHandle,
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        PlanOptimizerResult result = optimize(infoSchemaScan, session, builder, idAllocator);

        // Information schema table scan should be wrapped with a gathering exchange
        assertTrue(result.getPlanNode() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) result.getPlanNode();
        assertEquals(exchange.getType(), GATHER);
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
        assertSame(exchange.getSources().get(0), infoSchemaScan);
    }

    @Test
    public void testTableFinishGetsGatherExchange()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", BIGINT);
        VariableReferenceExpression rowCount = builder.variable("rowCount", BIGINT);
        TableScanNode source = builder.tableScan(
                "regularCatalog",
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        TableFinishNode tableFinish = new TableFinishNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                Optional.empty(),
                rowCount,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanOptimizerResult result = optimize(tableFinish, session, builder, idAllocator);

        // TableFinishNode's child should be wrapped with a gather exchange
        assertTrue(result.isOptimizerTriggered());
        assertTrue(result.getPlanNode() instanceof TableFinishNode);
        TableFinishNode resultFinish = (TableFinishNode) result.getPlanNode();
        assertTrue(resultFinish.getSource() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) resultFinish.getSource();
        assertEquals(exchange.getType(), GATHER);
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
    }

    @Test
    public void testTableFinishWithExchangeChildConvertsToGather()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", BIGINT);
        VariableReferenceExpression rowCount = builder.variable("rowCount", BIGINT);
        PlanNode values = builder.values(col);

        // Create an exchange as the child of TableFinishNode
        PlanNode exchangeChild = builder.exchange(ex -> ex
                .addSource(values)
                .addInputsSet(col)
                .fixedHashDistributionPartitioningScheme(ImmutableList.of(col), ImmutableList.of(col))
                .scope(REMOTE_STREAMING));

        TableFinishNode tableFinish = new TableFinishNode(
                Optional.empty(),
                idAllocator.getNextId(),
                exchangeChild,
                Optional.empty(),
                rowCount,
                Optional.empty(),
                Optional.empty(),
                Optional.empty());

        PlanOptimizerResult result = optimize(tableFinish, session, builder, idAllocator);

        // When child is already an ExchangeNode, it should be converted to a GATHER exchange
        assertTrue(result.isOptimizerTriggered());
        assertTrue(result.getPlanNode() instanceof TableFinishNode);
        TableFinishNode resultFinish = (TableFinishNode) result.getPlanNode();
        assertTrue(resultFinish.getSource() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) resultFinish.getSource();
        assertEquals(exchange.getType(), GATHER);
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
    }

    @Test
    public void testExplainAnalyzeGetsGatherExchange()
    {
        Session session = sessionWithSingleNodeExecution(true);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", BIGINT);
        VariableReferenceExpression outputVar = builder.variable("output", VARCHAR);
        TableScanNode source = builder.tableScan(
                "regularCatalog",
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        ExplainAnalyzeNode explainAnalyze = new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                outputVar,
                false,
                ExplainFormat.Type.TEXT);

        PlanOptimizerResult result = optimize(explainAnalyze, session, builder, idAllocator);

        // ExplainAnalyzeNode's child should be wrapped with a gather exchange
        assertTrue(result.isOptimizerTriggered());
        assertTrue(result.getPlanNode() instanceof ExplainAnalyzeNode);
        ExplainAnalyzeNode resultExplain = (ExplainAnalyzeNode) result.getPlanNode();
        assertTrue(resultExplain.getSource() instanceof ExchangeNode);
        ExchangeNode exchange = (ExchangeNode) resultExplain.getSource();
        assertEquals(exchange.getType(), GATHER);
        assertEquals(exchange.getScope(), REMOTE_STREAMING);
    }

    @Test
    public void testOptimizerDisabledWhenSingleNodeExecutionFalse()
    {
        Session session = sessionWithSingleNodeExecution(false);
        PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
        PlanBuilder builder = new PlanBuilder(session, idAllocator, getQueryRunner().getMetadata());

        VariableReferenceExpression col = builder.variable("col", VARCHAR);
        ConnectorId systemConnectorId = createSystemTablesConnectorId(new ConnectorId("local"));
        TableHandle systemTableHandle = new TableHandle(
                systemConnectorId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        TableScanNode systemTableScan = builder.tableScan(
                systemTableHandle,
                ImmutableList.of(col),
                ImmutableMap.of(col, new TestingColumnHandle("col")));

        PlanOptimizerResult result = optimize(systemTableScan, session, builder, idAllocator);

        // When single-node execution is disabled, the optimizer should not modify the plan
        assertFalse(result.isOptimizerTriggered());
        assertSame(result.getPlanNode(), systemTableScan);
    }

    @Test
    public void testOptimizerIsEnabled()
    {
        AddExchangesForSingleNodeExecution optimizer = new AddExchangesForSingleNodeExecution(getQueryRunner().getMetadata());

        Session enabledSession = sessionWithSingleNodeExecution(true);
        assertTrue(optimizer.isEnabled(enabledSession));

        Session disabledSession = sessionWithSingleNodeExecution(false);
        assertFalse(optimizer.isEnabled(disabledSession));
    }

    private Session sessionWithSingleNodeExecution(boolean enabled)
    {
        return Session.builder(getQueryRunner().getDefaultSession())
                .setSystemProperty(SINGLE_NODE_EXECUTION_ENABLED, String.valueOf(enabled))
                .build();
    }

    private PlanOptimizerResult optimize(PlanNode plan, Session session, PlanBuilder builder, PlanNodeIdAllocator idAllocator)
    {
        AddExchangesForSingleNodeExecution optimizer = new AddExchangesForSingleNodeExecution(getQueryRunner().getMetadata());
        VariableAllocator variableAllocator = new VariableAllocator(builder.getTypes().allVariables());
        return optimizer.optimize(
                plan,
                session,
                builder.getTypes(),
                variableAllocator,
                idAllocator,
                WarningCollector.NOOP);
    }
}
