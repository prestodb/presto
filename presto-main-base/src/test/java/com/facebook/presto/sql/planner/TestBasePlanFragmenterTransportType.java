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

import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.ExplainAnalyzeNode;
import com.facebook.presto.sql.planner.plan.TransportType;
import com.facebook.presto.sql.tree.ExplainFormat;
import com.facebook.presto.testing.TestingMetadata.TestingTableHandle;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.metadata.AbstractMockMetadata.dummyMetadata;
import static com.facebook.presto.sql.planner.PlannerUtils.containsCoordinatorOnlyNode;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

@Test(singleThreaded = true)
public class TestBasePlanFragmenterTransportType
{
    private PlanBuilder planBuilder;
    private PlanNodeIdAllocator idAllocator;
    private VariableReferenceExpression col;

    @BeforeMethod
    public void setUp()
    {
        idAllocator = new PlanNodeIdAllocator();
        planBuilder = new PlanBuilder(TEST_SESSION, idAllocator, dummyMetadata());
        col = new VariableReferenceExpression(Optional.empty(), "col", BigintType.BIGINT);
    }

    // -----------------------------------------------------------------------
    // Tests for containsCoordinatorOnlyNode
    // -----------------------------------------------------------------------

    @Test
    public void testWorkerTableScanDoesNotRunOnCoordinator()
    {
        // A regular (non-system) table scan should NOT be detected as coordinator
        assertFalse(containsCoordinatorOnlyNode(
                planBuilder.tableScan("hive", ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testInformationSchemaTableScanRunsOnCoordinator()
    {
        // information_schema connector is a system connector → should run on coordinator
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertTrue(containsCoordinatorOnlyNode(
                planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testSystemTablesConnectorRunsOnCoordinator()
    {
        // $system@ connector is a system connector → should run on coordinator
        ConnectorId systemId = ConnectorId.createSystemTablesConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                systemId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());
        assertTrue(containsCoordinatorOnlyNode(
                planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of())));
    }

    @Test
    public void testExplainAnalyzeNodeRunsOnCoordinator()
    {
        // ExplainAnalyzeNode is a coordinator-only plan node
        ValuesNode source = planBuilder.values(col);
        ExplainAnalyzeNode explainAnalyze = new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                col,
                false,
                ExplainFormat.Type.TEXT);
        assertTrue(containsCoordinatorOnlyNode(explainAnalyze));
    }

    @Test
    public void testValuesNodeDoesNotRunOnCoordinator()
    {
        // A plain ValuesNode is not coordinator-only
        assertFalse(containsCoordinatorOnlyNode(planBuilder.values(col)));
    }

    @Test
    public void testStopsAtRemoteExchangeBoundary()
    {
        // A coordinator-only node behind a remote exchange should NOT be detected,
        // because remote exchanges create fragment boundaries
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        ExchangeNode remoteExchange = planBuilder.exchange(e -> e
                .scope(ExchangeNode.Scope.REMOTE_STREAMING)
                .type(ExchangeNode.Type.GATHER)
                .addSource(planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of()))
                .addInputsSet(ImmutableList.of())
                .singleDistributionPartitioningScheme(ImmutableList.of()));

        // The remote exchange itself should NOT be flagged as coordinator
        // (containsCoordinatorOnlyNode stops at remote exchange boundaries)
        assertFalse(containsCoordinatorOnlyNode(remoteExchange));
    }

    @Test
    public void testWalksThroughLocalExchange()
    {
        // A coordinator-only node behind a LOCAL exchange SHOULD be detected,
        // because local exchanges don't create fragment boundaries
        ConnectorId infoSchemaId = ConnectorId.createInformationSchemaConnectorId(new ConnectorId("hive"));
        TableHandle handle = new TableHandle(
                infoSchemaId,
                new TestingTableHandle(),
                TestingTransactionHandle.create(),
                Optional.empty());

        ExchangeNode localExchange = planBuilder.exchange(e -> e
                .scope(ExchangeNode.Scope.LOCAL)
                .type(ExchangeNode.Type.GATHER)
                .addSource(planBuilder.tableScan(handle, ImmutableList.of(), ImmutableMap.of()))
                .addInputsSet(ImmutableList.of())
                .singleDistributionPartitioningScheme(ImmutableList.of()));

        assertTrue(containsCoordinatorOnlyNode(localExchange));
    }

    // -----------------------------------------------------------------------
    // Tests for FragmentProperties transport type
    // -----------------------------------------------------------------------

    @Test
    public void testFragmentPropertiesDefaultsToHttp()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        assertEquals(props.getOutputTransportType(), TransportType.HTTP);
    }

    @Test
    public void testFragmentPropertiesSetTransportType()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        props.setOutputTransportType(TransportType.ANY);
        assertEquals(props.getOutputTransportType(), TransportType.ANY);
    }

    @Test
    public void testHasCoordinatorOnlyDistribution()
    {
        BasePlanFragmenter.FragmentProperties props = new BasePlanFragmenter.FragmentProperties(
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)));
        // Default is not coordinator-only
        assertFalse(props.hasCoordinatorOnlyDistribution());

        // After setting coordinator-only distribution via a coordinator-only node, it returns true
        ValuesNode source = planBuilder.values(col);
        ExplainAnalyzeNode explainAnalyze = new ExplainAnalyzeNode(
                Optional.empty(),
                idAllocator.getNextId(),
                source,
                col,
                false,
                ExplainFormat.Type.TEXT);
        props.setCoordinatorOnlyDistribution(explainAnalyze);
        assertTrue(props.hasCoordinatorOnlyDistribution());
    }

    // -----------------------------------------------------------------------
    // Tests for PlanFragment transport type serialization
    // -----------------------------------------------------------------------

    @Test
    public void testPlanFragmentConvenienceConstructorDefaultsToHttp()
    {
        PlanFragment fragment = new PlanFragment(
                new com.facebook.presto.spi.plan.PlanFragmentId(0),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.HTTP);
    }

    @Test
    public void testPlanFragmentFullConstructorPreservesAny()
    {
        PlanFragment fragment = new PlanFragment(
                new com.facebook.presto.spi.plan.PlanFragmentId(1),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                TransportType.ANY,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.ANY);
    }

    @Test
    public void testPlanFragmentNullTransportTypeDefaultsToHttp()
    {
        // Simulates backward-compatible deserialization where outputTransportType is null
        PlanFragment fragment = new PlanFragment(
                new com.facebook.presto.spi.plan.PlanFragmentId(2),
                planBuilder.values(col),
                com.google.common.collect.ImmutableSet.of(col),
                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                ImmutableList.of(),
                new com.facebook.presto.spi.plan.PartitioningScheme(
                        com.facebook.presto.spi.plan.Partitioning.create(
                                SystemPartitioningHandle.SINGLE_DISTRIBUTION,
                                ImmutableList.of()),
                        ImmutableList.of(col)),
                Optional.empty(),
                com.facebook.presto.spi.plan.StageExecutionDescriptor.ungroupedExecution(),
                false,
                null,
                Optional.empty(),
                Optional.empty());
        assertEquals(fragment.getOutputTransportType(), TransportType.HTTP);
    }
}
