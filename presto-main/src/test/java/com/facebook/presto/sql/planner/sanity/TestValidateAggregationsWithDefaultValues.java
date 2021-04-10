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
package com.facebook.presto.sql.planner.sanity;

import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.spi.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.spi.plan.AggregationNode.groupingSets;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE_STREAMING;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestValidateAggregationsWithDefaultValues
        extends BasePlanTest
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private Metadata metadata;
    private PlanBuilder builder;
    private VariableReferenceExpression variable;
    private TableScanNode tableScanNode;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(TEST_SESSION, new PlanNodeIdAllocator(), metadata);
        ConnectorId connectorId = getCurrentConnectorId();
        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TableHandle nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));
        TpchColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        variable = builder.variable("nationkey");
        tableScanNode = builder.tableScan(nationTableHandle, ImmutableList.of(variable), ImmutableMap.of(variable, nationkeyColumnHandle));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by remote hash exchange")
    public void testGloballyDistributedFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                .source(tableScanNode))));
        validatePlan(root, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by local hash exchange")
    public void testSingleNodeFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                .source(tableScanNode))));
        validatePlan(root, true);
    }

    @Test
    public void testSingleThreadFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                .source(builder.values()))));
        validatePlan(root, true);
    }

    @Test
    public void testGloballyDistributedFinalAggregationSeparatedFromPartialAggregationByRemoteHashExchange()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(REMOTE_STREAMING)
                                .fixedHashDistributionParitioningScheme(ImmutableList.of(variable), ImmutableList.of(variable))
                                .addInputsSet(variable)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode))))));
        validatePlan(root, false);
    }

    @Test
    public void testSingleNodeFinalAggregationSeparatedFromPartialAggregationByLocalHashExchange()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(LOCAL)
                                .fixedHashDistributionParitioningScheme(ImmutableList.of(variable), ImmutableList.of(variable))
                                .addInputsSet(variable)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode))))));
        validatePlan(root, true);
    }

    @Test
    public void testWithPartialAggregationBelowJoin()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.join(
                                INNER,
                                builder.exchange(e -> e
                                        .type(REPARTITION)
                                        .scope(LOCAL)
                                        .fixedHashDistributionParitioningScheme(ImmutableList.of(variable), ImmutableList.of(variable))
                                        .addInputsSet(variable)
                                        .addSource(builder.aggregation(ap -> ap
                                                .step(PARTIAL)
                                                .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                                .source(tableScanNode)))),
                                builder.values())));
        validatePlan(root, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by local hash exchange")
    public void testWithPartialAggregationBelowJoinWithoutSeparatingExchange()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                        .source(builder.join(
                                INNER,
                                builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .groupingSets(groupingSets(ImmutableList.of(variable), 2, ImmutableSet.of(0)))
                                        .source(tableScanNode)),
                                builder.values())));
        validatePlan(root, true);
    }

    private void validatePlan(PlanNode root, boolean forceSingleNode)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateAggregationsWithDefaultValues(forceSingleNode).validate(root, session, metadata, SQL_PARSER, builder.getTypes(), WarningCollector.NOOP);
            return null;
        });
    }
}
