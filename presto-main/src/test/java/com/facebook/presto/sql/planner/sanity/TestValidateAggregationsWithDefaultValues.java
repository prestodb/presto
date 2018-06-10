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

import com.facebook.presto.connector.ConnectorId;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.metadata.TableLayoutHandle;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.FINAL;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.PARTIAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.LOCAL;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Scope.REMOTE;
import static com.facebook.presto.sql.planner.plan.ExchangeNode.Type.REPARTITION;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestValidateAggregationsWithDefaultValues
        extends BasePlanTest
{
    private static final SqlParser SQL_PARSER = new SqlParser();

    private Metadata metadata;
    private PlanBuilder builder;
    private Symbol symbol;
    private TableScanNode tableScanNode;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        builder = new PlanBuilder(new PlanNodeIdAllocator(), metadata);
        ConnectorId connectorId = getCurrentConnectorId();
        TableHandle nationTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "nation", 1.0));
        TableLayoutHandle nationTableLayoutHandle = new TableLayoutHandle(connectorId,
                TestingTransactionHandle.create(),
                new TpchTableLayoutHandle((TpchTableHandle) nationTableHandle.getConnectorHandle(), TupleDomain.all()));
        TpchColumnHandle nationkeyColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        symbol = new Symbol("nationkey");
        tableScanNode = builder.tableScan(nationTableHandle, ImmutableList.of(symbol), ImmutableMap.of(symbol, nationkeyColumnHandle), Optional.of(nationTableLayoutHandle));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by remote hash exchange")
    public void testGloballyDistributedFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .addGroupingSet()
                                .addGroupingSet(symbol)
                                .source(tableScanNode))));
        validatePlan(root, false);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by local hash exchange")
    public void testSingleNodeFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .addGroupingSet()
                                .addGroupingSet(symbol)
                                .source(tableScanNode))));
        validatePlan(root, true);
    }

    @Test
    public void testSingleThreadFinalAggregationInTheSameStageAsPartialAggregation()
    {
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.aggregation(ap -> ap
                                .step(PARTIAL)
                                .addGroupingSet()
                                .addGroupingSet(symbol)
                                .source(builder.values()))));
        validatePlan(root, true);
    }

    @Test
    public void testGloballyDistributedFinalAggregationSeparatedFromPartialAggregationByRemoteHashExchange()
    {
        Symbol symbol = new Symbol("symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(REMOTE)
                                .fixedHashDistributionParitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                .addInputsSet(symbol)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .addGroupingSet()
                                        .addGroupingSet(symbol)
                                        .source(tableScanNode))))));
        validatePlan(root, false);
    }

    @Test
    public void testSingleNodeFinalAggregationSeparatedFromPartialAggregationByLocalHashExchange()
    {
        Symbol symbol = new Symbol("symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.exchange(e -> e
                                .type(REPARTITION)
                                .scope(LOCAL)
                                .fixedHashDistributionParitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                .addInputsSet(symbol)
                                .addSource(builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .addGroupingSet()
                                        .addGroupingSet(symbol)
                                        .source(tableScanNode))))));
        validatePlan(root, true);
    }

    @Test
    public void testWithPartialAggregationBelowJoin()
    {
        Symbol symbol = new Symbol("symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.join(
                                INNER,
                                builder.exchange(e -> e
                                        .type(REPARTITION)
                                        .scope(LOCAL)
                                        .fixedHashDistributionParitioningScheme(ImmutableList.of(symbol), ImmutableList.of(symbol))
                                        .addInputsSet(symbol)
                                        .addSource(builder.aggregation(ap -> ap
                                                .step(PARTIAL)
                                                .addGroupingSet()
                                                .addGroupingSet(symbol)
                                                .source(tableScanNode)))),
                                builder.values())));
        validatePlan(root, true);
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Final aggregation with default value not separated from partial aggregation by local hash exchange")
    public void testWithPartialAggregationBelowJoinWithoutSeparatingExchange()
    {
        Symbol symbol = new Symbol("symbol");
        PlanNode root = builder.aggregation(
                af -> af.step(FINAL)
                        .addGroupingSet()
                        .addGroupingSet(symbol)
                        .source(builder.join(
                                INNER,
                                builder.aggregation(ap -> ap
                                        .step(PARTIAL)
                                        .addGroupingSet()
                                        .addGroupingSet(symbol)
                                        .source(tableScanNode)),
                                builder.values())));
        validatePlan(root, true);
    }

    private void validatePlan(PlanNode root, boolean forceSingleNode)
    {
        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateAggregationsWithDefaultValues(forceSingleNode).validate(root, session, metadata, SQL_PARSER, TypeProvider.empty());
            return null;
        });
    }
}
