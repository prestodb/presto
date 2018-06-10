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
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.AggregationNode.Step.SINGLE;

public class TestValidateStreamingAggregations
        extends BasePlanTest
{
    private Metadata metadata;
    private SqlParser sqlParser;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;
    private TableLayoutHandle nationTableLayoutHandle;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        sqlParser = getQueryRunner().getSqlParser();

        ConnectorId connectorId = getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                new TpchTableHandle(connectorId.toString(), "nation", 1.0));

        nationTableLayoutHandle = new TableLayoutHandle(connectorId,
                TestingTransactionHandle.create(),
                new TpchTableLayoutHandle((TpchTableHandle) nationTableHandle.getConnectorHandle(), TupleDomain.all()));
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .addGroupingSet(p.symbol("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                                Optional.of(nationTableLayoutHandle)))));

        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .addGroupingSet(p.symbol("unique"), p.symbol("nationkey"))
                                .preGroupedSymbols(p.symbol("unique"), p.symbol("nationkey"))
                                .source(
                                        p.assignUniqueId(p.symbol("unique"),
                                                p.tableScan(
                                                        nationTableHandle,
                                                        ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                        ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                                        Optional.of(nationTableLayoutHandle))))));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Streaming aggregation with input not grouped on the grouping keys")
    public void testValidateFailed()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .addGroupingSet(p.symbol("nationkey"))
                                .preGroupedSymbols(p.symbol("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.symbol("nationkey", BIGINT)),
                                                ImmutableMap.of(p.symbol("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)),
                                                Optional.of(nationTableLayoutHandle)))));
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder builder = new PlanBuilder(idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        TypeProvider types = builder.getTypes();

        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateStreamingAggregations().validate(planNode, session, metadata, sqlParser, types);
            return null;
        });
    }
}
