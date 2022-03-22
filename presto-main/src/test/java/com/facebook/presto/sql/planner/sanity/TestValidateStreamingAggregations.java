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
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
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

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.spi.plan.AggregationNode.Step.SINGLE;

public class TestValidateStreamingAggregations
        extends BasePlanTest
{
    private Metadata metadata;
    private SqlParser sqlParser;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;

    @BeforeClass
    public void setup()
    {
        metadata = getQueryRunner().getMetadata();
        sqlParser = getQueryRunner().getSqlParser();

        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        ConnectorId connectorId = getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.variable("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.variable("nationkey", BIGINT)),
                                                ImmutableMap.of(p.variable("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))));

        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.variable("unique"), p.variable("nationkey"))
                                .preGroupedVariables(p.variable("unique"), p.variable("nationkey"))
                                .source(
                                        p.assignUniqueId(
                                                p.variable("unique"),
                                                p.tableScan(
                                                        nationTableHandle,
                                                        ImmutableList.of(p.variable("nationkey", BIGINT)),
                                                        ImmutableMap.of(p.variable("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT)))))));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Streaming aggregation with input not grouped on the grouping keys")
    public void testValidateFailed()
    {
        validatePlan(
                p -> p.aggregation(
                        a -> a.step(SINGLE)
                                .singleGroupingSet(p.variable("nationkey"))
                                .preGroupedVariables(p.variable("nationkey"))
                                .source(
                                        p.tableScan(
                                                nationTableHandle,
                                                ImmutableList.of(p.variable("nationkey", BIGINT)),
                                                ImmutableMap.of(p.variable("nationkey", BIGINT), new TpchColumnHandle("nationkey", BIGINT))))));
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder builder = new PlanBuilder(TEST_SESSION, idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        TypeProvider types = builder.getTypes();

        getQueryRunner().inTransaction(session -> {
            // metadata.getCatalogHandle() registers the catalog for the transaction
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateStreamingAggregations().validate(planNode, session, metadata, sqlParser, types, WarningCollector.NOOP);
            return null;
        });
    }
}
