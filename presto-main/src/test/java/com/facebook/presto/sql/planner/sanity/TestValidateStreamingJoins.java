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

import com.facebook.presto.Session;
import com.facebook.presto.common.predicate.TupleDomain;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorId;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.testing.TestingTransactionHandle;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tpch.TpchTableLayoutHandle;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;
import static com.facebook.presto.testing.TestingSession.testSessionBuilder;

public class TestValidateStreamingJoins
        extends BasePlanTest
{
    private Session testSession;
    private Metadata metadata;
    private SqlParser sqlParser;
    private PlanNodeIdAllocator idAllocator = new PlanNodeIdAllocator();
    private TableHandle nationTableHandle;
    private TableHandle supplierTableHandle;
    private ColumnHandle nationColumnHandle;
    private ColumnHandle suppColumnHandle;

    @BeforeClass
    public void setup()
    {
        Session.SessionBuilder sessionBuilder = testSessionBuilder()
                .setCatalog("local")
                .setSchema("tiny");
        testSession = sessionBuilder.build();
        metadata = getQueryRunner().getMetadata();
        sqlParser = getQueryRunner().getSqlParser();

        TpchTableHandle nationTpchTableHandle = new TpchTableHandle("nation", 1.0);
        TpchTableHandle supplierTpchTableHandle = new TpchTableHandle("supplier", 1.0);
        ConnectorId connectorId = getCurrentConnectorId();
        nationTableHandle = new TableHandle(
                connectorId,
                nationTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(nationTpchTableHandle, TupleDomain.all())));
        supplierTableHandle = new TableHandle(
                connectorId,
                supplierTpchTableHandle,
                TestingTransactionHandle.create(),
                Optional.of(new TpchTableLayoutHandle(supplierTpchTableHandle, TupleDomain.all())));
        nationColumnHandle = new TpchColumnHandle("nationkey", BIGINT);
        suppColumnHandle = new TpchColumnHandle("suppkey", BIGINT);
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
    {
        testSession = null;
        metadata = null;
        sqlParser = null;
        idAllocator = null;
        nationTableHandle = null;
        supplierTableHandle = null;
    }

    @Test
    public void testValidateSuccessful()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.exchange(e -> e
                                .scope(ExchangeNode.Scope.LOCAL)
                                .type(ExchangeNode.Type.REPARTITION)
                                .addSource(p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)))
                                .addInputsSet(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)))
                                .fixedHashDistributionParitioningScheme(ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableList.of(p.variable("nationkeyS", BIGINT)))),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()));
    }

    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "Build side needs an additional local exchange for join: [0-9]*")
    public void testValidateFailed()
    {
        validatePlan(
                p -> p.join(
                        INNER,
                        p.tableScan(nationTableHandle, ImmutableList.of(p.variable("nationkeyN", BIGINT)), ImmutableMap.of(p.variable("nationkeyN", BIGINT), nationColumnHandle)),
                        p.tableScan(supplierTableHandle, ImmutableList.of(p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)), ImmutableMap.of(p.variable("nationkeyS", BIGINT), nationColumnHandle, p.variable("suppkey", BIGINT), suppColumnHandle)),
                        ImmutableList.of(new JoinNode.EquiJoinClause(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT))),
                        ImmutableList.of(p.variable("nationkeyN", BIGINT), p.variable("nationkeyS", BIGINT), p.variable("suppkey", BIGINT)),
                        Optional.empty()));
    }

    private void validatePlan(Function<PlanBuilder, PlanNode> planProvider)
    {
        PlanBuilder builder = new PlanBuilder(TEST_SESSION, idAllocator, metadata);
        PlanNode planNode = planProvider.apply(builder);
        TypeProvider types = builder.getTypes();
        getQueryRunner().inTransaction(testSession, session -> {
            session.getCatalog().ifPresent(catalog -> metadata.getCatalogHandle(session, catalog));
            new ValidateStreamingJoins().validate(planNode, session, metadata, sqlParser, types, WarningCollector.NOOP);
            return null;
        });
    }
}
